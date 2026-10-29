"""Reference single-machine executors (plan M7).

A thread pool AND a process pool, both running the same `graphed.core.Plan` to one reduced result.

Both share the driver in :mod:`._driver`; they differ only in the worker pool and how per-worker
`open_once` resources are held (thread-local vs a per-process global set by an initializer). Fixed
partition sets use the deterministic straggler-tolerant tree reduction; an adaptive `next_tasks`
plan uses a running fold over a partition set discovered from observed timings. A worker failure
(thread or process) propagates to the driver intact — in particular a picklable
`graphed.debug.StageError` is re-raised as-is, never degraded to an opaque string (plan A.3 #8).
"""

from __future__ import annotations

import atexit
import contextlib
import functools
import hashlib
import multiprocessing
import os
import pickle
import queue
import sys
import threading
import time
import warnings
from collections import OrderedDict, deque
from collections.abc import Callable, Iterator
from concurrent.futures import Executor as _PoolExecutor
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import (
    # the stdlib pool; our public ProcessPoolExecutor wraps it
    ProcessPoolExecutor as _StdProcessPool,
)
from multiprocessing.managers import SyncManager
from typing import Any, TypeVar, cast

from graphed.core import Partition, Plan, RunControl, Task
from graphed.core.execution import (
    LocalResources,
    Monitor,
    TaskEvent,
    TaskPhase,
    WorkerProfiler,
    complete_events,
    emit_task,
    partition_label,
)
from graphed.debug import StageError

from .._window import wait_until
from ._driver import _BaseExecutor
from ._peer import OUTBOX_EXIT_WAIT_S, PeerControl, process_and_reduce
from ._pinned_pool import exit_with_driver
from ._portable import Portable
from ._process_peer import _ProcessPeerBase
from ._transport import build_transports

if sys.platform != "win32":
    import resource

R = TypeVar("R")


def _exceeds_fd_budget(w: int) -> bool:
    """Return whether a full-registry IPC pool of ``w`` workers strains the per-process fd limit.

    Each worker in :class:`ProcessPoolExecutor` inherits every inbox (~``2*(w+1)`` fds), so the
    registry is O(N²) in fds; on large many-core machines (>~128 cores, or any low
    ``RLIMIT_NOFILE``) that approaches the limit. This is advisory only — it drives a warning
    that recommends :class:`PinnedPoolExecutor` (whose bounded O(log N) overlay inherits ~``log
    w`` fds per worker); it never switches pools silently. The user picks the executor
    explicitly.
    """
    if sys.platform == "win32":  # no POSIX RLIMIT; Windows fd/handle limits are high (CRT 512)
        soft = 512
    else:
        soft = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    return 2 * (w + 1) + 64 > soft // 2  # full-registry fds would exceed half the limit


# ---- per-worker resources --------------------------------------------------
_thread_local = threading.local()
_proc_resources: LocalResources | None = None
# M37: a worker->driver side channel for dashboard events (a bounded queue proxy, set by the
# process pool initializer) + a per-worker statistical profiler. Both are None unless a Monitor is
# attached. Emission is best-effort and MUST NOT block a worker (drop-on-full).
_proc_event_q: object | None = None
_proc_profiler: WorkerProfiler | None = None
# m65 B: with per-worker push this process's own monitor (built once by the initializer) receives
# its task events and profile trees instead of the event queue; lean drops STARTED and the label.
_proc_monitor: Monitor | None = None
_proc_lean = False
_MISSING = object()

# M37 (telemetry OFF the data path): a worker emits events into a local in-process buffer (a deque
# append, ~0.1us) instead of doing a Manager().Queue() put per task (~21us IPC round-trip, ~35x
# slower). A per-worker daemon thread drains the buffer on a cadence and ships the whole batch with
# ONE Manager put, so IPC latency + batching never touch the task's critical path. The buffer is
# bounded (drops oldest if the drain can't keep up — best-effort, never blocks the worker).
_proc_buffer: deque[tuple[str, object]] | None = None
_proc_drain_stop: threading.Event | None = None
_proc_drain_thread: threading.Thread | None = None
_proc_last_flush = 0.0
_PROC_DRAIN_INTERVAL = 0.05  # seconds between worker->driver batch flushes
_PROC_BUFFER_CAP = 50000  # bounded local buffer (drop-oldest on overflow)
# how long a stopping drain thread may finish its send; a hung one is abandoned
_PROC_EXIT_JOIN_S = 2.0
_PROFILE_FLUSH_INTERVAL = 1.0  # serialize the profiler at most ~1/s, never per task
# How long a complete_events hub run waits, after its leaf futures are done, for their terminal
# events to cross the process collector (forty drain ticks); read at call time.
_HUB_EVENT_DRAIN_S = 2.0

# M37: every statistical profiler we start (thread-local or per-process) is registered here so a
# single atexit handler can stop it — its background sampler thread must be joined before the
# worker's interpreter tears down module globals, or a late sample touches half-collected state.
_live_profilers: list[WorkerProfiler] = []
_profiler_lock = threading.Lock()
_atexit_registered = False


def _register_profiler(profiler: WorkerProfiler) -> None:
    global _atexit_registered
    with _profiler_lock:
        _live_profilers.append(profiler)
        if not _atexit_registered:
            atexit.register(_stop_all_profilers)
            _atexit_registered = True


def _stop_all_profilers() -> None:
    with _profiler_lock:
        for profiler in _live_profilers:
            with contextlib.suppress(Exception):
                profiler.stop()
        _live_profilers.clear()


# M31: a process callable embedding a large compiled IR would otherwise be re-pickled and
# re-shipped on EVERY submit (concurrent.futures does not dedupe callables). Instead it is
# broadcast to each worker ONCE, cached here by content hash, and tasks ship only (token,
# partition). The cache is keyed by hash so re-running the same plan reuses the cached process.
# capacity of the per-worker shared-process cache AND the driver's broadcast-token set --
# the SAME value on both sides, FIFO-evicted in broadcast order (identical across workers
# because every worker receives the same broadcast sequence), so "the driver thinks token T
# is primed" stays equivalent to "every worker has T".
_SHARED_CACHE_CAP = 32
_shared_objects: OrderedDict[str, object] = OrderedDict()


def _thread_resources() -> LocalResources:
    res = getattr(_thread_local, "res", None)
    if res is None:
        res = LocalResources()
        _thread_local.res = res
    return res


def _proc_init(
    profiler_factory: Callable[[], WorkerProfiler] | None = None,
    event_q: object | None = None,
    *,
    monitor_factory: Callable[[], Monitor] | None = None,
    lean: bool = False,
) -> None:
    global _proc_resources, _proc_event_q, _proc_profiler, _proc_buffer
    global _proc_drain_stop, _proc_drain_thread, _proc_monitor, _proc_lean
    if _proc_drain_stop is not None:  # a re-init must not leave the old drain thread shipping
        _proc_drain_stop.set()
    if _proc_drain_thread is not None:
        _proc_drain_thread.join(timeout=_PROC_EXIT_JOIN_S)
    exit_with_driver()
    _proc_resources = LocalResources()
    _proc_event_q = event_q
    _proc_profiler = _proc_buffer = _proc_drain_stop = _proc_drain_thread = _proc_monitor = None
    _proc_lean = lean
    if monitor_factory is not None:
        # built before _proc_drain_final registers, so LIFO atexit runs that hook (the final
        # profile) while this monitor's own exit flush has not yet run
        with contextlib.suppress(Exception):  # a monitor that won't build just disables telemetry
            _proc_monitor = monitor_factory()
    if profiler_factory is not None:
        with contextlib.suppress(Exception):  # a profiler that won't start just disables sampling
            prof = profiler_factory()
            prof.start()
            _proc_profiler = prof
            _register_profiler(prof)  # stopped at worker-process exit (silences sampler teardown)
    # a monitor is attached -> run the off-path drain thread for this worker
    if event_q is not None:
        _proc_buffer = deque(maxlen=_PROC_BUFFER_CAP)
        _proc_drain_stop = threading.Event()
        _proc_drain_thread = threading.Thread(
            target=_proc_drain_loop, name="graphed-dash-worker-drain", daemon=True
        )
        _proc_drain_thread.start()
    if event_q is not None or _proc_monitor is not None:
        atexit.register(_proc_drain_final)


def _proc_emit(item: tuple[str, object]) -> None:
    """Append an event to this worker's local buffer (in-process, ~0.1us).

    The drain thread ships it; the data path never blocks on IPC. The bounded deque drops the
    oldest on overflow (best-effort).
    """
    buf = _proc_buffer
    if buf is not None:
        buf.append(item)


def _proc_drain_batch() -> None:
    """Move everything currently buffered to the driver in ONE Manager-queue put.

    Amortizes IPC.
    """
    buf, q = _proc_buffer, _proc_event_q
    if buf is None or q is None:
        return
    batch: list[tuple[str, object]] = []
    while True:
        try:
            batch.append(buf.popleft())
        except IndexError:
            break
    if batch:
        with contextlib.suppress(Exception):  # a full driver queue drops the batch; never blocks
            q.put_nowait(("batch", batch))  # type: ignore[attr-defined]


def _proc_drain_loop() -> None:
    stop = _proc_drain_stop
    assert stop is not None
    while not stop.is_set():
        stop.wait(_PROC_DRAIN_INTERVAL)
        _proc_drain_batch()


def _proc_drain_final() -> None:
    """At worker exit.

    Stop sampling (same thread that started it), buffer the final session, and flush whatever
    remains so the last events/profile are not lost.
    """
    if _proc_drain_stop is not None:
        _proc_drain_stop.set()
    if _proc_drain_thread is not None:  # a daemon: exit would kill it mid-send of a batch it holds
        _proc_drain_thread.join(timeout=_PROC_EXIT_JOIN_S)
    if _proc_profiler is not None:
        with contextlib.suppress(Exception):
            payload = _proc_profiler.stop()
            if payload and _proc_monitor is not None:
                _proc_monitor.on_profile(str(os.getpid()), payload)
            elif payload and _proc_buffer is not None:
                _proc_buffer.append(("profile", (str(os.getpid()), payload)))
    _proc_drain_batch()


def _proc_profile_due() -> bool:
    """Return True at most once per ``_PROFILE_FLUSH_INTERVAL``.

    Keeps the profiler serialize (JSON-encoding the accumulated stack tree) off the per-task
    path. Called on the worker thread.
    """
    global _proc_last_flush
    now = time.monotonic()
    if now - _proc_last_flush >= _PROFILE_FLUSH_INTERVAL:
        _proc_last_flush = now
        return True
    return False


def _run_with_emit(
    process: Callable[[Partition, LocalResources], object],
    task: Task,
    resources: LocalResources,
    *,
    worker: str,
    emit_event: Callable[[TaskEvent], None],
    emit_profile: Callable[[str, bytes], None],
    profiler: WorkerProfiler | None,
    profile_due: Callable[[], bool],
    lean: bool = False,
) -> object:
    """Run one task, emitting STARTED before and FINISHED/ERRORED after (worker-side timing).

    In ``lean`` mode only the terminal event, unlabelled. ``emit_event`` is cheap (a local buffer
    append for processes, an in-process enqueue for threads); the (expensive) profiler serialize
    runs only when ``profile_due()`` says so (time-throttled, off the per-task path). SUBMITTED is
    emitted driver-side. Emission is best-effort.
    """
    label = "" if lean else partition_label(task.partition)
    n = task.partition.n_entries
    if not lean:
        emit_event(TaskEvent(TaskPhase.STARTED, task.key, worker, time.perf_counter(), label, n))
    try:
        result = process(task.partition, resources)
    except BaseException as exc:
        emit_event(
            TaskEvent(
                TaskPhase.ERRORED,
                task.key,
                worker,
                time.perf_counter(),
                label,
                n,
                error=_render_error(exc),
            )
        )
        raise
    emit_event(TaskEvent(TaskPhase.FINISHED, task.key, worker, time.perf_counter(), label, n))
    if profiler is not None and profile_due():
        with contextlib.suppress(Exception):
            payload = profiler.flush()
            if payload:
                emit_profile(worker, payload)
    return result


def _render_error(exc: BaseException) -> str:
    """Return a concise, picklable error summary for the dashboard.

    A graphed-debug ``StageError`` already str()s to a user-source-mapped message (op +
    analysis line), so its own text is the best summary; anything else gets a plain ``Type:
    message``.
    """
    if isinstance(exc, StageError):
        return str(exc)
    return f"{type(exc).__name__}: {exc}"


def _thread_profiler(monitor: Monitor | None) -> WorkerProfiler | None:
    """Return a per-thread profiler.

    Built once from the monitor's factory and reused for this worker thread's later tasks. ``None``
    when no monitor or no factory (MVP tier).
    """
    if monitor is None:
        return None
    cached = getattr(_thread_local, "prof", _MISSING)
    if cached is not _MISSING:
        return cast("WorkerProfiler | None", cached)
    prof: WorkerProfiler | None = None
    with contextlib.suppress(Exception):
        factory = monitor.worker_profiler_factory()
        if factory is not None:
            prof = factory()
            prof.start()
            _register_profiler(prof)  # stopped at process exit (silences sampler teardown)
    _thread_local.prof = prof
    return prof


def _thread_profile_due() -> bool:
    """Per-thread time throttle for the profiler serialize (see :func:`_proc_profile_due`)."""
    now = time.monotonic()
    last = cast("float", getattr(_thread_local, "last_flush", 0.0))
    if now - last >= _PROFILE_FLUSH_INTERVAL:
        _thread_local.last_flush = now
        return True
    return False


def _thread_task(
    process: Callable[[Partition, LocalResources], object],
    task: Task,
    monitor: Monitor | None,
    lean: bool = False,
) -> object:
    if monitor is None:
        return process(task.partition, _thread_resources())

    def emit_event(ev: TaskEvent) -> None:
        # in-process enqueue; the NetworkMonitor's sender thread does the I/O
        emit_task(monitor, ev)

    def emit_profile(worker: str, payload: bytes) -> None:
        if monitor is not None:
            with contextlib.suppress(Exception):
                monitor.on_profile(worker, payload)

    return _run_with_emit(
        process,
        task,
        _thread_resources(),
        worker=threading.current_thread().name,
        emit_event=emit_event,
        emit_profile=emit_profile,
        profiler=_thread_profiler(monitor),
        profile_due=_thread_profile_due,
        lean=lean,
    )


def _prime_shared(token: str, payload: bytes) -> int:
    """Cache the broadcast process under ``token`` and return this worker's pid.

    The pid lets the driver confirm coverage. Bounded FIFO (cap ``_SHARED_CACHE_CAP``), evicting
    the oldest broadcast -- identical across workers because every worker sees the same broadcast
    order, so it stays in lockstep with the driver's token set. The brief hold makes a worker keep
    this task long enough for its siblings to each claim one (single-round coverage).
    """
    if token not in _shared_objects:  # idempotent: re-priming an existing token does not reorder
        _shared_objects[token] = pickle.loads(payload)
        while len(_shared_objects) > _SHARED_CACHE_CAP:
            _shared_objects.popitem(last=False)
    time.sleep(0.002)
    return os.getpid()


def _proc_task_shared(token: str, task: Task) -> object:
    assert _proc_resources is not None  # set by the pool initializer
    process = cast("Callable[[Partition, LocalResources], object]", _shared_objects[token])
    monitor = _proc_monitor
    if monitor is not None:  # per-worker push: straight to this process's own monitor

        def emit_event(ev: TaskEvent) -> None:
            emit_task(monitor, ev)

        def emit_profile(worker: str, payload: bytes) -> None:
            with contextlib.suppress(Exception):
                monitor.on_profile(worker, payload)

    elif _proc_buffer is not None:

        def emit_event(ev: TaskEvent) -> None:
            _proc_emit(("task", ev))  # local buffer append; the drain thread ships it off-path

        def emit_profile(worker: str, payload: bytes) -> None:
            _proc_emit(("profile", (worker, payload)))

    else:  # no monitor: nothing to emit, so build no event and format no label
        return process(task.partition, _proc_resources)
    return _run_with_emit(
        process,
        task,
        _proc_resources,
        worker=str(os.getpid()),
        emit_event=emit_event,
        emit_profile=emit_profile,
        profiler=_proc_profiler,
        profile_due=_proc_profile_due,
        lean=_proc_lean,
    )


def _proc_task_portable(token: str, task: Task) -> object:
    """Run the process pool's leaf entry.

    The partial returns through a pickle it may not survive.
    """
    return Portable(_proc_task_shared(token, task))


def _proc_combine_task(
    combine: Callable[[object, object], object], a: object, b: object
) -> object:
    return Portable(combine(a, b))


class ThreadExecutor(_BaseExecutor):
    """Thread-pool executor: a thread-safe worker pool with thread-local `open_once` resources."""

    def _pool(self) -> _PoolExecutor:
        return ThreadPoolExecutor(max_workers=self.max_workers)

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        monitor, lean = self._run_monitor, self._run_lean
        return lambda task: pool.submit(_thread_task, process, task, monitor, lean)

    def _peer_threads(
        self,
        plan: Plan[R],
        n: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        transports: dict[str, Any],
        release: Any,
    ) -> tuple[list[threading.Thread], dict[str, dict[str, int]], dict[str, BaseException]]:
        """Build an unstarted actor thread per worker and the dicts its witness and error go to."""
        witness: dict[str, dict[str, int]] = {}
        complete = complete_events(self._run_monitor)
        # a worker thread's exception (captured, then re-raised)
        errors: dict[str, BaseException] = {}

        def actor(addr: str) -> None:
            try:
                witness[addr] = process_and_reduce(
                    addr,
                    transports[addr],
                    n,
                    bounds,
                    worker_addrs,
                    plan.process,
                    plan.combine,
                    items[addr],
                    LocalResources(),  # fresh per worker thread (closed when the actor returns)
                    steal=self._steal,
                    emit=self._run_monitor is not None,
                    profiler_factory=self._peer_profiler_factory(),
                    lean=self._run_lean,
                    keys=self._run_keys,
                    complete=complete,
                    release=release,
                )
            # a thread exception is otherwise lost -> capture + propagate
            except BaseException as exc:
                errors[addr] = exc

        threads = [
            threading.Thread(target=actor, args=(a,), name=f"graphed-peer-{a}", daemon=True)
            for a in worker_addrs
        ]
        return threads, witness, errors

    def _peer_execute(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        # threads share the process, so the in-process transports (queue.Queue or loopback HTTP)
        # are built once by the driver and handed to each worker thread directly.
        transports = build_transports(self._comms or "ipc", ("driver", *worker_addrs))
        release_r, release_w = multiprocessing.Pipe(duplex=False)  # EOF once the driver closes it
        threads, witness, errors = self._peer_threads(
            plan, n, bounds, worker_addrs, items, transports, release_r
        )
        driver_t = transports["driver"]
        ctl = (
            PeerControl(control, driver_t, n, plan.combine, plan.empty)
            if control is not None
            else None
        )
        try:
            for t in threads:
                t.start()
            try:
                # a worker failed -> the root will never form; stop waiting and re-raise
                out = self._await_peer_root(driver_t, n, ctl, lambda: bool(errors))
            except TimeoutError:
                driver_t.broadcast(("done",))  # release the actors the finally joins
                raise
            if ctl is not None:  # a queued tag lands before `done`
                ctl.close(0.0 if errors else OUTBOX_EXIT_WAIT_S)
            driver_t.broadcast(("done",))
            for t in threads:
                t.join(timeout=30.0)
            for a in worker_addrs:  # an HTTP lane still POSTs a returned actor's last events
                transports[a].close()
            # drain trailing monitor events the workers shipped
            for _sender, payload in driver_t.poll():
                self._forward_peer_events(payload)
            if errors:
                raise next(iter(errors.values()))  # re-raise a worker exception intact (M6)
            assert out is not None
            return out
        finally:
            release_w.close()  # first: the joins below wait on actors that only this ends
            if ctl is not None:
                ctl.close(0.0)
            for t in threads:
                if t.is_alive():  # an interrupt can land before every thread started
                    t.join(timeout=30.0)
            release_r.close()
            for tr in transports.values():
                tr.close()
            self._last_peer_witness = [witness[a] for a in worker_addrs if a in witness]


class _ProcessExecutorBase(_ProcessPeerBase):
    """Shared process-pool machinery.

    Spawn-based worker processes (cross-platform + free-threaded-safe), each with its own
    ``open_once`` resources. The Plan ``process`` callable and its partials must be picklable;
    a remote ``StageError`` round-trips to the driver intact.

    The two public process executors differ ONLY in which pool peer-reduction IPC uses, selected by
    the class attribute ``_peer_pool_is_pinned`` (no silent runtime switch):
    :class:`ProcessPoolExecutor` (full-registry stdlib pool) vs :class:`PinnedPoolExecutor`
    (identity-pinned, bounded O(log N) overlay). Everything else — the main task pool, tree
    reduction, monitoring — is identical.

    M37: when a :class:`Monitor` is attached, workers cannot reach the driver's monitor object, so
    they push events onto a bounded ``Manager().Queue()`` that a driver-side **collector daemon
    thread** drains and replays into the monitor. The queue is best-effort (drop-on-full) so
    instrumentation never back-pressures a worker.
    """

    def __init__(
        self,
        max_workers: int | None = None,
        *,
        on_combine: Callable[[int], None] | None = None,
        pooled_combines: bool = False,
        persistent: bool = False,
        monitor: Monitor | None = None,
        comms: str | None = "ipc",
        steal: bool = True,
        control: RunControl | None = None,
        max_in_flight: int = 2,
    ):
        super().__init__(
            max_workers,
            on_combine=on_combine,
            pooled_combines=pooled_combines,
            persistent=persistent,
            monitor=monitor,
            comms=comms,
            steal=steal,
            control=control,
            max_in_flight=max_in_flight,
        )
        self._mgr: SyncManager | None = None
        # the kept hub pool's initializer identity
        self._kept_init: tuple[bytes, bool] | None = None
        # False while a kept pool whose workers ship to the collector may still deliver an earlier
        # run's events: from each hub entry until a complete_events run's event wait returns
        self._kept_settled = True
        self._hub_terms = 0  # terminal events the collector handed to a monitor this hub run
        self._event_q: object | None = None
        self._collector: threading.Thread | None = None
        self._collector_stop: threading.Event | None = None

    def _pool_init(self) -> tuple[Any, Any, Any, bool]:
        """Return what the worker initializer fixes.

        These are the profiler factory, event queue, push factory and lean flag.
        """
        factory = (
            self._run_monitor.worker_profiler_factory() if self._run_monitor is not None else None
        )
        push = self._run_push
        return factory, None if push is not None else self._event_q, push, self._run_lean

    def _pool(self) -> _PoolExecutor:
        factory, event_q, push, lean = self._pool_init()
        return _StdProcessPool(
            max_workers=self.max_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=functools.partial(_proc_init, monitor_factory=push, lean=lean),
            initargs=(factory, event_q),
        )

    def _submit_combine(
        self,
        pool: _PoolExecutor,
        combine: Callable[[object, object], object],
        a: object,
        b: object,
    ) -> Future[object]:
        # inputs and output cross processes
        return pool.submit(_proc_combine_task, combine, Portable(a), Portable(b))

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        payload = pickle.dumps(process)  # pickled ONCE; the bytes are reused for the broadcast
        token = hashlib.sha256(payload).hexdigest()
        self._broadcast(pool, token, payload)
        return lambda task: pool.submit(_proc_task_portable, token, task)

    # ---- M37 dashboard collector (worker side-channel -> driver monitor) ----

    @contextlib.contextmanager
    def _acquired_pool(self) -> Iterator[_PoolExecutor]:
        # a kept pool whose initializer fixed other than what this run needs is respawned
        self._ensure_collector()
        factory, event_q, push, lean = self._pool_init()
        event_q_used = event_q is not None
        init = (pickle.dumps((factory, push, lean)), event_q_used)
        if init != self._kept_init and self._kept_pool is not None:
            self._kept_pool.shutdown(wait=True)
            self._kept_pool = None
        self._kept_init = init
        self._hub_terms = 0
        self._kept_settled = not event_q_used
        try:
            with super()._acquired_pool() as pool:
                yield pool
        finally:
            # The collector's lifecycle follows the POOL, not the run. Non-persistent: the pool is
            # shut down above, so drain + stop now. Persistent: workers stay alive and keep
            # draining their buffers *after* run() returns (emission is off-path/async), so the
            # collector must stay alive across runs or those trailing events are lost — close()
            # stops it.
            if not self._persistent:
                self._stop_collector()

    def _await_run_events(self, target: int) -> None:
        if self._run_push is None:  # the collector serves this run: count its terminals in
            wait_until(lambda: self._hub_terms >= target, _HUB_EVENT_DRAIN_S)
        self._kept_settled = True

    def _switch_in(self, monitor: Monitor | None) -> None:
        # a kept pool that may still ship an earlier run's events hands them to the previous
        # monitor (the collector's final drain) and is respawned, so the incoming complete_events
        # run gets none
        if complete_events(monitor) and self._kept_pool is not None and not self._kept_settled:
            self._release_kept_pool()
            self._stop_collector()

    def _ensure_collector(self) -> None:
        if self._run_monitor is None or self._run_push is not None:  # pushing workers bypass it
            return
        if self._mgr is None:  # one manager + queue for this executor's lifetime
            self._mgr = multiprocessing.get_context("spawn").Manager()
            self._event_q = self._mgr.Queue(maxsize=10000)
        if self._collector is None or not self._collector.is_alive():  # idempotent across runs
            self._collector_stop = threading.Event()
            self._collector = threading.Thread(
                target=self._collect_loop, name="graphed-dash-collector", daemon=True
            )
            self._collector.start()

    def _collect_loop(self) -> None:
        q = self._event_q
        stop = self._collector_stop
        assert q is not None and stop is not None
        while not stop.is_set():
            try:
                item = q.get(timeout=0.05)  # type: ignore[attr-defined]
            except queue.Empty:
                continue
            self._dispatch(item)
        while True:  # final drain after the pool's work has settled
            try:
                item = q.get_nowait()  # type: ignore[attr-defined]
            except queue.Empty:
                break
            self._dispatch(item)

    def _dispatch(self, item: tuple[str, object]) -> None:
        monitor = self._run_monitor
        if monitor is None:
            return
        kind, payload = item
        if kind == "batch":  # a worker drain thread coalesces many events into one Manager put
            for sub in cast("list[tuple[str, object]]", payload):
                self._dispatch(sub)
            return
        with contextlib.suppress(Exception):
            if kind == "task":
                monitor.on_task(cast("TaskEvent", payload))
            elif kind == "profile":
                worker, data = cast("tuple[str, bytes]", payload)
                monitor.on_profile(worker, data)
        # counted whether on_task returned or raised, never before it returns
        if kind == "task" and cast("TaskEvent", payload).phase in (
            TaskPhase.FINISHED,
            TaskPhase.ERRORED,
        ):
            self._hub_terms += 1

    def _stop_collector(self) -> None:
        if self._collector is not None and self._collector_stop is not None:
            self._collector_stop.set()
            self._collector.join(timeout=5.0)
            self._collector = None

    def close(self) -> None:
        super().close()
        self._stop_collector()
        self._close_peer()
        if self._mgr is not None:
            self._mgr.shutdown()
            self._mgr = None
            self._event_q = None

    def _broadcast(self, pool: _PoolExecutor, token: str, payload: bytes) -> None:
        """Prime every worker with ``payload`` exactly once.

        concurrent.futures exposes no worker identity, so we submit priming tasks (each
        holds briefly, then returns its pid) until the set of pids covers the whole pool —
        the prime is idempotent, so extra hits are harmless.
        """
        if token in self._broadcast_tokens:
            self._broadcast_tokens.move_to_end(token)  # idempotent; keep recency
            return
        target = self.max_workers  # concrete (resolved in __init__) -- no private pool attribute
        seen: set[int] = set()
        # a deadline, not a round count: booted workers answer every round while a sibling still
        # boots
        t0 = time.monotonic()
        while len(seen) < target and time.monotonic() - t0 < self._worker_start_timeout_s:
            batch = [pool.submit(_prime_shared, token, payload) for _ in range(target - len(seen))]
            # a dead worker surfaces as BrokenProcessPool here
            seen.update(f.result() for f in batch)
        if len(seen) < target:  # never silently mark a token primed without full coverage (P1-3)
            raise RuntimeError(
                f"broadcast reached only {len(seen)}/{target} workers in "
                f"{time.monotonic() - t0:.1f}s; "
                "refusing to cache an under-primed process (would KeyError on an unprimed worker)"
            )
        self._broadcast_tokens[token] = None
        while len(self._broadcast_tokens) > _SHARED_CACHE_CAP:  # FIFO, lockstep with the workers
            self._broadcast_tokens.popitem(last=False)


class ProcessPoolExecutor(_ProcessExecutorBase):
    """Process executor whose peer-reduction IPC uses a **full-registry** worker pool.

    That pool is the stdlib :class:`concurrent.futures.ProcessPoolExecutor`: every worker inherits
    every peer's queue via the pool initializer. Simple and fast — the right default up to roughly
    the per-process file-descriptor limit. Because inheritance is O(N²) in fds, on large many-core
    machines (>~128 cores, or any low ``RLIMIT_NOFILE``) it warns and you should switch to
    :class:`PinnedPoolExecutor` instead. This is the original M7 behaviour, now named explicitly so
    the pool choice is never hidden.
    """

    _peer_pool_is_pinned = False
    _warned_fd_budget = False  # the fd-budget warning fires at most once per executor

    def _check_fd_budget(self, w: int) -> None:
        if not self._warned_fd_budget and _exceeds_fd_budget(w):
            self._warned_fd_budget = True
            warnings.warn(
                f"ProcessPoolExecutor peer reduction inherits ~2*(w+1) queue fds per worker; with "
                f"w={w} this likely exceeds the per-process file-descriptor limit on this "
                "machine. Use PinnedPoolExecutor for a bounded O(log N) overlay on large "
                "many-core machines.",
                stacklevel=3,
            )


class PinnedPoolExecutor(_ProcessExecutorBase):
    """Process executor whose peer-reduction IPC uses an **identity-pinned** worker pool.

    That pool is :class:`~graphed_executors.local._pinned_pool.PinnedProcessPool`: each worker is
    spawned once and inherits ONLY its own inbox plus its O(log N) overlay peers (segment-tree
    reduction targets + hypercube steal lifelines + the driver). The registry is therefore O(N log
    N), not O(N²), so it stays under the per-process fd limit — the executor to pick for large
    many-core machines. Identical results to :class:`ProcessPoolExecutor` (bit-for-bit), only the
    communication footprint differs.
    """

    _peer_pool_is_pinned = True


class ProcessExecutor(ProcessPoolExecutor):
    """Deprecated alias for :class:`ProcessPoolExecutor`.

    The full-registry pool — its original M7 meaning. Kept for back-compat; pick
    :class:`ProcessPoolExecutor` or :class:`PinnedPoolExecutor` explicitly so the IPC pool is
    visible at the call site rather than chosen silently.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Warn that this alias is deprecated, then build a :class:`ProcessPoolExecutor`."""
        warnings.warn(
            "ProcessExecutor is deprecated; use ProcessPoolExecutor (full-registry, the same "
            "behaviour) "
            "or PinnedPoolExecutor (identity-pinned, bounded O(log N) overlay) explicitly.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
