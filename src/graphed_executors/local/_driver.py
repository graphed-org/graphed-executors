"""The driver every local executor shares: plan dispatch, the hub routes, and the peer entry.

:class:`_BaseExecutor` owns the run (one plan at a time, run control, the monitor snapshot) and
the three hub routes (fixed tree, pooled combines, adaptive). Subclasses in :mod:`.executors` and
:mod:`._process_peer` supply the worker pool, the worker entry points and the peer actors.
"""

from __future__ import annotations

import contextlib
import os
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Executor as _PoolExecutor
from concurrent.futures import Future, as_completed, wait
from typing import Any, TypeVar, cast

from graphed.core import (
    ExecContext,
    ExecResult,
    Partition,
    Plan,
    RunControl,
    RunState,
    StopReason,
    Task,
)
from graphed.core.execution import (
    LocalResources,
    Monitor,
    TaskEvent,
    TaskPhase,
    WorkerProfiler,
    complete_events,
    emit_task,
    finalize_result,
    lean_events,
    partition_label,
    worker_monitor_factory,
)
from graphed.services import require_bound

from .._plan_queue import PlanQueue
from .._window import Window
from ..common import refuse_staged
from ._peer import PeerControl, make_bounds, slice_items
from ._reduce import LazyReducer, plan_tree, running_fold, tree_reduce

R = TypeVar("R")


class _RunLeaves:
    """A complete_events hub run's leaves.

    Only the unresolved futures are held, so a leaf's result is never kept alive by the settle.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.pending: set[Future[object]] = set()
        self.submitted = 0
        self.cancelled = 0  # exact at the settle: only the driver cancels, and cancel() calls back

    def add(self, fut: Future[object]) -> None:
        with self._lock:
            self.submitted += 1
            self.pending.add(fut)
        fut.add_done_callback(self._done)  # outside the lock: a done future calls back at once

    def _done(self, fut: Future[object]) -> None:
        with self._lock:
            self.pending.discard(fut)
            self.cancelled += fut.cancelled()

    def settle(self) -> int:
        """Wait for every leaf; return how many were not cancelled."""
        with self._lock:
            pending = list(self.pending)
        wait(pending)
        return self.submitted - self.cancelled


class _BaseExecutor:
    """Shared driver. Subclasses supply the worker pool + the (picklable) worker entry point.

    ``pooled_combines=True`` (M10) schedules the tree-reduce combines onto the SAME worker pool as
    the leaves, instead of running them serially on the driver thread — for heavy partials (large
    histograms, many partitions) the driver otherwise becomes a serial combine bottleneck. The
    combine pairing is the same fixed `plan_tree`, so results stay bit-identical; with a process
    pool, `plan.combine` must be picklable (module-level), exactly like `plan.process`.
    """

    #: How long a peer driver waits for the root, counting only time the run is not paused.
    _peer_root_timeout_s = 300.0

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
        self._plans = PlanQueue(self.run, max_in_flight)
        self.max_workers = max_workers if max_workers is not None else (os.cpu_count() or 1)
        # test hook: called per tree-reduce combine with #leaves so far
        self._on_combine = on_combine
        self._pooled_combines = pooled_combines
        # persistent=True keeps ONE pool across run() calls (amortizing the import-heavy spawn
        # over many plans — notebooks, sweeps); the default stays a fresh pool per run.
        self._persistent = persistent
        self._kept_pool: _PoolExecutor | None = None
        # M31/M34: primed tokens, FIFO-bounded in lockstep with each worker cache
        self._broadcast_tokens: OrderedDict[str, None] = OrderedDict()
        self.monitor = monitor  # M37: a passive dashboard observer (None => no instrumentation)
        self.control = control  # m65: pause/resume/cancel, read at each run (plan-A1 A-2)
        # the running plan's snapshot: a reassignment mid-run reaches the next plan, not this one
        self._run_monitor: Monitor | None = None
        self._run_push: Callable[[], Monitor] | None = None  # the monitor's per-worker factory
        self._run_lean = False
        self._run_keys: list[int] | None = None  # a peer run's task keys, when not 0..n-1
        self._run_leaves: _RunLeaves | None = None  # a complete_events hub run's leaves
        # M38: comms=None -> the hub reduction (driver combines); "ipc"/"http" -> PEER reduction
        # (combines run across the workers over that transport, off the driver). Result is
        # identical (same fixed plan_tree grouping); peer just relocates the combines. steal=True
        # (peer only) lets an idle worker take leaves from a busy peer (process work moves; the
        # leaf's owner still reduces it -> result unchanged). The last run's per-worker witness
        # stats are kept for tests.
        self._comms = comms
        self._steal = steal
        self._last_peer_witness: list[dict[str, int]] = []
        # Per-run state lives on the instance (kept pool, peer registry, broadcast tokens).
        self._run_lock = threading.Lock()
        # STRICT: peer reduction (comms set) emits monitor events + fires on_combine, but it CANNOT
        # run pooled combines (its whole model is off-driver combines — `pooled_combines` is a
        # hub-only mechanism). Rather than SILENTLY falling back to the hub path (hub-mode sneaking
        # into a run the caller asked to be peer), refuse loudly, so a big calculation can never
        # quietly run hub when peer was requested. (comms=None => hub, which supports
        # pooled_combines.)
        if comms is not None and pooled_combines:
            raise ValueError(
                f"peer reduction (comms={comms!r}) does not support pooled_combines (a hub-only "
                f"mechanism); use comms=None for the hub path, or drop pooled_combines"
            )

    def _pool(self) -> _PoolExecutor:
        raise NotImplementedError

    def _raw_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        """Deliver ``process`` to the pool's workers as needed and return ``submit(task)``.

        Threads share memory (no delivery); processes broadcast the process once (M31).
        """
        raise NotImplementedError

    def _submit_combine(
        self,
        pool: _PoolExecutor,
        combine: Callable[[object, object], object],
        a: object,
        b: object,
    ) -> Future[object]:
        """Submit one pooled combine; a shared-memory pool runs ``combine`` on its inputs as-is."""
        return pool.submit(combine, a, b)

    def _leaf_submit(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        """``_raw_submit``'s callable.

        Keeping each leaf future for the settle when the run's monitor asks for complete events (a
        base cannot wrap what a subclass's ``_raw_submit`` returns).
        """
        raw = self._raw_submit(pool, process)
        leaves = self._run_leaves
        if leaves is None:
            return raw

        def submit(task: Task) -> Future[object]:
            fut = raw(task)
            leaves.add(fut)
            return fut

        return submit

    def _prepare(
        self, pool: _PoolExecutor, process: Callable[[Partition, LocalResources], object]
    ) -> Callable[[Task], Future[object]]:
        """Wrap the subclass submit with a driver-side SUBMITTED emission (M37).

        Worker-side STARTED/FINISHED/ERRORED come from the worker entry; here we record the
        moment of submission.
        """
        raw = self._leaf_submit(pool, process)
        monitor = self._run_monitor
        if monitor is None:
            return raw

        def submit(task: Task) -> Future[object]:
            emit_task(
                monitor,
                TaskEvent(
                    TaskPhase.SUBMITTED,
                    task.key,
                    "driver",
                    time.perf_counter(),
                    partition_label(task.partition),
                    task.partition.n_entries,
                ),
            )
            return raw(task)

        return submit

    def _emit_submitted(self, tasks: Iterable[Task]) -> None:
        """Driver-side SUBMITTED for every task, when it becomes known (M37)."""
        if self._run_monitor is None:
            return
        for t in tasks:
            emit_task(
                self._run_monitor,
                TaskEvent(
                    TaskPhase.SUBMITTED,
                    t.key,
                    "driver",
                    time.perf_counter(),
                    partition_label(t.partition),
                    t.partition.n_entries,
                ),
            )

    def _folded(
        self, plan: Plan[R], pieces: list[tuple[int, R]], done: int, n_combines: int, stopped: bool
    ) -> ExecResult[R]:
        """Return the result from what the combine tree left.

        The root, or on a cancel the completed subtrees folded by first leaf (plan-A2
        A2-1).
        """
        value, k = running_fold(iter(pieces), plan.combine, plan.empty)
        for _ in range(k):
            self._combine_cb(done)
        return ExecResult(
            value, done, n_combines + k, StopReason.CANCELLED if stopped else StopReason.EXHAUSTED
        )

    def _combine_cb(self, leaves_done: int) -> None:
        """Run the tree-reduce combine callback.

        The legacy test hook AND the dashboard monitor (M37).
        """
        if self._on_combine is not None:
            self._on_combine(leaves_done)
        if self._run_monitor is not None:
            with contextlib.suppress(Exception):
                self._run_monitor.on_combine(leaves_done)

    @contextlib.contextmanager
    def _acquired_pool(self) -> Iterator[_PoolExecutor]:
        self._run_leaves = _RunLeaves() if complete_events(self._run_monitor) else None
        if not self._persistent:
            self._broadcast_tokens = OrderedDict()  # a fresh pool: nothing is primed yet
            # settle before the shutdown's exit flush
            with self._pool() as pool, self._settled(pool):
                yield pool
            return
        if self._kept_pool is None:
            self._broadcast_tokens = OrderedDict()  # newly (re)spawned workers hold no cache
            self._kept_pool = self._pool()
        with self._settled(self._kept_pool):
            yield self._kept_pool  # kept alive for the next run()

    @contextlib.contextmanager
    def _settled(self, pool: _PoolExecutor) -> Iterator[_PoolExecutor]:
        """On a normal or ``Exception`` exit of a complete_events run, wait for its leaf futures.

        Then wait for their events; a ``KeyboardInterrupt`` leaves at once.
        """
        leaves = self._run_leaves
        if leaves is None:
            yield pool
            return
        try:
            yield pool
        except Exception:
            self._await_run_events(leaves.settle())
            raise
        else:
            self._await_run_events(leaves.settle())
        finally:
            self._run_leaves = None

    def _await_run_events(self, target: int) -> None:
        """Wait until the leaves' events have reached the monitor.

        A thread worker calls ``on_task`` before its future resolves, so here the future
        wait was the whole wait.
        """

    def _switch_in(self, monitor: Monitor | None) -> None:
        """Prepare for ``monitor``; called in ``run()`` before it becomes the run's monitor.

        The thread hub binds each leaf's monitor at submit, so nothing earlier can reach
        it.
        """

    def _release_kept_pool(self) -> None:
        if self._kept_pool is not None:
            self._kept_pool.shutdown(wait=True)
            self._kept_pool = None
            self._broadcast_tokens = OrderedDict()  # respawned workers will need re-priming

    @property
    def max_in_flight(self) -> int:
        return self._plans.max_in_flight

    def submit(self, plan: Plan[R]) -> Future[ExecResult[R]]:
        """Queue ``plan`` and return a future of its :meth:`run` result without waiting for it.

        Plans run one at a time in submit order; this blocks while ``max_in_flight``
        submitted plans are unfinished.
        """
        return self._plans.submit(plan)

    def close(self) -> None:
        """Finish every submitted plan, then release a persistent pool (idempotent).

        A later run() or submit() lazily respawns.
        """
        self._plans.close()
        self._release_kept_pool()

    def __enter__(self) -> _BaseExecutor:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def run(self, plan: Plan[R]) -> ExecResult[R]:
        """Run ``plan`` to its reduced result.

        One plan runs at a time per executor; a concurrent caller waits for the running
        plan to finish. A plan whose services are not bound is refused before any task
        (this runner resolves no services: bind them, or use ``SubmitRunner``).
        """
        refuse_staged(plan, type(self).__name__, "ThreadBackend()")
        require_bound(plan)
        with self._run_lock:
            self._switch_in(self.monitor)
            self._run_monitor = self.monitor
            self._run_push = worker_monitor_factory(self.monitor)
            self._run_lean = lean_events(self.monitor)
            control = self.control
            try:
                if control is not None and control.state is RunState.CANCELLED:
                    result = ExecResult(plan.empty(), 0, 0, StopReason.CANCELLED)
                elif plan.next_tasks is not None:
                    result = self._run_adaptive(plan, control)
                elif self._comms is not None:
                    result = self._run_peer(plan, control)
                elif self._pooled_combines:
                    result = self._run_fixed_pooled(plan, control)
                elif control is not None:
                    result = self._run_fixed_windowed(plan, control)
                else:
                    result = self._run_fixed(plan)
            finally:
                if control is not None and control.state is RunState.CANCELLED:
                    control.reset()  # a cancel ends this run only, whether or not a check saw it
            return finalize_result(plan, result)

    def _run_peer(self, plan: Plan[R], control: RunControl | None = None) -> ExecResult[R]:
        """M38 peer reduction.

        Partition the leaves into contiguous per-worker ranges and reduce them across the
        workers over ``self._comms``, off the driver. The subclass spawns the W actors
        (threads or processes); the result is bit-for-bit the hub path's (same fixed tree).

        Monitor parity (so peer can be the default): the driver emits SUBMITTED per task here, the
        workers emit STARTED/FINISHED/ERRORED over the transport (forwarded to the monitor by the
        collect loop), and the driver fires the n-1 combine callbacks below (completed-1 on a
        cancel).
        """
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        w = max(1, min(self.max_workers, n))
        bounds = make_bounds(n, w)
        worker_addrs = tuple(f"w{i}" for i in range(w))
        items = slice_items([t.partition for t in tasks], bounds, worker_addrs)
        self._emit_submitted(tasks)  # worker-side STARTED/FINISHED/ERRORED stream in
        self._run_keys = None
        if self._run_monitor is not None and any(t.key != i for i, t in enumerate(tasks)):
            self._run_keys = [t.key for t in tasks]  # workers emit keys[leaf]
        # A thread actor starts its first leaf before the driver's first poll, so no tag could hold
        # a run entered paused: it waits here, before any actor exists.
        if control is not None and control.wait() is RunState.CANCELLED:
            return ExecResult(plan.empty(), 0, 0, StopReason.CANCELLED)
        value, done = self._peer_execute(plan, n, w, bounds, worker_addrs, items, control)
        for _ in range(done - 1):  # peer ran done-1 combines across the workers; report the count
            self._combine_cb(done)
        stopped = StopReason.CANCELLED if done < n else StopReason.EXHAUSTED
        return ExecResult(value, done, max(done - 1, 0), stopped)

    def _peer_profiler_factory(self) -> Callable[[], WorkerProfiler] | None:
        """Return the picklable per-worker profiler factory (M37), or None.

        Peer workers then profile exactly like the hub path and ``Dashboard(profile=True)`` works
        under peer (no silent loss).
        """
        return (
            self._run_monitor.worker_profiler_factory() if self._run_monitor is not None else None
        )

    def _forward_peer_events(self, payload: tuple[Any, ...]) -> bool:
        """Forward a worker monitor-event batch or profile sample-tree to the monitor.

        If ``payload`` is a worker monitor-event batch (task lifecycle) or a profile sample-tree,
        forward it to the monitor and return True; else return False (the caller handles root/other
        messages). Best-effort (M37 passivity) — a raising monitor never breaks the run.
        """
        if payload[0] == "events":
            for ev in payload[1]:
                emit_task(self._run_monitor, ev)
            return True
        if payload[0] == "profile" and self._run_monitor is not None:
            with contextlib.suppress(Exception):
                self._run_monitor.on_profile(payload[1], payload[2])
            return True
        return False

    def _await_peer_root(
        self,
        driver_t: Any,
        n: int,
        ctl: PeerControl[R] | None,
        failed: Callable[[], bool],
        on_via: Callable[[Any], None] | None = None,
    ) -> tuple[R, int] | None:
        """Poll the driver transport until the root arrives or a cancel's hand-ins fold.

        Forwards monitor events, hands ``via`` messages to ``on_via`` and relays run control;
        paused time does not count against ``_peer_root_timeout_s``. Returns what is in hand once
        ``failed()`` reports a worker failure, and raises ``TimeoutError`` past the deadline.
        """
        deadline = time.monotonic() + self._peer_root_timeout_s
        out: tuple[R, int] | None = None
        while out is None:
            got = driver_t.recv(timeout=0.05)
            if got is not None and got[1][0] == "root":
                return got[1][1], n
            if got is not None and on_via is not None and got[1][0] == "via":
                on_via(got[1])
                continue
            # worker STARTED/FINISHED/ERRORED -> monitor; a cancel hand-in -> the fold once all
            # are in
            if got is not None and not self._forward_peer_events(got[1]) and ctl is not None:
                out = ctl.take(got[1])
            if failed():
                break
            if ctl is not None and ctl.relay():
                deadline = time.monotonic() + self._peer_root_timeout_s
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"peer reduction did not produce a root within {self._peer_root_timeout_s}s"
                )
        return out

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
        """Return the reduced value and the number of leaves it holds.

        Fewer than ``n`` only on a cancel.
        """
        raise NotImplementedError

    def _run_fixed_pooled(self, plan: Plan[R], control: RunControl | None = None) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        combines, root = plan_tree(n)
        assert root is not None  # n >= 1
        waiting: dict[int, list[int]] = {}  # input node -> combine indices needing it
        remaining: dict[int, set[int]] = {}  # combine index -> still-unready inputs
        first = list(range(n))  # node -> its first leaf (a combine's left input comes first)
        for ci, (_out, a, b) in enumerate(combines):
            remaining[ci] = {a, b}
            waiting.setdefault(a, []).append(ci)
            waiting.setdefault(b, []).append(ci)
            first.append(first[a])

        window = Window(control, self.max_workers, enumerate(tasks))
        with self._acquired_pool() as pool:
            submit = self._leaf_submit(pool, plan.process)
            self._emit_submitted(tasks)
            node_of: dict[Future[object], int] = {}
            ready: dict[int, R] = {}
            n_combines = 0
            leaves_done = 0
            sent = 0
            while True:
                for i, t in window.take(sent - leaves_done):
                    node_of[submit(t)] = i
                    sent += 1
                if not node_of and not window.held:
                    break
                for fut in window.wait(list(node_of)):
                    node = node_of.pop(fut)
                    ready[node] = cast(R, fut.result())  # re-raises a worker error intact
                    if node < n:
                        leaves_done += 1
                    for ci in waiting.get(node, ()):
                        remaining[ci].discard(node)
                        if not remaining[ci]:
                            out, a, b = combines[ci]
                            # a<b -> deterministic left/right grouping, same tree as the driver
                            # path
                            combine = cast("Callable[[object, object], object]", plan.combine)
                            f2 = self._submit_combine(pool, combine, ready.pop(a), ready.pop(b))
                            node_of[f2] = out
                            n_combines += 1
                            self._combine_cb(leaves_done)
        pieces = [(first[node], v) for node, v in ready.items()]
        return self._folded(
            plan, sorted(pieces, key=lambda p: p[0]), leaves_done, n_combines, window.stopped
        )

    def _run_fixed(self, plan: Plan[R]) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        with self._acquired_pool() as pool:
            submit = self._prepare(pool, plan.process)
            leaf_of: dict[Future[object], int] = {submit(t): i for i, t in enumerate(tasks)}

            def completed() -> Iterator[tuple[int, R]]:
                for fut in as_completed(leaf_of):
                    # fut.result() re-raises a worker error
                    yield leaf_of[fut], cast(R, fut.result())

            value, n_combines = tree_reduce(
                n, completed(), plan.combine, plan.empty, on_combine=self._combine_cb
            )
        return ExecResult(value, n, n_combines, StopReason.EXHAUSTED)

    def _run_fixed_windowed(self, plan: Plan[R], control: RunControl) -> ExecResult[R]:
        """``_run_fixed`` through a :class:`Window`.

        The same fixed tree, fed as leaves complete.
        """
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        reducer: LazyReducer[R] = LazyReducer(
            len(tasks), plan.combine, plan.empty, on_combine=self._combine_cb
        )
        window = Window(control, self.max_workers, enumerate(tasks))
        with self._acquired_pool() as pool:
            submit = self._leaf_submit(pool, plan.process)
            self._emit_submitted(tasks)
            leaf_of: dict[Future[object], int] = {}
            while True:
                for leaf, t in window.take(len(leaf_of)):
                    leaf_of[submit(t)] = leaf
                if not leaf_of and not window.held:
                    break
                for fut in window.wait(list(leaf_of)):
                    # re-raises a worker error
                    reducer.feed(leaf_of.pop(fut), cast(R, fut.result()))
        return self._folded(
            plan, reducer.frontier(), reducer.delivered, reducer.n_combines, window.stopped
        )

    def _run_adaptive(self, plan: Plan[R], control: RunControl | None = None) -> ExecResult[R]:
        assert plan.next_tasks is not None
        ctx = ExecContext()
        start = time.perf_counter()
        results: list[tuple[int, R]] = []
        # future -> (key, n_entries, t0)
        submitted: dict[Future[object], tuple[int, int, float]] = {}
        stopped: StopReason | None = None
        next_tasks = plan.next_tasks
        window: Window[Task] = Window(control, self.max_workers)

        with self._acquired_pool() as pool:
            submit = self._leaf_submit(pool, plan.process)

            def refill() -> None:
                batch = next_tasks(ctx)  # DONE == None
                if batch:
                    self._emit_submitted(batch)
                    window.held.extend(batch)

            refill()
            while True:
                for task in window.take(len(submitted)):
                    # t0 at submission, so time held in the window never counts as run time
                    submitted[submit(task)] = (
                        task.key,
                        task.partition.n_entries,
                        time.perf_counter(),
                    )
                if not submitted and not window.held:
                    break
                done = window.wait(list(submitted))
                if not done:
                    continue
                for fut in done:
                    key, n_entries, t0 = submitted.pop(fut)
                    results.append((key, cast(R, fut.result())))  # re-raises a worker error intact
                    ctx.n_done += 1
                    ctx.events_done += n_entries
                    ctx.last_durations[key] = time.perf_counter() - t0
                ctx.elapsed_s = time.perf_counter() - start
                # the next_tasks call is skipped; no stop exit ends the drain
                if window.cancelled():
                    continue
                reason = plan.stop.reason(ctx) if plan.stop else None
                if reason is not None:
                    stopped = reason
                    for fut in submitted:
                        fut.cancel()
                    break
                refill()

        value, n_combines = running_fold(iter(results), plan.combine, plan.empty)
        stopped = stopped or (StopReason.CANCELLED if window.stopped else StopReason.EXHAUSTED)
        return ExecResult(value, ctx.n_done, n_combines, stopped)
