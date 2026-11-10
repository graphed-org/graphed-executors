"""``ParslBackend`` (plan §1.2).

A :class:`SubmitBackend` over a **started** parsl executor used via DIRECT executor submit — no
DFK. The DFK unwraps future args on the head node anyway (dflow.py:863-901), so it would add a
global singleton + config-global retries/caching (fighting the determinism gate) for zero
capability gain; direct ``executor.submit(func, resource_specification, *args)`` is public
(executor.py:690) and measured working with three integration moves the
:mod:`~graphed_executors.parsl_backend.launch` helper encodes.

Per-INSTANCE capability vectors (plan §1.1): ``ParslBackend(HighThroughputExecutor)`` is the
all-seven-False "parsl floor" (future args resolve on the submit host, so ``peer_data_movement`` is
False even when the m47 transport plane exists — setting it True would falsely open the m43
``_require_peer`` gate to head-node routing); ``ParslBackend(ThreadPoolExecutor)`` is the
:class:`ThreadBackend` shape (``peer_data_movement=True`` alone — same-process shared memory). Any
other executor type is refused with a loud ``TypeError`` naming both verified classes.

This module imports NOTHING from parsl at load (the ``_lazy`` accessor + the parsl-free ``_shim``);
``test_parsl_no_cross_import`` pins that importing the package leaves parsl/dask/distributed out of
``sys.modules``.
"""

from __future__ import annotations

import time
import weakref
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, InvalidStateError
from contextlib import suppress
from typing import Any, cast

from graphed.core import RunControl

from graphed_executors.common.transport_run import in_chain
from graphed_executors.submit import SubmitCapabilities, SubmitRunner
from graphed_executors.submit.protocol import SubmitFuture

from ._lazy import _parsl_executor_types
from ._shim import EventBatch, _parsl_task_shim, dispatch_events
from .launch import start_htex, stop_htex

# The two frozen 7-field vectors (m42 pin — NO new fields, NO subclass; the m47 transport dispatch
# rides a backend-private attribute, never a capability field).
_HTEX_CAPS = SubmitCapabilities(
    peer_data_movement=False,
    scatter_broadcast=False,
    pin_to_worker=False,
    per_task_retries=False,
    per_task_resources=False,
    cancel_running=False,
    worker_file_cache=False,
)
_TPE_CAPS = SubmitCapabilities(
    peer_data_movement=True,  # same-process shared memory — the ThreadBackend precedent
    scatter_broadcast=False,
    pin_to_worker=False,
    per_task_retries=False,
    per_task_resources=False,
    cancel_running=False,
    worker_file_cache=False,
)

# bounded wait for >=1 HTEX manager (the exec-local wait_for_workers bound)
_N_WORKERS_WAIT_S = 30.0
# parsl death classes, matched by chain name-string
_WORKER_DEATH_NAMES = ("WorkerLost", "ManagerLost")


class _ParslFuture:
    """A thin :class:`SubmitFuture` over a parsl future.

    ``result()`` unwraps the ``(result, events)`` shim envelope and dispatches the buffered
    monitor events to the backend's driver-side handler registry exactly once
    (completion-granularity piggyback); ``done``/``exception``/``cancel``/``cancelled``
    delegate.
    """

    __slots__ = ("_dispatched", "_handlers", "_key", "_raw")

    def __init__(
        self,
        raw: Any,
        handlers: dict[str, Callable[[list[dict[str, object]]], None]],
        key: str = "",
    ) -> None:
        self._raw = raw
        self._key = key
        self._handlers = handlers
        self._dispatched = False

    def result(self, timeout: float | None = None) -> object:
        try:
            result, events = cast("tuple[object, list[EventBatch]]", self._raw.result(timeout))
        except BaseException as exc:
            if in_chain(exc, *_WORKER_DEATH_NAMES):  # parsl's own carries no task key
                exc.graphed_key = self._key  # type: ignore[attr-defined]
            raise
        if not self._dispatched:
            self._dispatched = True
            dispatch_events(events, self._handlers)
        return result

    def done(self) -> bool:
        return bool(self._raw.done())

    def exception(self, timeout: float | None = None) -> BaseException | None:
        return cast("BaseException | None", self._raw.exception(timeout))

    def add_done_callback(self, fn: Callable[[Any], None]) -> None:
        # deliver THIS adapter (not the raw future) so a backend-neutral as_completed keys on it
        self._raw.add_done_callback(lambda _raw: fn(self))

    def cancel(self) -> bool:
        return bool(self._raw.cancel())

    def cancelled(self) -> bool:
        return bool(self._raw.cancelled())


class ParslBackend:
    """A :class:`SubmitBackend` over a direct parsl executor submit (plan §1.2)."""

    def __init__(self, executor: Any) -> None:
        """Run tasks on a started parsl HighThroughputExecutor or ThreadPoolExecutor."""
        htex_cls, tpe_cls = _parsl_executor_types()
        if isinstance(executor, htex_cls):
            self.capabilities = _HTEX_CAPS
            self._is_htex = True
        elif isinstance(executor, tpe_cls):
            self.capabilities = _TPE_CAPS
            self._is_htex = False
        else:
            raise TypeError(
                "ParslBackend supports a started parsl HighThroughputExecutor or "
                "ThreadPoolExecutor "
                f"(the two verified executor classes), got {type(executor).__name__!r} — TaskVine/"
                "WorkQueue are the Phase-2 extension seam (per-instance honesty means not "
                "vouching "
                "for an unverified executor's capability vector)"
            )
        self._executor = executor
        self._handlers: dict[str, Callable[[list[dict[str, object]]], None]] = {}

    @property
    def peer_transport(self) -> bool:
        """m47: True on HTEX (the §1.4 peer plane runs on distinct worker PROCESSES), False on TPE.

        Driver-process threads — a 'peer exchange' there is loopback head-node routing. A
        parsl_backend-private attribute the facade gates ``shuffle_method='transport'`` on;
        NEVER a ``SubmitCapabilities`` field (the frozen 7-field vector stays unchanged).
        """
        return self._is_htex

    def n_workers(self) -> int:
        """Return the number of workers, waiting a bounded time for HTEX managers to register."""
        if not self._is_htex:
            return int(self._executor.max_threads)
        # the bounded manager-registration wait, not an assertion
        deadline = time.monotonic() + _N_WORKERS_WAIT_S
        total = self._count_htex_workers()
        while total == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
            total = self._count_htex_workers()
        if total == 0:
            raise RuntimeError(
                "no parsl managers registered after "
                f"{_N_WORKERS_WAIT_S}s — start_htex uses fixed blocks (init_blocks==min_blocks=="
                "max_blocks); check the interchange/manager launch (never a silent 0)"
            )
        return total

    def task_slots(self) -> int:
        """Tasks the executor runs at once, without :meth:`n_workers`' wait.

        0 on an HTEX with no manager connected yet.
        """
        return self._count_htex_workers() if self._is_htex else int(self._executor.max_threads)

    def _count_htex_workers(self) -> int:
        return sum(
            int(m["worker_count"])
            for m in self._executor.connected_managers()
            if m.get("active", True) and not m.get("draining", False)
        )

    def submit(
        self,
        fn: Callable[..., object],
        /,
        *args: object,
        key: str,
        retries: int = 0,
        priority: int = 0,
        resources: Mapping[str, float] | None = None,
        workers: Sequence[str] | None = None,
    ) -> SubmitFuture:
        """Resolve any :class:`_ParslFuture` args DRIVER-side.

        Parsl workers cannot resolve a driver-process future — the §1.1 head-node round-trip that
        makes ``peer_data_movement`` False. Then submit the concrete values wrapped in the
        module-level shim. ``resource_spec`` is ``{"priority": p}`` on HTEX iff ``p != 0`` (the one
        key HTEX forwards, executor.py:393) else ``{}``; ALWAYS ``{}`` on TPE (measured: it raises
        ``InvalidResourceSpecification`` on anything else). A task on a lost worker is resubmitted
        from the driver up to ``retries`` times: parsl's own retries live in the DFK this backend
        does not use. ``key``/``resources``/``workers`` are advisory hints this backend ignores.
        The shim wrap happens HERE, after any spy seam, so a submitted-fn-name witness records the
        raw ``fn`` (design-minor-3).
        """
        resolved = tuple(a.result() if isinstance(a, _ParslFuture) else a for a in args)
        spec: dict[str, int] = {"priority": priority} if (self._is_htex and priority) else {}

        def attempt() -> Any:
            return self._executor.submit(_parsl_task_shim, spec, fn, *resolved)

        raw = (
            attempt()
            if retries <= 0
            else _resubmitting(attempt, retries, cancels=not self._is_htex)
        )
        return _ParslFuture(raw, self._handlers, key)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        # degenerate on scatter_broadcast=False (the ThreadBackend form): the task fn receives the
        # raw bytes directly; each consuming task ships its own copy through the submit host.
        """Return ``payload``: each task carries its own copy."""
        return payload

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        """Call ``handler`` with each event batch on ``topic``; return the unsubscribe."""
        self._handlers[topic] = handler

        def unsub() -> None:
            self._handlers.pop(topic, None)

        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        """Best-effort, pre-run only (``cancel_running=False``); a no-op on HTEX.

        HTEX's futures are never marked running, so a dispatched task's future still
        cancels, and when its result lands parsl's result thread raises
        ``InvalidStateError`` on it and dies: every later task of the executor then never
        resolves, and the thread's zmq socket stays open, so a later collection of its
        context blocks in ``term()`` (the ``transport_peer`` cleanup avoids cancel for the
        same reason). A TPE future is cancelled only if its thread has not taken it.
        """
        if self._is_htex:
            return
        for fut in futures:
            cancel = getattr(fut, "cancel", None)
            if cancel is not None:
                cancel()

    def close(self) -> None:
        """Do nothing: the caller owns the executor."""
        return None  # the caller owns the executor; the launch helper documents teardown

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        """Map a parsl ``WorkerLost``/``ManagerLost`` to an attributable worker death.

        Matched by CLASS NAME anywhere in the exception chain, so this module stays parsl-free at
        load. The key is the one :class:`_ParslFuture` tagged the exception with; an exception
        that reached here another way has none, and its message stands in. Anything unrelated
        returns ``None``.
        """
        death = in_chain(exc, *_WORKER_DEATH_NAMES)
        if death is None:
            return None
        label = str(death) or type(death).__name__
        who = label.rpartition("loss of ")[2].removeprefix("worker ")
        return getattr(exc, "graphed_key", "") or label, who


def _resubmitting(attempt: Callable[[], Any], retries: int, *, cancels: bool) -> Future[Any]:
    """Return a future of ``attempt()`` that calls ``attempt`` again on each lost worker.

    ``retries`` times at most. With ``cancels``, cancelling it cancels the attempt in flight (never
    on HTEX: see ``cancel``).
    """
    outer: Future[Any] = Future()
    current: list[Any] = []

    def watch(raw: Any, left: int) -> None:
        if raw.cancelled():
            outer.cancel()
            return
        exc = raw.exception()
        if left > 0 and not outer.cancelled() and in_chain(exc, *_WORKER_DEATH_NAMES) is not None:
            try:
                current[0] = attempt()
            # a raise in a done-callback is only logged: settle instead
            except Exception as resubmit:
                exc = resubmit
            else:
                current[0].add_done_callback(lambda r: watch(r, left - 1))
                return
        with suppress(InvalidStateError):  # cancelled meanwhile
            if exc is None:
                outer.set_result(raw.result())
            else:
                outer.set_exception(exc)

    current.append(attempt())
    current[0].add_done_callback(lambda r: watch(r, retries))
    if cancels:
        outer.add_done_callback(lambda f: f.cancelled() and current[0].cancel())
    return outer


class ParslRunner(SubmitRunner):
    """A :class:`SubmitRunner` on parsl.

    ``ParslRunner(executor)`` runs on a started executor the caller owns; ``close()`` leaves it
    running. :meth:`local` starts its own and stops it on ``close()``, or at interpreter exit.
    """

    def __init__(
        self,
        executor: Any,
        *,
        monitor: Any = None,
        retries: int = 3,
        max_in_flight: int = 2,
        services: Mapping[str, str] | None = None,
        control: RunControl | None = None,
        retry_user_errors: int = 0,
    ) -> None:
        """Run on ``executor``; the keywords are :class:`SubmitRunner`'s."""
        super().__init__(
            ParslBackend(executor),
            monitor=monitor,
            retries=retries,
            max_in_flight=max_in_flight,
            services=services,
            control=control,
            retry_user_errors=retry_user_errors,
        )
        self._stop: Callable[[], Any] = lambda: None

    @classmethod
    def local(cls, *, workers: int, run_dir: str = "runinfo", **options: Any) -> ParslRunner:
        """Run on ``workers`` local HTEX processes; ``options`` are the constructor's keywords."""
        executor = start_htex(workers=workers, run_dir=run_dir)
        try:
            runner = cls(executor, **options)
        except BaseException:
            stop_htex(executor)
            raise
        runner._stop = weakref.finalize(runner, stop_htex, executor)
        return runner

    def close(self) -> None:
        """Finish every submitted plan, then stop the executor if this runner started it."""
        try:
            super().close()
        finally:
            self._stop()


def parsl_runner(
    executor: Any,
    *,
    monitor: Any = None,
    retries: int = 3,
    max_in_flight: int = 2,
    services: Mapping[str, str] | None = None,
    control: RunControl | None = None,
    retry_user_errors: int = 0,
) -> ParslRunner:
    """Build the user-facing runner in one line.

    A :class:`ParslRunner` on a started parsl executor. ``close()`` does NOT shut the caller's
    executor down (the launch helper documents teardown via ``stop_htex``). ``services`` are the
    runner's given service endpoints.
    """
    return ParslRunner(
        executor,
        monitor=monitor,
        retries=retries,
        max_in_flight=max_in_flight,
        services=services,
        control=control,
        retry_user_errors=retry_user_errors,
    )
