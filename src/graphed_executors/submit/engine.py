"""The generic Plan engine ``SubmitRunner`` over any :class:`SubmitBackend` (plan §1.2).

One engine, N backends — the same factorization as the generic shuffle engine over
``ShuffleBackend``. The fixed path mirrors the local reduction topology EXACTLY: leaves are
``process`` tasks, combines follow the ``plan_tree`` shape as future dependencies, and the driver
takes the value of the single root future (so bit-for-bit equality vs ``SequentialRunner`` is
inherited, not re-derived), raising at the first task that fails. The adaptive path folds
completions with ``running_fold`` and cancels outstanding work on stop. Both are reused from
``graphed_executors.local._reduce`` — no duplication.

The task functions it submits, and the worker seam they read, live in ``submit/worker.py``.

A submission's scope is its run (plan-services §3.1): ``run`` opens one ``ExitStack`` and registers
on it everything the submission acquires — the plan's :class:`ServiceSet` (``submit/services.py``)
when ``plan.services`` is non-empty, then a ``cancel`` of its plan tasks not yet done — so no
service, record or queued task of a run outlives it or reaches another. The body runs on the plan
with the run's endpoints bound (graphed's ``bind_services``, before the first plan-task submit),
and the value is resolved against the plan while its services are still up.
"""

from __future__ import annotations

import contextlib
import hashlib
import pickle
import queue
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Future
from dataclasses import replace
from typing import Any, TypeVar, cast, overload

import cloudpickle
from graphed import services as graphed_services
from graphed.core import (
    DurablePlanV2,
    ExecContext,
    ExecResult,
    Plan,
    RunControl,
    RunState,
    StopReason,
    Task,
)
from graphed.core.execution import (
    Monitor,
    TaskEvent,
    TaskPhase,
    emit_task,
    finalize_result,
    lean_events,
    partition_label,
    worker_monitor_factory,
)
from graphed.debug import SourceFrame, StageError
from graphed.shuffle import pick

from graphed_executors._plan_queue import PlanQueue
from graphed_executors._window import PAUSED_WAKE_S, Window, wait_until
from graphed_executors.local._reduce import TreeProgress, plan_tree, running_fold

from .protocol import SubmitBackend, SubmitFuture
from .services import ServiceSet
from .worker import RunContext as RunContext
from .worker import WorkerEnv as WorkerEnv
from .worker import _combine_task, _event_from_dict, _leaf_task, _map_task, _stage_task
from .worker import _identity as _identity
from .worker import _reset_worker_env as _reset_worker_env
from .worker import _service_checked as _service_checked
from .worker import current_env as current_env
from .worker import set_worker_env as set_worker_env

R = TypeVar("R")
T = TypeVar("T")
_DRAIN_TIMEOUT_S = 30.0  # bounded wait for trailing worker events before unsubscribing (off-path)


def _fingerprint(
    obj: object, dumps: Callable[[object], bytes] = pickle.dumps
) -> tuple[str, bytes]:
    """(12-hex content fingerprint, pickled payload) — the M31 broadcast-token idiom."""
    payload = dumps(obj)
    return hashlib.sha256(payload).hexdigest()[:12], payload


def _key(plan_fp: str, run_nonce: str, kind: str, idx: int) -> str:
    """Return a namespaced, per-run-nonced, collision-proof task key (plan §1.2.3).

    The ``graphed-`` + fingerprint prefix guarantees no key collides with a user string arg
    (dask/dask#9969); the nonce makes an explicit ``key=`` re-execute across two ``run()``
    calls instead of returning a cached future.
    """
    return f"graphed-{plan_fp}-{run_nonce}-{kind}-{idx}"


def _drain(ctx: RunContext, events_seen: list[int], n_leaves: int) -> None:
    """Wait, at most ``_DRAIN_TIMEOUT_S``, for the worker events of ``n_leaves`` consumed leaves.

    Run before unsubscribing, so a leaf's trailing events still reach the monitor.
    """
    if ctx.monitor_topic is not None:
        wait_until(lambda: events_seen[0] >= ctx.events_per_leaf * n_leaves, _DRAIN_TIMEOUT_S)


class _RunTasks:
    """One run's plan-task submits (the ``_RunLeaves`` idiom).

    Only the futures not yet done are held, since a held dask future pins its result in cluster
    memory, and :meth:`cancel` cancels those. With ``checks`` (the run's services), every task
    runs through :func:`_service_checked`.
    """

    def __init__(
        self, backend: SubmitBackend, checks: tuple[tuple[str, str, str], ...] = ()
    ) -> None:
        self._backend = backend
        self._checks = checks
        self._lock = threading.Lock()
        self._pending: set[SubmitFuture] = set()

    def submit(
        self, fn: Callable[..., object], /, *args: object, key: str, retries: int
    ) -> SubmitFuture:
        if self._checks:
            args, fn = (self._checks, fn, *args), _service_checked
        fut = self._backend.submit(fn, *args, key=key, retries=retries)
        with self._lock:
            self._pending.add(fut)
        fut.add_done_callback(self._done)  # outside the lock: a done future calls back at once
        return fut

    def _done(self, fut: SubmitFuture) -> None:
        with self._lock:
            self._pending.discard(fut)

    def cancel(self) -> None:
        """Cancel the run's plan tasks not yet done (best-effort, as ``backend.cancel`` is)."""
        with self._lock:
            # a done one may not have called back
            pending = [fut for fut in self._pending if not fut.done()]
        if pending:
            self._backend.cancel(pending)


class SubmitRunner:
    """A :class:`graphed.core.Executor` over any :class:`SubmitBackend`.

    ``run`` dispatches to the adaptive path when the plan carries ``next_tasks``, else the
    fixed ``plan_tree`` future graph.

    ``services`` (service name -> ``scheme://host:port``) are the leg-1 endpoints of every run,
    read once at each run's start like ``monitor``; a user-held :class:`ServiceSet`'s endpoints
    keep its services warm across runs.
    """

    def __init__(
        self,
        backend: SubmitBackend,
        *,
        monitor: Monitor | None = None,
        retries: int = 3,
        max_in_flight: int = 2,
        control: RunControl | None = None,
        services: Mapping[str, str] | None = None,
    ) -> None:
        """Run plans on ``backend``, passing ``retries`` to each task's submit."""
        self._plans = PlanQueue(self.run, max_in_flight)
        self.backend = backend
        self.monitor = monitor  # read once at each run's start (Dashboard.attach assigns it)
        self.control = control  # likewise
        self.services = services  # likewise
        self._retries = retries

    def __enter__(self) -> SubmitRunner:
        """Return the runner."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Close the runner."""
        self.close()

    @property
    def max_in_flight(self) -> int:
        """The most submitted plans that may be unfinished at once."""
        return self._plans.max_in_flight

    def submit(self, plan: Plan[R]) -> Future[ExecResult[R]]:
        """Queue ``plan`` and return a future of its :meth:`run` result without waiting for it.

        Plans run one at a time in submit order; this blocks while ``max_in_flight``
        submitted plans are unfinished.
        """
        return self._plans.submit(plan)

    def close(self) -> None:
        """Finish every submitted plan, then close the backend."""
        self._plans.close()
        self.backend.close()

    @overload
    def run(self, plan: Plan[R]) -> ExecResult[R]: ...
    @overload
    def run(self, plan: DurablePlanV2) -> ExecResult[Any]: ...
    def run(self, plan: Plan[R] | DurablePlanV2) -> ExecResult[R] | ExecResult[Any]:
        """Run ``plan``.

        Its services are resolved, checked, probed and bound before the first plan task is
        submitted, and released, with any of its tasks not yet done, when ``run`` returns
        or raises. A :class:`~graphed.core.DurablePlanV2` (a join or repartition) runs
        stage by stage to ``plan.value`` of its last stage; a cancel returns
        ``value=None``. The plan's ``finalize`` runs here, after its services are released.
        """
        monitor = self.monitor
        control = self.control
        given = self.services
        ctx = self._context(uuid.uuid4().hex[:8], monitor)
        # the submission's scope: released in reverse on exit
        with contextlib.ExitStack() as scope:
            result = self._run_scoped(plan, monitor, control, given, ctx, scope)
        return finalize_result(plan, result)

    def _run_scoped(
        self,
        plan: Plan[R] | DurablePlanV2,
        monitor: Monitor | None,
        control: RunControl | None,
        given: Mapping[str, str] | None,
        ctx: RunContext,
        scope: contextlib.ExitStack,
    ) -> ExecResult[R] | ExecResult[Any]:
        events_seen = [0]
        # handlers run on worker threads; += is not atomic without the GIL
        seen_lock = threading.Lock()
        unsub: Callable[[], None] | None = None
        try:
            if control is not None and control.state is RunState.CANCELLED:
                empty = None if isinstance(plan, DurablePlanV2) else plan.empty()
                return ExecResult(empty, 0, 0, StopReason.CANCELLED)
            bound = plan
            checks: tuple[tuple[str, str, str], ...] = ()
            if plan.services:
                services = ServiceSet(
                    plan.services, self.backend, endpoints=given, scope=ctx.run_nonce
                )
                endpoints = scope.enter_context(services)
                bound = graphed_services.bind_services(plan, endpoints)
                checks = tuple(
                    (spec.name, endpoints[spec.name], spec.check) for spec in plan.services
                )
            submits = _RunTasks(self.backend, checks)
            # registered after the services, so it runs before their release
            scope.callback(submits.cancel)
            if ctx.monitor_topic is not None and ctx.events_per_leaf:

                def handler(events: list[dict[str, object]]) -> None:
                    with seen_lock:
                        events_seen[0] += len(events)
                    for d in events:
                        # swallows a raising monitor (passivity)
                        emit_task(monitor, _event_from_dict(d))

                unsub = self.backend.subscribe_events(ctx.monitor_topic, handler)
            result: ExecResult[R] | ExecResult[Any]
            if isinstance(bound, DurablePlanV2):
                result = self._run_stages(bound, control, ctx, submits)
            elif control is not None:  # the default path never meets a window
                if bound.next_tasks is not None:
                    result = self._run_adaptive_windowed(
                        bound, monitor, control, ctx, events_seen, submits
                    )
                else:
                    result = self._run_fixed_windowed(
                        bound, monitor, control, ctx, events_seen, submits
                    )
            elif bound.next_tasks is not None:
                result = self._run_adaptive(bound, monitor, ctx, events_seen, submits)
            else:
                result = self._run_fixed(bound, monitor, ctx, events_seen, submits)
            if not plan.services:
                return result
            # graphed's walk, through collate and aggregate_plan, while the services are still up
            return replace(result, value=graphed_services.resolve_services(bound, result.value))
        finally:
            if unsub is not None:
                unsub()
            if control is not None and control.state is RunState.CANCELLED:
                control.reset()  # a cancel ends this run only, whether or not a check saw it

    # ---- fixed path: plan_tree as the future graph (plan §1.2.1) ----

    def _run_fixed(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        ctx: RunContext,
        events_seen: list[int],
        submits: _RunTasks,
    ) -> ExecResult[R]:
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        if n == 0:
            return ExecResult(plan.empty(), 0, 0, StopReason.EXHAUSTED)
        plan_fp, ptoken, phandle = self._broadcast(plan.process, ctx)
        key_to_task: dict[str, Task] = {}
        futs: dict[int, SubmitFuture] = {}
        # a raise drains nothing: a piggyback backend ships a leaf's events only in its result()
        drained = 0
        try:
            for i, task in enumerate(tasks):
                if monitor is not None:
                    # driver-side SUBMITTED (leaves only)
                    emit_task(monitor, self._submitted_event(task))
                key = _key(plan_fp, ctx.run_nonce, "leaf", i)
                key_to_task[key] = task
                futs[i] = submits.submit(
                    _leaf_task, ctx, phandle, ptoken, task, key=key, retries=self._retries
                )
            combines, root = plan_tree(n)
            assert root is not None  # n >= 1
            _cfp, ctoken, chandle = self._broadcast(plan.combine, ctx)
            for out, a, b in combines:  # a < b: deterministic left/right, the plan_tree shape
                key = _key(plan_fp, ctx.run_nonce, "combine", out)
                futs[out] = submits.submit(
                    _combine_task,
                    ctx,
                    chandle,
                    ctoken,
                    futs[a],
                    futs[b],
                    key=key,
                    retries=self._retries,
                )
            # a combine settles only once all its inputs have, so the root alone learns of a
            # failure late
            done_q: queue.Queue[SubmitFuture] = queue.Queue()
            for fut in futs.values():
                fut.add_done_callback(done_q.put)
            while not futs[root].done():
                fut = done_q.get()
                if fut.exception() is not None:  # exception() transfers no result on dask
                    self._result(fut, key_to_task)  # raises it, translated
            value = cast(R, self._result(futs[root], key_to_task))
            drained = n
            return ExecResult(value, n, len(combines), StopReason.EXHAUSTED)
        finally:
            _drain(ctx, events_seen, drained)

    def _run_fixed_windowed(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        control: RunControl,
        ctx: RunContext,
        events_seen: list[int],
        submits: _RunTasks,
    ) -> ExecResult[R]:
        """``_run_fixed`` through a :class:`Window`.

        The same ``plan_tree``, with each combine submitted once both inputs completed, so
        a cancel leaves completed subtrees to fold by first leaf.
        """
        tasks = sorted(plan.tasks, key=lambda t: t.key)  # deterministic leaf order
        n = len(tasks)
        tree: TreeProgress[SubmitFuture] = TreeProgress(n)
        plan_fp, ptoken, phandle = self._broadcast(plan.process, ctx)
        _cfp, ctoken, chandle = self._broadcast(plan.combine, ctx)
        if monitor is not None:
            for task in tasks:
                emit_task(monitor, self._submitted_event(task))
        window = Window(control, self._task_slots(), enumerate(tasks))
        done_q: queue.Queue[SubmitFuture] = queue.Queue()
        node_of: dict[SubmitFuture, int] = {}  # outstanding future -> its plan_tree node
        key_to_task: dict[str, Task] = {}
        sent = done = n_combines = 0  # leaves submitted, leaves completed, combines submitted
        # a raise drains nothing: a piggyback backend ships a leaf's events only in its result()
        drained = 0

        def start(item: tuple[int, Task]) -> None:
            nonlocal sent
            i, task = item
            key = _key(plan_fp, ctx.run_nonce, "leaf", i)
            key_to_task[key] = task
            fut = submits.submit(
                _leaf_task, ctx, phandle, ptoken, task, key=key, retries=self._retries
            )
            sent += 1
            node_of[fut] = i
            fut.add_done_callback(done_q.put)

        def in_flight() -> int:
            return sent - done

        try:
            for fut in self._completions(window, done_q, node_of, in_flight, start):
                node = node_of.pop(fut)
                if fut.exception() is not None:  # exception() transfers no result on dask
                    self._result(fut, key_to_task)  # raises it, translated
                done += node < n
                for out, fa, fb in tree.complete(node, fut):
                    key = _key(plan_fp, ctx.run_nonce, "combine", out)
                    f2 = submits.submit(
                        _combine_task, ctx, chandle, ctoken, fa, fb, key=key, retries=self._retries
                    )
                    n_combines += 1
                    node_of[f2] = out
                    f2.add_done_callback(done_q.put)
            pieces = sorted(
                ((tree.first[m], self._result(f, key_to_task)) for m, f in tree.ready.items()),
                key=lambda p: p[0],
            )
            value, k = running_fold(
                iter(cast("list[tuple[int, R]]", pieces)), plan.combine, plan.empty
            )
            stopped = StopReason.CANCELLED if window.stopped else StopReason.EXHAUSTED
            drained = sent
            return ExecResult(value, done, n_combines + k, stopped)
        finally:
            _drain(ctx, events_seen, drained)

    # ---- adaptive path + stop (plan §1.2.4) ----

    def _run_adaptive(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        ctx: RunContext,
        events_seen: list[int],
        submits: _RunTasks,
    ) -> ExecResult[R]:
        assert plan.next_tasks is not None
        next_tasks = plan.next_tasks
        plan_fp, ptoken, phandle = self._broadcast(plan.process, ctx)
        exec_ctx = ExecContext()
        done_q: queue.Queue[SubmitFuture] = queue.Queue()
        outstanding: dict[SubmitFuture, tuple[int, int]] = {}  # future -> (task.key, n_entries)
        # F2b: dask key -> Task, so a worker death names the partition
        key_to_task: dict[str, Task] = {}
        results: list[tuple[int, R]] = []
        stopped: StopReason | None = None
        seq = 0

        def refill() -> None:
            nonlocal seq
            batch = next_tasks(exec_ctx)  # DONE == None
            if not batch:
                return
            for task in batch:
                if monitor is not None:
                    emit_task(monitor, self._submitted_event(task))
                dask_key = _key(plan_fp, ctx.run_nonce, "leaf", seq)
                # F2b: attribute a KilledWorker to this chunk (not the key)
                key_to_task[dask_key] = task
                fut = submits.submit(
                    _leaf_task, ctx, phandle, ptoken, task, key=dask_key, retries=self._retries
                )
                seq += 1
                outstanding[fut] = (task.key, task.partition.n_entries)
                fut.add_done_callback(done_q.put)  # backend-neutral as_completed (queue.Queue)

        try:
            refill()
            while outstanding:
                fut = done_q.get()
                if fut not in outstanding:  # a cancelled/duplicate callback
                    continue
                key, n_entries = outstanding.pop(fut)
                # F2b: real map, not {}
                results.append((key, cast(R, self._result(fut, key_to_task))))
                exec_ctx.n_done += 1
                exec_ctx.events_done += n_entries
                reason = plan.stop.reason(exec_ctx) if plan.stop else None
                if reason is not None:
                    stopped = reason
                    # best-effort; cancel_running=False backend no-ops
                    self.backend.cancel(list(outstanding))
                    break
                refill()

            value, n_combines = running_fold(iter(results), plan.combine, plan.empty)
            return ExecResult(value, exec_ctx.n_done, n_combines, stopped or StopReason.EXHAUSTED)
        finally:
            _drain(ctx, events_seen, exec_ctx.n_done)  # F2a: every consumed leaf

    def _run_adaptive_windowed(
        self,
        plan: Plan[R],
        monitor: Monitor | None,
        control: RunControl,
        ctx: RunContext,
        events_seen: list[int],
        submits: _RunTasks,
    ) -> ExecResult[R]:
        """``_run_adaptive`` with each ``next_tasks`` batch held in a :class:`Window`."""
        assert plan.next_tasks is not None
        next_tasks = plan.next_tasks
        plan_fp, ptoken, phandle = self._broadcast(plan.process, ctx)
        exec_ctx = ExecContext()
        done_q: queue.Queue[SubmitFuture] = queue.Queue()
        outstanding: dict[SubmitFuture, tuple[int, int]] = {}  # future -> (task.key, n_entries)
        # F2b: dask key -> Task, so a worker death names the partition
        key_to_task: dict[str, Task] = {}
        results: list[tuple[int, R]] = []
        stopped: StopReason | None = None
        seq = 0
        window: Window[Task] = Window(control, self._task_slots())

        def refill() -> None:
            batch = next_tasks(exec_ctx)  # DONE == None
            if not batch:
                return
            for task in batch:
                if monitor is not None:
                    emit_task(monitor, self._submitted_event(task))
            window.held.extend(batch)

        def start(task: Task) -> None:
            nonlocal seq
            dask_key = _key(plan_fp, ctx.run_nonce, "leaf", seq)
            # F2b: attribute a KilledWorker to this chunk (not the key)
            key_to_task[dask_key] = task
            fut = submits.submit(
                _leaf_task, ctx, phandle, ptoken, task, key=dask_key, retries=self._retries
            )
            seq += 1
            outstanding[fut] = (task.key, task.partition.n_entries)
            fut.add_done_callback(done_q.put)  # backend-neutral as_completed (queue.Queue)

        try:
            refill()
            in_flight = outstanding.__len__
            for fut in self._completions(window, done_q, outstanding, in_flight, start):
                key, n_entries = outstanding.pop(fut)
                # F2b: real map, not {}
                results.append((key, cast(R, self._result(fut, key_to_task))))
                exec_ctx.n_done += 1
                exec_ctx.events_done += n_entries
                if window.cancelled():  # then no next_tasks and no stop exit
                    continue
                reason = plan.stop.reason(exec_ctx) if plan.stop else None
                if reason is not None:
                    stopped = reason
                    # best-effort; cancel_running=False backend no-ops
                    self.backend.cancel(list(outstanding))
                    break
                refill()

            value, n_combines = running_fold(iter(results), plan.combine, plan.empty)
            stopped = stopped or (StopReason.CANCELLED if window.stopped else StopReason.EXHAUSTED)
            return ExecResult(value, exec_ctx.n_done, n_combines, stopped)
        finally:
            _drain(ctx, events_seen, exec_ctx.n_done)  # F2a: every consumed leaf

    # ---- staged path: a DurablePlanV2's stages as barriers ----

    def _run_stages(
        self, plan: DurablePlanV2, control: RunControl | None, ctx: RunContext, submits: _RunTasks
    ) -> ExecResult[Any]:
        """Submit each stage once the stages it reads have finished.

        A ``map_write`` task's payload reaches each gather as that gather's slice alone:
        picked in a task beside it where the backend moves data between workers, else
        picked here from its one fetch.
        """
        peer = self.backend.capabilities.peer_data_movement
        plan_fp = plan.ir_fingerprint()[:12]
        done_q: queue.Queue[SubmitFuture] = queue.Queue()
        # stage task not yet seen done -> its stage has inputs
        outstanding: dict[SubmitFuture, bool] = {}
        key_to_task: dict[str, Task] = {}
        stage_futs: list[list[SubmitFuture]] = []
        ran = [0, 0]  # tasks of stages with no inputs, tasks of the others

        def settle(needed: list[SubmitFuture]) -> bool:
            """Wait until ``needed`` is done; False when a completion finds the run cancelled."""
            remaining = {f for f in needed if f in outstanding}
            while remaining:
                fut = done_q.get()  # each stage task completes into the queue once
                ran[outstanding.pop(fut)] += 1
                remaining.discard(fut)
                if fut.exception() is not None:  # exception() transfers no result on dask
                    self._result(fut, key_to_task)  # raises it, translated
                if control is not None and control.state is RunState.CANCELLED:
                    return False
            return True

        for s, stage in enumerate(plan.stages):
            upstream = [f for i in stage.inputs for f in stage_futs[i]]
            if not settle(upstream) or (
                control is not None and control.wait() is RunState.CANCELLED
            ):
                return ExecResult(None, ran[0], ran[1], StopReason.CANCELLED)
            # by value: a resolved opaque process is a copy stdlib pickle cannot name by reference
            _fp, token, handle = self._broadcast(stage.process.resolve(), ctx, cloudpickle.dumps)
            fn = _map_task if stage.kind == "map_write" else _stage_task
            # driver edge: each map result is fetched once, then sliced per dest here
            fetched = {
                i: [cast("dict[int, bytes]", self._result(f, key_to_task)) for f in stage_futs[i]]
                for i in stage.inputs
                if plan.stages[i].kind == "map_write" and not peer
            }
            futs: list[SubmitFuture] = []
            for t, task in enumerate(stage.tasks):
                args: list[object] = []
                for i in stage.inputs:
                    if i in fetched:
                        args.extend(pick(m, task.key) for m in fetched[i])
                    elif plan.stages[i].kind == "map_write":
                        args.extend(
                            submits.submit(
                                pick,
                                f,
                                task.key,
                                key=_key(plan_fp, ctx.run_nonce, f"pick.{i}.{m}", task.key),
                                retries=self._retries,
                            )
                            for m, f in enumerate(stage_futs[i])
                        )
                    else:
                        args.extend(stage_futs[i])
                key = _key(plan_fp, ctx.run_nonce, f"{stage.kind}.{s}", t)
                key_to_task[key] = task
                fut = submits.submit(
                    fn, handle, token, task, *args, key=key, retries=self._retries
                )
                outstanding[fut] = bool(stage.inputs)
                fut.add_done_callback(done_q.put)
                futs.append(fut)
            stage_futs.append(futs)
        if not settle(list(outstanding)):
            return ExecResult(None, ran[0], ran[1], StopReason.CANCELLED)
        last = [cast("bytes", self._result(f, key_to_task)) for f in stage_futs[-1]]
        return ExecResult(plan.value(last), ran[0], ran[1], StopReason.EXHAUSTED)

    # ---- helpers ----

    def _broadcast(
        self, obj: object, ctx: RunContext, dumps: Callable[[object], bytes] = pickle.dumps
    ) -> tuple[str, str, object]:
        """Broadcast ``obj`` for this run; return its fingerprint, token and handle."""
        fp, payload = _fingerprint(obj, dumps)
        token = f"{fp}-{ctx.run_nonce}"
        return fp, token, self.backend.broadcast(payload, token=token)

    def _completions(
        self,
        window: Window[T],
        done_q: queue.Queue[SubmitFuture],
        outstanding: Mapping[SubmitFuture, object],
        in_flight: Callable[[], int],
        start: Callable[[T], None],
    ) -> Iterator[SubmitFuture]:
        """Yield each completion of ``outstanding`` while ``window`` starts its held work.

        Ends once nothing is outstanding or held; paused with nothing running, it waits on the
        window's control.
        """
        control = window.control
        assert control is not None  # a windowed run always has a control
        while True:
            if window.held:  # an empty window has nothing to start or drop
                self._fill(window, in_flight(), start)
            if not outstanding and not window.held:
                return
            if not outstanding:  # paused with nothing running
                control.wait()
                continue
            try:
                fut = done_q.get(timeout=PAUSED_WAKE_S if window.held else None)
            except queue.Empty:  # a timed wake only re-reads the slots and re-runs take
                continue
            if fut in outstanding:  # else a cancelled/duplicate callback
                yield fut

    def _task_slots(self) -> int:
        """Return the controlled window's size, floored at one.

        ``task_slots()`` where the backend has it (off the pinned Protocol; it never
        waits), else ``n_workers()``.
        """
        task_slots = getattr(self.backend, "task_slots", None)
        slots = task_slots() if task_slots is not None else self.backend.n_workers()
        return max(1, int(slots))

    def _fill(self, window: Window[T], in_flight: int, start: Callable[[T], None]) -> None:
        """Start the held work the window releases.

        A full window holding work re-reads the slots and resizes, so workers that joined
        since the last read are used.
        """
        while True:
            items = window.take(in_flight)
            for item in items:
                start(item)
            in_flight += len(items)
            if not window.held or in_flight < window.size:
                return
            size = self._task_slots()
            grew = size > window.size
            window.size = size  # follows each re-read, so a shrunk pool narrows the window too
            if not grew:
                return

    def _result(self, fut: SubmitFuture, key_to_task: dict[str, Task]) -> object:
        """Resolve a future, translating a backend worker-death signal.

        The signal is a ``KilledWorker``; it becomes an attributed :class:`StageError` (plan §1.4).
        Ordinary exceptions re-raise intact.
        """
        try:
            return fut.result()
        except BaseException as exc:
            translated = self._translate(exc, key_to_task)
            if translated is not None:
                raise translated from exc
            raise

    def _translate(self, exc: BaseException, key_to_task: dict[str, Task]) -> StageError | None:
        describe = getattr(self.backend, "describe_failure", None)
        if describe is None:
            return None
        info = describe(exc)
        if info is None:
            return None
        failing_key, last_worker = info
        task = key_to_task.get(failing_key)
        partition = partition_label(task.partition) if task is not None else str(failing_key)
        frame = SourceFrame(
            filename=task.partition.uri if task is not None else str(failing_key), lineno=0
        )
        return StageError(
            op="run",
            frames=(frame,),
            input_forms=(),
            partition=partition,
            cause_type=type(exc).__name__ if isinstance(exc, TimeoutError) else "KilledWorker",
            cause_message=(
                str(exc)
                if isinstance(exc, TimeoutError)
                else f"worker {last_worker} died (segfault/OOM/preemption suspected; note: blame "
                "can be "
                f"unfair under co-located tasks)"
            ),
            opt_level=0,
        )

    def _submitted_event(self, task: Task) -> TaskEvent:
        return TaskEvent(
            TaskPhase.SUBMITTED,
            task.key,
            "driver",
            time.perf_counter(),
            partition_label(task.partition),
            task.partition.n_entries,
        )

    def _context(self, run_nonce: str, monitor: Monitor | None) -> RunContext:
        if monitor is None:
            return RunContext(run_nonce, None, None)
        push = worker_monitor_factory(monitor)
        return RunContext(
            run_nonce,
            f"graphed-monitor-{run_nonce}",
            self._profiler_payload(monitor),
            pickle.dumps(push) if push is not None else None,
            lean_events(monitor),
        )

    def _profiler_payload(self, monitor: Monitor | None) -> bytes | None:
        # Worker-side statistical sampling is not wired for the dask backend in m42 (no frozen test
        # exercises on_profile); the RunContext field is populated per the §1.1 contract so the
        # seam is ready, but the shim does not start a profiler. ponytail: wire when a profile test
        # lands.
        if monitor is None:
            return None
        factory = monitor.worker_profiler_factory()
        return pickle.dumps(factory) if factory is not None else None
