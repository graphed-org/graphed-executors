"""``skip_failed`` gives the same answer on every runner, a lost worker included."""

from __future__ import annotations

import concurrent.futures
import contextlib
import copy
import dataclasses
import functools
import gc
import json
import os
import sys
import traceback
import warnings
import weakref
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
from graphed import WorkerLostError
from graphed.core import (
    ExecContext,
    ExecResult,
    MonitorBase,
    Partition,
    Plan,
    RunControl,
    RunState,
    SequentialRunner,
    StopReason,
    Task,
    TaskEvent,
    TaskPhase,
    TooManyFailedChunksError,
    chunk_label,
    skipping,
)

from graphed_executors import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor
from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit.engine import _RunTasks

N = 20
END = {"FINISHED", "ERRORED"}


@dataclasses.dataclass(frozen=True)
class Chunks:
    bad: frozenset[int] = frozenset()
    poison: frozenset[int] = frozenset()

    def __call__(self, part: Partition, res: object) -> int:
        if part.entry_start in self.poison:
            os._exit(17)
        if part.entry_start in self.bad:
            raise OSError(f"bad chunk {part.entry_start}")
        return 1 << part.entry_start


def add(a: int, b: int) -> int:
    return a + b


def zero() -> int:
    return 0


def plan_of(*bad: int, poison: tuple[int, ...] = ()) -> Plan[int]:
    tasks = tuple(Task(i, Partition("f.root", "Events", i, i + 1)) for i in range(N))
    process = Chunks(frozenset(bad), frozenset(poison))
    return Plan(process=process, combine=add, empty=zero, tasks=tasks)


def without(*bad: int) -> int:
    return (1 << N) - 1 - sum(1 << i for i in bad)


Run = Callable[..., ExecResult[int]]


def _sequential(plan: Plan[int], monitor: Any = None, **kw: Any) -> ExecResult[int]:
    return SequentialRunner(monitor=monitor).run(plan, **kw)


def _local(cls: Any, **options: Any) -> Run:
    def run(plan: Plan[int], monitor: Any = None, **kw: Any) -> ExecResult[int]:
        with cls(workers=2, retries=0, monitor=monitor, **options) as runner:
            return runner.run(plan, **kw)  # type: ignore[no-any-return]

    return run


def _thread_backend(plan: Plan[int], monitor: Any = None, **kw: Any) -> ExecResult[int]:
    with SubmitRunner(ThreadBackend(2), retries=0, monitor=monitor) as runner:
        return runner.run(plan, **kw)


PROCESS_LEGS: dict[str, Run] = {
    "ipc": _local(ProcessPoolExecutor),
    "http": _local(ProcessPoolExecutor, comms="http"),
    "pinned": _local(PinnedPoolExecutor),
    "hub": _local(ProcessPoolExecutor, comms=None),
}
LEGS: dict[str, Run] = {
    "sequential": _sequential,
    "thread": _local(ThreadExecutor),
    "thread-hub": _local(ThreadExecutor, comms=None),
    "thread-backend": _thread_backend,
    **PROCESS_LEGS,
}


ONE_TASK_PER_WORKER = {
    "distributed.scheduler.allowed-failures": 0,
    "distributed.scheduler.worker-saturation": 1.0,
}


@contextlib.contextmanager
def _dask_cluster(config: Mapping[str, object] = ONE_TASK_PER_WORKER) -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    dask = pytest.importorskip("dask")
    with (
        dask.config.set(config),
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        yield client


@pytest.fixture(scope="module")
def dask_client() -> Iterator[Any]:
    with _dask_cluster() as client:
        yield client


@pytest.fixture
def fresh_dask_client() -> Iterator[Any]:
    """Yield a cluster no earlier worker death has touched."""
    with _dask_cluster() as client:
        yield client


@pytest.fixture(scope="module")
def htex(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    if sys.platform == "win32":
        pytest.skip("parsl HTEX requires POSIX")
    pytest.importorskip("parsl")
    from graphed_executors.parsl_backend import start_htex, stop_htex  # noqa: PLC0415

    here = str(Path(__file__).parent)
    with pytest.MonkeyPatch.context() as env:
        env.setenv("PYTHONPATH", os.pathsep.join([here, os.environ.get("PYTHONPATH", "")]))
        executor = start_htex(
            workers=2, run_dir=str(tmp_path_factory.mktemp("htex")), heartbeat_period=2
        )
        yield executor
        stop_htex(executor)


def _dask_leg(client: Any) -> Run:
    from graphed_executors.dask_backend import dask_runner  # noqa: PLC0415

    def run(plan: Plan[int], monitor: Any = None, **kw: Any) -> ExecResult[int]:
        with dask_runner(client, retries=0, monitor=monitor) as runner:
            return runner.run(plan, **kw)

    return run


def _parsl_leg(executor: Any) -> Run:
    from graphed_executors.parsl_backend import parsl_runner  # noqa: PLC0415

    def run(plan: Plan[int], monitor: Any = None, **kw: Any) -> ExecResult[int]:
        with parsl_runner(executor, retries=0, monitor=monitor) as runner:
            return runner.run(plan, **kw)

    return run


@pytest.fixture(params=[*LEGS, "dask", "parsl"])
def run(request: pytest.FixtureRequest) -> Run:
    if request.param == "dask":
        return _dask_leg(request.getfixturevalue("dask_client"))
    if request.param == "parsl":
        return _parsl_leg(request.getfixturevalue("htex"))
    return LEGS[request.param]


@pytest.fixture(params=[*PROCESS_LEGS, "parsl"])
def run_with_workers(request: pytest.FixtureRequest) -> Run:
    if request.param == "parsl":
        return _parsl_leg(request.getfixturevalue("htex"))
    return PROCESS_LEGS[request.param]


def test_a_bad_chunk_is_left_out_on_every_runner(run: Run) -> None:
    with pytest.warns(UserWarning, match=r"1 failed chunk\(s\)"):
        res = run(plan_of(7), skip_failed=10)
    assert res.value == without(7)
    (failed,) = res.failed
    assert failed.partition == "f.root:Events (entries 7-8)"
    assert failed.error == "OSError: bad chunk 7"
    assert json.loads(json.dumps(dataclasses.asdict(failed)))["record"]["entry_start"] == 7


class Phases(MonitorBase):
    def __init__(self) -> None:
        self.events: list[TaskEvent] = []

    def on_task(self, event: TaskEvent) -> None:
        self.events.append(event)


def test_a_left_out_chunk_is_an_errored_task_on_every_runner(run: Run) -> None:
    monitor = Phases()
    run(plan_of(3), skip_failed=1, monitor=monitor)
    ends = {e.key: (e.phase, e.error) for e in monitor.events if e.phase.name in END}
    assert ends == {
        i: (TaskPhase.ERRORED, "OSError: bad chunk 3") if i == 3 else (TaskPhase.FINISHED, None)
        for i in range(N)
    }


def _condor() -> Any:
    from graphed_executors.htcondor_backend import (  # noqa: PLC0415
        HTCondorBackend,
        HTCondorRunner,
        LocalPilots,
    )

    return HTCondorRunner(HTCondorBackend(LocalPilots(pythonpath=[str(Path(__file__).parent)]), 2))


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(functools.partial(cls, workers=2, retries=0), id=cls.__name__)
        for cls in (ThreadExecutor, ProcessPoolExecutor, PinnedPoolExecutor)
    ]
    + [pytest.param(_condor, id="HTCondorRunner")],
)
def test_the_missing_chunk_warning_names_the_callers_line(make: Callable[[], Any]) -> None:
    with make() as runner, pytest.warns(UserWarning, match="failed chunk") as w:
        runner.run(plan_of(3), skip_failed=1)
    assert [x.filename for x in w if "failed chunk" in str(x.message)] == [__file__]


def test_a_budget_exhausted_raises_on_every_runner(run: Run) -> None:
    with pytest.raises(TooManyFailedChunksError) as info:
        run(plan_of(*range(11)), skip_failed=10)
    assert len(info.value.failed) > 10


def test_without_a_budget_the_first_error_raises(run: Run) -> None:
    with pytest.raises(OSError, match="bad chunk"):
        run(plan_of(7))


def test_the_failed_set_fixes_the_answer(run: Run) -> None:
    bad = (0, 1, 2, 9, 10, 19)
    with pytest.warns(UserWarning, match="6 failed"):
        res = run(plan_of(*bad), skip_failed=len(bad))
    with pytest.warns(UserWarning, match="6 failed"):
        reference = _sequential(plan_of(*bad), skip_failed=len(bad))
    assert (res.value, res.failed) == (reference.value, reference.failed)


def test_a_lost_worker_is_a_failed_chunk(run_with_workers: Run) -> None:
    with pytest.warns(UserWarning, match=r"1 failed chunk\(s\)"):
        res = run_with_workers(plan_of(poison=(5,)), skip_failed=3)
    assert res.value == without(5)
    (failed,) = res.failed
    assert failed.partition == "f.root:Events (entries 5-6)"
    assert failed.error.startswith("WorkerLostError")


def test_lost_workers_count_against_the_budget(run_with_workers: Run) -> None:
    with pytest.raises(TooManyFailedChunksError) as info:
        run_with_workers(plan_of(poison=(3, 5)), skip_failed=1)
    assert len(info.value.failed) == 2


def test_dask_lists_every_chunk_it_leaves_out(fresh_dask_client: Any) -> None:
    plan = plan_of(poison=(5,))
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            res = _dask_leg(fresh_dask_client)(plan, skip_failed=N)
    except WorkerLostError as err:
        chunks = {chunk_label(t.partition) for t in plan.tasks}
        # a leaf key here is a chunk the engine failed to label, not a non-chunk blame
        blamed_no_chunk = err.partition not in chunks and "-leaf-" not in err.partition
        assert blamed_no_chunk, "only a loss blamed on no chunk may end the run"
        pytest.skip("dask blamed a combine, which names no chunk to leave out")
    assert any("failed chunk" in str(w.message) for w in caught)
    left_out = [f.record["entry_start"] for f in res.failed]
    assert 5 in left_out
    assert res.value == without(*left_out)


class _LostError(Exception):
    pass


def _raise_lost(key: str) -> None:
    raise _LostError(key)


class _LosesAWorkerOnce(ThreadBackend):
    """Lose ``chunk``'s first leaf's worker, blamed on ``blame`` or that leaf.

    Each leaf's error is kept, as a dask client keeps task state; a combine here fails only
    through a leaf, and its worker frame holds upstream futures that dask's would not.
    """

    def __init__(self, chunk: int | None, blame: str | None = None) -> None:
        super().__init__(2)
        self.chunk = chunk
        self.blame = blame
        self.lost = False
        self.futures: list[weakref.ref[Any]] = []
        self.errors: list[BaseException | None] = []

    def submit(self, fn: Callable[..., object], /, *args: object, key: str, **kw: Any) -> Any:
        leaf = any(isinstance(a, Task) for a in args)
        if not self.lost and any(
            isinstance(a, Task) and a.partition.entry_start == self.chunk for a in args
        ):
            self.lost = True
            fn, args = functools.partial(_raise_lost, key), ()
        fut = super().submit(fn, *args, key=key, **kw)
        self.futures.append(weakref.ref(fut))
        if leaf:
            fut.add_done_callback(
                lambda f: self.errors.append(None if f.cancelled() else f.exception())
            )
        return fut

    def _invoke(self, fn: Callable[..., object], args: tuple[object, ...]) -> object:
        # a dependent fails with a copy, as dask's never re-raise the client's error object
        deps = [a for a in args if isinstance(a, concurrent.futures.Future)]
        concurrent.futures.wait(deps)
        for dep in deps:
            if not dep.cancelled() and (err := dep.exception()) is not None:
                raise copy.copy(err)
        return super()._invoke(fn, args)

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        return (self.blame or str(exc.args[0]), "w") if isinstance(exc, _LostError) else None


def test_a_rerun_lets_go_of_the_lost_attempts_futures() -> None:
    backend = _LosesAWorkerOnce(5)
    with SubmitRunner(backend, retries=0) as runner:
        res = runner.run(plan_of(), skip_failed=N)
    gc.collect()
    assert [f.record["entry_start"] for f in res.failed] == [5]
    assert [w for w in backend.futures if w() is not None] == []


def test_a_failed_run_lets_go_of_its_futures() -> None:
    backend = _LosesAWorkerOnce(None)
    with SubmitRunner(backend, retries=0) as runner, pytest.raises(OSError, match="bad chunk"):
        runner.run(plan_of(3))
    gc.collect()
    assert any(backend.errors)
    assert [w for w in backend.futures if w() is not None] == []


def test_a_failed_run_keeps_the_users_locals() -> None:
    with (
        SubmitRunner(ThreadBackend(2), retries=0) as runner,
        pytest.raises(OSError, match="bad chunk") as caught,
    ):
        runner.run(plan_of(3))
    mine = [
        f
        for f, _ in traceback.walk_tb(caught.value.__traceback__)
        if f.f_globals["__name__"] == __name__
    ]
    assert [f.f_locals["part"].entry_start for f in mine if "part" in f.f_locals] == [3]


@pytest.mark.parametrize("key", ["graphed-fp-nonce-combine-3", "graphed-bcast-fp-nonce"])
def test_a_loss_blamed_on_no_chunk_ends_the_run(key: str) -> None:
    with (
        SubmitRunner(_LosesAWorkerOnce(0, blame=key), retries=0) as runner,
        pytest.raises(WorkerLostError) as caught,
    ):
        runner.run(plan_of(), skip_failed=N)
    assert caught.value.partition == key


def _stream(seen: list[int]) -> Callable[[ExecContext], list[Task] | None]:
    def next_tasks(ctx: ExecContext) -> list[Task] | None:
        seen.append(ctx.errors)
        n = ctx.n_done
        return [Task(n, Partition("f.root", "Events", n, n + 1))] if n < N else None

    return next_tasks


@pytest.mark.parametrize("run", [LEGS["thread"], _thread_backend], ids=["local", "submit"])
def test_a_pull_stream_sees_the_errors_and_stops_at_the_budget(run: Run) -> None:
    seen: list[int] = []
    plan: Plan[int] = dataclasses.replace(plan_of(*range(N)), tasks=(), next_tasks=_stream(seen))
    with pytest.raises(TooManyFailedChunksError):
        run(plan, skip_failed=2)
    assert max(seen) >= 1
    assert len(seen) < N


def test_a_pull_stream_under_budget_finishes() -> None:
    seen: list[int] = []
    plan: Plan[int] = dataclasses.replace(plan_of(3), tasks=(), next_tasks=_stream(seen))
    with pytest.warns(UserWarning, match="1 failed"):
        res = LEGS["thread"](plan, skip_failed=2)
    assert res.value == without(3)
    assert res.stopped is StopReason.EXHAUSTED


def _describe_lost(exc: BaseException) -> tuple[str, str] | None:
    return (str(exc.args[0]), "w") if isinstance(exc, _LostError) else None


class _CoLost(ThreadBackend):
    """Lose each leaf of ``lost`` on its first submit, as dask fails all a dead worker held."""

    describe_failure = staticmethod(_describe_lost)

    def __init__(self, lost: frozenset[int]) -> None:
        super().__init__(2)
        self.lost = set(lost)
        self.leaves = 0

    def submit(self, fn: Callable[..., object], /, *args: object, key: str, **kw: Any) -> Any:
        task = next((a for a in args if isinstance(a, Task)), None)
        if task is not None:
            self.leaves += 1
            if task.partition.entry_start in self.lost:
                self.lost.discard(task.partition.entry_start)
                fut: concurrent.futures.Future[object] = concurrent.futures.Future()
                fut.set_exception(_LostError(key))
                return fut
        return super().submit(fn, *args, key=key, **kw)


def test_without_a_budget_the_first_lost_leaf_raises_with_no_rerun() -> None:
    backend = _CoLost(frozenset({1, 2, 5}))
    with SubmitRunner(backend, retries=0) as runner, pytest.raises(WorkerLostError) as caught:
        runner.run(plan_of())
    assert caught.value.partition == "f.root:Events (entries 1-2)"
    assert backend.leaves == N


def _charged(
    lost: list[int], poison: set[int], blame: str | None = None
) -> tuple[list[int], list[tuple[int, ...]]]:
    plan = skipping(plan_of(), 3)
    assert isinstance(plan, Plan)
    tasks = {t.partition.entry_start: t for t in plan.tasks}
    err = WorkerLostError(partition=blame or chunk_label(tasks[lost[0]].partition), worker="w")
    runs: list[tuple[int, ...]] = []

    def lose_together(plan: Plan[Any], group: tuple[Task, ...]) -> bool:
        runs.append(tuple(t.partition.entry_start for t in group))
        return any(t.partition.entry_start in poison for t in group)

    rerun = SubmitRunner._without_culprits(plan, err, [tasks[e] for e in lost], lose_together)
    with pytest.warns(UserWarning, match="failed chunk"):
        res = SequentialRunner().run(rerun)
    assert res.value == without(*(f.record["entry_start"] for f in res.failed))
    return [f.record["entry_start"] for f in res.failed], runs


BCAST = "graphed-bcast-fp-nonce"


@pytest.mark.parametrize(
    ("lost", "poison", "blame", "charged", "runs"),
    [
        ([1, 2, 5, 7], {5}, None, [5], [(1, 2), (5, 7), (5,), (7,)]),
        ([5, 1, 2], {5}, None, [5], [(1,), (2, 5), (2,), (5,)]),
        ([1, 2, 5, 7], {2, 7}, None, [2, 7], [(1, 2), (1,), (2,), (5, 7), (5,), (7,)]),
        ([1, 2, 7], set(), None, [1], [(1,), (2, 7)]),
        ([3], {3}, None, [3], []),
        ([3], {3}, BCAST, [3], [(3,)]),
    ],
    ids=[
        "culprit-in-a-half",
        "culprit-first",
        "two-culprits",
        "no-culprit-charges-the-blamed",
        "the-blamed-alone",
        "a-lone-suspect-of-a-broadcast",
    ],
)
def test_of_the_leaves_lost_together_only_those_that_lose_a_worker_alone_are_charged(
    lost: list[int],
    poison: set[int],
    blame: str | None,
    charged: list[int],
    runs: list[tuple[int, ...]],
) -> None:
    assert _charged(lost, poison, blame) == (charged, runs)


def test_a_lone_suspect_of_a_broadcast_blame_that_runs_alone_ends_the_run() -> None:
    with pytest.raises(WorkerLostError) as caught:
        _charged([3], set(), blame=BCAST)
    assert caught.value.partition == BCAST


def test_dask_charges_only_the_poison_chunk_of_those_its_dead_workers_held() -> None:
    # dask's default saturation: each worker holds more than the leaf it runs
    crowded = {"distributed.scheduler.allowed-failures": 1}
    with _dask_cluster(crowded) as client, pytest.warns(UserWarning, match="1 failed"):
        res = _dask_leg(client)(plan_of(poison=(2,)), skip_failed=1)
    assert [f.record["entry_start"] for f in res.failed] == [2]
    assert res.value == without(2)


class _LateFuture(concurrent.futures.Future[object]):
    """Done at once, its callbacks held, as dask runs them later on a thread of its own."""

    def __init__(self, key: str) -> None:
        super().__init__()
        self.set_exception(_LostError(key))
        self.held: list[Callable[[concurrent.futures.Future[object]], object]] = []

    def add_done_callback(self, fn: Callable[[concurrent.futures.Future[object]], object]) -> None:
        self.held.append(fn)


class _LateCallbacks(ThreadBackend):
    describe_failure = staticmethod(_describe_lost)

    def submit(self, fn: Callable[..., object], /, *args: object, key: str, **kw: Any) -> Any:
        return _LateFuture(key)


def test_a_leaf_lost_before_its_callback_runs_is_counted_once_settled() -> None:
    lost: list[Task] = []
    submits = _RunTasks(_LateCallbacks(1), lost)
    task = plan_of().tasks[3]
    fut = submits.submit(print, task, key="k", retries=0, task=task)
    assert isinstance(fut, _LateFuture)
    assert lost == []
    submits.settle()
    assert lost == [task]
    for fn in fut.held:  # the late callback counts nothing twice
        fn(fut)
    assert lost == [task]


def test_a_probe_leaves_a_cancel_for_the_rerun() -> None:
    control = RunControl()
    with SubmitRunner(ThreadBackend(2), retries=0, control=control) as runner:
        control.cancel()
        plan = skipping(plan_of(), 3)
        assert isinstance(plan, Plan)
        assert runner._lose_together(None, plan, tuple(plan.tasks[:2])) is False
    assert control.state is RunState.CANCELLED


class _FirstCallbackOnly(ThreadBackend):
    """Lose each leaf of ``lost``; only the first one's callbacks run before the run raises."""

    describe_failure = staticmethod(_describe_lost)

    def __init__(self, lost: frozenset[int]) -> None:
        super().__init__(2)
        self.lost, self.first = lost, True

    def submit(self, fn: Callable[..., object], /, *args: object, key: str, **kw: Any) -> Any:
        task = next((a for a in args if isinstance(a, Task)), None)
        if task is None or task.partition.entry_start not in self.lost:
            return super().submit(fn, *args, key=key, **kw)
        if not self.first:
            return _LateFuture(key)
        self.first = False
        fut: concurrent.futures.Future[object] = concurrent.futures.Future()
        fut.set_exception(_LostError(key))
        return fut


def test_a_failed_run_counts_the_leaves_lost_with_it_whose_callbacks_have_not_run() -> None:
    plan = skipping(plan_of(), 3)
    lost: list[Task] = []
    backend = _FirstCallbackOnly(frozenset({1, 2, 5}))
    with (
        SubmitRunner(backend, retries=0) as runner,
        pytest.raises(WorkerLostError),
        contextlib.ExitStack() as scope,
    ):
        runner._run_scoped(plan, None, None, None, runner._context("n", None), scope, lost)
    assert sorted(t.partition.entry_start for t in lost) == [1, 2, 5]
