"""``skip_failed`` gives the same answer on every runner, a lost worker included."""

from __future__ import annotations

import dataclasses
import json
import os
import sys
import warnings
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from graphed import WorkerLostError
from graphed.core import (
    ExecContext,
    ExecResult,
    Partition,
    Plan,
    SequentialRunner,
    StopReason,
    Task,
    TooManyFailedChunksError,
)

from graphed_executors import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor
from graphed_executors.submit import SubmitRunner, ThreadBackend

N = 20


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


def _sequential(plan: Plan[int], **kw: Any) -> ExecResult[int]:
    return SequentialRunner().run(plan, **kw)


def _local(cls: Any, **options: Any) -> Run:
    def run(plan: Plan[int], **kw: Any) -> ExecResult[int]:
        with cls(workers=2, retries=0, **options) as runner:
            return runner.run(plan, **kw)  # type: ignore[no-any-return]

    return run


def _thread_backend(plan: Plan[int], **kw: Any) -> ExecResult[int]:
    with SubmitRunner(ThreadBackend(2), retries=0) as runner:
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


@pytest.fixture(scope="module")
def dask_client() -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    dask = pytest.importorskip("dask")
    with (
        dask.config.set(
            {
                "distributed.scheduler.allowed-failures": 0,
                "distributed.scheduler.worker-saturation": 1.0,
            }
        ),
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
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

    def run(plan: Plan[int], **kw: Any) -> ExecResult[int]:
        with dask_runner(client, retries=0) as runner:
            return runner.run(plan, **kw)

    return run


def _parsl_leg(executor: Any) -> Run:
    from graphed_executors.parsl_backend import parsl_runner  # noqa: PLC0415

    def run(plan: Plan[int], **kw: Any) -> ExecResult[int]:
        with parsl_runner(executor, retries=0) as runner:
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


def test_dask_lists_every_chunk_it_leaves_out(dask_client: Any) -> None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = _dask_leg(dask_client)(plan_of(poison=(5,)), skip_failed=N)
    except WorkerLostError as err:
        assert "combine" in err.partition, "only a loss blamed on no chunk may end the run"
        return
    left_out = [f.record["entry_start"] for f in res.failed]
    assert 5 in left_out
    assert res.value == without(*left_out)


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
