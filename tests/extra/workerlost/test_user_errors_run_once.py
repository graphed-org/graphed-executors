"""A user exception runs once unless asked; a lost worker is what ``retries`` is for."""

from __future__ import annotations

import dataclasses
import os
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from graphed.core import DurablePlanV2, OpSpec, Partition, Plan, StageSpec, Task

from graphed_executors.common import RetriedProcess
from graphed_executors.submit import SubmitRunner, ThreadBackend

HERE = str(Path(__file__).parent)


class Flaky:
    def __init__(self, marks: str, failures: int) -> None:
        self.marks = marks
        self.failures = failures

    def __call__(self, part: Partition, res: object) -> int:
        seen = len(os.listdir(self.marks))
        Path(self.marks, f"{seen}-{uuid.uuid4().hex}").touch()
        if seen < self.failures:
            raise ValueError("crazy muon pt")
        return 1


class LostOnce:
    def __init__(self, marks: str) -> None:
        self.marks = marks

    def __call__(self, part: Partition, res: object) -> int:
        mark = Path(self.marks, "died")
        if not mark.exists():
            mark.touch()
            os._exit(17)
        return 1


def add(a: int, b: int) -> int:
    return a + b


def zero() -> int:
    return 0


def plan_of(process: Any) -> Plan[int]:
    return Plan(
        process=process,
        combine=add,
        empty=zero,
        tasks=(Task(0, Partition("f.root", "Events", 0, 1)),),
    )


Make = Callable[..., Any]


@pytest.fixture(scope="module")
def dask_client() -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    with (
        distributed.LocalCluster(
            n_workers=1, threads_per_worker=1, processes=True, dashboard_address=":0"
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

    with pytest.MonkeyPatch.context() as env:
        env.setenv("PYTHONPATH", os.pathsep.join([HERE, os.environ.get("PYTHONPATH", "")]))
        executor = start_htex(
            workers=1, run_dir=str(tmp_path_factory.mktemp("htex")), heartbeat_period=2
        )
        yield executor
        stop_htex(executor)


@pytest.fixture(params=["dask", "parsl", "submit"])
def make(request: pytest.FixtureRequest) -> Make:
    if request.param == "dask":
        from graphed_executors.dask_backend import dask_runner  # noqa: PLC0415

        client = request.getfixturevalue("dask_client")
        return lambda **kw: dask_runner(client, **kw)
    if request.param == "parsl":
        from graphed_executors.parsl_backend import parsl_runner  # noqa: PLC0415

        executor = request.getfixturevalue("htex")
        return lambda **kw: parsl_runner(executor, **kw)
    return lambda **kw: SubmitRunner(ThreadBackend(1), **kw)


def test_a_user_error_runs_once_by_default(make: Make, tmp_path: Path) -> None:
    with make() as runner, pytest.raises(ValueError, match="crazy muon pt"):
        runner.run(plan_of(Flaky(str(tmp_path), 99)))
    assert len(os.listdir(tmp_path)) == 1


def test_retry_user_errors_is_the_extra_tries(make: Make, tmp_path: Path) -> None:
    with make(retry_user_errors=2) as runner:
        assert runner.run(plan_of(Flaky(str(tmp_path), 2))).value == 1
    assert len(os.listdir(tmp_path)) == 3


def test_retries_run_out_with_the_user_error(make: Make, tmp_path: Path) -> None:
    with make(retry_user_errors=1) as runner, pytest.raises(ValueError, match="crazy muon pt"):
        runner.run(plan_of(Flaky(str(tmp_path), 99)))
    assert len(os.listdir(tmp_path)) == 2


def test_parsl_retries_a_lost_worker(htex: Any, tmp_path: Path) -> None:
    from graphed_executors.parsl_backend import parsl_runner  # noqa: PLC0415

    with parsl_runner(htex, retries=1) as runner:
        assert runner.run(plan_of(LostOnce(str(tmp_path)))).value == 1
    assert Path(tmp_path, "died").exists()


def one(part: Partition, res: object) -> int:
    return 1


class LibraryRetries(ThreadBackend):
    capabilities = dataclasses.replace(ThreadBackend.capabilities, per_task_retries=True)

    def __init__(self) -> None:
        super().__init__(1)
        self.seen: list[int] = []

    def submit(self, fn: Any, /, *args: Any, key: str, retries: int = 0, **kw: Any) -> Any:
        self.seen.append(retries)
        return super().submit(fn, *args, key=key, retries=retries, **kw)


def test_a_library_retry_is_never_asked_for() -> None:
    backend = LibraryRetries()
    with SubmitRunner(backend, retries=3) as runner:
        runner.run(plan_of(one))
    assert set(backend.seen) == {0}


class Bound:
    def __init__(self, endpoints: Any = None) -> None:
        self.endpoints = endpoints

    def __call__(self, part: Partition, res: object) -> int:
        return 1

    def bind_services(self, endpoints: Any) -> Bound:
        return Bound(endpoints)

    def resolve_services(self, value: int) -> int:
        return value + 100


def test_services_bind_through_the_retry() -> None:
    bound = RetriedProcess(Bound(), 2).bind_services({"a": "tcp://h:1"})
    assert bound.process.endpoints == {"a": "tcp://h:1"}  # type: ignore[attr-defined]


def test_an_unbindable_process_binds_to_itself() -> None:
    retried = RetriedProcess(zero, 2)
    assert retried.bind_services({}) is retried


def test_services_resolve_through_the_retry() -> None:
    assert RetriedProcess(Bound(), 2).resolve_services(1) == 101
    assert RetriedProcess(zero, 2).resolve_services(1) == 1


def test_a_negative_retry_count_is_refused() -> None:
    with pytest.raises(ValueError, match="retry_user_errors must be >= 0"):
        SubmitRunner(ThreadBackend(1), retry_user_errors=-1)


def test_a_staged_plan_refuses_user_error_retries() -> None:
    stage = StageSpec(
        "map", process=OpSpec("ref", "t:map"), tasks=(Task(0, Partition("f", "t", 0, 1)),)
    )
    with (
        SubmitRunner(ThreadBackend(1), retry_user_errors=1) as runner,
        pytest.raises(TypeError, match="retry_user_errors needs a Plan"),
    ):
        runner.run(DurablePlanV2(ir=b"retry-v2", stages=(stage,)))
