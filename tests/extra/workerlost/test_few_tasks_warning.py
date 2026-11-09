"""A plan that cannot use every worker says so, once per run."""

from __future__ import annotations

import dataclasses
import operator
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest
from graphed.core import ExecContext, Partition, Plan, Task

from graphed_executors import ProcessPoolExecutor, ThreadExecutor
from graphed_executors.submit import SubmitRunner, ThreadBackend

RUNNERS = [
    lambda: ThreadExecutor(workers=4),
    lambda: ProcessPoolExecutor(workers=4),
    lambda: SubmitRunner(ThreadBackend(4)),
]


def one(part: Partition, res: object) -> int:
    return 1


def plan_of(n: int) -> Plan[int]:
    tasks = tuple(Task(i, Partition("f", "t", i, i + 1)) for i in range(n))
    return Plan(process=one, combine=operator.add, empty=int, tasks=tasks)


@pytest.mark.parametrize("make", RUNNERS, ids=["thread", "process", "submit"])
def test_fewer_tasks_than_workers_warn_once(make: Any) -> None:
    with make() as runner, pytest.warns(UserWarning, match="1 task") as caught:
        runner.run(plan_of(1))
    few = [w for w in caught if "for 4 workers" in str(w.message)]
    assert len(few) == 1
    assert "pass events_per_task= or steps_per_file= to the plan" in str(few[0].message)


@pytest.mark.parametrize("make", RUNNERS, ids=["thread", "process", "submit"])
def test_enough_tasks_stay_quiet(make: Any, recwarn: pytest.WarningsRecorder) -> None:
    with make() as runner:
        runner.run(plan_of(4))
    assert not [w for w in recwarn if "workers;" in str(w.message)]


def test_a_pull_stream_stays_quiet(recwarn: pytest.WarningsRecorder) -> None:
    def next_tasks(ctx: ExecContext) -> None:
        return None

    plan = dataclasses.replace(plan_of(1), next_tasks=next_tasks)
    with ThreadExecutor(workers=4) as runner:
        runner.run(plan)
    assert not [w for w in recwarn if "workers;" in str(w.message)]


def test_a_pull_stream_never_asks_the_backend_for_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    batches = [plan_of(1).tasks]

    def next_tasks(ctx: ExecContext) -> Sequence[Task] | None:
        return batches.pop() if batches else None

    def no_slots(self: SubmitRunner) -> int:
        raise AssertionError("asked for the slot count")

    monkeypatch.setattr(SubmitRunner, "_task_slots", no_slots)
    plan = dataclasses.replace(plan_of(1), next_tasks=next_tasks)
    with SubmitRunner(ThreadBackend(4)) as runner:
        assert runner.run(plan).n_partitions == 1


def _warned_once(run: Any) -> None:
    with pytest.warns(UserWarning, match="1 task") as caught:
        run(plan_of(1))
    assert len([w for w in caught if "for 2 workers" in str(w.message)]) == 1


@pytest.fixture(scope="module")
def dask_client() -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=False, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        yield client


def test_the_dask_peer_reduction_warns(dask_client: Any) -> None:
    from graphed_executors.dask_backend import DaskBackend  # noqa: PLC0415
    from graphed_executors.dask_backend.transport import GraphedTransportPlugin  # noqa: PLC0415
    from graphed_executors.dask_backend.transport_peer import transport_run_plan  # noqa: PLC0415

    dask_client.register_plugin(GraphedTransportPlugin())
    _warned_once(lambda plan: transport_run_plan(plan, DaskBackend(dask_client)))


def test_the_parsl_peer_reduction_warns() -> None:
    pytest.importorskip("parsl")
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m47"))
    from parsl_transport_harness import p2p_tpe_pool  # noqa: PLC0415

    from graphed_executors.parsl_backend import ParslBackend  # noqa: PLC0415
    from graphed_executors.parsl_backend.transport_peer import parsl_run_plan  # noqa: PLC0415

    with p2p_tpe_pool(max_threads=2) as tpe:
        _warned_once(lambda plan: parsl_run_plan(plan, ParslBackend(tpe), workers=2))
