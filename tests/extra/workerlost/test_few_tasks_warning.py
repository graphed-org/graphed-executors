"""A plan that cannot use every worker says so, once per run."""

from __future__ import annotations

import dataclasses
import operator
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
