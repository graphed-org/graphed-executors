"""Both executors run a Plan to the same reduced result (plan M7)."""

from __future__ import annotations

import analyses
import pytest
from graphed.core import Partition, Plan, StopReason, Task

from graphed_exec_local import ProcessExecutor, ThreadExecutor

EXECUTORS = [ThreadExecutor, ProcessExecutor]


def _count_plan(n: int) -> Plan[int]:
    tasks = [Task(i, Partition("f", "E", i * 100, (i + 1) * 100)) for i in range(n)]
    return Plan(
        process=analyses.count_entries,
        combine=analyses.add_int,
        empty=analyses.zero_int,
        tasks=tasks,
    )


@pytest.mark.parametrize("executor_cls", EXECUTORS)
def test_reduces_to_the_correct_total(executor_cls: type) -> None:
    r = executor_cls(max_workers=4).run(_count_plan(50))
    assert r.value == 5000  # 50 partitions * 100 entries
    assert r.n_partitions == 50
    assert r.n_combines == 49  # n-1 combines
    assert r.stopped is StopReason.EXHAUSTED


@pytest.mark.parametrize("executor_cls", EXECUTORS)
def test_empty_plan_returns_identity(executor_cls: type) -> None:
    r = executor_cls().run(
        Plan(
            process=analyses.count_entries,
            combine=analyses.add_int,
            empty=analyses.zero_int,
            tasks=[],
        )
    )
    assert r.value == 0 and r.n_partitions == 0 and r.n_combines == 0


@pytest.mark.parametrize("executor_cls", EXECUTORS)
def test_single_partition(executor_cls: type) -> None:
    r = executor_cls().run(_count_plan(1))
    assert r.value == 100 and r.n_combines == 0


def test_thread_and_process_agree() -> None:
    plan = _count_plan(37)
    assert (
        ThreadExecutor(max_workers=4).run(plan).value
        == ProcessExecutor(max_workers=4).run(plan).value
    )
