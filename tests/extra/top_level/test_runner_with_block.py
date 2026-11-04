"""One ``with`` block runs on every runner, so a debugging swap rewrites one line (graphed#151)."""

from __future__ import annotations

import operator
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

import pytest
from graphed.core import Partition, Plan, SequentialRunner, Task

from graphed_executors import ProcessPoolExecutor, ThreadExecutor


def one(part: Partition, res: object) -> int:
    return 1


PLAN: Plan[int] = Plan(
    process=one,
    combine=operator.add,
    empty=int,
    tasks=tuple(Task(i, Partition("f", "t", i, i + 1)) for i in range(4)),
)
RUNNERS: dict[str, Callable[[], Any]] = {
    "sequential": SequentialRunner,
    "thread": lambda: ThreadExecutor(2),
    "process": lambda: ProcessPoolExecutor(2),
}


@pytest.mark.parametrize("make", RUNNERS.values(), ids=list(RUNNERS))
def test_the_same_block_runs_on_every_runner(make: Callable[[], Any]) -> None:
    with make() as runner:
        assert runner.run(PLAN).value == 4
        future = runner.submit(PLAN)
        assert type(future) is Future
        assert future.result().value == 4
    runner.close()
    assert runner.run(PLAN).value == 4
    runner.close()
