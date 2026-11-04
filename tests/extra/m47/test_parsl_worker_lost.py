"""A parsl worker death is the shared ``graphed.WorkerLostError``."""

from __future__ import annotations

import os
from concurrent.futures import Future
from pathlib import Path

import graphed
import pytest
from graphed.core import Partition, Plan, Task, chunk_label
from graphed.debug import StageError

from graphed_executors.parsl_backend import parsl_runner, start_htex, stop_htex
from graphed_executors.parsl_backend.backend import ParslBackend, _ParslFuture
from graphed_executors.parsl_backend.transport_shuffle import _shuffle_stage_error

pytest.importorskip("parsl")


def die(part: Partition, res: object) -> int:
    os._exit(17)


def add(a: int, b: int) -> int:
    return a + b


def zero() -> int:
    return 0


def test_a_lost_worker_on_a_combine_input_names_its_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    here = str(Path(__file__).parent)  # workers import ``die`` by reference
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([here, os.environ.get("PYTHONPATH", "")]))
    tasks = tuple(Task(i, Partition("data.root", "Events", i, i + 1)) for i in range(3))
    plan = Plan(process=die, combine=add, empty=zero, tasks=tasks)
    htex = start_htex(workers=2, run_dir=str(tmp_path), heartbeat_period=2)
    try:
        with (
            parsl_runner(htex, retries=0) as runner,
            pytest.raises(graphed.WorkerLostError) as info,
        ):
            runner.run(plan)
    finally:
        stop_htex(htex)
    err = info.value
    assert err.partition in {chunk_label(t.partition) for t in tasks}
    assert " on host " in err.worker and "loss of" not in err.worker
    assert err.frames == (), "the source-line slot never holds a data file"


class WorkerLost(Exception):  # noqa: N818  (parsl's name is what is matched)
    def __str__(self) -> str:
        return "Task failure due to loss of worker 1 on host h"


class ManagerLost(Exception):  # noqa: N818  (parsl's name is what is matched)
    def __str__(self) -> str:
        return "Task failure due to loss of manager abc on host h"


def test_a_lost_manager_names_the_manager_not_parsls_sentence() -> None:
    backend = object.__new__(ParslBackend)
    assert backend.describe_failure(ManagerLost()) == (str(ManagerLost()), "manager abc on host h")


def test_a_future_tags_its_exception_with_the_task_key() -> None:
    raw: Future[object] = Future()
    raw.set_exception(WorkerLost())
    with pytest.raises(WorkerLost) as info:
        _ParslFuture(raw, {}, "graphed-abc-leaf-3").result()
    backend = object.__new__(ParslBackend)
    assert backend.describe_failure(info.value) == ("graphed-abc-leaf-3", "1 on host h")


def test_a_shuffle_death_is_a_worker_lost_error_that_keeps_the_signal() -> None:
    backend = object.__new__(ParslBackend)
    err = _shuffle_stage_error(WorkerLost(), backend)
    assert isinstance(err, graphed.WorkerLostError)
    assert "WorkerLost" in err.worker and "1 on host h" in err.worker
    assert type(_shuffle_stage_error(ValueError("x"), backend)) is StageError
