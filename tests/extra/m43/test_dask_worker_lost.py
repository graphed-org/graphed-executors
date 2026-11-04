"""A dask worker death is the shared ``graphed.WorkerLostError``."""

from __future__ import annotations

import pickle
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import graphed
import pytest
from graphed.core import Plan, Task, chunk_label

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m42"))
from submit_backends import (
    cluster_client,
    concat,
    empty_text,
    exit_process,
    make_dask_runner,
    mem_partitions,
)

pytest.importorskip("distributed")


@pytest.fixture()
def client() -> Iterator[Any]:
    with cluster_client(2, processes=True, allowed_failures=0) as c:
        yield c


def test_a_killed_worker_is_a_worker_lost_error(client: Any) -> None:
    parts = mem_partitions(2, "wl")
    plan = Plan(
        process=exit_process,
        combine=concat,
        empty=empty_text,
        tasks=tuple(Task(i, p) for i, p in enumerate(parts)),
    )
    with (
        make_dask_runner(client, retries=0) as runner,
        pytest.raises(graphed.WorkerLostError) as info,
    ):
        runner.run(plan)
    err = info.value
    assert err.partition in {chunk_label(p) for p in parts}
    assert err.worker.startswith("tcp://")
    assert err.frames == (), "the source-line slot never holds a data file"
    assert pickle.loads(pickle.dumps(err)).worker == err.worker
