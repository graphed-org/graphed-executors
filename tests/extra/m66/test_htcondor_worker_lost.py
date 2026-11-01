"""A pilot lost twice is ``graphed.WorkerLostError``; the server's name keeps working."""

from __future__ import annotations

import pickle
import socket
import sys
from pathlib import Path
from typing import Any

import graphed
import pytest
from graphed.core.execution import Plan, Task, chunk_label

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m66"))
from htcondor_harness import (
    PoisonUriProcess,
    concat,
    empty_text,
    htcondor_api,
    local_backend,
    make_runner,
    mem_partitions,
    run_bounded,
    server_api,
)


def test_a_twice_lost_leaf_names_its_chunk_and_pilot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server_api(), "LEASE_S", 2.0)
    parts = mem_partitions(4, "wl")
    plan = Plan(
        process=PoisonUriProcess(parts[1].uri),
        combine=concat,
        empty=empty_text,
        tasks=tuple(Task(i, p) for i, p in enumerate(parts)),
    )
    with make_runner(local_backend(3)) as runner, pytest.raises(graphed.WorkerLostError) as info:
        run_bounded(lambda: runner.run(plan))
    err = info.value
    assert err.partition == chunk_label(parts[1])
    assert err.worker.startswith(f"{socket.gethostname()}:")
    assert err.frames == (), "the source-line slot never holds a data file"


def test_the_server_error_is_the_shared_one() -> None:
    lost = htcondor_api().WorkerLostError("graphed-abc-leaf-3", "host.example:4242")
    assert isinstance(lost, graphed.WorkerLostError)
    again: Any = pickle.loads(pickle.dumps(lost))
    named = ("graphed-abc-leaf-3", "host.example:4242")
    assert (again.key, again.pilot) == named
    assert (again.partition, again.worker) == named
