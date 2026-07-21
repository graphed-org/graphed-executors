"""m43 carried follow-up F2b (m42 review, wf_20cb8bd3-42f APPROVE): a worker death on an ADAPTIVE
leaf must surface as a `StageError` naming the PARTITION, not the raw dask key.

`_run_adaptive` currently resolves futures via `self._result(fut, {})` (engine.py:352) — an EMPTY
key→Task map — so the KilledWorker translation (which the fixed path performs correctly, frozen by
m42 `test_dask_killed_worker`) degrades: `err.partition` falls back to `str(failing_key)`, i.e.
the opaque `graphed-…-leaf-0` scheduler key instead of the user's `mem://…` chunk. This test is
the adaptive twin of the m42 killed-worker pin and FAILS on current HEAD at the partition-label
assertion (a behavioral right-reason failure — the StageError itself already arrives).

The m42 blame discipline is copied verbatim: a dedicated cluster, `allowed-failures: 1` (not the
quirk-prone 0 — gh#6078), retries=0, and a SINGLE-task batch so the scheduler's blame is
deterministic (no co-located task to blame unfairly)."""

from __future__ import annotations

import pytest
from dask_shuffle_backends import (
    AlwaysDieProcess,
    BatchFeed,
    add_ints,
    build_dask_backend,
    f2_partitions,
    install_worker_plugin,
    int_zero,
    make_submit_runner,
    shuffle_cluster,
)
from graphed.core.execution import Plan, Task
from graphed.debug import StageError

distributed = pytest.importorskip("distributed")


@pytest.fixture()
def client():
    # dedicated per-test cluster: worker deaths must not bleed into other modules' fixtures
    with shuffle_cluster(2, processes=True, allowed_failures=1) as c:
        yield c


def test_adaptive_worker_death_names_the_partition_not_the_dask_key(client) -> None:  # type: ignore[no-untyped-def]
    (partition,) = f2_partitions(1, "f2b")
    plan = Plan(
        process=AlwaysDieProcess(partition.uri),
        combine=add_ints,
        empty=int_zero,
        next_tasks=BatchFeed([[Task(0, partition)]]),  # single task: deterministic blame
    )
    install_worker_plugin(client)
    backend = build_dask_backend(client)
    with make_submit_runner(backend, retries=0) as runner, pytest.raises(StageError) as excinfo:
        runner.run(plan)
    err = excinfo.value

    assert not isinstance(err, distributed.KilledWorker)  # translated, not leaked
    assert type(err) is StageError
    # THE F2b pin — on current HEAD err.partition is the raw "graphed-…-leaf-0" scheduler key
    assert partition.uri in err.partition, (
        f"the adaptive path must attribute a worker death to the user's chunk; got partition "
        f"{err.partition!r} — the raw dask key (empty key_to_task at engine.py _run_adaptive)"
    )
    assert "graphed-" not in err.partition, "the scheduler key must not leak as the partition label"
    text = f"{err.cause_type} {err.cause_message} {err}"
    assert "KilledWorker" in text  # the cause is named for the user (fixed-path parity)
    assert "tcp://" in text  # ...and so is the last worker that held the task
