"""m42 failure attribution: a task whose worker DIES (os._exit — segfault/OOM/preemption stand-in)
surfaces as an attributed `StageError` naming the partition and the last worker, never as a raw
`distributed.KilledWorker` (plan §1.4 bullet 2; blueprint §3 `test_dask_killed_worker`).

`distributed.scheduler.allowed-failures` is set explicitly (1, not the quirk-prone 0 — gh#6078) on
a dedicated cluster via dask.config; the single-task plan makes the scheduler's blame deterministic
(no co-located task to blame unfairly). Discriminates: letting KilledWorker escape raw, and an
attribution that loses the partition or the worker identity."""

from __future__ import annotations

import pytest
from graphed.core.execution import Plan, Task
from graphed.debug import StageError
from submit_backends import (
    cluster_client,
    concat,
    empty_text,
    exit_process,
    make_dask_runner,
    mem_partitions,
)

distributed = pytest.importorskip("distributed")


@pytest.fixture()
def client():
    # dedicated per-test cluster: worker deaths must not bleed into other modules' fixtures
    with cluster_client(2, processes=True, allowed_failures=1) as c:
        yield c


def test_killed_worker_becomes_an_attributed_stage_error(client) -> None:
    (partition,) = mem_partitions(1, "kw")
    # combine/empty are module-level picklables; the single leaf never completes so neither runs
    plan = Plan(process=exit_process, combine=concat, empty=empty_text, tasks=(Task(0, partition),))
    with make_dask_runner(client, retries=0) as runner, pytest.raises(StageError) as excinfo:
        runner.run(plan)
    err = excinfo.value

    assert not isinstance(err, distributed.KilledWorker)  # translated, not leaked
    assert type(err) is StageError
    assert partition.uri in err.partition  # names the user's chunk, not a scheduler key
    text = f"{err.cause_type} {err.cause_message} {err}"
    assert "KilledWorker" in text  # the cause is named for the user
    assert "tcp://" in text  # ...and so is the last worker that held the task
