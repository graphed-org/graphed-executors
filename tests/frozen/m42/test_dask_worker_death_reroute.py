"""m42 reroute: a worker killed MID-RUN (below the allowed-failures budget) costs nothing — the
run completes bit-for-bit, the dead worker's task re-runs in a different process (plan §1.4
bullets 2-3; blueprint §3 `test_dask_worker_death_reroute`).

The poisoned leaf hard-kills its worker on the FIRST attempt (after atomically recording the pid),
then succeeds when rescheduled — a deterministic mid-run death, no external kill races. A twin
clean plan (same compute, no poison) provides the expected value, since running the poisoned plan
through SequentialRunner would kill the test process. Discriminates: a runner that deadlocks on
lost futures, and one whose result changes when a leaf is recomputed elsewhere."""

from __future__ import annotations

import pathlib

import pytest
from graphed.core.execution import Plan, StopReason, Task
from submit_backends import (
    DieOnceProcess,
    cluster_client,
    expected_concat,
    make_dask_runner,
    mem_partitions,
    prov_combine,
    prov_empty,
    prov_plan,
    worker_pids,
)

pytest.importorskip("distributed")


@pytest.fixture()
def client():
    # dedicated cluster, generous death budget: one death must reroute, never blame the task
    with cluster_client(2, processes=True, allowed_failures=10) as c:
        yield c


def test_mid_run_worker_death_reroutes_and_completes_bit_for_bit(client, tmp_path: pathlib.Path) -> None:
    n = 8
    parts = mem_partitions(n, "reroute")
    poison_uri = parts[3].uri
    marker = tmp_path / "died"
    plan = Plan(
        process=DieOnceProcess(str(marker), poison_uri),
        combine=prov_combine,
        empty=prov_empty,
        tasks=tuple(Task(i, p) for i, p in enumerate(parts)),
    )
    with make_dask_runner(client) as runner:
        result = runner.run(plan)
    prov = result.value

    # bit-for-bit complete despite the death (payload == the clean key-ordered concat)
    assert prov.payload == expected_concat(n, "reroute")
    assert len(prov.leaves) == n
    assert result.n_partitions == n and result.n_combines == n - 1
    assert result.stopped is StopReason.EXHAUSTED

    # the death REALLY happened (non-vacuity) and the leaf re-ran in a DIFFERENT process
    assert marker.exists(), "the poisoned leaf never killed its worker — scenario did not engage"
    first_attempt_pid = int(marker.read_text())
    (successful_pid,) = {pid for uri, pid, _ in prov.leaves if uri == poison_uri}
    assert successful_pid != first_attempt_pid, "the successful attempt reports the DEAD process"
    assert successful_pid in set(worker_pids(client).values())


def test_clean_twin_matches_sequential(client) -> None:
    """Control: the same compute without the poison equals the provenance run's payload — the
    reroute test's expected value is anchored, not self-referential."""
    with make_dask_runner(client) as runner:
        prov = runner.run(prov_plan(8, "reroute")).value
    assert prov.payload == expected_concat(8, "reroute")
