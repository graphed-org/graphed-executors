"""m42 coffea#1490 guard: on a fixed-size 2-worker cluster, the leaves of one run spread across
BOTH workers (plan §1.2.2 mitigation 1; blueprint §3 `test_dask_multi_worker_spread`).

The open upstream suspicion (scikit-hep/coffea#1490): the broadcast identity-future pins scheduling
locality so every task lands on the single worker holding the payload. This frozen witness fails CI
if the engine's broadcast pattern regresses to single-worker pinning — 16 leaves on 2 one-thread
workers must not all co-locate."""

from __future__ import annotations

import os

import pytest
from submit_backends import (
    cluster_client,
    expected_concat,
    make_dask_runner,
    prov_plan,
    worker_pids,
)

pytest.importorskip("distributed")


@pytest.fixture(scope="module")
def client():
    with cluster_client(2, processes=True) as c:
        yield c


def test_leaves_spread_across_workers_on_a_fixed_size_cluster(client) -> None:
    with make_dask_runner(client) as runner:
        result = runner.run(prov_plan(16, "spread"))
    prov = result.value
    assert prov.payload == expected_concat(16, "spread")  # non-vacuity: the full run happened
    assert len(prov.leaves) == 16
    assert {pid for _, pid, _ in prov.leaves} <= set(worker_pids(client).values())
    assert os.getpid() not in {pid for _, pid, _ in prov.leaves}
    leaf_workers = {worker for _, _, worker in prov.leaves}
    assert len(leaf_workers) >= 2, (
        f"all 16 leaves ran on {leaf_workers} — broadcast-future scheduling pinned to one worker "
        "(the coffea#1490 regression)"
    )
