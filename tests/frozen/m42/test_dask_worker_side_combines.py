"""m42 worker-side reduction: combines execute ON WORKERS with peer-fetched inputs, never on the
driver (plan §1.2.1 "worker-side combines for free"; blueprint §3 `test_dask_worker_side_combines`;
witness style: tests/frozen/m38/test_peer_reduce.py `test_witness_reduction_is_genuinely_off_driver`).

Discriminates: an implementation that gathers all partials to the driver and combines there
(combine pids == driver pid), and one that funnels every combine to a single worker without any
cross-worker input movement (no (producer != combiner) move pair)."""

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


def test_combines_run_on_workers_with_cross_worker_inputs(client) -> None:
    driver_pid = os.getpid()
    wpids = set(worker_pids(client).values())
    assert driver_pid not in wpids  # tier-B precondition: workers are real separate processes

    with make_dask_runner(client) as runner:
        result = runner.run(prov_plan(16, "wsc"))
    prov = result.value

    # the run itself is complete and correct (order-witnessing payload)
    assert prov.payload == expected_concat(16, "wsc")
    assert len(prov.leaves) == 16
    assert len(prov.marks) == 16 + 15  # every leaf AND every combine accounted for

    leaf_pids = {pid for _, pid, _ in prov.leaves}
    combine_pids = {pid for pid, _ in prov.combine_sites}
    assert leaf_pids <= wpids, "leaves ran outside the worker processes"
    assert combine_pids <= wpids, "a combine ran outside the worker processes"
    assert driver_pid not in combine_pids, "a combine executed on the driver (driver-side fold)"

    # peer data movement engaged: >=1 combine consumed an input MATERIALIZED on a different worker
    cross = {(src, dst) for src, dst in prov.moves if src != dst}
    assert cross, "no combine consumed a cross-worker input (everything funneled to one place)"
