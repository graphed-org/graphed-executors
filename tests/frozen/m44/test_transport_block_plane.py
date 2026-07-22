"""m44 block plane — bulk bytes travel worker→worker over ``graphed_block_pull``, NEVER through
the driver, and no per-epoch state survives a run (plan §1.4 pull-model data plane, §1.2
teardown; blueprint §3 `test_transport_block_plane`). Tier B: worker processes have pids distinct
from the driver, so serving provenance is unfakeable by construction.

Witnesses: every worker's plugin served block bytes (``bytes_served > 0`` on BOTH workers — the
interleaved-key scenario guarantees each holder owns fragments some REMOTE gather needs);
``serve_pid`` equals the live worker pid and never the driver pid; ``cross_node_fetches > 0``;
the result is byte-identical to the local oracle; and after the run NO epoch of the run is still
active on any worker (evict + purge — the unmanaged-memory guarantee).

Discriminates: a driver-relay implementation (blocks riding task returns/futures through the
client process) has ``bytes_served == 0`` and no worker-pid serving provenance; an engine that
skips teardown leaves its epoch resident and fails the purge probe."""

from __future__ import annotations

import os

import pytest
from transport_harness import (
    build_dask_backend,
    install_transport_plugin,
    live_worker_pids,
    numpy_adapter,
    probe_active_epochs,
    repartition_blocks,
    sorted_worker_addresses,
    transport_cluster,
    transport_repartition,
    tw_per_worker,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip("distributed")

PARTS = 8


@pytest.fixture(scope="module")
def client():
    with transport_cluster(2, processes=True) as c:
        yield c


def test_block_bytes_are_served_worker_to_worker_and_state_is_purged(client) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_adapter()
    src = repartition_blocks(adapter, copies=2)
    local = run_repartition(adapter.backend, src, PARTS)

    install_transport_plugin(client)
    r = transport_repartition(adapter.backend, src, PARTS, dbackend=build_dask_backend(client))
    assert r.dest_block_hashes == local.dest_block_hashes

    addrs = sorted_worker_addresses(client)
    worker_pids = live_worker_pids(client)
    driver_pid = os.getpid()
    pw = tw_per_worker(r)
    for addr in addrs:
        served = pw[addr].get("bytes_served", 0)
        assert served > 0, (
            f"worker {addr} served no block bytes — with interleaved keys every holder owns "
            "fragments a REMOTE gather must pull; zero means the bytes routed through the driver "
            "or through task-return values"
        )
        serve_pid = pw[addr].get("serve_pid", 0)
        assert serve_pid == worker_pids[addr], (
            f"blocks for {addr} were served from pid {serve_pid}, not the live worker pid "
            f"{worker_pids[addr]}"
        )
        assert serve_pid != driver_pid, "block bytes were served from the DRIVER process"

    assert r.witness.cross_node_fetches > 0, "no cross-node pull was ever recorded"

    # evict + per-epoch purge: nothing of this run survives on any worker
    active = client.run(probe_active_epochs)
    for nonce in r.transport.epoch_nonces:
        for addr, epochs in active.items():
            assert nonce not in epochs, (
                f"epoch {nonce} still active on {addr} after the run — block stores/spill dirs "
                "were not purged (unmanaged-memory leak, §1.2 teardown)"
            )
