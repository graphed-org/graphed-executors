"""m44 behavioral golden (m39 carried) — ``transport_run_repartition`` reproduces the LOCAL
engine's ``dest_block_hashes`` BYTE-IDENTICALLY on identical inputs, on both real backends, and
across two consecutive dask runs (plan §1.4, DoD "Behavioral parity"; blueprint §3
`test_transport_repartition_parity`).

Witnesses: sha256 dest hashes equal to ``graphed_executors.local.shuffle.run_repartition``'s
(cross-ENGINE, not just cross-run); per-dest key sets equal to the golden-pinned ``partition``
routing oracle; row conservation. Any routing drift, merge-order drift (non-ascending-task
concat), or wire-format drift fails byte equality — the same discrimination the m39/m43 hash
gates carry, now against the transport engine."""

from __future__ import annotations

import pytest
from transport_harness import (
    TransportAdapter,
    build_dask_backend,
    install_transport_plugin,
    repartition_blocks,
    result_dest_keys,
    route_oracle_dest_keys,
    total_result_rows,
    transport_adapters,
    transport_cluster,
    transport_repartition,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip("distributed")

ADAPTERS = transport_adapters()
PARTS = 8


@pytest.fixture(scope="module")
def client():
    with transport_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> TransportAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_dest_block_hashes_byte_identical_to_local_engine_and_across_runs(adapter: TransportAdapter, client) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks(adapter)
    local = run_repartition(adapter.backend, src, PARTS)

    install_transport_plugin(client)
    dbackend = build_dask_backend(client)
    r1 = transport_repartition(adapter.backend, src, PARTS, dbackend=dbackend)
    r2 = transport_repartition(adapter.backend, src, PARTS, dbackend=dbackend)

    assert r1.dest_block_hashes == local.dest_block_hashes, (
        "transport-engine dest hashes diverged from the local engine on identical inputs — "
        "routing, merge order, or wire bytes drifted"
    )
    assert r2.dest_block_hashes == local.dest_block_hashes, "second run diverged (determinism gate)"
    assert list(r1.transport.epoch_nonces) != list(r2.transport.epoch_nonces), (
        "every run must mint a fresh epoch nonce"
    )

    # content, not just bytes: the per-dest key routing equals the golden-pinned partition oracle
    assert result_dest_keys(adapter, r1.value) == route_oracle_dest_keys(adapter, src, PARTS)
    assert total_result_rows(r1.value) == sum(len(b) for b in src), "rows were lost or invented"  # type: ignore[arg-type]
