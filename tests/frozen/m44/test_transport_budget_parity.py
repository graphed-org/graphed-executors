"""m44 budget goldens (m41 carried + F12 holder plane) — the pull-model gather reproduces the
local reader-plane budgets 1:1, and the NEW holder plane is bounded too (plan §1.4 points 1-2,
§8 F12/FU1; blueprint §3 `test_transport_budget_parity`).

Every scenario first runs the LOCAL engine (workers=2, matching k) as an in-test SCENARIO GUARD —
the budget provably engages the mechanism there — then pins the transport engine's counters:

- fetch plane: ``fetch_spill_count > 0`` and EQUAL to the local engine's; ``peak_fetch_bytes``
  EQUAL to the local engine's (FU1's pinned-achievable parity pair) and independently bounded by
  ``budget + one in-flight block``; ``bulk_fetch_count`` bounded by P·k (coalesced per
  (gather, holder) — no per-block chatter); budgets never change the result bytes.
- disk plane (T3): under a hot-key skew with a small per-node disk budget the driver-side
  arbitration engages (``disk_backpressure_events > 0``), the budget is echoed, and the per-node
  map is keyed by real worker addresses. Exact ``disk_backpressure_events`` equality with the
  local engine is a recorded DISPUTE CANDIDATE (FU1) and is deliberately NOT pinned.
- holder plane (F12): with a small ``holder_budget_bytes`` the producer-local retention spills
  (``holder_spill_count > 0``), ``peak_holder_bytes`` stays within budget + one dest wire, and
  the bytes are unchanged — hard constraint 7 caught on BOTH planes."""

from __future__ import annotations

import pytest
from transport_harness import (
    TransportAdapter,
    build_dask_backend,
    hot_key_blocks,
    install_transport_plugin,
    repartition_blocks,
    sorted_worker_addresses,
    transport_adapters,
    transport_cluster,
    transport_repartition,
    tw_per_worker,
    tw_total,
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


def _dest_wire_bytes(adapter: TransportAdapter, base: object) -> dict[int, int]:
    be = adapter.backend
    return {d: len(be.to_wire(b)) for d, b in base.value.items()}  # type: ignore[attr-defined]


def test_fetch_budget_engages_and_matches_local(adapter: TransportAdapter, client) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks(adapter, copies=4)
    base = run_repartition(adapter.backend, src, PARTS, workers=2)
    one_block = max(_dest_wire_bytes(adapter, base).values())  # no fragment exceeds a whole dest
    budget = max(1, one_block // 2)

    local = run_repartition(adapter.backend, src, PARTS, workers=2, fetch_budget_bytes=budget)
    assert local.witness.fetch_spill_count > 0, (
        "scenario guard misfired: this budget must engage the LOCAL fetch spill"
    )

    install_transport_plugin(client)
    r = transport_repartition(
        adapter.backend, src, PARTS, dbackend=build_dask_backend(client), fetch_budget_bytes=budget
    )
    w = r.witness
    assert r.dest_block_hashes == base.dest_block_hashes, "a fetch budget must never change the bytes"
    assert w.fetch_spill_count > 0, "the transport gather never engaged the fetch spill"
    assert w.peak_fetch_bytes <= budget + one_block, (
        f"peak_fetch_bytes={w.peak_fetch_bytes} exceeded budget+one_block={budget + one_block} — "
        "the gather must stream to its local spill, not hold whole dests"
    )
    assert w.fetch_spill_count == local.witness.fetch_spill_count, (
        "fetch_spill_count diverged from the local engine on identical inputs/budget — the gather "
        "loop is not the 1:1 reproduction §1.4 pins"
    )
    assert w.peak_fetch_bytes == local.witness.peak_fetch_bytes, (
        "peak_fetch_bytes diverged from the local engine on identical inputs/budget"
    )
    assert 0 < w.bulk_fetch_count <= PARTS * 2, (
        f"bulk_fetch_count={w.bulk_fetch_count}: pulls must coalesce per (gather, holder) — at "
        f"most P*k={PARTS * 2} batches, never per-block chatter"
    )


def test_disk_budget_backpressure_under_skew(adapter: TransportAdapter, client) -> None:  # type: ignore[no-untyped-def]
    src = hot_key_blocks(adapter, 300)
    base = run_repartition(adapter.backend, src, PARTS, workers=2)
    total_bytes = sum(_dest_wire_bytes(adapter, base).values())
    disk_budget = max(1, total_bytes // 3)  # the hot node cannot hold its own spill

    local = run_repartition(
        adapter.backend, src, PARTS, workers=2, fetch_budget_bytes=1, disk_budget_bytes=disk_budget
    )
    assert local.witness.disk_backpressure_events > 0, (
        "scenario guard misfired: this skew must trigger the LOCAL T3 redistribution"
    )

    install_transport_plugin(client)
    r = transport_repartition(
        adapter.backend, src, PARTS, dbackend=build_dask_backend(client),
        fetch_budget_bytes=1, disk_budget_bytes=disk_budget,
    )
    w = r.witness
    assert r.dest_block_hashes == base.dest_block_hashes, "disk budgets must never change the bytes"
    assert w.disk_budget_bytes == disk_budget, "the run must echo the configured disk budget"
    assert w.disk_backpressure_events > 0, (
        "the driver-side T3 arbitration never engaged under skew (FU1: engagement is pinned; "
        "exact equality with the local count is a recorded dispute candidate, not asserted)"
    )
    assert set(w.per_node_disk_bytes) == set(sorted_worker_addresses(client)), (
        "per_node_disk_bytes must be keyed by the real dask worker addresses"
    )


def test_holder_budget_bounds_producer_retention(adapter: TransportAdapter, client) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks(adapter, copies=4)
    base = run_repartition(adapter.backend, src, PARTS, workers=2)
    dest_bytes = _dest_wire_bytes(adapter, base)
    one_wire = max(dest_bytes.values())
    holder_budget = max(1, sum(dest_bytes.values()) // 8)  # far below one producer's retention

    install_transport_plugin(client)
    r = transport_repartition(
        adapter.backend, src, PARTS, dbackend=build_dask_backend(client),
        holder_budget_bytes=holder_budget,
    )
    assert r.dest_block_hashes == base.dest_block_hashes, "a holder budget must never change the bytes"
    assert tw_total(r, "holder_spill_count") > 0, (
        "the producer-local retention never spilled under a small holder budget — the holder plane "
        "is unbounded (the unmanaged-memory pause/terminate trap, F12)"
    )
    peak = max(c.get("peak_holder_bytes", 0) for c in tw_per_worker(r).values())
    assert peak <= holder_budget + one_wire, (
        f"peak_holder_bytes={peak} exceeded budget+one_wire={holder_budget + one_wire}"
    )
