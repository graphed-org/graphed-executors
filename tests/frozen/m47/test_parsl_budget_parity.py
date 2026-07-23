"""m47 budget goldens (m41 carried via the shared bodies) — the peer-exchange gather and join
bound their working sets with the SAME ``common/transport_tasks`` counters the dask job gates
(plan §1.4/§1.6; blueprint §3 `test_parsl_budget_parity`).

Each arm first runs the LOCAL engine with the same budget as an in-test SCENARIO GUARD (the
budget provably engages the mechanism there — numbers pre-validated while authoring: fetch
one_block=832/budget=416 -> local spill_count 7, peak 640; join 64x64 at budget=output//8 ->
local spilled 7 == chunks_read 7, peak 15360, rows 4096), then pins the p2p engine's counters:

- fetch plane: ``fetch_spill_count > 0`` and ``peak_fetch_bytes <= budget + one_block`` (the
  gather streams to spill instead of holding whole dests), with the result bytes UNCHANGED.
- join plane: ``join_spilled_partitions > 0`` and ``== join_chunks_read`` (F5: every spilled
  radix partition read back exactly once), ``peak_join_bytes <= budget``, and the duplicated
  output row count exact — counters produced by genuine ``_join_with_budget`` reuse; a
  re-implementation diverges from the shared counter semantics."""

from __future__ import annotations

import sys

import pytest
from parsl_transport_harness import (
    join_hot_key_sides,
    make_parsl_backend,
    numpy_p2p_adapter,
    p2p_htex_pool,
    repartition_blocks,
    run_bounded,
    transport_join,
    transport_repartition,
)

from graphed_executors.local.shuffle import run_join, run_repartition

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

PARTS = 8


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=2) as ex:
        yield ex


def test_fetch_budget_engages_and_never_changes_the_bytes(htex) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    be = adapter.backend
    src = repartition_blocks(adapter, copies=4)
    base = run_repartition(be, src, PARTS, workers=2)
    one_block = max(len(bytes(be.to_wire(b))) for b in base.value.values())  # type: ignore[attr-defined]
    budget = max(1, one_block // 2)

    local = run_repartition(be, src, PARTS, workers=2, fetch_budget_bytes=budget)
    assert local.witness.fetch_spill_count > 0, (
        "scenario guard misfired: this budget must engage the LOCAL fetch spill (pre-validated: 7)"
    )

    r = run_bounded(
        lambda: transport_repartition(
            be, src, PARTS, pbackend=make_parsl_backend(htex), fetch_budget_bytes=budget
        )
    )
    assert r.dest_block_hashes == base.dest_block_hashes, "a fetch budget must never change the bytes"
    assert r.witness.fetch_spill_count > 0, "the p2p gather never engaged the fetch spill"
    assert r.witness.peak_fetch_bytes <= budget + one_block, (
        f"peak_fetch_bytes={r.witness.peak_fetch_bytes} exceeded budget+one_block="
        f"{budget + one_block} — the gather must stream to spill, not hold whole dests (T1)"
    )


def test_join_budget_spill_counters_engage_with_f5_read_back_parity(htex) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    be = adapter.backend
    n = 64  # 64x64 -> 4096 duplicated output rows in one hot dest (the pre-validated B5 blowup)
    left, right = join_hot_key_sides(adapter, n, n)
    row_bytes = be.estimated_bytes(left[0]) // n + be.estimated_bytes(right[0]) // n  # type: ignore[attr-defined]
    budget = (n * n * row_bytes) // 8

    local = run_join(be, left, right, PARTS, broadcast=False, mem_budget_bytes=budget)
    assert local.witness.join_spilled_partitions > 0, (
        "scenario guard misfired: the budget must engage the LOCAL join spill (pre-validated: 7)"
    )

    r = run_bounded(
        lambda: transport_join(
            be,
            left,
            right,
            PARTS,
            pbackend=make_parsl_backend(htex),
            broadcast=False,
            mem_budget_bytes=budget,
        )
    )
    w = r.witness
    assert w.join_spilled_partitions > 0, "the p2p gather-join never engaged the spill"
    assert w.join_spilled_partitions == w.join_chunks_read, (
        "F5: every spilled radix partition must be read back exactly once — a divergence means "
        "_join_with_budget was re-implemented, not reused through common/transport_tasks"
    )
    assert w.peak_join_bytes <= budget, "the join working set exceeded its budget (B5)"
    assert w.join_output_rows == n * n, "the duplicated output row count drifted"
    assert r.dest_block_hashes == local.dest_block_hashes, "a join budget must never change the bytes"
