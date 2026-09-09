"""m47 review remediation (finding 1, non-gating): the per-peer join spill-key collision that
silently drops rows.

Pre-fix, ``_gather_join`` started each peer's overflow (budget-spill) keys at ``next_key = parts``,
so two peers each spilling a hot dest emitted colliding keys ``parts, parts+1, …`` and the driver's
``assemble`` merged them with ``value.update(res["value"])`` — silently overwriting one peer's
chunks. The join witness (``join_output_rows``) still summed correctly, so ONLY the assembled value
diverged (the reviewer's witness/value divergence). The fix re-keys overflow driver-side into ONE
global sequence at ``assemble``.

This runs over an in-process ThreadPoolExecutor (peers as driver threads): the collision is
driver-side ``assemble`` logic, backend-agnostic, so it reproduces without HTEX. Two hot keys are
placed on dests owned by DIFFERENT peers at k=2 (owner = ``worker_addrs[dest % k]``), each in its
OWN source block so ``n_producer_bound == 2`` and k resolves to 2 (with a single block/side k would
collapse to 1 and no collision would occur — the pre-fix scenario that hid the bug).

Discriminating (measured): pre-fix this yields 4096 of 8192 rows and 7 of 14 dest keys with
``join_output_rows == 8192`` unchanged; post-fix 8192/8192 and 14/14, matching the local engine and
the independent duplicating oracle byte-for-byte.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("parsl", reason="graphed-executors[parsl] extra not installed")

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m47"))

from parsl_transport_harness import (
    dup_join_all,
    numpy_p2p_adapter,
    observed_join_all,
    p2p_tpe_pool,
    run_bounded,
    total_result_rows,
)

from graphed_executors.local.shuffle import run_join

PARTS = 8
N = 64  # 64x64 -> 4096 duplicated rows per hot dest (the frozen budget-parity spill scale)
KEY_A, KEY_B = 0, 2  # scenario-guarded below to land on different-parity dests


def _dest_of(be: object, key: int) -> int:
    dt = np.dtype([("__joinkey__", np.uint64), ("lval", np.int64)])
    block: np.ndarray = np.zeros(1, dtype=dt)
    block["__joinkey__"] = key
    for dest, sub in enumerate(be.partition(block, "__joinkey__", PARTS)):  # type: ignore[attr-defined]
        if len(sub):
            return dest
    raise AssertionError("routing probe produced no dest")


def test_two_peer_spilled_join_is_row_exact() -> None:
    adapter = numpy_p2p_adapter()
    be = adapter.backend

    # scenario guard: the two hot keys must land on dests owned by DIFFERENT peers at k=2 — that
    # different-peer, both-spilling shape is the collision trigger the fix addresses.
    dest_a, dest_b = _dest_of(be, KEY_A), _dest_of(be, KEY_B)
    assert dest_a % 2 != dest_b % 2, (
        f"scenario guard: keys {KEY_A},{KEY_B} -> dests {dest_a},{dest_b} on the SAME peer at k=2 "
        "(the collision needs the two hot dests owned by different peers)"
    )

    # ONE hot key per source block -> n_producer_bound = 2 -> k = 2 (a single block/side collapses
    # k to 1, one peer owning both dests, and the collision never fires — the pre-fix blind spot).
    left = [
        adapter.make_side([KEY_A] * N, "lval", list(range(N))),
        adapter.make_side([KEY_B] * N, "lval", list(range(N, 2 * N))),
    ]
    right = [
        adapter.make_side([KEY_A] * N, "rval", list(range(1000, 1000 + N))),
        adapter.make_side([KEY_B] * N, "rval", list(range(2000, 2000 + N))),
    ]
    row_bytes = be.estimated_bytes(left[0]) // N + be.estimated_bytes(right[0]) // N
    budget = (N * N * row_bytes) // 8

    local = run_join(be, left, right, PARTS, broadcast=False, mem_budget_bytes=budget)
    assert local.witness.join_spilled_partitions >= 2, (
        "scenario guard: the budget must engage the local join spill on BOTH hot dests"
    )
    assert total_result_rows(local.value) == 2 * N * N

    with p2p_tpe_pool(max_threads=2) as tpe:
        from graphed_executors.parsl_backend import ParslBackend  # noqa: PLC0415
        from graphed_executors.parsl_backend.transport_shuffle import transport_run_join  # noqa: PLC0415

        r = run_bounded(
            lambda: transport_run_join(
                be, left, right, PARTS, pbackend=ParslBackend(tpe), broadcast=False, mem_budget_bytes=budget
            )
        )

    # the witness sums correctly EVEN when rows are dropped (pre-fix: 8192) — so it is NOT the
    # discriminator; the assembled value is (pre-fix: 4096 rows / 7 keys).
    assert r.witness.join_output_rows == 2 * N * N
    assert total_result_rows(r.value) == 2 * N * N, (
        f"row loss: assembled {total_result_rows(r.value)} of {2 * N * N} rows — colliding per-peer "
        "spill keys were overwritten at the driver assemble"
    )
    assert len(r.value) == len(local.value), (
        f"dest-key loss: {len(r.value)} keys vs the local engine's {len(local.value)} — a peer's "
        "overflow chunks were dropped by the collision"
    )
    assert observed_join_all(adapter, r.value) == dup_join_all(adapter, left, right, PARTS), (
        "p2p join rows diverged from the independent duplicating oracle (dropped spill chunks)"
    )
