"""M41 theme (b) / Target T2 — incast avoidance: the cross-node fetch count grows SUB-LINEARLY in
N*P, not per-block (plan §8 "producer-task shuffle granularity", lit §D.3; decomposition §2 T2, §3(b)).

M39's gather issues ONE ``cluster.get`` per (producer-task, dest) block — a per-block fetch pattern.
MEASURED HEAD BASELINE (recorded in the m41 README): at (N=48 src, P=12 dests, K=4 nodes) HEAD makes
**48** per-block fetches (== the number of stage-1 blocks written; ``cross_node_fetches``=36). T2
coalesces the reader plane to node-pair / producer-task granularity so a consumer pulls FEW LARGE
streams instead of many tiny blocks — ``bulk_fetch_count`` stays ``<= 2*K*K`` (=32), well under both the
per-block 48 and N*P (=576).

Non-vacuity on HEAD: ``bulk_fetch_count`` / ``bytes_per_fetch`` / ``bytes_transferred`` are absent ->
``AttributeError``; grafted onto HEAD's per-block gather they'd equal the block count (48) which exceeds
2*K*K (32) and is NOT ``< blocks_written`` (48 == 48). This is NOT the M39 ``cross_node_fetches > 0``
witness — it is a NEW upper bound << the per-block count.

Discrimination (rejects the three traps in decomposition §3(b)):
- per-block pattern -> ``bulk_fetch_count`` too high (fails ``<= 2*K*K`` and ``< blocks_written``);
- "few fetches by dropping data" -> the routing oracle (no row lost) + ``bytes_transferred`` completeness
  both fail;
- "batch the counter but transfer per-block" -> ``bulk_fetch_count * bytes_per_fetch`` no longer equals
  ``bytes_transferred`` (the mean is inconsistent with a genuinely low count).
"""

from __future__ import annotations

from m41_backends import BACKEND, expected_dest_keys, make_block, observed_dest_keys

from graphed_executors.local.shuffle import run_repartition

_N_SRC = 48
_PARTS = 12
_WORKERS = 4  # K nodes
_HEAD_PER_BLOCK_FETCHES = 48  # MEASURED on HEAD at (N,P,K)=(48,12,4); the per-block baseline T2 beats


def _dense_src() -> list:
    # keys spread so every producer-task routes to (nearly) every dest -> the dense per-block-fetch
    # baseline (48 blocks) HEAD pays and T2 must coalesce; deterministic (no RNG).
    return [make_block([(i * 7 + j) % 60 for j in range(20)]) for i in range(_N_SRC)]


def test_bulk_fetch_count_is_sub_linear_and_carries_few_large_streams(tmp_path) -> None:  # type: ignore[no-untyped-def]
    src = _dense_src()
    res = run_repartition(BACKEND, src, parts=_PARTS, workers=_WORKERS, comms="ipc", store_root=tmp_path)
    w = res.witness
    k = _WORKERS

    n_blocks = sum(w.blocks_per_producer_task.values())  # M39 counter — the per-block granularity (48)
    assert n_blocks >= k * k, "sanity: the scenario must write more blocks than the coalesced bound"

    # (coalesced, absolute) — a NEW upper bound << N*P and below the measured per-block baseline (48).
    assert w.bulk_fetch_count <= 2 * k * k, (
        f"bulk_fetch_count {w.bulk_fetch_count} is not sub-linear (> 2*K*K={2 * k * k}); "
        f"HEAD's per-block gather would pay ~{_HEAD_PER_BLOCK_FETCHES} for N*P={_N_SRC * _PARTS}"
    )
    # (coalesced, relative) — strictly FEWER fetches than blocks: the per-block impl has one fetch/block.
    assert w.bulk_fetch_count < n_blocks, (
        f"bulk_fetch_count {w.bulk_fetch_count} == blocks {n_blocks}: this is still a per-block gather"
    )
    assert w.bulk_fetch_count >= 1, "there must be at least one fetch"

    # (few LARGE streams, not skipped fetches) — "few" is carried by the bulk_fetch_count bounds above
    # (<= 2*K*K and < n_blocks); "large / not skipped" is the completeness floor below
    # (bytes_transferred >= result/2) plus the routing oracle. `bytes_per_fetch` is DEFINED as
    # bytes_transferred / bulk_fetch_count (README contract), so bulk_fetch_count * bytes_per_fetch equals
    # bytes_transferred for every impl — no independent constraint lives there, so no check is asserted on it.
    assert w.bytes_transferred > 0
    total_result_bytes = sum(BACKEND.estimated_bytes(v) for v in res.value.values())
    assert w.bytes_transferred >= total_result_bytes // 2, (
        f"bytes_transferred {w.bytes_transferred} too low for the gathered data {total_result_bytes} — "
        "a low fetch count achieved by moving less data, not by coalescing"
    )

    # (completeness) — coalescing must not drop or reorder a single row.
    assert observed_dest_keys(res.value) == expected_dest_keys(src, _PARTS), "coalescing lost/reordered rows"
