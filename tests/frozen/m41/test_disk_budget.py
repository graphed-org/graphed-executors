"""M41 Target T3 — per-node disk budgeting under a skewed key distribution (the Dask P2P #8433
disk-imbalance failure; plan §8 M41, lit §A.1; decomposition §2 T3, §3 T3).

Under skew (one hot key), every row routes to a SINGLE dest whose gather node would pile the whole
512 000 B of spilled shuffle intermediates onto ITS Store dir — the #8433 imbalance. T3 gives each node
a disk budget: a node's Store must never exceed it, so the impl backpressures / redistributes the spill
across nodes (512 000 B over K=4 nodes at a 200 000 B cap needs ~3 nodes — feasible) instead of blowing
one node's disk. The RESULT stays complete + bit-exact.

Non-vacuity on HEAD: ``disk_budget_bytes`` is not a parameter (nor ``fetch_budget_bytes``) ->
``TypeError``; the counters ``per_node_disk_bytes`` / ``disk_budget_bytes`` / ``disk_backpressure_events``
are absent. Grafted onto HEAD (no per-node cap) the hot node's Store dir holds the whole 512 000 B, so
the INDEPENDENT ``du`` of that node exceeds the 200 000 B budget and the assertion fails.

Discrimination (rejects the traps in decomposition §3 T3):
- AGGREGATE-cap trap -> we assert the HOT node SPECIFICALLY (and the per-node max), not a sum;
- a counter without enforcement -> the independent ``du`` of each node's Store must ALSO be under budget;
- backpressure that drops data -> the routing oracle (no row lost) fails;
- distinct from theme (a): (a) bounds the RAM the spill *comes from*; T3 bounds the DISK it lands *on*.
"""

from __future__ import annotations

from m41_backends import (
    BACKEND,
    BYTES_PER_ROW,
    expected_dest_keys,
    hot_dest,
    make_block,
    observed_dest_keys,
    store_bytes_on_disk,
)

from graphed_executors.local.shuffle import run_repartition

_N_SRC = 8
_ROWS = 4000
_HOT_KEY = 7
_PARTS = 8
_WORKERS = 4  # K nodes
_FETCH_BUDGET = 32_000  # tiny -> almost all of the hot dest spills to disk
_DISK_BUDGET = 200_000  # < the 512 000 B hot pile -> one node cannot legally hold it all
_FS_SLACK = _ROWS * BYTES_PER_ROW  # one whole block: disk placement is block-granular, not byte-exact
_DEST_BYTES = _N_SRC * _ROWS * BYTES_PER_ROW  # 512 000


def test_per_node_disk_stays_under_budget_when_one_hot_key_over_receives(tmp_path) -> None:  # type: ignore[no-untyped-def]
    src = [make_block([_HOT_KEY] * _ROWS) for _ in range(_N_SRC)]  # skew: all rows -> one dest
    oracle = expected_dest_keys(src, _PARTS)

    res = run_repartition(
        BACKEND,
        src,
        parts=_PARTS,
        workers=_WORKERS,
        comms="ipc",
        store_root=tmp_path,
        fetch_budget_bytes=_FETCH_BUDGET,  # HEAD: TypeError (knobs absent) — the right-reason failure
        disk_budget_bytes=_DISK_BUDGET,
    )
    w = res.witness

    # (backpressure engaged, non-vacuous) — the skew genuinely exceeded a single node's cap.
    assert w.disk_backpressure_events > 0, "the per-node disk cap was never exercised (no backpressure)"
    assert w.disk_budget_bytes == _DISK_BUDGET, "the run must record the configured disk budget"

    # (counter: every node under cap — the max is the hot node, so this is NOT an aggregate cap).
    assert w.per_node_disk_bytes, "per-node disk accounting is missing"
    assert max(w.per_node_disk_bytes.values()) <= _DISK_BUDGET, (
        f"a node's Store exceeded the disk budget: {w.per_node_disk_bytes}"
    )

    # (INDEPENDENT disk: real du of EACH node under cap — catches a counter that lies about enforcement).
    for addr, node_dir in w.node_store_dirs.items():
        du = store_bytes_on_disk(node_dir)
        assert du <= _DISK_BUDGET + _FS_SLACK, f"node {addr} du {du} exceeded budget {_DISK_BUDGET}"

    # (HOT node specifically — the #8433 over-receiver — defeats the aggregate trap explicitly).
    hot_addr = f"node-{hot_dest(_HOT_KEY, _PARTS) % _WORKERS}"
    assert hot_addr in w.node_store_dirs
    assert store_bytes_on_disk(w.node_store_dirs[hot_addr]) <= _DISK_BUDGET + _FS_SLACK, (
        "the hot gather node's disk was not capped"
    )

    # (skew really stressed the cap) — more than one node's budget of data actually hit disk, so a
    # no-cap impl WOULD have overflowed one node; and the counters reflect the real on-disk totals.
    total_on_disk = sum(store_bytes_on_disk(d) for d in w.node_store_dirs.values())
    assert total_on_disk > _DISK_BUDGET, "the scenario did not spill enough to exercise the per-node cap"
    total_counter = sum(w.per_node_disk_bytes.values())
    assert abs(total_counter - total_on_disk) <= total_on_disk // 4 + 8192, (
        f"per_node_disk_bytes total {total_counter} disagrees with on-disk {total_on_disk}"
    )

    # (completeness) — redistribution/backpressure must not drop or reorder a row.
    assert observed_dest_keys(res.value) == oracle, "disk backpressure lost or reordered rows"
