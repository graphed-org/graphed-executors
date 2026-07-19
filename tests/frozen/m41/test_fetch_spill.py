"""M41 theme (a) / Target T1 — a RAM budget + spill on the FETCH/GATHER (reader) plane (plan §4.3,
§8 M41; decomposition §2 T1, §3(a)).

The M39 gather (``_stage2_gather`` / ``_gather_side``) does ``resp.read()`` then builds a LIST of every
``from_wire``'d block for a dest and ``concat``s it — so its peak reader memory is the WHOLE dest
resident, with **no reader-plane spill** (M39 only bounded the WRITER via ``ROW_GROUP_BYTES``, and M40
only spills the JOIN OUTPUT — two different planes, §4.3). T1 adds the third, reader-plane budget:
over ``fetch_budget_bytes`` the fetch streams the block to the node-local Store (byte-backpressure),
bounding peak reader RAM while the full dest still assembles correctly.

Scenario: one hot key routes every row to a SINGLE dest whose gathered input (512 000 B over 8 blocks)
is far larger than the fetch RAM budget (64 000 B, deliberately SMALLER than one 64 000 B block so a
block is "larger than the RAM budget" — the theme-(a) headline).

Non-vacuity on HEAD: ``fetch_budget_bytes`` is not a parameter -> ``TypeError`` (the knob is absent);
even grafted on, HEAD's list-then-concat holds the whole 512 000 B dest -> ``peak_fetch_bytes`` (honest)
> budget and nothing is on disk. Discrimination:
- ``peak_fetch_bytes <= budget + one_block`` — a "load whole dest then trim" impl reports > that;
- ``fetch_spilled_bytes > 0`` AND those bytes are REALLY on disk (independent ``du`` of the node Store
  ``objects/`` dirs) — a lying counter that never spilled disagrees with the disk measurement;
- the ``tracemalloc`` peak stays under a generous full-materialisation ceiling — a grossly wasteful
  (whole-dest-buffered) impl is caught by a real measurement too;
- the gathered dest equals the M39 routing oracle EXACTLY — a spill that drops/reorders rows fails.

VACUITY GUARD: this drives the pure REPARTITION reader plane (no join) — it never touches M40
``_join_with_budget`` / ``join_spilled_partitions`` nor the writer ``peak_writer_buffer_bytes``.
"""

from __future__ import annotations

import tracemalloc

from m41_backends import (
    BACKEND,
    BYTES_PER_ROW,
    expected_dest_keys,
    make_block,
    observed_dest_keys,
    store_bytes_on_disk,
)

from graphed_executors.local.shuffle import run_repartition

_N_SRC = 8
_ROWS = 4000  # one 64 000 B block per producer-task -> a block == the budget (a "block larger than RAM")
_HOT_KEY = 7
_PARTS = 8
_WORKERS = 8
_FETCH_BUDGET = 64_000
_DEST_BYTES = _N_SRC * _ROWS * BYTES_PER_ROW  # 512 000 — the whole hot dest, 8x the budget


def test_fetch_plane_spills_and_bounds_peak_under_a_budget_smaller_than_the_dest(tmp_path) -> None:  # type: ignore[no-untyped-def]
    src = [make_block([_HOT_KEY] * _ROWS) for _ in range(_N_SRC)]  # all rows -> one dest
    oracle = expected_dest_keys(src, _PARTS)
    assert sum(len(v) for v in oracle.values()) == _N_SRC * _ROWS, "the scenario must pile one dest high"

    # Warm imports + allocator arenas so the measured window below reflects the operation's STEADY-STATE
    # peak, not the one-time first-touch growth (~0.9 MB) that only the FIRST tracemalloc window in a
    # process pays. Without this the ceiling is order-dependent (green full-dir because a prior test warms
    # the arena, red in isolation / under a different collection order). A throwaway run over the SAME code
    # path (public API, its own store_root) removes only that one-time cost — a wasteful impl's per-op
    # copies are re-allocated INSIDE the measured window and are still caught by the ceiling.
    warm_root = tmp_path / "warmup"
    warm_root.mkdir()
    run_repartition(
        BACKEND,
        src,
        parts=_PARTS,
        workers=_WORKERS,
        comms="ipc",
        store_root=warm_root,
        fetch_budget_bytes=_FETCH_BUDGET,
    )

    tracemalloc.start()
    try:
        res = run_repartition(
            BACKEND,
            src,
            parts=_PARTS,
            workers=_WORKERS,
            comms="ipc",
            store_root=tmp_path,
            fetch_budget_bytes=_FETCH_BUDGET,  # HEAD: TypeError (knob absent) — the right-reason failure
        )
        _cur, tm_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    w = res.witness
    one_block = _DEST_BYTES // max(1, w.n_producer_tasks)  # a single in-flight block is unavoidable

    # (spill engaged, non-vacuous) — the reader-plane budget was actually exercised.
    assert w.fetch_spill_count > 0, "the fetch/gather plane must spill (else the budget never engaged)"
    assert w.fetch_spilled_bytes > 0, "spilled byte count must be positive"

    # (reader RAM bounded) — the direct streaming gate; a whole-dest-resident impl reports peak == dest.
    assert w.peak_fetch_bytes <= _FETCH_BUDGET + one_block, (
        f"reader peak {w.peak_fetch_bytes} exceeded budget {_FETCH_BUDGET} + one block {one_block} "
        f"(the whole dest is {_DEST_BYTES}) — the gather must stream to the Store, not hold the dest"
    )

    # (INDEPENDENT disk cross-check) — the spilled bytes are REALLY on the node-local Store dirs.
    on_disk = sum(store_bytes_on_disk(d) for d in w.node_store_dirs.values())
    assert on_disk > 0, "the spill counter claims a spill but nothing hit the node Store dirs"
    assert on_disk >= w.fetch_spilled_bytes // 2, (
        f"on-disk bytes {on_disk} do not corroborate fetch_spilled_bytes {w.fetch_spilled_bytes}"
    )

    # (MEASURED corroboration — a LOOSE net) — repartition is row-conserving, so the gathered dest itself
    # is resident in the result and building it (read-back + concat) legitimately holds ~2x the dest for a
    # moment; the real discriminators above are `peak_fetch_bytes` (the reader-plane counter) + the on-disk
    # spill. tracemalloc only catches a GROSSLY wasteful (many-copy) impl below a generous ceiling.
    assert tm_peak <= 3 * _DEST_BYTES + 4 * _FETCH_BUDGET, (
        f"measured peak {tm_peak} exceeded the generous full-materialisation ceiling"
    )

    # (result exact) — spill must be content- and order-transparent (the §7.1 determinism requirement).
    assert observed_dest_keys(res.value) == oracle, "the fetch-plane spill dropped or reordered rows"
