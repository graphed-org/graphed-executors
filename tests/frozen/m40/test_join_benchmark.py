"""M40 benchmark gates — algorithmic/complexity, not wall-clock (plan §8, B6). Deterministic counts.

A distributed join is two co-partitioned exchanges + a gather-join, so its block/announcement traffic
must stay O(#producer-tasks · P) PER SIDE (each block large), never the O(#src_pid · P) MxR
tiny-fragment blowup, and never per-row. Gates:

- block counts per side are O(#producer-tasks · P) and strictly below the O(#src_pid · P) blowup;
  producer-tasks track workers (T ~ W), not src_pids;
- counts are independent of the ROW count — 10x the rows-per-block leaves the block/announcement counts
  unchanged (a per-row-message impl scales with rows and fails);
- the broadcast-vs-shuffle crossover is MEASURED from the pinned cost rule (both regimes realised), not
  asserted — the artifact must exist and be internally consistent.
"""

from __future__ import annotations

import pytest
from join_backends import JoinAdapter, adapters

from graphed_exec_local.shuffle import broadcast_join_choice, run_join

ADAPTERS = adapters()
pytestmark = pytest.mark.skipif(not ADAPTERS, reason="no exchange backend installed")


def _sides(adapter: JoinAdapter, n_src: int, rows_per: int) -> tuple[list[object], list[object]]:
    left = [
        adapter.make_side(list(range(i, i + rows_per)), "lval", list(range(rows_per))) for i in range(n_src)
    ]
    right = [
        adapter.make_side(list(range(i, i + rows_per)), "rval", list(range(rows_per))) for i in range(n_src)
    ]
    return left, right


def test_block_counts_are_O_producer_tasks_times_P_per_side_not_MxR(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ad = ADAPTERS[0]
    n_src = 8
    for workers, parts in ((2, 4), (2, 8), (4, 8)):
        left, right = _sides(ad, n_src, rows_per=12)
        w = run_join(
            ad.backend,
            left,
            right,
            parts,
            broadcast=False,
            workers=workers,
            store_root=tmp_path / f"{workers}_{parts}",
        ).witness
        assert w.n_producer_tasks <= workers, "producer-tasks track workers (T ~ W), not src_pids"
        for side_blocks in (w.build_side_blocks, w.large_side_blocks):
            assert side_blocks <= w.n_producer_tasks * parts, (
                "O(#producer-tasks * P) — one block per dest per task"
            )
            assert side_blocks < n_src * parts, "must NOT be the O(#src_pid * P) tiny-fragment blowup"


def test_counts_do_not_scale_with_row_count(tmp_path) -> None:  # type: ignore[no-untyped-def]
    # 10x the rows-per-block must NOT change the block/announcement counts (they are per-block, per-dest,
    # never per-row). A per-row-message impl scales with rows here and fails.
    ad = ADAPTERS[0]
    n_src, parts, workers = 6, 4, 2
    small = run_join(
        ad.backend,
        *_sides(ad, n_src, rows_per=4),
        parts,
        broadcast=False,
        workers=workers,
        store_root=tmp_path / "small",
    ).witness
    big = run_join(
        ad.backend,
        *_sides(ad, n_src, rows_per=40),
        parts,
        broadcast=False,
        workers=workers,
        store_root=tmp_path / "big",
    ).witness
    assert small.build_side_blocks == big.build_side_blocks, (
        "build-side block count must be row-count-independent"
    )
    assert small.large_side_blocks == big.large_side_blocks, (
        "probe-side block count must be row-count-independent"
    )
    assert small.announcements_sent == big.announcements_sent, "announcements are per-block, never per-row"


def test_broadcast_crossover_is_measured_from_the_pinned_rule(tmp_path) -> None:  # type: ignore[no-untyped-def]
    # both regimes are realisable and consistent with the recorded cost rule (crossover measured, not
    # hard-coded): a tiny-build scenario broadcasts, a huge-build scenario shuffles.
    ad = ADAPTERS[0]
    parts, workers = 4, 3
    small_build = [ad.make_side([1, 2, 3], "lval", [10, 20, 30])]
    big_probe = [ad.make_side(list(range(k, k + 12)), "rval", list(range(12))) for k in range(6)]
    b = run_join(
        ad.backend, small_build, big_probe, parts, broadcast=None, workers=workers, store_root=tmp_path / "b"
    )
    build_bytes = sum(ad.backend.estimated_bytes(x) for x in small_build)
    probe_bytes = sum(ad.backend.estimated_bytes(x) for x in big_probe)
    assert b.witness.broadcast_chosen == broadcast_join_choice(build_bytes, probe_bytes, workers)
    assert broadcast_join_choice(build_bytes, probe_bytes, workers) is True, (
        "a tiny build vs a big probe must broadcast"
    )
    # and the inverse regime shuffles under the same rule:
    assert broadcast_join_choice(probe_bytes, build_bytes, workers) is False, "a big build must shuffle"
