"""m47 anti-super-linear guard, reshaped for k persistent peers (plan §3 omissions item 3 —
tests-M3): the submit-seam witness is **submit-count == k + O(1) control, independent of T and
P**, asserted across a {parts, 2*parts} x {T, 2*T} ladder, plus row-count independence. The m44
O(T+P) per-submit gate does NOT port (the map/pull/gather loops run INSIDE the k peer tasks,
invisible at the submit seam); its other two thirds live in `test_parsl_block_plane` (pull
coalescing <= k*k) and `test_parsl_rendezvous_barrier` (exactly k hellos).

Counted at the ONLY pinned seam — ``pbackend.submit`` — via the harness ``SpyP2PBackend``,
classified by the pinned module-level fn name ``_parsl_peer_main``. Per-cell EXACT equality:
``_parsl_peer_main == k`` in every cell, zero pick/map/gather-shaped submits (any ``_dask_*``
name at this seam means the engine degenerated into a relay or m43 shape), and the TOTAL submit
count is one constant across all four ladder cells (the O(1) control tail). 8x the rows changes
nothing. Counts, never clocks (R0.10a).

Discriminates: a per-partition-task engine (count grows with P); a per-src-task engine (grows
with T); a relay-in-disguise (``_dask_map_write``/``_dask_gather`` submits); per-row/per-fragment
task creation (the M39 anti-pattern)."""

from __future__ import annotations

import sys
from collections import Counter

import pytest
from parsl_transport_harness import (
    SpyP2PBackend,
    make_parsl_backend,
    many_src_blocks,
    numpy_p2p_adapter,
    p2p_htex_pool,
    repartition_blocks,
    run_bounded,
    transport_repartition,
)

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

K = 2
LADDER_T = (6, 12)  # source-block counts (the T knob)
LADDER_P = (4, 8)  # dest counts (the P knob)
FORBIDDEN = ("_dask_map_write", "_dask_pick", "_dask_gather", "_dask_gather_join")


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=K) as ex:
        yield ex


def _spy_run(htex, src, parts: int) -> SpyP2PBackend:  # type: ignore[no-untyped-def]
    spy = SpyP2PBackend(make_parsl_backend(htex))
    run_bounded(
        lambda: transport_repartition(numpy_p2p_adapter().backend, src, parts, pbackend=spy, workers=K)
    )
    return spy


def test_submit_count_is_k_plus_constant_independent_of_T_and_P(htex) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    totals: set[int] = set()
    for n_src in LADDER_T:
        for parts in LADDER_P:
            names: Counter[str] = _spy_run(htex, many_src_blocks(adapter, n_src), parts).fn_names()
            assert names["_parsl_peer_main"] == K, (
                f"(T={n_src}, P={parts}): expected exactly k={K} _parsl_peer_main submits, got "
                f"{dict(names)} — peers are persistent; work rides the spec and the pull plane"
            )
            for forbidden in FORBIDDEN:
                assert names[forbidden] == 0, (
                    f"(T={n_src}, P={parts}): {forbidden} submitted ({dict(names)}) — a relay/m43 "
                    "shape is standing in for the peer-exchange engine"
                )
            assert not any("pick" in n for n in names), f"pick-shaped submits: {dict(names)}"
            totals.add(sum(names.values()))
    assert len(totals) == 1, (
        f"total submit count varies across the ladder ({sorted(totals)}) — the scheduler graph "
        "must be k + O(1) control, independent of T and P (the reshaped anti-super-linear gate)"
    )


def test_submit_counts_are_row_count_independent(htex) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    lean = _spy_run(htex, repartition_blocks(adapter, copies=1), 6).fn_names()
    fat = _spy_run(htex, repartition_blocks(adapter, copies=8), 6).fn_names()
    assert lean == fat, (
        f"8x rows changed the submit counts ({dict(lean)} -> {dict(fat)}) — per-row/per-fragment "
        "task creation (the M39 anti-pattern the guard exists to kill)"
    )
