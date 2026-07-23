"""m46 relay engine — F6 plan choice + the broadcast plane on HTEX (plan §1.3; §3 m46
`test_parsl_relay_broadcast_join`; the m43 `test_dask_join_plan_choice` lineage).

The discrimination gap is built into the scenario: with byte-comparable sides and ``parts=8`` the
pinned rule (`graphed.shuffle.broadcast_join_choice`, keyed on ``parts``) says SHUFFLE, while the
same rule keyed on a live ``n_workers()==1`` says BROADCAST — so a live-recompute implementation
takes the broadcast path on a 1-worker pool and fails the Counter gates. The 1-vs-2-worker runs
must agree bit-for-bit (topology invariance). The path taken is witnessed STRUCTURALLY through
the raw submitted fn-name Counter, never inferred from the value: broadcast ⇒ zero stage-1
submits for either side and exactly one `_dask_broadcast_join_part` per probe block (the large
side is NEVER shuffled — its bytes ride `backend.broadcast`, degenerate per-task reship on
scatter_broadcast=False); the how=left unmatched-build tail is EXACTLY one extra part submit
(once-only, the m40 F2 pin). Forced choices are honoured against the model in both directions.

Discriminates: the F6 live-`n_workers()` recompute, an executor overriding a forced choice, a
"broadcast" that secretly map-writes the large side, and a per-probe-block re-emission of
unmatched build rows (tail count and multiset both fail)."""

from __future__ import annotations

import sys

import pytest
from graphed.shuffle import broadcast_join_choice
from parsl_harness import (
    BACKEND,
    SpyParslBackend,
    dup_join_all,
    htex_pool,
    join_equal_sides,
    join_sides,
    join_small_build_sides,
    make_parsl_backend,
    make_runner,
    nullable_join_multiset,
    observed_join_all,
    relay_join,
    run_bounded,
    side_bytes,
)

from graphed_executors.local.shuffle import run_join

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (parsl ships no Windows support; Windows keeps the base "
    "package only — plan §4 G8)",
)

PARTS = 8


@pytest.fixture(scope="module")
def htex1():  # type: ignore[no-untyped-def]
    with htex_pool(workers=1) as ex:
        yield ex


@pytest.fixture(scope="module")
def htex2():  # type: ignore[no-untyped-def]
    with htex_pool(workers=2) as ex:
        yield ex


def _run(htex, left, right, parts: int, **kwargs):  # type: ignore[no-untyped-def]
    spy = SpyParslBackend(make_parsl_backend(htex))
    res = run_bounded(lambda: relay_join(BACKEND, left, right, parts, runner=make_runner(spy), **kwargs))
    return spy, res


def test_auto_choice_is_parts_keyed_and_worker_count_invariant(htex1, htex2) -> None:  # type: ignore[no-untyped-def]
    left, right = join_equal_sides()
    bb, pb = side_bytes(left), side_bytes(right)
    # scenario sanity: the DISCRIMINATION GAP is real for these measured bytes
    assert broadcast_join_choice(bb, pb, PARTS) is False, "parts-keyed rule must say SHUFFLE here"
    assert broadcast_join_choice(bb, pb, 1) is True, "worker-keyed rule on 1 worker must say BROADCAST"

    spy1, r1 = _run(htex1, left, right, PARTS)
    names1 = spy1.fn_names()
    assert names1["_dask_map_write"] > 0, (
        "auto choice on a 1-WORKER pool must still SHUFFLE (parts-keyed rule) — a broadcast here "
        "is the F6 live-n_workers() recompute bug"
    )
    assert names1["_dask_broadcast_join_part"] == 0
    assert r1.witness.broadcast_chosen is False

    spy2, r2 = _run(htex2, left, right, PARTS)
    assert r2.witness.broadcast_chosen is False
    assert spy2.fn_names()["_dask_broadcast_join_part"] == 0
    assert r1.dest_block_hashes == r2.dest_block_hashes, (
        "the same logical join produced different content-addressed blocks under 1 vs 2 workers"
    )
    assert r1.dest_block_hashes, "no joined blocks produced — scenario degenerate"


def test_forced_broadcast_never_shuffles_the_large_side(htex2) -> None:  # type: ignore[no-untyped-def]
    left, right = join_equal_sides()
    bb, pb = side_bytes(left), side_bytes(right)
    assert broadcast_join_choice(bb, pb, PARTS) is False  # the model would shuffle: the force is real

    spy, res = _run(htex2, left, right, PARTS, broadcast=True)
    names = spy.fn_names()
    assert (
        names["_dask_map_write"] == 0
        and names["_dask_pick"] == 0
        and names["_dask_gather"] == 0
        and names["_dask_gather_join"] == 0
    ), "forced broadcast must retire stage-1 (and its gather-join) entirely — nothing is shuffled"
    assert names["_dask_broadcast_join_part"] == len(right), (
        "one broadcast-join part per probe block (the §1.3 relay broadcast shape)"
    )
    assert res.witness.broadcast_chosen is True
    assert res.witness.head_node_routed is True  # the relay label rides every arm (design-M2)
    assert observed_join_all(res.value) == dup_join_all(left, right, PARTS), (
        "broadcast result diverged from the duplicating relational oracle"
    )


def test_unmatched_build_tail_is_exactly_one_extra_part(htex2) -> None:  # type: ignore[no-untyped-def]
    # MEASURED: the how=left null-filled tail produces numpy MASKED blocks, wired over Arrow —
    # pyarrow is a runtime need here exactly as in the non-inner relay arms.
    pytest.importorskip("pyarrow", reason="non-inner numpy joins wire masked blocks over Arrow")
    parts = 4
    left, right = join_sides()  # left-only key 13 (twice) -> a REAL unmatched build tail
    spy, res = _run(htex2, left, right, parts, how="left", broadcast=True)

    names = spy.fn_names()
    assert names["_dask_map_write"] == 0, "the broadcast path must not shuffle either side"
    assert names["_dask_broadcast_join_part"] == len(right) + 1, (
        f"how=left with unmatched build rows must submit len(right)={len(right)} parts + EXACTLY "
        f"one once-only tail, got {names['_dask_broadcast_join_part']} — more is per-block "
        "re-emission (duplicated unmatched rows), fewer drops them"
    )
    got = nullable_join_multiset(res.value)
    local = run_join(BACKEND, left, right, parts, how="left", broadcast=True)
    assert got == nullable_join_multiset(local.value), "broadcast-left diverged from the local engine"
    n13 = sum(c for (k, _lv, rv), c in got.items() if k == 13 and rv is None)
    assert n13 == 2, "the two unmatched build rows (key 13) must appear EXACTLY once each"


def test_forced_shuffle_is_honoured_against_the_cost_model(htex2) -> None:  # type: ignore[no-untyped-def]
    parts = 4  # measured: probe ≈ 6x build bytes -> the pinned rule says BROADCAST at 4 parts
    left, right = join_small_build_sides()
    assert broadcast_join_choice(side_bytes(left), side_bytes(right), parts) is True

    spy, res = _run(htex2, left, right, parts, broadcast=False)
    names = spy.fn_names()
    assert names["_dask_broadcast_join_part"] == 0, "forced shuffle must not take the broadcast path"
    assert names["_dask_map_write"] > 0
    assert res.witness.broadcast_chosen is False
    assert observed_join_all(res.value) == dup_join_all(left, right, parts)
