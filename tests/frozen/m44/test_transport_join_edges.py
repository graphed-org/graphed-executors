"""m44 FROZEN FIXUP — join edge surfaces mutation testing found frozen-unexecuted (orchestrator
mandate after REVIEW; the m43 `test_dask_join_parity_edges.py` precedent, new repo-unique
basename). Two gaps, pinned through the SAME entry points and harness as the main suite:

GAP-A — broadcast left/outer with never-matched BUILD rows: the forced-broadcast path must emit
the null-fill tail for build rows that match nothing in the probe (and, under outer, the
probe-only tail too). Witnessed relationally (== local ``run_join(broadcast=True)`` == the
null-aware pandas oracle, option-typed None — the ``-1``-sentinel trap re-armed on the broadcast
tail) AND structurally (the broadcast path really ran: ``broadcast_chosen``, ZERO
``_transport_map_task``, one ``_transport_broadcast_join_part`` per probe block).

GAP-B — one side entirely EMPTY, all hows: the entry points distinguish a zero-PARTITION side
(no carrier: the local oracle passes the surviving side through WITHOUT the missing column) from
a zero-ROW carrier block (full 3-field null-filled output, pandas-equivalent) — both measured on
the local engine at authoring time (row table below). Pinned by byte-exact ``dest_block_hashes``
equality with the local oracle (field-shape-agnostic), the measured row totals,
``witness.join_output_rows == rows`` (counter sanity), and — for the carrier cases — the pandas
multiset. Every transport call runs under the F2 hard-timeout driver: an engine that WAITS on a
side with zero map tasks must surface as a failure, never a hang.

Measured local-oracle row totals (7 = the surviving side's rows pass through / null-fill):
inner: 0/0 · left: L-empty 0, R-empty 7 · right: L-empty 7, R-empty 0 · outer: 7/7.

Discriminates: a broadcast join that drops (or double-emits) the unmatched build tail; a
sentinel-null fill; an engine that crashes or hangs on a partitionless side; one that treats a
zero-row carrier like a missing side (loses the null-filled column, hashes diverge); a counter
that stops naming the emitted rows."""

from __future__ import annotations

import pytest
from transport_harness import (
    SpyDaskBackend,
    TransportAdapter,
    build_dask_backend,
    install_transport_plugin,
    nullable_join_multiset,
    pandas_join_oracle,
    run_bounded,
    side_columns,
    total_result_rows,
    transport_adapters,
    transport_cluster,
    transport_join,
)

from graphed_executors.local.shuffle import run_join

pytest.importorskip("distributed")
pytest.importorskip("pandas")

ADAPTERS = transport_adapters()
PARTS = 8

# the authoring-time measured local-oracle row totals for one-side-empty joins
EXPECT_ROWS = {
    ("inner", "left"): 0,
    ("inner", "right"): 0,
    ("left", "left"): 0,
    ("left", "right"): 7,
    ("right", "left"): 7,
    ("right", "right"): 0,
    ("outer", "left"): 7,
    ("outer", "right"): 7,
}


@pytest.fixture(scope="module")
def client():
    with transport_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> TransportAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _bcast_tail_sides(adapter: TransportAdapter) -> tuple[list[object], list[object]]:
    """Build keys 98/99 match NOTHING in the probe (the GAP-A null-fill tail); probe key 55
    matches nothing in the build (outer's other tail)."""
    left = [adapter.make_side([1, 2, 3, 98, 99], "lval", [10, 20, 30, 980, 990])]
    probe_keys = [1, 1, 2, 3, 3, 3, 2, 2, 1, 3, 1, 2, 3, 1, 2, 3, 2, 55]
    right = [
        adapter.make_side(probe_keys, "rval", [100 * b + i for i in range(len(probe_keys))])
        for b in (1, 2, 3)
    ]
    return left, right


def _full_side(adapter: TransportAdapter, field: str) -> list[object]:
    return [
        adapter.make_side([1, 2, 3, 7], field, [10, 20, 30, 70]),
        adapter.make_side([3, 5, 1], field, [31, 50, 11]),
    ]


@pytest.mark.parametrize("how", ["left", "outer"])
def test_broadcast_join_emits_the_null_filled_build_tail(adapter: TransportAdapter, client, how: str) -> None:  # type: ignore[no-untyped-def]
    left, right = _bcast_tail_sides(adapter)
    local = run_join(adapter.backend, left, right, PARTS, how=how, broadcast=True)

    install_transport_plugin(client)
    spy = SpyDaskBackend(build_dask_backend(client))
    outcome = run_bounded(
        lambda: transport_join(adapter.backend, left, right, PARTS, how=how, dbackend=spy, broadcast=True),
        timeout_s=240.0,
    )
    assert "error" not in outcome, f"forced-broadcast {how} join raised: {outcome.get('error')!r}"
    r = outcome["result"]

    # the broadcast path really ran (the GAP-A surface, not a silent shuffle fallback)
    assert r.witness.broadcast_chosen is True
    names = spy.submitted_fn_names()
    assert names["_transport_map_task"] == 0, f"broadcast {how} join shuffled a side: {dict(names)}"
    # one pinned part task per LOCAL-oracle output partition: len(right) probe parts + the
    # unmatched-build tail partition the non-inner arms emit (measured: local dests == [0,1,2,3])
    assert names["_transport_broadcast_join_part"] == len(local.value), (
        f"how={how}: {names['_transport_broadcast_join_part']} broadcast part tasks for "
        f"{len(local.value)} local-oracle output partitions — the null-fill tail partition is "
        "missing or duplicated"
    )

    got = nullable_join_multiset(r.value)
    assert got == nullable_join_multiset(local.value), f"how={how}: diverged from the local oracle"
    lk, lv = side_columns(left, adapter, "lval")
    rk, rv = side_columns(right, adapter, "rval")
    assert got == pandas_join_oracle(lk, lv, rk, rv, how), (
        f"how={how}: diverged from pandas — unmatched build rows must surface with option-typed "
        "None rval, never a sentinel and never dropped"
    )
    null_tail = sum(1 for row in got if row[2] is None)
    assert null_tail >= 2, f"how={how}: the never-matched build rows (98, 99) emitted no null tail"
    if how == "outer":
        assert any(row[1] is None for row in got), "outer lost the probe-only (55) null tail"
    assert r.dest_block_hashes == local.dest_block_hashes, f"how={how}: broadcast bytes diverged from local"
    assert r.witness.join_output_rows == sum(got.values())


@pytest.mark.parametrize("kind", ["none", "carrier"])
@pytest.mark.parametrize("empty_side", ["left", "right"])
@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
def test_one_side_empty_matches_the_local_oracle(adapter: TransportAdapter, client, how: str, empty_side: str, kind: str) -> None:  # type: ignore[no-untyped-def]
    empty_field = "lval" if empty_side == "left" else "rval"
    empty: list[object] = [] if kind == "none" else [adapter.make_side([], empty_field, [])]
    if empty_side == "left":
        left, right = empty, _full_side(adapter, "rval")
    else:
        left, right = _full_side(adapter, "lval"), empty

    local = run_join(adapter.backend, left, right, PARTS, how=how, broadcast=False)

    install_transport_plugin(client)
    dbackend = build_dask_backend(client)
    outcome = run_bounded(
        lambda: transport_join(adapter.backend, left, right, PARTS, how=how, dbackend=dbackend, broadcast=False),
        timeout_s=240.0,
    )
    assert "error" not in outcome, (
        f"{how}/{empty_side}-empty/{kind}: the engine crashed on a "
        f"{'partitionless' if kind == 'none' else 'zero-row carrier'} side: {outcome.get('error')!r}"
    )
    r = outcome["result"]

    rows = total_result_rows(r.value)
    assert rows == EXPECT_ROWS[(how, empty_side)], (
        f"{how}/{empty_side}-empty/{kind}: {rows} rows, expected {EXPECT_ROWS[(how, empty_side)]} "
        "(the measured local-oracle table)"
    )
    assert r.dest_block_hashes == local.dest_block_hashes, (
        f"{how}/{empty_side}-empty/{kind}: bytes diverged from the local oracle — the "
        "zero-partition pass-through / carrier null-fill distinction was not preserved"
    )
    assert r.witness.join_output_rows == rows, "join_output_rows stopped naming the emitted rows"
    if kind == "carrier" and rows:
        lk, lv = side_columns(left, adapter, "lval")
        rk, rv = side_columns(right, adapter, "rval")
        assert nullable_join_multiset(r.value) == pandas_join_oracle(lk, lv, rk, rv, how), (
            f"{how}/{empty_side}-empty/carrier: the null-filled column diverged from pandas"
        )
