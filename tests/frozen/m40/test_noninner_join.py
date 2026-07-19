"""M40 iteration-2 — the NON-INNER relational contract for ``run_join`` (plan §3.3 "SQL/pandas
relational duplication", §4.2), on BOTH real backends. The inner-only frozen suite left every
non-inner mode uncovered; adversarial review found them broken (F1/F2/F3). This file freezes the
missing coverage.

The pin (plan §3.3): a distributed hash join is the SAME relation SQL/pandas produce.
- ``how=left``  keeps EVERY build (left) row; an unmatched build row emits ONE output row with the
  probe side's non-key fields NULL. ``how=right`` keeps every probe row. ``how=outer`` keeps both.
- **Key coalescing:** the ``on`` key column on a miss row takes the PRESENT side's value (SQL
  COALESCE) — it is NEVER null. Only the ABSENT side's NON-key fields are null.
- Duplication still holds (k matches ⇒ k rows) and each unmatched row appears EXACTLY ONCE — under
  broadcast that means once across ALL probe blocks, not once per block.

The reference is a DUPLICATING, NULL-PRESERVING, KEY-COALESCING oracle: ``pandas.merge(how=...)``.
It is a wholly independent relational engine (not the join kernel), so the executor cannot share a
bug with it. The observed reader is null-aware (awkward option → ``None``; numpy masked → ``None``),
so a null key or a dropped row is a real multiset difference, not a crash.

These tests FAIL against the pre-fix executor for the pinned reasons (each proven at freeze time):
- SHUFFLE drops any dest that is one-sided → unmatched build/probe rows vanish (F1). On awkward the
  probe-only survivor even carries a NULL join key (F3) instead of the coalesced value.
- BROADCAST joins each probe block against the whole build side → every unmatched build row is
  re-emitted once per probe block (F2), N-fold instead of once.
"""

from __future__ import annotations

from collections import Counter

import numpy as np
import pytest
from join_backends import JoinAdapter, adapters

from graphed_exec_local.shuffle import run_join

pd = pytest.importorskip("pandas")  # the independent relational oracle

ADAPTERS = adapters()

# a null-aware joined row: key is always present (coalesced); an absent side's value is None
NRow = tuple[int | None, int | None, int | None]


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> JoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


# ---- null-aware readers + the pandas oracle -----------------------------------------------------
def _col(c: object) -> list[int | None]:
    """Read one joined column to Python ints with ``None`` for a null/masked entry — the ONLY reader
    that survives the option/masked rows the non-inner arms produce (``JoinAdapter.joined_rows`` casts
    every element with ``int()`` and would crash on a miss row)."""
    to_list = getattr(c, "to_list", None)
    if to_list is not None:  # awkward option array -> [.., None, ..]
        return [None if v is None else int(v) for v in to_list()]
    arr = np.ma.asanyarray(c)  # numpy masked (or plain) structured field
    return [None if arr[i] is np.ma.masked else int(arr[i]) for i in range(len(arr))]


def observed_nullable(value: dict[int, object]) -> Counter[NRow]:
    """The whole relational result as one null-aware multiset over ALL blocks (partitioning-
    independent — the join RESULT does not depend on how it is partitioned)."""
    total: Counter[NRow] = Counter()
    for block in value.values():
        ks = _col(block["__joinkey__"])
        lv = _col(block["lval"])
        rv = _col(block["rval"])
        total += Counter(zip(ks, lv, rv, strict=True))
    return total


def pandas_oracle(
    lk: list[int], lv: list[int], rk: list[int], rv: list[int], how: str
) -> Counter[NRow]:
    """``pandas.merge`` on ``__joinkey__``: duplicating (k matches ⇒ k rows), null-preserving (absent
    side → NaN → ``None``), key-coalescing (the merged key column is always present). A wholly
    independent relational engine — no shared code with the join kernel under test."""
    left = pd.DataFrame({"__joinkey__": lk, "lval": lv})
    right = pd.DataFrame({"__joinkey__": rk, "rval": rv})
    merged = pd.merge(left, right, on="__joinkey__", how=how)
    out: Counter[NRow] = Counter()
    for _, row in merged.iterrows():
        key = int(row["__joinkey__"])  # coalesced: the merged key column never carries NaN here
        lval = None if pd.isna(row["lval"]) else int(row["lval"])
        rval = None if pd.isna(row["rval"]) else int(row["rval"])
        out[(key, lval, rval)] += 1
    return out


def _diff(observed: Counter[NRow], oracle: Counter[NRow]) -> str:
    missing = oracle - observed  # rows the relation requires but the executor did not emit
    extra = observed - oracle  # rows the executor emitted that the relation forbids
    return f"missing (dropped/miscoalesced)={dict(missing)} extra (over-emitted)={dict(extra)}"


# A scenario with UNMATCHED rows on BOTH sides so left/right/outer are each discriminating:
#   build keys {1, 2, 2, 4}   probe keys {2, 3, 3, 5}
#   key 2 = the only overlap (2 build rows x 1 probe row -> duplication); 1,4 build-only; 3,3,5 probe-only.
_LK, _LV = [1, 2, 2, 4], [10, 21, 22, 40]
_RK, _RV = [2, 3, 3, 5], [200, 300, 301, 500]


@pytest.mark.parametrize("how", ["left", "right", "outer"])
def test_shuffle_noninner_matches_pandas_oracle(adapter: JoinAdapter, how: str, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """SHUFFLE path, non-inner: the whole result must equal the duplicating null-preserving pandas
    oracle. Catches F1 (a one-sided dest is dropped, so unmatched build/probe rows vanish) and F3
    (an unmatched survivor's join key must be COALESCED to the present side, never null)."""
    left = [adapter.make_side(_LK, "lval", _LV)]
    right = [adapter.make_side(_RK, "rval", _RV)]
    oracle = pandas_oracle(_LK, _LV, _RK, _RV, how)
    # non-vacuity: the oracle really contains unmatched (None-bearing) rows for this how.
    assert any(None in row for row in oracle), "scenario must produce unmatched rows (else vacuous)"

    res = run_join(
        adapter.backend, left, right, 4, how=how, broadcast=False, workers=2, store_root=tmp_path
    )
    # mechanism witness: the SHUFFLE path actually ran (not a silent broadcast fallback).
    assert res.witness.broadcast_chosen is False and res.witness.large_side_blocks > 0, (
        "the shuffle path must route the probe side through map_write"
    )

    observed = observed_nullable(res.value)

    # (F3) every emitted row's join key is coalesced from the present side — never null.
    assert all(key is not None for (key, _, _) in observed), (
        f"how={how}: the join key must be COALESCED (present-side value), never null; got {dict(observed)}"
    )
    # (F1) left/outer keep ALL build rows; right/outer keep ALL probe rows — projected as (key,val).
    if how in ("left", "outer"):
        build_proj = Counter(zip(_LK, _LV, strict=True))
        assert build_proj <= Counter((k, lv) for (k, lv, _) in observed), (
            f"how={how}: left/outer must keep every build row; {_diff(observed, oracle)}"
        )
    if how in ("right", "outer"):
        probe_proj = Counter(zip(_RK, _RV, strict=True))
        assert probe_proj <= Counter((k, rv) for (k, _, rv) in observed), (
            f"how={how}: right/outer must keep every probe row; {_diff(observed, oracle)}"
        )
    # the comprehensive relational check (value + null-ness + duplication).
    assert observed == oracle, f"how={how}: {_diff(observed, oracle)}"


# Broadcast scenario: a small build side with rows that NEVER match, replicated to every node, joined
# against several probe blocks. Under the bug each unmatched build row re-appears once per probe block.
_B_LK, _B_LV = [1, 2, 3], [10, 20, 30]  # tiny build side (1 and 3 will be unmatched)
_B_PROBE = [([2, 2], [200, 201]), ([2], [202]), ([2], [203])]  # 3 probe blocks, only key 2 matches


@pytest.mark.parametrize("how", ["left", "outer"])
def test_broadcast_noninner_unmatched_build_emitted_once(adapter: JoinAdapter, how: str, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """BROADCAST path, left/outer: an unmatched build row must appear EXACTLY ONCE across ALL probe
    blocks. Catches F2 — the current path joins each probe block against the whole build side, so it
    re-emits every unmatched build row once per probe block (N-fold)."""
    left = [adapter.make_side(_B_LK, "lval", _B_LV)]
    right = [adapter.make_side(k, "rval", v) for (k, v) in _B_PROBE]
    rk = [k for (ks, _) in _B_PROBE for k in ks]
    rv = [v for (_, vs) in _B_PROBE for v in vs]
    oracle = pandas_oracle(_B_LK, _B_LV, rk, rv, how)

    res = run_join(
        adapter.backend, left, right, 4, how=how, broadcast=True, workers=3, store_root=tmp_path
    )
    # mechanism witness: the BROADCAST path actually ran (build replicated to every node, probe not
    # shuffled) — so the exactly-once assertion below is testing broadcast, not a shuffle fallback.
    assert res.witness.broadcast_chosen is True, "broadcast=True must be recorded as chosen"
    assert res.witness.broadcast_puts == 3, "the build side must be replicated to every node"
    assert res.witness.large_side_blocks == 0, "the probe side must NOT be shuffled under broadcast"

    observed = observed_nullable(res.value)

    # (F2) the unmatched build rows (keys 1 and 3, no probe match) each appear EXACTLY ONCE total.
    unmatched_build = {(1, 10, None), (3, 30, None)}
    for row in unmatched_build:
        assert observed[row] == 1, (
            f"how={how}: unmatched build row {row} must appear exactly once across all probe blocks, "
            f"got {observed[row]} (broadcast N-fold duplication); {_diff(observed, oracle)}"
        )
    # the comprehensive relational check: broadcast must equal the same duplicating pandas oracle.
    assert observed == oracle, f"how={how}: broadcast != oracle; {_diff(observed, oracle)}"
