"""M40 iteration-3 — EMPTY / ZERO-ROW partitions in a distributed NON-INNER ``run_join`` (plan §3.3
"SQL/pandas relational duplication", the null carrier + key coalescing), on BOTH real backends.

The iteration-2 non-inner suite (``test_noninner_join.py``) only ships NON-EMPTY blocks on both sides,
so two realistic HEP shapes are UNCOVERED and slip through the green gates:

  * a **zero-row LEADING block** (index 0) on the ABSENT side of a left/right/outer join — a skim/cut
    that empties the first partition while it still carries its full schema/dtype. Under §3.3 the join
    must keep every present-side row (unmatched -> null other side) and null-fill against the empty
    block's schema. The current path instead ``take``s index 0 out of the length-0 concatenated block
    and raises ``IndexError`` (awkward: "cannot slice RecordArray (of length 0) with array([0])";
    numpy: "index 0 is out of bounds for axis 0 with size 0"). The bug is SPECIFICALLY the LEADING
    block: with the empty block in a MIDDLE/TRAILING slot the result is already correct (measured), so
    each test below asserts position-invariance — the empty block is swept across leading/middle/
    trailing and the whole relation must equal the oracle for every position; the LEADING iteration is
    the one that fails on HEAD.
  * an **entirely partitionless absent side** (``right_blocks == []`` for how=left, or
    ``left_blocks == []`` for how=right): the join silently returns ZERO rows, dropping every
    present-side (build/probe) row. A partitionless side carries NO schema the executor can null-fill
    from, so the ONLY contract pinned here is "no present-side row is silently dropped" (we do NOT
    assert the absent side's column nulls — there is no schema to produce them).

The reference is a wholly independent relational engine, ``pandas.merge`` — never the join kernel — so
the executor cannot share a bug with the oracle. Expected values are hand-derived from SQL/pandas
LEFT/RIGHT/OUTER semantics only. The reader is null-aware (awkward option -> ``None``; numpy masked ->
``None``), so a dropped row is a real multiset difference, not a crash.
"""

from __future__ import annotations

from collections import Counter

import numpy as np
import pytest
from join_backends import JoinAdapter, adapters

from graphed_exec_local.shuffle import run_join

pd = pytest.importorskip("pandas")  # the independent relational oracle

ADAPTERS = adapters()

# a null-aware joined row: the key is always present (coalesced), an absent side's value is None
NRow = tuple[int | None, int | None, int | None]


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> JoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


# ---- null-aware readers + the pandas oracle -----------------------------------------------------
def _col(c: object) -> list[int | None]:
    """One joined column -> Python ints with ``None`` for a null/masked entry (the only reader that
    survives the option/masked rows the non-inner arms produce)."""
    to_list = getattr(c, "to_list", None)
    if to_list is not None:  # awkward option array -> [.., None, ..]
        return [None if v is None else int(v) for v in to_list()]
    arr = np.ma.asanyarray(c)  # numpy masked (or plain) structured field
    return [None if arr[i] is np.ma.masked else int(arr[i]) for i in range(len(arr))]


def observed_nullable(value: dict[int, object]) -> Counter[NRow]:
    """The whole relational result as one null-aware multiset over ALL blocks (partitioning is
    irrelevant — the join RELATION does not depend on how it is partitioned)."""
    total: Counter[NRow] = Counter()
    for block in value.values():
        total += Counter(zip(_col(block["__joinkey__"]), _col(block["lval"]), _col(block["rval"]), strict=True))
    return total


def observed_pairs(value: dict[int, object], field: str) -> Counter[tuple[int | None, int | None]]:
    """(key, ``field``) projection over ALL blocks — used for the partitionless-side "no drop" check,
    which cannot read the (schema-less) absent side's column."""
    total: Counter[tuple[int | None, int | None]] = Counter()
    for block in value.values():
        total += Counter(zip(_col(block["__joinkey__"]), _col(block[field]), strict=True))
    return total


def pandas_oracle(lk: list[int], lv: list[int], rk: list[int], rv: list[int], how: str) -> Counter[NRow]:
    """``pandas.merge`` on ``__joinkey__``: duplicating (k matches => k rows), null-preserving (absent
    side -> NaN -> ``None``), key-coalescing (the merged key column is always present). A wholly
    independent relational engine — no shared code with the join kernel under test."""
    left = pd.DataFrame({"__joinkey__": lk, "lval": lv})
    right = pd.DataFrame({"__joinkey__": rk, "rval": rv})
    merged = pd.merge(left, right, on="__joinkey__", how=how)
    out: Counter[NRow] = Counter()
    for _, row in merged.iterrows():
        key = int(row["__joinkey__"])  # coalesced: never NaN
        lval = None if pd.isna(row["lval"]) else int(row["lval"])
        rval = None if pd.isna(row["rval"]) else int(row["rval"])
        out[(key, lval, rval)] += 1
    return out


def _diff(observed: Counter[NRow], oracle: Counter[NRow]) -> str:
    return f"missing (dropped)={dict(oracle - observed)} extra={dict(observed - oracle)}"


def _sides_with_empty_at(adapter: JoinAdapter, real: list[object], empty: object, pos: int) -> list[object]:
    blocks = list(real)
    blocks.insert(pos, empty)
    return blocks


# ---- shuffle path: a zero-row LEADING block on the ABSENT side ----------------------------------
def test_shuffle_left_zero_row_leading_right_block(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """SHUFFLE, how=left: an unmatched build row must survive even when the RIGHT (probe/absent) side
    has a zero-row LEADING block. Build {1,2,4}; probe = two real {2} blocks with an empty block swept
    across leading/middle/trailing. The whole relation must equal the pandas LEFT-merge for EVERY
    position; on HEAD the LEADING position raises IndexError (take index 0 out of a length-0 block)."""
    lk, lv = [1, 2, 4], [10, 20, 40]
    rk, rv = [2, 2], [200, 201]  # union of the two real probe blocks
    oracle = pandas_oracle(lk, lv, rk, rv, "left")
    assert any(None in row for row in oracle), "scenario must produce unmatched build rows (else vacuous)"

    real = [adapter.make_side([2], "rval", [200]), adapter.make_side([2], "rval", [201])]
    empty = adapter.make_side([], "rval", [])  # zero-row block, full schema
    for pos in (0, 1, 2):  # leading first: this is the position that fails on HEAD
        right = _sides_with_empty_at(adapter, real, empty, pos)
        res = run_join(adapter.backend, [adapter.make_side(lk, "lval", lv)], right, 4,
                       how="left", broadcast=False, workers=2, store_root=tmp_path)
        assert res.witness.broadcast_chosen is False, "must exercise the shuffle path"
        observed = observed_nullable(res.value)
        assert all(key is not None for (key, _, _) in observed), f"pos={pos}: join key must be coalesced, never null"
        assert observed == oracle, f"pos={pos} (0=leading): {_diff(observed, oracle)}"


def test_shuffle_right_zero_row_leading_left_block(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """SHUFFLE, how=right (probe-only survival): every probe row must survive when the LEFT
    (build/absent) side has a zero-row LEADING block. Symmetric to the left case."""
    lk, lv = [2, 2], [20, 21]  # union of the two real build blocks
    rk, rv = [1, 2, 3], [100, 200, 300]
    oracle = pandas_oracle(lk, lv, rk, rv, "right")
    assert any(None in row for row in oracle), "scenario must produce unmatched probe rows (else vacuous)"

    real = [adapter.make_side([2], "lval", [20]), adapter.make_side([2], "lval", [21])]
    empty = adapter.make_side([], "lval", [])
    for pos in (0, 1, 2):
        left = _sides_with_empty_at(adapter, real, empty, pos)
        res = run_join(adapter.backend, left, [adapter.make_side(rk, "rval", rv)], 4,
                       how="right", broadcast=False, workers=2, store_root=tmp_path)
        assert res.witness.broadcast_chosen is False, "must exercise the shuffle path"
        observed = observed_nullable(res.value)
        assert all(key is not None for (key, _, _) in observed), f"pos={pos}: join key must be coalesced, never null"
        assert observed == oracle, f"pos={pos} (0=leading): {_diff(observed, oracle)}"


# ---- broadcast path: a zero-row LEADING probe block ---------------------------------------------
@pytest.mark.parametrize("how", ["left", "outer"])
def test_broadcast_noninner_zero_row_leading_probe_block(adapter: JoinAdapter, how: str, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """BROADCAST, left/outer: a small build side replicated to every node, joined against probe blocks
    the FIRST of which is zero-row. Every unmatched build row must survive exactly once; on HEAD the
    leading empty probe block raises IndexError before any row is emitted."""
    lk, lv = [1, 2, 3], [10, 20, 30]  # keys 1,3 never match -> unmatched build rows
    rk, rv = [2, 2], [200, 201]  # union of the real probe blocks (only key 2 matches)
    oracle = pandas_oracle(lk, lv, rk, rv, how)
    assert any(None in row for row in oracle), "scenario must produce unmatched build rows (else vacuous)"

    real = [adapter.make_side([2, 2], "rval", [200, 201])]
    empty = adapter.make_side([], "rval", [])
    right = [empty, *real]  # zero-row LEADING probe block
    res = run_join(adapter.backend, [adapter.make_side(lk, "lval", lv)], right, 4,
                   how=how, broadcast=True, workers=3, store_root=tmp_path)
    # mechanism witness: the BROADCAST path actually ran (not a silent shuffle fallback).
    assert res.witness.broadcast_chosen is True, "broadcast=True must be recorded as chosen"
    assert res.witness.large_side_blocks == 0, "the probe side must NOT be shuffled under broadcast"

    observed = observed_nullable(res.value)
    assert all(key is not None for (key, _, _) in observed), f"how={how}: join key must be coalesced, never null"
    # each unmatched build row appears EXACTLY once across all probe blocks (not once per block).
    for row in ((1, 10, None), (3, 30, None)):
        assert observed[row] == 1, f"how={how}: unmatched build {row} must appear once; {_diff(observed, oracle)}"
    assert observed == oracle, f"how={how}: {_diff(observed, oracle)}"


# ---- entirely partitionless absent side: no present-side row may be dropped ---------------------
def test_left_join_entirely_empty_right_keeps_all_build(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """how=left with an ENTIRELY empty right side (``right_blocks == []``): a partitionless probe side
    has no schema to null-fill from, so the ONLY pinned contract is that EVERY build row survives
    (SQL LEFT JOIN keeps all left rows). On HEAD the join silently returns zero rows — the build rows
    are lost. We assert only the (key, lval) projection; the absent side's nulls are not asserted."""
    lk, lv = [1, 2, 4], [10, 20, 40]
    res = run_join(adapter.backend, [adapter.make_side(lk, "lval", lv)], [], 4,
                   how="left", broadcast=False, workers=2, store_root=tmp_path)
    build = Counter(zip(lk, lv, strict=True))
    observed = observed_pairs(res.value, "lval")
    assert build <= observed, (
        f"how=left, empty right side: every build row must survive (SQL LEFT keeps all left rows); "
        f"missing={dict(build - observed)} (total kept={sum(observed.values())}, expected>={len(lk)})"
    )


def test_right_join_entirely_empty_left_keeps_all_probe(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """how=right with an ENTIRELY empty left side (``left_blocks == []``): symmetric — every probe row
    must survive. On HEAD the join silently returns zero rows."""
    rk, rv = [1, 2, 4], [100, 200, 400]
    res = run_join(adapter.backend, [], [adapter.make_side(rk, "rval", rv)], 4,
                   how="right", broadcast=False, workers=2, store_root=tmp_path)
    probe = Counter(zip(rk, rv, strict=True))
    observed = observed_pairs(res.value, "rval")
    assert probe <= observed, (
        f"how=right, empty left side: every probe row must survive (SQL RIGHT keeps all right rows); "
        f"missing={dict(probe - observed)} (total kept={sum(observed.values())}, expected>={len(rk)})"
    )
