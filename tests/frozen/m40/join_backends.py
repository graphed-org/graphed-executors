"""Shared harness for the M40 exec-local distributed-JOIN suite: BOTH real backends + the pinned
``run_join`` contract + a TEST-AUTHORED relational oracle.

M40 extends the M39 two-phase exchange into a distributed hash JOIN. The same a2 backend-independence
discipline applies: the generic join kernel deals only in the opaque ``ShuffleBackend`` primitives
(the M39 exchange half + the M40 ``match_indices``/``take``/``merge_records`` half), so its correctness
is witnessed by EXECUTION over the REAL ``AwkwardBackend`` AND ``NumpyBackend`` — an awkward-shaped
join primitive fails on numpy.

Pinned execution contract (test-author decision — the implementer builds ``graphed_exec_local.shuffle``
to this; see the m40 README). ``run_join`` mirrors M39 ``run_repartition`` field-for-field, with two
inputs (``left_blocks`` = build side, ``right_blocks`` = probe side):

    run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",), how="inner",
             workers=1, comms="ipc", store_root=None, broadcast=None, salt=0,
             mem_budget_bytes=None, steal=False, faults=ShuffleFaults()) -> ShuffleResult

    broadcast_join_choice(build_bytes, probe_bytes, n_nodes) -> bool   # the pinned cost-model rule

``broadcast=None`` = the cost model chooses (recorded, deterministic); ``broadcast=True/False`` = the
executor HONOURS a plan-recorded choice (E5). ``ShuffleResult.value`` is ``{part -> joined block}``
(``part`` = dest under shuffle, probe-partition index under broadcast — so the JOIN RESULT is the union
over blocks, while the partitioning differs). ``dest_block_hashes`` is the content-addressed
determinism key. ``ShuffleWitness`` gains the join counters (see the README).

The relational contract (§3.3 pin, trap 3): a probe row with k matching build rows ⇒ **k output rows**
(SQL/pandas merge duplication), NOT a list-of-matches / grouped shape. The oracle below is a DUPLICATING
merge, so a grouped/dedup impl fails it. The oracle reuses only the pre-existing M39 ``partition`` (the
golden-vector-pinned routing) + plain field reads — never the join kernel — so the executor cannot
share a bug with the oracle.
"""

from __future__ import annotations

import contextlib
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import numpy as np
import pytest

# a joined output row, backend-independent: (key, left value, right value)
Row = tuple[int, int, int]


@dataclass
class JoinAdapter:
    name: str
    backend: object
    # (keys, value_field, values) -> a 2-column record block carrying __joinkey__ + value_field
    make_side: Callable[[Sequence[int], str, Sequence[int]], object]
    # a side block + its value field -> [(key, value), ...] (uses only pre-existing reads)
    side_rows: Callable[[object, str], list[tuple[int, int]]]
    # a JOINED block (fields __joinkey__, lval, rval) -> [(key, lval, rval), ...]
    joined_rows: Callable[[object], list[Row]]


def _awkward_adapter() -> JoinAdapter:
    ak = pytest.importorskip("awkward")
    from graphed.awkward import AwkwardBackend  # noqa: PLC0415

    def make_side(keys: Sequence[int], field: str, values: Sequence[int]) -> object:
        return ak.Array(
            {
                "__joinkey__": np.array(list(keys), dtype=np.uint64),
                field: np.array(list(values), dtype=np.int64),
            }
        )

    def side_rows(block: object, field: str) -> list[tuple[int, int]]:
        ks = [int(k) for k in block["__joinkey__"].to_list()]
        vs = [int(v) for v in block[field].to_list()]
        return list(zip(ks, vs, strict=True))

    def joined_rows(block: object) -> list[Row]:
        ks = [int(k) for k in block["__joinkey__"].to_list()]
        lv = [int(v) for v in block["lval"].to_list()]
        rv = [int(v) for v in block["rval"].to_list()]
        return list(zip(ks, lv, rv, strict=True))

    return JoinAdapter("awkward", AwkwardBackend(), make_side, side_rows, joined_rows)


def _numpy_adapter() -> JoinAdapter:
    from graphed.numpy import NumpyBackend  # noqa: PLC0415

    def make_side(keys: Sequence[int], field: str, values: Sequence[int]) -> object:
        dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
        block = np.zeros(len(keys), dtype=dt)
        block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
        block[field] = np.array(list(values), dtype=np.int64)
        return block

    def side_rows(block: object, field: str) -> list[tuple[int, int]]:
        return list(zip([int(k) for k in block["__joinkey__"]], [int(v) for v in block[field]], strict=True))

    def joined_rows(block: object) -> list[Row]:
        return list(
            zip(
                [int(k) for k in block["__joinkey__"]],
                [int(v) for v in block["lval"]],
                [int(v) for v in block["rval"]],
                strict=True,
            )
        )

    return JoinAdapter("numpy", NumpyBackend(), make_side, side_rows, joined_rows)


def adapters() -> list[JoinAdapter]:
    out: list[JoinAdapter] = []
    for factory in (_awkward_adapter, _numpy_adapter):
        with contextlib.suppress(Exception):  # a backend not installed drops from the parametrization
            out.append(factory())
    return out


# ---- scenario builders (shared across themes) ---------------------------------------------------
def broadcast_sides(adapter: JoinAdapter) -> tuple[list[object], list[object]]:
    """A SMALL build (left) side + a LARGER probe (right) side over a shared key population, so the
    cost model prefers broadcast and the (b) theme can witness the large side is never shuffled."""
    left = [adapter.make_side([1, 2, 3], "lval", [10, 20, 30])]  # tiny build side (one small block)
    right = [
        adapter.make_side([1, 1, 2, 3, 3, 3], "rval", [100, 101, 200, 300, 301, 302]),
        adapter.make_side([2, 2, 1, 3, 1, 2], "rval", [201, 202, 102, 303, 103, 203]),
        adapter.make_side([3, 1, 2, 3, 2, 1], "rval", [304, 104, 204, 305, 205, 105]),
    ]
    return left, right


def general_sides(adapter: JoinAdapter) -> tuple[list[object], list[object]]:
    """A multi-block, multi-key both-large scenario (several src_pids per side, overlapping keys so a
    dest gathers rows from several src_pids) — the co-partitioned shuffle join used by determinism."""
    left = [
        adapter.make_side([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
        adapter.make_side([5, 4, 3, 2, 1, 0], "lval", [16, 17, 18, 19, 20, 21]),
        adapter.make_side([1, 1, 2, 2, 3, 3], "lval", [22, 23, 24, 25, 26, 27]),
        adapter.make_side([7, 8, 9, 0, 1, 2], "lval", [28, 29, 30, 31, 32, 33]),
    ]
    right = [
        adapter.make_side([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
        adapter.make_side([3, 3, 2, 1, 1, 4], "rval", [106, 107, 108, 109, 110, 111]),
        adapter.make_side([9, 8, 7, 2, 1, 0], "rval", [112, 113, 114, 115, 116, 117]),
    ]
    return left, right


def skewed_sides(adapter: JoinAdapter) -> tuple[list[object], list[object]]:
    """A heavily skewed key: ~90% of rows carry the single hot key 7, so a naive hash-route piles one
    dest sky-high — the (f) salt theme must still be deterministic and relationally correct."""
    hot = 7
    lkeys = [hot] * 27 + [3, 11, 42]
    rkeys = [hot] * 27 + [3, 11, 99]
    left = [adapter.make_side(lkeys, "lval", list(range(len(lkeys))))]
    right = [adapter.make_side(rkeys, "rval", list(range(100, 100 + len(rkeys))))]
    return left, right


def one_dest_sides(adapter: JoinAdapter, n_left: int, n_right: int) -> tuple[list[object], list[object]]:
    """A single hot key (all rows key=7 -> one dest) with n_left build x n_right probe rows, so the
    inner join emits n_left*n_right OUTPUT rows (relational duplication) — the B5 output-side blowup:
    the joined dest_pid is far larger than any input-derived budget (trap 2)."""
    left = [adapter.make_side([7] * n_left, "lval", list(range(n_left)))]
    right = [adapter.make_side([7] * n_right, "rval", list(range(n_right)))]
    return left, right


# ---- test-authored relational oracle ------------------------------------------------------------
def _per_dest_side(
    adapter: JoinAdapter, blocks: list[object], field: str, parts: int
) -> dict[int, list[tuple[int, int]]]:
    out: dict[int, list[tuple[int, int]]] = {}
    for block in blocks:  # ascending src_pid
        for dest, sub in enumerate(adapter.backend.partition(block, "__joinkey__", parts)):
            rows = adapter.side_rows(sub, field)
            if rows:
                out.setdefault(dest, []).extend(rows)
    return out


def expected_join_per_dest(
    adapter: JoinAdapter, left: list[object], right: list[object], parts: int
) -> dict[int, Counter[Row]]:
    """DUPLICATING relational inner join, per dest, as a multiset (§3.3 pin). Routes each side with the
    backend's own ``partition`` (golden-pinned routing, NOT re-implemented) then, within each dest,
    emits one ``(key, lval, rval)`` row for every matching (build, probe) pair — a probe row with k
    build matches ⇒ k rows. A grouped/list-of-matches or dedup impl produces a different multiset."""
    left_by_dest = _per_dest_side(adapter, left, "lval", parts)
    right_by_dest = _per_dest_side(adapter, right, "rval", parts)
    per_dest: dict[int, Counter[Row]] = {}
    for dest in set(left_by_dest) & set(right_by_dest):
        rows = [(k, lv, rv) for (k, lv) in left_by_dest[dest] for (k2, rv) in right_by_dest[dest] if k == k2]
        if rows:
            per_dest[dest] = Counter(rows)
    return per_dest


def expected_join_all(
    adapter: JoinAdapter, left: list[object], right: list[object], parts: int
) -> Counter[Row]:
    """The whole relational result as one multiset (partitioning-independent) — for broadcast==shuffle."""
    total: Counter[Row] = Counter()
    for c in expected_join_per_dest(adapter, left, right, parts).values():
        total += c
    return total


def observed_join_per_dest(adapter: JoinAdapter, value: dict[int, object]) -> dict[int, Counter[Row]]:
    out: dict[int, Counter[Row]] = {}
    for part, block in value.items():
        rows = adapter.joined_rows(block)
        if rows:
            out[part] = Counter(rows)
    return out


def observed_join_all(adapter: JoinAdapter, value: dict[int, object]) -> Counter[Row]:
    total: Counter[Row] = Counter()
    for block in value.values():
        total += Counter(adapter.joined_rows(block))
    return total
