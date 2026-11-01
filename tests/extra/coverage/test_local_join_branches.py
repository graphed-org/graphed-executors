"""The local join engine's one-sided, pass-through and broadcast-tail branches without pandas.

The frozen m40 relational tests need pandas, which the main CI job does not install; these pin the
same branches on the awkward backend against hand-written answers.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest

from graphed_executors.local.shuffle import run_join

ak = pytest.importorskip("awkward")

PARTS = 4  # routes key 3 alone to dest 0 and key 17 alone to dest 3: one-sided dests
Row = tuple[int, int | None, int | None]
MATCHED: list[Row] = [(1, 10, 10), (1, 10, 10), (2, 20, 20)]
EXPECTED: dict[str, list[Row]] = {
    "left": [*MATCHED, (3, 30, None)],
    "right": [*MATCHED, (17, None, 170)],
    "outer": [*MATCHED, (3, 30, None), (17, None, 170)],
}


@pytest.fixture()
def backend() -> Any:
    from graphed.awkward import AwkwardBackend  # noqa: PLC0415

    return AwkwardBackend()


def _side(keys: list[int], field: str) -> Any:
    return ak.Array(
        {
            "__joinkey__": np.array(keys, dtype=np.uint64),
            field: np.array([10 * k for k in keys], dtype=np.int64),
        }
    )


def _rows(value: dict[int, Any]) -> list[Row]:
    return sorted(
        (r["__joinkey__"], r.get("lval"), r.get("rval"))
        for b in value.values()
        for r in b.to_list()
    )


@pytest.mark.parametrize("broadcast", [False, True])
@pytest.mark.parametrize("how", ["left", "right", "outer"])
def test_a_non_inner_join_keeps_each_unmatched_row_once(
    backend: Any, how: str, broadcast: bool
) -> None:
    left, right = [_side([1, 2, 3], "lval")], [_side([1, 1, 2, 17], "rval")]

    res = run_join(backend, left, right, PARTS, how=how, broadcast=broadcast)

    assert res.witness.broadcast_chosen is broadcast
    assert _rows(res.value) == EXPECTED[how]
    assert res.witness.join_output_rows == len(EXPECTED[how])


def test_a_side_with_no_partitions_passes_the_other_through(backend: Any) -> None:
    left, right = [_side([1, 2, 3], "lval")], [_side([1, 1, 2, 17], "rval")]

    kept_left = run_join(backend, left, [], PARTS, how="left", broadcast=False)
    kept_right = run_join(backend, [], right, PARTS, how="right", broadcast=False)

    assert _rows(kept_left.value) == [(1, 10, None), (2, 20, None), (3, 30, None)]
    assert _rows(kept_right.value) == [
        (1, None, 10),
        (1, None, 10),
        (2, None, 20),
        (17, None, 170),
    ]


def test_a_broadcast_join_with_no_build_partitions_returns_nothing(backend: Any) -> None:
    res = run_join(backend, [], [_side([1, 2], "rval")], PARTS, how="inner", broadcast=True)

    assert res.value == {}
    assert res.witness.broadcast_puts == 0
