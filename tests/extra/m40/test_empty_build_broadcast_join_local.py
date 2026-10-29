"""A broadcast join with no build partitions keeps every probe row for how=right/outer (#91).

The cost model picks broadcast for an empty build side, so the default call path is pinned too.
"""

from __future__ import annotations

from contextlib import closing
from typing import Any

import numpy as np
import pytest
from graphed.numpy import NumpyBackend

from graphed_executors.common.relay_engine import relay_run_join
from graphed_executors.local.shuffle import run_join
from graphed_executors.submit import SubmitRunner, ThreadBackend

PARTS = 4
PROBE = [[1, 1, 2, 17], [3, 5]]
ALL_PROBE_ROWS = sorted((k, 10 * k) for keys in PROBE for k in keys)


def _side(keys: list[int]) -> np.ndarray:
    dt = np.dtype([("__joinkey__", np.uint64), ("rval", np.int64)])
    block: np.ndarray = np.zeros(len(keys), dtype=dt)
    block["__joinkey__"] = keys
    block["rval"] = [10 * k for k in keys]
    return block


def _rows(value: dict[int, Any]) -> list[tuple[int, int]]:
    return sorted(
        (int(k), int(v))
        for b in value.values()
        for k, v in zip(b["__joinkey__"], b["rval"], strict=True)
    )


def _local(how: str, broadcast: bool | None) -> Any:
    return run_join(
        NumpyBackend(), [], [_side(k) for k in PROBE], PARTS, how=how, broadcast=broadcast
    )


def _relay(how: str, broadcast: bool | None) -> Any:
    runner = SubmitRunner(ThreadBackend(2))
    with closing(runner):
        return relay_run_join(
            NumpyBackend(),
            [],
            [_side(k) for k in PROBE],
            PARTS,
            how=how,
            runner=runner,
            broadcast=broadcast,
        )


ENGINES = {"local": _local, "relay": _relay}


@pytest.mark.parametrize("broadcast", [None, True])
@pytest.mark.parametrize("how", ["right", "outer"])
@pytest.mark.parametrize("engine", list(ENGINES))
def test_right_and_outer_keep_every_probe_row(
    engine: str, how: str, broadcast: bool | None
) -> None:
    res = ENGINES[engine](how, broadcast)

    assert res.witness.broadcast_chosen is True
    assert _rows(res.value) == ALL_PROBE_ROWS
    assert res.witness.join_output_rows == len(ALL_PROBE_ROWS)


@pytest.mark.parametrize("broadcast", [None, True])
@pytest.mark.parametrize("engine", list(ENGINES))
def test_inner_still_returns_nothing(engine: str, broadcast: bool | None) -> None:
    res = ENGINES[engine]("inner", broadcast)

    assert res.witness.broadcast_chosen is True
    assert res.value == {}
