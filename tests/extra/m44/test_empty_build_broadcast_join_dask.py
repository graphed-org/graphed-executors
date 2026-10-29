"""#91 on dask: a broadcast join with no build partitions keeps every probe row for right/outer.

Both dask engines: the transport (``transport_run_join``) and the peer tasks engine
(``dask_run_join``), each through the cost-model default and a forced broadcast.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from graphed.numpy import NumpyBackend

pytest.importorskip("distributed")

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m44"))

from transport_harness import (
    build_dask_backend,
    install_transport_plugin,
    run_bounded,
    transport_cluster,
    transport_join,
)

from graphed_executors.dask_backend import dask_run_join

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


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    with transport_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=["transport", "tasks"])
def engine(request: pytest.FixtureRequest, client: Any) -> Callable[[str, bool | None], Any]:
    from graphed_executors.dask_backend.plugin import GraphedWorkerPlugin  # noqa: PLC0415
    from graphed_executors.submit import SubmitRunner  # noqa: PLC0415

    right = [_side(k) for k in PROBE]
    if request.param == "transport":
        install_transport_plugin(client)
        dbackend = build_dask_backend(client)

        def run(how: str, broadcast: bool | None) -> Any:
            return transport_join(
                NumpyBackend(), [], right, PARTS, how=how, dbackend=dbackend, broadcast=broadcast
            )

        return run
    client.register_plugin(GraphedWorkerPlugin())
    runner = SubmitRunner(build_dask_backend(client))

    def run_tasks(how: str, broadcast: bool | None) -> Any:
        return dask_run_join(
            NumpyBackend(), [], right, PARTS, how=how, runner=runner, broadcast=broadcast
        )

    return run_tasks


def _bounded(fn: Callable[[], Any]) -> Any:
    outcome = run_bounded(fn, timeout_s=240.0)
    assert "error" not in outcome, outcome.get("error")
    return outcome["result"]


@pytest.mark.parametrize("broadcast", [None, True])
@pytest.mark.parametrize("how", ["right", "outer"])
def test_right_and_outer_keep_every_probe_row(
    engine: Callable[[str, bool | None], Any], how: str, broadcast: bool | None
) -> None:
    res = _bounded(lambda: engine(how, broadcast))

    assert res.witness.broadcast_chosen is True
    assert _rows(res.value) == ALL_PROBE_ROWS
    assert res.witness.join_output_rows == len(ALL_PROBE_ROWS)


@pytest.mark.parametrize("broadcast", [None, True])
def test_inner_still_returns_nothing(
    engine: Callable[[str, bool | None], Any], broadcast: bool | None
) -> None:
    res = _bounded(lambda: engine("inner", broadcast))

    assert res.witness.broadcast_chosen is True
    assert res.value == {}
