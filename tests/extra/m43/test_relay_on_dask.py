"""The relay engine over a dask runner: the shared tasks driver with the pick tier on the driver.

The relay runs over any ``SubmitBackend``; on dask it must match the local engine's blocks while
submitting no ``_dask_pick`` at all, where the peer engine submits one per (producer, dest).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from typing import Any

import numpy as np
import pytest

from graphed_executors.common.relay_engine import (
    RelayShuffleWitness,
    relay_run_join,
    relay_run_repartition,
)
from graphed_executors.dask_backend import dask_run_repartition
from graphed_executors.local.shuffle import run_join, run_repartition

pytest.importorskip("distributed")

PARTS = 4


def _block(keys: list[int], field: str) -> np.ndarray:
    dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
    block: np.ndarray = np.zeros(len(keys), dtype=dt)
    block["__joinkey__"] = keys
    block[field] = [10 * k for k in keys]
    return block


@pytest.fixture(scope="module")
def client() -> Iterator[Any]:
    import distributed  # noqa: PLC0415  (importorskip'd at module top)

    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=False, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        yield c


@pytest.fixture()
def spied(client: Any) -> tuple[Any, Counter[str]]:
    from graphed_executors.dask_backend import DaskBackend  # noqa: PLC0415  (lazy dask imports)
    from graphed_executors.dask_backend.plugin import GraphedWorkerPlugin  # noqa: PLC0415
    from graphed_executors.submit import SubmitRunner  # noqa: PLC0415

    client.register_plugin(GraphedWorkerPlugin())
    backend = DaskBackend(client)
    names: Counter[str] = Counter()
    submit = backend.submit

    def counting(fn: Any, *args: Any, **kwargs: Any) -> Any:
        names[fn.__name__] += 1
        return submit(fn, *args, **kwargs)

    backend.submit = counting  # type: ignore[method-assign]
    return SubmitRunner(backend), names


@pytest.fixture()
def numpy_backend() -> Any:
    from graphed.numpy import NumpyBackend  # noqa: PLC0415

    return NumpyBackend()


def test_relay_repartition_picks_on_the_driver(
    numpy_backend: Any, spied: tuple[Any, Counter[str]]
) -> None:
    runner, names = spied
    src = [_block([1, 2, 3, 4, 5], "v"), _block([6, 7, 8], "v"), _block([2, 9], "v")]

    res = relay_run_repartition(numpy_backend, src, PARTS, runner=runner)

    assert res.dest_block_hashes == run_repartition(numpy_backend, src, PARTS).dest_block_hashes
    assert names["_dask_pick"] == 0 and names["_dask_gather"] == PARTS, dict(names)
    witness = res.witness
    assert isinstance(witness, RelayShuffleWitness)
    assert witness.head_node_routed is True and witness.driver_relay_bytes > 0
    # control: the peer engine over the same runner does submit its pick tier
    dask_run_repartition(numpy_backend, src, PARTS, runner=runner)
    assert names["_dask_pick"] > 0, dict(names)


@pytest.mark.parametrize(("broadcast", "how"), [(False, "outer"), (True, "left")])
def test_relay_join_matches_local_without_submitting_picks(
    numpy_backend: Any, spied: tuple[Any, Counter[str]], broadcast: bool, how: str
) -> None:
    runner, names = spied
    left = [_block([1, 2, 13], "l")]
    right = [_block([1, 1, 2, 17], "r"), _block([2, 3], "r")]

    res = relay_run_join(
        numpy_backend, left, right, PARTS, how=how, runner=runner, broadcast=broadcast
    )
    local = run_join(numpy_backend, left, right, PARTS, how=how, broadcast=broadcast)

    assert res.dest_block_hashes == local.dest_block_hashes
    assert local.dest_block_hashes, "the local join produced no blocks"
    assert names["_dask_pick"] == 0, dict(names)
    witness = res.witness
    assert isinstance(witness, RelayShuffleWitness)
    assert witness.broadcast_chosen is broadcast and witness.driver_relay_bytes > 0
