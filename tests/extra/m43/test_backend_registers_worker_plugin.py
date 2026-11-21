"""``DaskBackend(client)`` alone sets up what its workers need (graphed-executors#132)."""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest

pytest.importorskip("distributed")


def _side(keys: list[int], field: str) -> np.ndarray:
    block: np.ndarray = np.zeros(len(keys), dtype=[("__joinkey__", np.uint64), (field, np.int64)])
    block["__joinkey__"] = keys
    block[field] = np.arange(len(keys))
    return block


def test_a_tasks_join_runs_on_a_client_nothing_else_set_up() -> None:
    import distributed  # noqa: PLC0415  (importorskip'd at module top)
    from graphed.numpy import NumpyBackend  # noqa: PLC0415

    from graphed_executors.dask_backend import DaskBackend, dask_run_join  # noqa: PLC0415
    from graphed_executors.local.shuffle import run_join  # noqa: PLC0415
    from graphed_executors.submit import SubmitRunner  # noqa: PLC0415

    backend = NumpyBackend()
    left, right = [_side([3, 7, 4, 3], "l")], [_side([3, 4, 9], "r")]
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=False, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        runner = SubmitRunner(DaskBackend(client))
        got = dask_run_join(backend, left, right, 4, runner=runner, broadcast=False)
        plugins: dict[str, Any] = client.run(lambda dask_worker: sorted(dask_worker.plugins))
    assert (
        got.dest_block_hashes
        == run_join(backend, left, right, 4, broadcast=False).dest_block_hashes
    )
    assert got.dest_block_hashes
    assert all("graphed-worker" in names for names in plugins.values())
