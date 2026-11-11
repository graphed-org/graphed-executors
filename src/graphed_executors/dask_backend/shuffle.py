"""``dask_backend.shuffle`` — the re-export shim over the as-tasks engine.

The as-tasks engine lives in :mod:`graphed_executors.common.tasks_engine`; this module re-exports
it so its ``dask_backend.shuffle.*`` dotted paths (``dask_backend.shuffle.dask_run_repartition``,
``._WorkerStore``, …) stay importable. A re-exported function object keeps its ``__name__``, so
submit-name counts are unchanged.

Like the engine, this shim imports NOTHING from dask/distributed or parsl — the frozen
``test_parsl_no_cross_import`` / ``test_submit_no_dask_import`` probes pin that the dask package
(this shim included) leaves parsl out of ``sys.modules``.
"""

from __future__ import annotations

from graphed_executors.common.tasks_engine import (
    DaskShuffleWitness,
    _dask_broadcast_join_part,
    _dask_gather,
    _dask_gather_join,
    _dask_map_write,
    _dask_pick,
    _GatherOut,
    _JoinOut,
    _MapOut,
    _require_peer,
    _result,
    _site,
    _skey,
    _WorkerStore,
    dask_run_join,
    dask_run_repartition,
)

__all__ = [
    "DaskShuffleWitness",
    "_GatherOut",
    "_JoinOut",
    "_MapOut",
    "_WorkerStore",
    "_dask_broadcast_join_part",
    "_dask_gather",
    "_dask_gather_join",
    "_dask_map_write",
    "_dask_pick",
    "_require_peer",
    "_result",
    "_site",
    "_skey",
    "dask_run_join",
    "dask_run_repartition",
]
