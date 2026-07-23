"""``dask_backend.shuffle`` — the re-export shim over the moved m43 engine (plan §1.6, m46 IT1).

The m43 as-tasks engine now lives in :mod:`graphed_executors.common.tasks_engine`; this module
re-exports it so every frozen m43/m45 dotted path (``dask_backend.shuffle.dask_run_repartition``,
``._WorkerStore``, …) stays importable at the identical path and the m44 transport code's
``from .shuffle import _WorkerStore`` keeps resolving. A moved function object keeps its
``__name__``, so the m43 submit-name Counter gates are witness-safe.

Like the moved engine, this shim imports NOTHING from dask/distributed or parsl — the frozen
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
