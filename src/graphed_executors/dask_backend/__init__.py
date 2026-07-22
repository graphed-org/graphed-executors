"""The dask.distributed executor backend (plan §1.3), behind the ``[dask]`` optional extra.

Importing this package pulls in NO dask/distributed: :class:`DaskBackend`/:func:`dask_runner` defer
the ``distributed`` import to construction, and the m43 shuffle + m44 worker-transport entry points
are dask-import-free at module load (their ``distributed``-touching code is deferred into function
bodies, F13) — so the entry points are re-exported eagerly here yet still leave ``distributed`` out
of ``sys.modules`` until a run actually starts.
"""

from __future__ import annotations

from .backend import DaskBackend, dask_runner
from .shuffle import dask_run_join, dask_run_repartition
from .transport_peer import transport_run_plan
from .transport_shuffle import transport_run_join, transport_run_repartition

__all__ = [
    "DaskBackend",
    "dask_run_join",
    "dask_run_repartition",
    "dask_runner",
    "transport_run_join",
    "transport_run_plan",
    "transport_run_repartition",
]
