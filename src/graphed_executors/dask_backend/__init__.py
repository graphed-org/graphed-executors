"""The dask.distributed executor backend (plan §1.3), behind the ``[dask]`` optional extra.

**Front door:** :func:`~graphed_executors.dask_backend.api.run_repartition` /
:func:`~graphed_executors.dask_backend.api.run_join` — the unified shuffle facade (m45). One
``shuffle_method="auto"|"transport"|"tasks"`` knob dispatches over the m43 as-tasks engine and the m44
worker-transport engine, so callers pick an engine with one word instead of two entry points. The direct
``dask_run_*`` (m43) and ``transport_run_*`` (m44) entry points stay public for ``monitor=`` / ``retries=``.

Importing this package pulls in NO dask/distributed: :class:`DaskBackend`/:func:`dask_runner` defer
the ``distributed`` import to construction, and the m43 shuffle + m44 worker-transport entry points and
the m45 facade are dask-import-free at module load (their ``distributed``-touching code is deferred into
function bodies, F13) — so the entry points are re-exported eagerly here yet still leave ``distributed``
out of ``sys.modules`` until a run actually starts.
"""

from __future__ import annotations

from .api import resolve_shuffle_method, run_join, run_repartition
from .backend import DaskBackend, dask_runner
from .shuffle import dask_run_join, dask_run_repartition
from .transport_peer import transport_run_plan
from .transport_shuffle import transport_run_join, transport_run_repartition

__all__ = [
    "DaskBackend",
    "dask_run_join",
    "dask_run_repartition",
    "dask_runner",
    "resolve_shuffle_method",
    "run_join",
    "run_repartition",
    "transport_run_join",
    "transport_run_plan",
    "transport_run_repartition",
]
