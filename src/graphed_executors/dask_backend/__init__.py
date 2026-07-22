"""The dask.distributed executor backend (plan §1.3), behind the ``[dask]`` optional extra.

Importing this package pulls in NO dask/distributed (the accessor and shim are lazy) — only
constructing :class:`DaskBackend` / calling :func:`dask_runner` touches distributed, raising an
actionable ImportError when the extra is absent.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .backend import DaskBackend, dask_runner

if TYPE_CHECKING:
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


def __getattr__(name: str) -> Any:
    # Lazy re-export of the m43 + m44 entry points: the submodules are dask-import-free (m44's
    # transport-touching code is deferred into function bodies, F13), but keeping them out of eager
    # package import matches the m42 "touch nothing until asked" seam.
    if name in ("dask_run_repartition", "dask_run_join"):
        from . import shuffle  # noqa: PLC0415  (lazy: the m43 shuffle entry points)

        return getattr(shuffle, name)
    if name == "transport_run_plan":
        from . import transport_peer  # noqa: PLC0415  (lazy: the m44 peer runner)

        return transport_peer.transport_run_plan
    if name in ("transport_run_repartition", "transport_run_join"):
        from . import transport_shuffle  # noqa: PLC0415  (lazy: the m44 shuffle engine)

        return getattr(transport_shuffle, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
