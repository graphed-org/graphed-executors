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

__all__ = ["DaskBackend", "dask_run_join", "dask_run_repartition", "dask_runner"]


def __getattr__(name: str) -> Any:
    # Lazy re-export of the m43 shuffle entry points: the submodule is dask-import-free, but keeping
    # it out of eager package import matches the m42 "touch nothing until asked" seam.
    if name in ("dask_run_repartition", "dask_run_join"):
        from . import shuffle  # noqa: PLC0415  (lazy: the shuffle entry points)

        return getattr(shuffle, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
