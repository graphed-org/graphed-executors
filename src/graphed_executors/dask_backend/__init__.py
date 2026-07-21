"""The dask.distributed executor backend (plan §1.3), behind the ``[dask]`` optional extra.

Importing this package pulls in NO dask/distributed (the accessor and shim are lazy) — only
constructing :class:`DaskBackend` / calling :func:`dask_runner` touches distributed, raising an
actionable ImportError when the extra is absent.
"""

from __future__ import annotations

from .backend import DaskBackend, dask_runner

__all__ = ["DaskBackend", "dask_runner"]
