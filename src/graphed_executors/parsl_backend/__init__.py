"""The parsl executor backend (plan §1.2), behind the ``[parsl]`` optional extra.

:class:`ParslBackend` runs over a **started** parsl ``HighThroughputExecutor`` or
``ThreadPoolExecutor`` via DIRECT executor submit (no DFK). Capability vectors are per-instance: HTEX
is the all-False "parsl floor" (the relay engine in :mod:`graphed_executors.common.relay_engine` is
its shuffle path); TPE is the :class:`ThreadBackend` shape (``peer_data_movement=True``), so the m43
as-tasks engine (moved to :mod:`graphed_executors.common.tasks_engine`) runs over it unchanged.

Importing this package pulls in NO parsl/dask/distributed: :class:`ParslBackend` defers the parsl
import to construction (via ``_lazy``), the shim is parsl-free, and :func:`start_htex` constructs
parsl objects only inside its body — so ``test_parsl_no_cross_import`` holds and the main CI matrix
collects the parsl suite parsl-free.
"""

from __future__ import annotations

from .backend import ParslBackend, parsl_runner
from .launch import start_htex, stop_htex

__all__ = [
    "ParslBackend",
    "parsl_runner",
    "start_htex",
    "stop_htex",
]
