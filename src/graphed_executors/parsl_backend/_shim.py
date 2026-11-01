"""The backend-owned worker shim (plan §1.2 ``_shim.py``, m46 IT3).

``ParslBackend.submit`` wraps every task fn in :func:`_parsl_task_shim` (a module-level fn —
pickled BY REFERENCE, never by value, so it is spawn-safe on HTEX workers), which installs the
:class:`WorkerEnv` the engine's neutral task fns read (``current_env().resources`` +
``current_env().emit``), runs ``fn``, and returns the result as an ``Emitted`` envelope with the
events buffered during the call — the :class:`_ParslFuture` unwraps it driver-side and dispatches
the events to subscribed handlers (the completion-granularity monitor piggyback, G10).

This module imports NEITHER parsl NOR dask — worker identity is recomputed in-task from parsl's own
``PARSL_WORKER_*`` env vars (measured present on 2026.7.20 HTEX workers). ``parsl_backend``
therefore leaves parsl out of
``sys.modules`` at import (frozen ``test_parsl_no_cross_import``).
"""

from __future__ import annotations

import os
import socket
from collections.abc import Callable

from graphed_executors.submit.worker import BufferedEnv, Emitted, run_buffered


def _worker_identity() -> str:
    """``f"{PARSL_WORKER_POOL_ID}:{PARSL_WORKER_RANK}"`` recomputed IN-TASK.

    From parsl's own env vars (HTEX), falling back to ``hostname:pid`` on the ThreadPoolExecutor
    (whose task threads live in the driver process and carry no parsl worker env vars).
    """
    pool = os.environ.get("PARSL_WORKER_POOL_ID")
    rank = os.environ.get("PARSL_WORKER_RANK")
    if pool is not None and rank is not None:
        return f"{pool}:{rank}"
    return f"{socket.gethostname()}:{os.getpid()}"


def _parsl_task_shim(fn: Callable[..., object], /, *args: object) -> Emitted:
    """Run ``fn(*args)`` under a :class:`BufferedEnv` on the worker.

    Future args were already resolved DRIVER-side by ``ParslBackend.submit`` (parsl workers cannot
    resolve a driver-process future — §1.1); a raising ``fn`` propagates through parsl's serialized
    exception path (no envelope on the error arc).
    """
    return run_buffered(BufferedEnv(_worker_identity()), fn, args)
