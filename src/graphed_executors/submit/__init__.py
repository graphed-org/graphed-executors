"""The common submit-backend seam (plan §1.1-§1.2): the :class:`SubmitBackend` protocol, the generic
:class:`SubmitRunner` Plan engine, the worker seam (:class:`RunContext`/:class:`WorkerEnv`), and the
stdlib :class:`ThreadBackend` conformance backend. Pure stdlib + ``graphed`` — imports no dask, so it
installs and runs with the base package. The first real backend lives in
``graphed_executors.dask_backend`` behind the ``[dask]`` extra.
"""

from __future__ import annotations

from .engine import RunContext, SubmitRunner, WorkerEnv, current_env, set_worker_env
from .protocol import SubmitBackend, SubmitCapabilities, SubmitFuture
from .threadpool import ThreadBackend

__all__ = [
    "RunContext",
    "SubmitBackend",
    "SubmitCapabilities",
    "SubmitFuture",
    "SubmitRunner",
    "ThreadBackend",
    "WorkerEnv",
    "current_env",
    "set_worker_env",
]
