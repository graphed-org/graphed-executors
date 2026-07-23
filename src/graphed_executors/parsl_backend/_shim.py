"""The backend-owned worker shim (plan §1.2 ``_shim.py``, m46 IT3). ``ParslBackend.submit`` wraps
every task fn in :func:`_parsl_task_shim` (a module-level fn — pickled BY REFERENCE, never by value,
so it is spawn-safe on HTEX workers), which installs the :class:`WorkerEnv` the engine's neutral
task fns read (``current_env().resources`` + ``current_env().emit``), runs ``fn``, and returns the
result together with the events buffered during the call — the :class:`_ParslFuture` unwraps that
envelope driver-side and dispatches the events to subscribed handlers (the completion-granularity
monitor piggyback, G10).

This module imports NEITHER parsl NOR dask — worker identity is recomputed in-task from parsl's own
``PARSL_WORKER_*`` env vars (measured present on 2026.7.20 HTEX workers) and the ``open_once``
resources are a process-global :class:`LocalResources` (so file locality holds across the many tasks
a reused worker process runs). ``parsl_backend`` therefore leaves parsl out of ``sys.modules`` at
import (frozen ``test_parsl_no_cross_import``).
"""

from __future__ import annotations

import os
import socket
import threading
from collections.abc import Callable

from graphed.core.execution import LocalResources, WorkerResources

from graphed_executors.submit.engine import _reset_worker_env, set_worker_env

# One LocalResources per worker PROCESS: open_once caches a uri's handle across the tasks a reused
# HTEX worker runs (the §1.2 file-locality contract), distinct across worker processes by construction.
_WORKER_RESOURCES: LocalResources | None = None
_WORKER_RESOURCES_LOCK = threading.Lock()

# A monitor event batch as buffered by the shim: (topic, events) — dispatched driver-side on completion.
EventBatch = tuple[str, list[dict[str, object]]]


def _worker_resources() -> LocalResources:
    global _WORKER_RESOURCES
    if _WORKER_RESOURCES is None:
        with _WORKER_RESOURCES_LOCK:
            if _WORKER_RESOURCES is None:
                _WORKER_RESOURCES = LocalResources()
    return _WORKER_RESOURCES


def _worker_identity() -> str:
    """``f"{PARSL_WORKER_POOL_ID}:{PARSL_WORKER_RANK}"`` recomputed IN-TASK from parsl's own env vars
    (HTEX), falling back to ``hostname:pid`` on the ThreadPoolExecutor (whose task threads live in the
    driver process and carry no parsl worker env vars)."""
    pool = os.environ.get("PARSL_WORKER_POOL_ID")
    rank = os.environ.get("PARSL_WORKER_RANK")
    if pool is not None and rank is not None:
        return f"{pool}:{rank}"
    return f"{socket.gethostname()}:{os.getpid()}"


class _ParslWorkerEnv:
    """A per-task :class:`WorkerEnv`: the process-global ``open_once`` resources, the in-task worker
    identity, and a BUFFERING ``emit`` (parsl has no worker->driver event transport, so events ride
    back with the task result and are dispatched driver-side — the completion-granularity piggyback)."""

    def __init__(self) -> None:
        self.resources: WorkerResources = _worker_resources()
        self.worker = _worker_identity()
        self.events: list[EventBatch] = []

    def emit(self, topic: str, events: list[dict[str, object]]) -> None:
        self.events.append((topic, list(events)))


def _parsl_task_shim(fn: Callable[..., object], /, *args: object) -> tuple[object, list[EventBatch]]:
    """Run ``fn(*args)`` under a :class:`_ParslWorkerEnv` on the worker, returning
    ``(result, buffered_events)``. Future args were already resolved DRIVER-side by
    ``ParslBackend.submit`` (parsl workers cannot resolve a driver-process future — §1.1); a raising
    ``fn`` propagates through parsl's serialized exception path (no envelope on the error arc)."""
    env = _ParslWorkerEnv()
    token = set_worker_env(env)
    try:
        result = fn(*args)
    finally:
        _reset_worker_env(token)
    return result, env.events


def dispatch_events(
    events: list[EventBatch], handlers: dict[str, Callable[[list[dict[str, object]]], None]]
) -> None:
    """Deliver a task's buffered event batches to the driver-side topic handlers (idempotent per
    :class:`_ParslFuture` — called once on the first successful result unwrap)."""
    for topic, batch in events:
        handler = handlers.get(topic)
        if handler is not None:
            handler(batch)
