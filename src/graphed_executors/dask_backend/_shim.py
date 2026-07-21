"""The backend-owned worker shim (plan §1.3.3, review r1 B1). ``DaskBackend.submit`` wraps every
task fn in ``_dask_task_shim`` (a module-level fn — pickled BY REFERENCE, never by value), so dask
resolves future args before invoking it and it installs the :class:`WorkerEnv` the engine's neutral
task fns read (``current_env().resources`` + ``current_env().emit``). This is where ALL dask-touching
worker code lives; it imports distributed at module top but is imported only lazily (driver-side at
``DaskBackend`` construction) or by-reference (worker-side unpickle), so ``submit/`` names dask nowhere.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Any

import distributed
from graphed.core.execution import WorkerResources

from graphed_executors.submit.engine import _reset_worker_env, set_worker_env


class _DaskWorkerEnv:
    """A :class:`WorkerEnv` over a dask worker: the plugin's ``open_once`` resources, the worker's
    dask address as identity, and ``emit`` bound to ``Worker.log_event`` (msgpack control channel,
    off the data path). Emission is swallow-on-error — telemetry never breaks a run (M37 passivity)."""

    def __init__(
        self, resources: WorkerResources, worker_name: str, log_event: Callable[[str, Any], None]
    ) -> None:
        self.resources = resources
        self.worker = worker_name
        self._log_event = log_event

    def emit(self, topic: str, events: list[dict[str, object]]) -> None:
        # passivity: a failed emit must never break the task (events are msgpack-safe scalar dicts).
        with contextlib.suppress(Exception):
            self._log_event(topic, events)


def _dask_task_shim(fn: Callable[..., object], /, *args: object) -> object:
    worker = distributed.get_worker()
    plugin = worker.plugins["graphed-worker"]  # registered by dask_runner / register_worker_plugin
    env = _DaskWorkerEnv(plugin.resources, worker.address, worker.log_event)
    token = set_worker_env(env)  # the submit/ contextvar seam (§1.1)
    try:
        return fn(*args)  # future args already resolved by dask
    finally:
        _reset_worker_env(token)
