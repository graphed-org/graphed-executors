"""The worker side of :class:`~graphed_executors.vine_graph_backend.VineGraphBackend`.

``VineGraphBackend.submit`` runs every engine task fn through :func:`_vine_task_shim` (module
level, so vine_graph sends it by reference and workers import it from the installed package). The
shim installs a :class:`BufferedEnv`. vine_graph forks each call from its library process, so
``open_once`` handles and the engine's payload cache last one task, and a call's pid is its own.
vine_graph has no channel for monitor events, so they travel with the task's value (``Emitted``)
or on its exception (``graphed_events``), only when the task emitted any.
"""

from __future__ import annotations

import contextlib
import os
import socket
from collections.abc import Callable

from graphed_executors.submit.worker import BufferedEnv, Emitted, EventBatch, run_buffered


def _vine_task_shim(fn: Callable[..., object], /, *args: object) -> object:
    # The parent of a forked call is its worker's library process, which outlives the call.
    env = BufferedEnv(f"{socket.gethostname()}:{os.getppid()}")
    try:
        return run_buffered(env, fn, args)
    except BaseException as exc:
        if env.events:
            with contextlib.suppress(Exception):  # telemetry never replaces the task's exception
                setattr(exc, "graphed_events", env.events)  # noqa: B010  (undeclared attribute)
        raise


def _take_events(value: object) -> list[EventBatch]:
    """Return the events of a task's value: a task of its own, so the driver loads only them."""
    return value.events if type(value) is Emitted else []
