"""The worker seam and the task entry points a :class:`SubmitRunner` submits (plan §1.1).

Per-run state travels as a picklable :class:`RunContext` first argument; per-worker capability
(``open_once`` resources + the event transport) arrives via a :class:`WorkerEnv` a BACKEND installs
through its own module-level shim — never via an import here, so ``submit/`` names dask nowhere
and ``test_submit_no_dask_import`` holds. The task functions are module-level so every backend
pickles them by reference.
"""

from __future__ import annotations

import contextlib
import contextvars
import pickle
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

from graphed.core import Task
from graphed.core.execution import (
    LocalResources,
    Monitor,
    TaskEvent,
    TaskPhase,
    WorkerResources,
    emit_task,
    partition_label,
)
from graphed.debug import StageError
from graphed.shuffle import split

from .services import PROBE_CHECK_S, ServiceUnreachableError, check_ready, host_identity

if TYPE_CHECKING:
    from graphed.core import Partition

_MISSING = object()


# ---- the worker seam: RunContext (picklable arg) + WorkerEnv (backend-installed contextvar) -----


@dataclass(frozen=True)
class RunContext:
    """Per-run state, shipped as the first arg to every task fn (stdlib-picklable)."""

    run_nonce: str
    # f"graphed-monitor-{run_nonce}" when a monitor is attached, else None
    monitor_topic: str | None
    # pickled worker_profiler_factory (small by contract), else None
    profiler_payload: bytes | None
    # pickled worker_monitor_factory: workers push, not the topic
    monitor_factory: bytes | None = None
    lean: bool = False  # lean events: no STARTED, and an unlabelled terminal event

    @property
    def events_per_leaf(self) -> int:
        """Worker events the driver's topic receives per leaf (none when workers push)."""
        return 0 if self.monitor_factory is not None else 1 if self.lean else 2


class WorkerEnv(Protocol):
    """Per-worker capability the backend installs.

    ``resources`` backs ``open_once``; ``worker`` is the backend's identity for this worker (a
    dask address / a thread name); ``emit`` ships event dicts on the run's monitor topic
    (swallow-on-error — telemetry never breaks a run).
    """

    resources: WorkerResources
    worker: str

    def emit(self, topic: str, events: list[dict[str, object]]) -> None:
        """Deliver ``events`` under ``topic`` to the driver."""
        ...


class _DefaultEnv:
    """The process-global fallback env for a task run outside any backend shim.

    Such a run is e.g. a bare ``process`` call; the env is a shared :class:`LocalResources` and a
    no-op emit.
    """

    def __init__(self) -> None:
        self.resources: WorkerResources = LocalResources()
        self.worker = "local"

    def emit(self, topic: str, events: list[dict[str, object]]) -> None:
        return None


_ENV: contextvars.ContextVar[WorkerEnv | None] = contextvars.ContextVar(
    "graphed_worker_env", default=None
)
_DEFAULT_ENV = _DefaultEnv()


def set_worker_env(env: WorkerEnv) -> contextvars.Token[WorkerEnv | None]:
    """Install ``env`` as the current worker env; returns a token for :func:`_reset_worker_env`."""
    return _ENV.set(env)


def _reset_worker_env(token: contextvars.Token[WorkerEnv | None]) -> None:
    _ENV.reset(token)


def current_env() -> WorkerEnv:
    """Return the worker env a task fn reads for ``resources``/``emit``.

    The backend-installed one, or the process-global default when no shim wrapped the call.
    """
    env = _ENV.get()
    return env if env is not None else _DEFAULT_ENV


# ---- broadcast-once worker-side token cache (mirror of local ``_shared_objects``) ---------------

_PAYLOAD_CACHE: OrderedDict[str, object] = OrderedDict()
_PAYLOAD_CACHE_CAP = 32


def _shared_payload(token: str, raw: bytes) -> object:
    """Deserialize the broadcast payload ONCE per worker process.

    A bounded FIFO cache keyed by ``token`` — even though the future's raw bytes are already cached
    by the backend per worker, the ``pickle.loads`` (and any ``__setstate__`` cost) must not repeat
    per task (dask/dask#5503).
    """
    cached = _PAYLOAD_CACHE.get(token, _MISSING)
    if cached is _MISSING:
        cached = pickle.loads(raw)
        _PAYLOAD_CACHE[token] = cached
        while len(_PAYLOAD_CACHE) > _PAYLOAD_CACHE_CAP:
            _PAYLOAD_CACHE.popitem(last=False)
    return cached


# ---- module-level spawn-safe task entry points (pickled BY REFERENCE, never by value) -----------


def _identity(x: bytes) -> bytes:
    """Serve as the broadcast identity future's body.

    It places ``x`` once, referenced by many tasks.
    """
    return x


def _event_dict(
    phase: TaskPhase, key: int, worker: str, label: str, n_entries: int, error: str | None
) -> dict[str, object]:
    """Return a msgpack-safe TaskEvent as a plain dict.

    Worker->driver transports require msgpack, not pickle; ``phase`` is stringified explicitly
    so a StrEnum never leaks.
    """
    return {
        "phase": str(phase),
        "key": key,
        "worker": worker,
        "t": time.perf_counter(),
        "partition": label,
        "n_entries": n_entries,
        "error": error,
    }


# One pushed monitor per worker process and shipped factory; two task threads can reach a fresh
# process's first build together, hence the lock.
_WORKER_MONITORS: dict[bytes, Monitor | None] = {}
_WORKER_MONITORS_LOCK = threading.Lock()


def _worker_monitor(payload: bytes) -> Monitor | None:
    with _WORKER_MONITORS_LOCK:
        if payload not in _WORKER_MONITORS:
            monitor: Monitor | None = None
            # a monitor that won't build just disables telemetry
            with contextlib.suppress(Exception):
                monitor = pickle.loads(payload)()
            _WORKER_MONITORS[payload] = monitor
        return _WORKER_MONITORS[payload]


def _emit_phase(
    ctx: RunContext, env: WorkerEnv, phase: TaskPhase, task: Task, *, error: str | None = None
) -> None:
    if ctx.monitor_topic is None:  # no monitor attached -> zero telemetry cost on the task path
        return
    label = "" if ctx.lean else partition_label(task.partition)
    if ctx.monitor_factory is not None:
        ev = TaskEvent(
            phase,
            task.key,
            env.worker,
            time.perf_counter(),
            label,
            task.partition.n_entries,
            error=error,
        )
        emit_task(_worker_monitor(ctx.monitor_factory), ev)
        return
    env.emit(
        ctx.monitor_topic,
        [_event_dict(phase, task.key, env.worker, label, task.partition.n_entries, error)],
    )


def _render_error(exc: BaseException) -> str:
    """Return a concise picklable summary for the dashboard.

    A StageError already str()s to a user-mapped message, so its own text is the best summary;
    anything else gets ``Type: message``.
    """
    if isinstance(exc, StageError):
        return str(exc)
    return f"{type(exc).__name__}: {exc}"


def _leaf_task(ctx: RunContext, payload: bytes, token: str, task: Task) -> object:
    """Run one ``process`` on a worker, emitting STARTED then FINISHED/ERRORED.

    Worker-side timing. ``payload`` is the resolved broadcast bytes; the token cache
    deserializes it once per worker.
    """
    process = cast(
        "Callable[[Partition, WorkerResources], object]", _shared_payload(token, payload)
    )
    env = current_env()
    if not ctx.lean:
        _emit_phase(ctx, env, TaskPhase.STARTED, task)
    try:
        result = process(task.partition, env.resources)
    except BaseException as exc:
        _emit_phase(ctx, env, TaskPhase.ERRORED, task, error=_render_error(exc))
        raise
    _emit_phase(ctx, env, TaskPhase.FINISHED, task)
    return result


def _combine_task(ctx: RunContext, payload: bytes, token: str, a: object, b: object) -> object:
    r"""Reduce two partials on a worker.

    ``a``/``b`` are resolved by the backend (dask fetches them peer-to-peer; ThreadBackend
    ``.result()``\ s them). Combines emit no task events (leaf keys only).
    """
    combine = cast("Callable[[object, object], object]", _shared_payload(token, payload))
    return combine(a, b)


def _stage_task(payload: bytes, token: str, task: Task, *inputs: bytes) -> bytes:
    """Run one task of a ``DurablePlanV2`` stage: ``process(task, inputs, resources)``."""
    process = cast("Callable[..., bytes]", _shared_payload(token, payload))
    return process(task, inputs, current_env().resources)


def _map_task(payload: bytes, token: str, task: Task, *inputs: bytes) -> dict[int, bytes]:
    """Run a ``map_write`` task, its payload split per dest.

    Each (map, dest) edge then moves one slice.
    """
    return split(_stage_task(payload, token, task, *inputs))


# ---- the driver's event decoder + the service-checked task wrapper ------------------------------


def _event_from_dict(d: dict[str, object]) -> TaskEvent:
    return TaskEvent(
        phase=TaskPhase(cast("str", d["phase"])),
        key=cast("int", d["key"]),
        worker=cast("str", d["worker"]),
        t=cast("float", d["t"]),
        partition=cast("str", d["partition"]),
        n_entries=cast("int", d["n_entries"]),
        error=cast("str | None", d.get("error")),
    )


def _service_checked(
    checks: tuple[tuple[str, str, str], ...], fn: Callable[..., object], *args: object
) -> object:
    """``fn(*args)``.

    When it raises, each ``(name, endpoint, check)`` is re-checked here, on the worker that ran it,
    and the first that fails raises :class:`ServiceUnreachableError` from the task's exception,
    which is otherwise re-raised unchanged: a plan's own error beside a live service stays a plan
    error.
    """
    try:
        return fn(*args)
    except Exception as exc:
        for name, endpoint, check in checks:
            why = check_ready(endpoint, check, PROBE_CHECK_S)
            if why is not None:
                raise ServiceUnreachableError(
                    name, endpoint, host_identity(), f"{why}; the task raised {exc!r}"
                ) from exc
        raise
