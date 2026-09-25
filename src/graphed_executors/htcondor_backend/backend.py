"""``HTCondorBackend``: a :class:`SubmitBackend` at the all-False floor whose workers are pilots it
launches itself. Pilots pull pickled tasks from the backend's :class:`TaskServer` and post results
back; the driver resolves future arguments before a task is queued, so pilots never talk to each other.
"""

from __future__ import annotations

import io
import pickle
import socket
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future
from contextlib import ExitStack
from pathlib import Path
from typing import Any, TypeVar

from graphed.core.execution import ExecResult, Plan

from graphed_executors.parsl_backend.backend import _ParslFuture
from graphed_executors.submit import SubmitCapabilities, SubmitRunner
from graphed_executors.submit.protocol import SubmitFuture

from .launch import CondorPilots, PilotLauncher
from .server import TaskServer, WorkerLost
from .sites import SiteProfile

R = TypeVar("R")

# Pilots queue like any other job, so a busy pool can take minutes to start the first one.
N_WORKERS_WAIT_S = 600.0

_FLOOR = SubmitCapabilities(
    peer_data_movement=False,
    scatter_broadcast=False,
    pin_to_worker=False,
    per_task_retries=False,
    per_task_resources=False,
    cancel_running=False,
    worker_file_cache=False,
)


class HTCondorBackend:
    """Starts a task server and ``n_pilots`` pilots through ``launcher``; :meth:`close` removes them.

    ``host`` is the name pilots dial back to (default: this machine's FQDN); the server binds the
    first free port of ``port_range`` on all interfaces.
    """

    def __init__(
        self,
        launcher: PilotLauncher,
        n_pilots: int,
        *,
        host: str | None = None,
        port_range: tuple[int, int] = (10000, 10100),
    ) -> None:
        self.capabilities = _FLOOR
        self.launcher = launcher
        self._handlers: dict[str, Callable[[list[dict[str, object]]], None]] = {}
        self._server = TaskServer(host or socket.getfqdn(), port_range, launcher)
        with ExitStack() as on_error:  # a refused start must not leave the server holding its port
            on_error.callback(self._server.shutdown)
            on_error.callback(self._server.close)
            launcher.start(self._server.url, self._server.secret, n_pilots)
            on_error.pop_all()

    def n_workers(self) -> int:
        """Pilots registered and live right now; never waits."""
        return self._server.live_pilots()

    def wait_for_pilots(self, n: int, timeout: float = N_WORKERS_WAIT_S) -> int:
        deadline = time.monotonic() + timeout
        while (live := self._server.live_pilots()) < n:
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"{live} of {n} pilots connected after {timeout}s; "
                    f"see the pilot logs in {getattr(self.launcher, 'log_dir', None)}"
                )
            time.sleep(0.05)
        return live

    def submit(
        self,
        fn: Callable[..., object],
        /,
        *args: object,
        key: str,
        retries: int = 0,
        priority: int = 0,
        resources: Mapping[str, float] | None = None,
        workers: Sequence[str] | None = None,
    ) -> SubmitFuture:
        """Never blocks on a future argument: the task queues once its arguments are done. The hints
        are ignored."""
        raw: Future[Any] = Future()
        self._server.add(key, fn, args, raw)
        return _ParslFuture(raw, self._handlers)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        return payload  # each task carries its own copy through the driver

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        self._handlers[topic] = handler

        def unsub() -> None:
            self._handlers.pop(topic, None)

        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        for fut in futures:  # pre-run only: a leased task runs to completion
            fut.cancel()

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        return (exc.key, exc.pilot) if isinstance(exc, WorkerLost) else None

    def close(self) -> None:
        self._server.close()
        self.launcher.stop()
        self._server.shutdown()


class _MainProbe(pickle.Unpickler):
    def find_class(self, module: str, name: str) -> Any:
        if module == "__main__":
            raise pickle.UnpicklingError(f"__main__.{name}")
        return super().find_class(module, name)


def _require_importable(obj: object, role: str) -> None:
    """Refuse ``obj`` when a pilot could not unpickle it: a lambda or local function does not pickle,
    and a ``__main__`` global pickles here but names a module the pilot does not have."""
    try:
        _MainProbe(io.BytesIO(pickle.dumps(obj))).load()
    except (pickle.PicklingError, pickle.UnpicklingError, AttributeError, TypeError) as exc:
        raise ValueError(
            f"pilots import plan.{role} by name; move {obj!r} into a module and pass it in "
            f"`user_modules=[...]` ({exc})"
        ) from exc


class HTCondorRunner(SubmitRunner):
    """A :class:`SubmitRunner` that refuses a plan pilots cannot import, and waits for ``min_pilots``
    before its first run so pilots that never start are an error instead of a queue that never drains."""

    backend: HTCondorBackend

    def __init__(
        self,
        backend: HTCondorBackend,
        *,
        min_pilots: int = 1,
        monitor: Any = None,
        retries: int = 3,
        max_in_flight: int = 2,
    ) -> None:
        super().__init__(backend, monitor=monitor, retries=retries, max_in_flight=max_in_flight)
        self._min_pilots = min_pilots
        self._waited = False

    def run(self, plan: Plan[R]) -> ExecResult[R]:
        _require_importable(plan.process, "process")
        _require_importable(plan.combine, "combine")
        if not self._waited:
            self.backend.wait_for_pilots(self._min_pilots)
            self._waited = True
        return super().run(plan)


def htcondor_runner(
    *,
    n_pilots: int,
    site: str | SiteProfile = "generic",
    image: str | None = None,
    request_cpus: int = 1,
    request_memory_mb: int = 2048,
    log_dir: str | Path | None = None,
    user_modules: Sequence[str | Path] = (),
    env: str | Path | None = None,
    extra_submit: Mapping[str, str] | None = None,
    host: str | None = None,
    min_pilots: int = 1,
    monitor: Any = None,
    retries: int = 3,
    max_in_flight: int = 2,
) -> HTCondorRunner:
    """``n_pilots`` pilot jobs on the ``site`` pool behind an :class:`HTCondorRunner`;
    ``runner.close()`` removes them."""
    pilots = CondorPilots(
        site,
        image=image,
        request_cpus=request_cpus,
        request_memory_mb=request_memory_mb,
        log_dir=log_dir,
        user_modules=user_modules,
        env=env,
        extra_submit=extra_submit,
    )
    backend = HTCondorBackend(pilots, n_pilots, host=host)
    return HTCondorRunner(
        backend, min_pilots=min_pilots, monitor=monitor, retries=retries, max_in_flight=max_in_flight
    )
