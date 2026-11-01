"""``VineGraphBackend``: a :class:`SubmitBackend` over a vine_graph ``Executor``.

vine_graph (``ndcctools.taskvine.vine_graph``) runs Python calls on TaskVine workers:
``submit(fn, *args)`` returns a future with done-callbacks, futures among the arguments become
dependencies whose results move between workers (or through the manager's vault), a lost result is
recomputed, and a failed task fails its dependents with a ``TaskError`` that carries the original
exception and the reason. That is the submit seam the engine needs, so this adapter only

- runs each task fn through the worker shim (``_shim._vine_task_shim``), which installs graphed's
  ``WorkerEnv``;
- wraps each vine_graph future in :class:`_VineFuture`, which raises the original exception of a
  failed task;
- returns broadcast payloads as the bytes themselves: vine_graph serializes an argument object once
  however many tasks pass it, and stages a large one as a file that workers cache, so the bytes
  travel once per worker;
- reads a leaf's monitor events in a task of their own (``_shim._take_events``) when a monitor
  needs them on the driver, so results stay on the workers. A monitor with a
  ``worker_monitor_factory`` takes no such task: its workers push.

The caller owns the vine_graph Executor. Workers need this package, graphed, and the analysis's own
dependencies in their environment. Modules that a plan refers to by import and that workers lack go
to every worker through the Executor's ``library_files``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from graphed_executors.submit import SubmitCapabilities, SubmitRunner
from graphed_executors.submit.worker import (
    EventHandlers,
    _leaf_task,
    dispatch_events,
    subscribe,
    unwrap,
)

from ._shim import _take_events, _vine_task_shim

__all__ = ["VineGraphBackend", "vine_graph_runner"]

# TaskError.reason of a task that the infrastructure, not its function, failed.
_LOST = frozenset({"signal", "workers", "retries"})


def _original(error: Any) -> BaseException:
    """Return the exception a TaskError carries, or the TaskError when it carries none."""
    return cast(BaseException, error if error.exception is None else error.exception)


class _VineFuture:
    """A :class:`SubmitFuture` over a vine_graph future."""

    __slots__ = ("_future", "key")

    def __init__(self, future: Any, key: str) -> None:
        self._future = future
        self.key = key

    def result(self, timeout: float | None = None) -> object:
        from ndcctools.taskvine.vine_graph import TaskError

        try:
            return unwrap(self._future.result(timeout))
        except TaskError as error:
            raise _original(error) from None

    def exception(self, timeout: float | None = None) -> BaseException | None:
        # vine_graph answers without loading the task's result, which the engine asks of every
        # finished future.
        error = self._future.exception(timeout)
        return None if error is None else _original(error)

    def done(self) -> bool:
        return cast(bool, self._future.done())

    def cancel(self) -> bool:
        return cast(bool, self._future.cancel())

    def add_done_callback(self, fn: Callable[[Any], None]) -> None:
        self._future.add_done_callback(lambda _: fn(self))

    def __repr__(self) -> str:
        return f"<_VineFuture {self.key} done={self.done()}>"


class VineGraphBackend:
    """A :class:`SubmitBackend` over a vine_graph ``Executor``.

    vine_graph moves results between workers (``peer_data_movement``), serializes a broadcast
    payload once (``scatter_broadcast``), cancels running tasks with their dependents
    (``cancel_running``), and keeps inputs in each worker's cache across tasks
    (``worker_file_cache``). It has no worker pins, and retries and resources are executor-wide
    settings, so those hints are ignored. The Executor's ``max-retries`` covers failures of the
    infrastructure; an exception that a task's function raises fails the task at once.
    """

    capabilities = SubmitCapabilities(
        peer_data_movement=True,
        scatter_broadcast=True,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=True,
        worker_file_cache=True,
    )

    def __init__(self, executor: Any) -> None:
        """Run tasks on ``executor``, which stays the caller's to close."""
        self._executor = executor
        self._handlers: EventHandlers = {}
        self._keys: dict[int, str] = {}  # vine_graph's key of each task -> the engine's

    def n_workers(self) -> int:
        """Return the workers connected now."""
        return cast(int, self._executor.capacity().workers)

    def task_slots(self) -> int:
        """Return the cores connected now: vine_graph's library gives each a function slot."""
        return cast(int, self._executor.capacity().cores)

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
    ) -> _VineFuture:
        """Submit ``fn`` through the worker shim; ``_VineFuture`` args become dependencies."""
        args = tuple(arg._future if type(arg) is _VineFuture else arg for arg in args)
        future = self._executor.submit(_vine_task_shim, fn, *args)
        self._keys[future.target.key] = key
        # Only leaves emit, and a checked task holds its fn as its second argument.
        if self._handlers and _leaf_task in (fn, *args[1:2]):
            self._executor.submit(_take_events, future).add_done_callback(self._dispatch)
        return _VineFuture(future, key)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        """Return ``payload``: vine_graph serializes one object once."""
        return payload

    def subscribe_events(
        self, topic: str, handler: Callable[[list[dict[str, object]]], None]
    ) -> Callable[[], None]:
        """Call ``handler`` with each event batch on ``topic``; return the unsubscribe."""
        return subscribe(self._handlers, topic, handler)

    def cancel(self, futures: Sequence[Any]) -> None:
        """Cancel ``futures`` and their dependents."""
        for future in futures:
            if type(future) is _VineFuture:
                future.cancel()

    def close(self) -> None:
        """Do nothing: the caller owns the Executor."""

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        """Return ``(key, message)`` of a task its infrastructure failed, else ``None``.

        Those are a killed function, workers that kept exiting, or attempts past ``max-retries``;
        the engine raises an attributed StageError for them. The key is the source's, since
        dependents fail with it and the engine may read one first. The message stands for the
        worker, which vine_graph does not name.
        """
        from ndcctools.taskvine.vine_graph import TaskError

        if not isinstance(exc, TaskError) or exc.reason not in _LOST:
            return None
        node = exc.task if exc.source is None else exc.source
        key = None if node is None else self._keys.get(node.key)
        return None if key is None else (key, str(exc))

    def _dispatch(self, future: Any) -> None:
        try:
            error = future.exception()
            if error is None:
                events = future.result()
            else:
                events = getattr(error.exception, "graphed_events", None)
        except Exception:  # the Executor closed before the task finished
            return
        dispatch_events(events or [], self._handlers)


def vine_graph_runner(
    executor: Any,
    *,
    monitor: Any = None,
    max_in_flight: int = 2,
    services: Mapping[str, str] | None = None,
    control: Any = None,
) -> SubmitRunner:
    """Return a :class:`SubmitRunner` over a :class:`VineGraphBackend` of ``executor``.

    ``close()`` leaves the executor open. There is no ``retries``: the Executor's ``max-retries``
    covers failures of the infrastructure, and vine_graph does not retry a function that raises.
    """
    return SubmitRunner(
        VineGraphBackend(executor),
        monitor=monitor,
        retries=0,
        max_in_flight=max_in_flight,
        services=services,
        control=control,
    )
