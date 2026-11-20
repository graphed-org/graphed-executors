"""``VineGraphBackend``: a :class:`SubmitBackend` over a vine_graph ``Executor``.

vine_graph (``ndcctools.taskvine.vine_graph``) runs the tasks of a ``vg.Graph`` on TaskVine
workers: a task that reads another becomes its dependent, whose input moves between workers (or
through the manager's vault), a lost result is recomputed, and a failed task fails its dependents
with a ``TaskError`` that carries the original exception, the reason, and the task whose own
failure explains it (``source``). The engine submits one task at a time and wants a done-callback
per task, so this adapter

- grows one ``vg.Graph`` with ``g.add(vg.Function(fn, *args))`` per engine submit, and makes the
  tasks live with one ``ex.submit(g)`` for all that the engine submitted since the last one;
- keeps one ``ex.as_completed([])`` stream on a driver-side thread, adds each live task to it, and
  fires each future's done-callbacks from that thread as the stream yields it;
- runs each task fn through the worker shim (``_shim._vine_task_shim``), which installs graphed's
  ``WorkerEnv``;
- wraps each task in :class:`_VineFuture`, which raises the original exception of a failed task;
- returns broadcast payloads as the bytes themselves: vine_graph serializes an argument object once
  however many tasks pass it, and stages a large one as a file that workers cache, so the bytes
  travel once per worker;
- reads a leaf's monitor events in a task of their own (:meth:`VineGraphBackend.take_events`), so
  the driver loads no partial result.

The caller owns the vine_graph Executor. Workers need this package, graphed, and the analysis's own
dependencies in their environment. Modules that a plan refers to by import and that workers lack go
to every worker through the Executor's ``library_files``.
"""

from __future__ import annotations

import functools
import threading
import weakref
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from graphed_executors.submit import SubmitCapabilities, SubmitRunner
from graphed_executors.submit.worker import (
    Emitted,
    EventBatch,
    EventHandlers,
    dispatch_events,
    subscribe,
)

from ._shim import _take_events, _vine_task_shim

__all__ = ["VineGraphBackend", "vine_graph_runner"]

# TaskError.reason of a task that the infrastructure, not its function, failed.
_LOST = frozenset({"signal", "workers", "retries"})


def _original(error: Any) -> BaseException:
    """Return the exception a TaskError carries, or the TaskError when it carries none."""
    return cast(BaseException, error if error.exception is None else error.exception)


def _dispatch(handlers: EventHandlers, future: _VineFuture) -> None:
    """Deliver the events a finished ``_take_events`` task returned or inherited."""
    try:
        error = future.error()
        if error is None:
            events = cast(list[EventBatch], future.value())
        else:
            events = getattr(error.exception, "graphed_events", [])
    except Exception:  # the Executor closed before the task finished
        return
    dispatch_events(events, handlers)


class _VineFuture:
    """A :class:`SubmitFuture` over a task of the backend's ``vg.Graph``.

    It is done once the backend's stream yielded its task, which is when its done-callbacks run, on
    the backend's driver thread. ``result()`` and ``exception()`` wait for that, then ask the task.
    """

    __slots__ = ("__weakref__", "_backend", "_callbacks", "_done", "key", "task")

    def __init__(self, backend: VineGraphBackend, task: Any, key: str) -> None:
        self._backend = backend
        self.task = task
        self.key = key
        self._done = threading.Event()
        self._callbacks: list[Callable[[Any], None]] = []

    def _wait(self, timeout: float | None) -> None:
        if not self._done.wait(timeout):
            raise TimeoutError(f"{self.key} did not finish in time")

    def error(self, timeout: float | None = None) -> Any:
        """Return the task's TaskError, or ``None`` when it completed, loading no result."""
        from ndcctools.taskvine.vine_graph import TaskError

        self._wait(timeout)
        if not self.task.failed() and not self.task.cancelled():
            return None
        try:
            self.task.result()  # raises without loading a result
        except TaskError as error:
            return error
        raise AssertionError("a failed task has a result")  # pragma: no cover

    def value(self) -> object:
        """Return the task's result as the task returned it."""
        self._wait(None)
        return self.task.result()

    def result(self, timeout: float | None = None) -> object:
        from ndcctools.taskvine.vine_graph import TaskError

        self._wait(timeout)
        try:
            return cast(Emitted, self.task.result()).value
        except TaskError as error:
            raise _original(error) from None

    def exception(self, timeout: float | None = None) -> BaseException | None:
        # Loads no result: the engine asks this of every finished future.
        error = self.error(timeout)
        return None if error is None else _original(error)

    def done(self) -> bool:
        return self._done.is_set()

    def cancel(self) -> bool:
        self._backend._flush()
        return cast(bool, self.task.cancel())

    def add_done_callback(self, fn: Callable[[Any], None]) -> None:
        with self._backend._lock:
            if not self._done.is_set():
                self._callbacks.append(fn)
                return
        fn(self)

    def _finish(self) -> None:
        """Mark the future done and run its callbacks. Called on the driver thread."""
        with self._backend._lock:
            self._done.set()
            callbacks, self._callbacks = self._callbacks, []
        for fn in callbacks:
            fn(self)


class VineGraphBackend:
    """A :class:`SubmitBackend` over a vine_graph ``Executor``.

    vine_graph moves results between workers (``peer_data_movement``), serializes a broadcast
    payload once (``scatter_broadcast``), cancels running tasks with their dependents
    (``cancel_running``), and keeps inputs in each worker's cache across tasks
    (``worker_file_cache``). It has no worker pins, and the engine's retries and resources are
    ignored. The Executor's ``max-retries`` covers failures of the infrastructure; an exception
    that a task's function raises fails the task at once.

    Engine submits grow one ``vg.Graph``. The driver thread makes what was added live with one
    ``ex.submit(g)`` each time it wakes, adds those tasks to its ``as_completed`` stream, and runs
    done-callbacks as the stream yields. Once every task submitted so far finished, the next submit
    starts a new Graph, so that a Graph and its results go once the engine drops their futures.
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
        # each vine_graph task -> its future, which names it for describe_failure
        self._futures: weakref.WeakValueDictionary[Any, _VineFuture] = (
            weakref.WeakValueDictionary()
        )
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._graph: Any = None
        self._added: list[_VineFuture] = []  # added to the Graph and not live yet
        self._live: dict[Any, _VineFuture] = {}  # task -> future of the live tasks not yet yielded
        self._stream: Any = None
        self._thread: threading.Thread | None = None
        self._waiting = False  # the driver thread waits in the stream
        self._woken = False  # a finished task was added to the stream to end that wait
        self._last: Any = None  # the last task the stream yielded, which wakes the driver thread
        self._closed = False

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
        """Add ``fn`` through the worker shim; ``_VineFuture`` args become dependencies."""
        return self._add(key, _vine_task_shim, fn, *args)

    def _add(self, key: str, fn: Callable[..., object], *args: object) -> _VineFuture:
        import ndcctools.taskvine.vine_graph as vg

        args = tuple(arg.task if type(arg) is _VineFuture else arg for arg in args)
        with self._lock:
            if self._graph is None or not (self._added or self._live):
                self._graph = vg.Graph()
            future = _VineFuture(self, self._graph.add(vg.Function(fn, *args)), key)
            self._added.append(future)
            self._futures[future.task] = future
            if self._thread is None:
                self._stream = self._executor.as_completed([])
                self._thread = threading.Thread(
                    target=self._drive, name="vine-graph-driver", daemon=True
                )
                self._thread.start()
            wake = self._waiting and not self._woken and self._last is not None
            self._woken |= wake
            inline = self._waiting and self._last is None
            self._changed.notify()
        if wake:
            # A task that finished ends the driver thread's wait in the stream, which then submits.
            self._stream.add(self._last)
        elif inline:
            # Nothing finished yet that could wake it: submit from this thread.
            self._flush()
        return future

    def _flush(self, waiting: bool = False) -> None:
        """Make the tasks added since the last flush live, and add them to the stream.

        ``waiting`` marks the driver thread as about to wait in the stream, in the same step.
        """
        with self._lock:
            added, self._added = self._added, []
            self._waiting |= waiting
            if not added:
                return
            self._executor.submit(self._graph)
            for future in added:
                self._live[future.task] = future
            self._stream.update([future.task for future in added])

    def _drive(self) -> None:
        """Submit what was added, and finish each future as the stream yields its task."""
        while True:
            with self._lock:
                while not self._added and not self._live and not self._closed:
                    self._changed.wait()
                if self._closed:
                    return
            try:
                self._flush(waiting=True)
                try:
                    task = next(self._stream, None)
                finally:
                    with self._lock:
                        self._waiting = self._woken = False
            except RuntimeError:  # the Executor closed: nothing finishes anymore
                return
            if task is None:
                continue
            with self._lock:
                self._last = task
                future = self._live.pop(task, None)
            if future is not None:
                future._finish()
            future = None  # the engine decides how long a future lives

    def take_events(self, future: _VineFuture) -> None:
        """Deliver a leaf's events, when a run wants them, from a task of their own.

        A forked call ends in ``os._exit``, so a monitor built on a worker would lose what it
        buffered, and the engine asks workers not to push. This run's handlers go along: events
        may land after the run unsubscribed.
        """
        if self._handlers:
            done = functools.partial(_dispatch, dict(self._handlers))
            self._add(f"{future.key}.events", _take_events, future).add_done_callback(done)

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
            future.cancel()

    def close(self) -> None:
        """Stop the driver thread once nothing runs; the caller owns the Executor."""
        with self._lock:
            self._closed = True
            self._changed.notify()

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        """Return ``(key, message)`` of a task its infrastructure failed, else ``None``.

        Those are a killed function, workers that kept exiting, or attempts past ``max-retries``;
        the engine raises an attributed StageError for them. The key is the source's
        (``TaskError.source``), since dependents fail with it and the engine may read one first.
        The message stands for the worker, which vine_graph does not name.
        """
        from ndcctools.taskvine.vine_graph import TaskError

        if not isinstance(exc, TaskError) or exc.reason not in _LOST:
            return None
        for task in (exc.source, exc.task):
            future = None if task is None else self._futures.get(task)
            if future is not None:
                return future.key, str(exc)
        return None


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
