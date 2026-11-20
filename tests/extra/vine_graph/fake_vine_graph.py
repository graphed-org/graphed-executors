"""An in-process stand-in for the part of ``ndcctools.taskvine.vine_graph`` the backend uses.

``Executor.submit(graph)`` runs the Graph's new tasks at once, each through a pickle of its result
as on a worker, and ``as_completed([]).add()`` yields them. Failures take the shape of the real
``Executor``: a fresh ``TaskError`` per ask, naming the task and its source, dependents failing
with their source's reason and exception, and a failed task's ``result()`` loading no result.
"""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import cloudpickle


class TaskError(RuntimeError):
    def __init__(
        self,
        message: str,
        task: Task | None,
        source: Task | None,
        reason: str,
        exception: BaseException | None,
    ) -> None:
        super().__init__(message)
        self.task = task
        self.source = source
        self.reason = reason
        self.exception = exception


class CancelledError(RuntimeError):
    pass


class Lost(BaseException):
    """Raised by a task fn to fail its task for ``reason`` with no exception of its own."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Capacity:
    workers: int
    cores: int


class Function:
    def __init__(self, func: Any, /, *args: object) -> None:
        self.func = func
        self.args = args


class Task:
    def __init__(self, graph: Graph, key: int, spec: Function) -> None:
        self._graph = graph
        self.key = key
        self.spec = spec
        self.live = False
        self.finished = False
        self.payload: bytes | None = None
        self.failure: tuple[Task, str, str, bytes] | None = (
            None  # source, message, reason, exception
        )
        self.was_cancelled = False

    def _check(self) -> None:
        executor = self._graph.executor
        if executor is not None and executor.closed:
            raise RuntimeError("the Executor is closed")

    def done(self) -> bool:
        self._check()
        return self.finished

    def failed(self) -> bool:
        self._check()
        return self.failure is not None

    def cancelled(self) -> bool:
        return self.was_cancelled

    def result(self, timeout: float | None = None) -> object:
        self._check()
        assert self.finished
        if self.was_cancelled:
            raise CancelledError("cancelled")
        if self.failure is not None:
            source, message, reason, exception = self.failure
            raise TaskError(message, self, source, reason, cloudpickle.loads(exception))
        assert self._graph.executor is not None
        self._graph.executor.loaded.append(self.spec.args[0] if self.shimmed else self.spec.func)
        assert self.payload is not None
        return cloudpickle.loads(self.payload)

    @property
    def shimmed(self) -> bool:
        return getattr(self.spec.func, "__name__", "") == "_vine_task_shim"

    def cancel(self) -> bool:
        self.was_cancelled = True
        return True


class Graph:
    def __init__(self) -> None:
        self.tasks: list[Task] = []
        self.new: list[Task] = []
        self.executor: Executor | None = None

    def add(self, spec: Function) -> Task:
        task = Task(self, len(self.tasks) + 1, spec)
        self.tasks.append(task)
        self.new.append(task)
        return task


class Stream:
    """``as_completed([])``: every task it is given finished at submission, so add() readies it."""

    def __init__(self, executor: Executor) -> None:
        self._executor = executor
        self._ready: deque[Task] = deque()
        self._changed = threading.Condition()

    def __iter__(self) -> Iterator[Task]:
        return self

    def __next__(self) -> Task:
        with self._changed:
            if not self._ready:
                raise StopIteration
            return self._ready.popleft()

    def add(self, task: Task) -> None:
        if not task.live:
            raise ValueError(f"{task!r} has not been submitted; submit its Graph first")
        with self._changed:
            if task not in self._ready:
                self._ready.append(task)

    def update(self, tasks: list[Task]) -> None:
        for task in tasks:
            self.add(task)


class Executor:
    def __init__(self, workers: int = 2, cores: int = 8) -> None:
        self.capacity_now = Capacity(workers, cores)
        self.closed = False
        self.submitted: list[Any] = []  # the task fn of each task, under the shim
        self.loaded: list[Any] = []  # the task fn of each result the driver loaded
        self.submits = 0  # calls of submit(graph)

    def capacity(self) -> Capacity:
        return self.capacity_now

    def as_completed(self, tasks: list[Task]) -> Stream:
        stream = Stream(self)
        stream.update(tasks)
        return stream

    def submit(self, graph: Graph) -> Graph:
        self.submits += 1
        graph.executor = self
        new, graph.new = graph.new, []
        for task in new:
            self._run(task)
        return graph

    def _run(self, task: Task) -> None:
        fn, args = task.spec.func, task.spec.args
        self.submitted.append(args[0] if task.shimmed else fn)
        task.live = task.finished = True
        failed = next((a for a in args if type(a) is Task and a.failure is not None), None)
        if failed is not None:
            task.failure = failed.failure
            return
        resolved = tuple(cloudpickle.loads(a.payload) if type(a) is Task else a for a in args)
        try:
            task.payload = cloudpickle.dumps(fn(*resolved))
        except Lost as lost:
            task.failure = (task, f"lost: {lost.reason}", lost.reason, cloudpickle.dumps(None))
        except BaseException as exc:
            task.failure = (task, f"failed: {exc}", "function", cloudpickle.dumps(exc))
