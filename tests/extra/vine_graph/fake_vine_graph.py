"""An in-process stand-in for the part of ``ndcctools.taskvine.vine_graph`` the backend uses.

Tasks run at submission, each through a pickle of its result as on a worker. Failures take the
shape of the real ``Executor``: a fresh ``TaskError`` per ask, dependents failing with their
source's reason and exception, and ``exception()`` loading no result.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import cloudpickle


@dataclass(frozen=True)
class Node:
    key: int


class TaskError(RuntimeError):
    def __init__(
        self,
        message: str,
        task: Node | None,
        source: Node | None,
        reason: str,
        exception: BaseException | None,
    ) -> None:
        super().__init__(message)
        self.task = task
        self.source = source
        self.reason = reason
        self.exception = exception


class Lost(BaseException):
    """Raised by a task fn to fail its task for ``reason`` with no exception of its own."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Capacity:
    workers: int
    cores: int


class Future:
    def __init__(self, executor: Executor, fn: Any, key: int) -> None:
        self._executor = executor
        self.fn = fn
        self.target = Node(key)
        self.payload: bytes | None = None
        self.failure: bytes | None = None
        self.cancelled = False

    def done(self) -> bool:
        if self._executor.closed:
            raise RuntimeError("the Executor is closed")
        return True

    def exception(self, timeout: float | None = None) -> TaskError | None:
        if self._executor.closed:
            raise RuntimeError("the Executor is closed")
        if self.failure is None:
            return None
        source, message, reason, exception = cloudpickle.loads(self.failure)
        return TaskError(message, self.target, Node(source), reason, exception)

    def result(self, timeout: float | None = None) -> object:
        error = self.exception()
        if error is not None:
            raise error
        self._executor.loaded.append(self.fn)
        assert self.payload is not None
        return cloudpickle.loads(self.payload)

    def cancel(self) -> bool:
        self.cancelled = True
        return True

    def add_done_callback(self, fn: Callable[[Future], None]) -> None:
        fn(self)


class Executor:
    def __init__(self, workers: int = 2, cores: int = 8) -> None:
        self.capacity_now = Capacity(workers, cores)
        self.closed = False
        self.submitted: list[
            Any
        ] = []  # the first argument of each submit: the task's fn under the shim
        self.loaded: list[Any] = []  # the shim's fn of each result the driver loaded

    def capacity(self) -> Capacity:
        return self.capacity_now

    def submit(self, fn: Callable[..., object], /, *args: object) -> Future:
        task_fn = args[0] if fn.__name__ == "_vine_task_shim" else fn
        self.submitted.append(task_fn)
        future = Future(self, task_fn, len(self.submitted))
        failed = next((a for a in args if type(a) is Future and a.failure is not None), None)
        if failed is not None:
            future.failure = failed.failure
            return future
        resolved = tuple(cloudpickle.loads(a.payload) if type(a) is Future else a for a in args)
        key = future.target.key
        try:
            future.payload = cloudpickle.dumps(fn(*resolved))
        except Lost as lost:
            future.failure = cloudpickle.dumps((key, f"lost: {lost.reason}", lost.reason, None))
        except BaseException as exc:
            future.failure = cloudpickle.dumps((key, f"failed: {exc}", "function", exc))
        return future
