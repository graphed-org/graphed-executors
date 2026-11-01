"""The dispatch window shared by the local executors' hub routes and the submit engine (plan-A2).

A :class:`Window` holds a route's not-yet-started work and lets it out through a bounded number of
slots, only while the run's :class:`~graphed.core.RunControl` is RUNNING.
"""

from __future__ import annotations

import sys
import time
from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import Any, Generic, TypeVar

from graphed.core import RunControl, RunState

T = TypeVar("T")

#: A paused route with tasks running wakes this often, so a resume refills the free slots without
#: waiting for a running task to end.
PAUSED_WAKE_S = 0.05


def wait_until(predicate: Callable[[], bool], timeout_s: float) -> None:
    """Poll ``predicate`` until it holds or ``timeout_s`` passes; never raises on the timeout."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)


class Window(Generic[T]):
    """The dispatch point of a hub route (plan-A2 A2-1).

    Held items go out through at most ``size`` slots, and only while the control is RUNNING;
    without a control every held item goes at once.
    """

    def __init__(self, control: RunControl | None, size: int, items: Iterable[T] = ()) -> None:
        self.control = control
        self.size = size if control is not None else sys.maxsize
        self.held: deque[T] = deque(items)
        self.stopped = False  # a check found the control CANCELLED with work left to start
        self._paused = False

    def cancelled(self) -> bool:
        """Drop the held work once the control is CANCELLED, and say whether it was."""
        if self.control is None or self.control.state is not RunState.CANCELLED:
            return False
        self.stopped = True
        self.held.clear()
        return True

    def take(self, in_flight: int) -> list[T]:
        """Run the check before starting tasks.

        The held items to submit now, given ``in_flight`` running.
        """
        state = self.control.state if self.control is not None else RunState.RUNNING
        self._paused = state is RunState.PAUSED
        if state is RunState.CANCELLED:
            self.stopped |= bool(self.held)
            self.held.clear()
        if state is not RunState.RUNNING:
            return []
        return [self.held.popleft() for _ in range(min(len(self.held), self.size - in_flight))]

    def wait(self, futures: list[Future[Any]]) -> set[Future[Any]]:
        """Return the next completions.

        Empty on a paused wake or a resume, which only re-run :meth:`take`.
        """
        if not futures:  # paused with work held and nothing running
            assert self.control is not None
            self.control.wait()
            return set()
        # the state take() saw, so a resume still wakes
        timeout = PAUSED_WAKE_S if self._paused else None
        return wait(futures, timeout=timeout, return_when=FIRST_COMPLETED)[0]
