"""Plan functions for the vine_graph tests, importable by name on a worker."""

from __future__ import annotations

import os
import signal
from pathlib import Path
from typing import Any

from graphed.core import Partition, Task


class Counted:
    """A partial result that records each load in the driver as a file in ``loads``."""

    def __init__(self, total: int, loads: str | None) -> None:
        self.total = total
        self.loads = loads

    def __reduce__(self) -> tuple[Any, ...]:
        return (_load_counted, (self.total, self.loads))


def _load_counted(total: int, loads: str | None) -> Counted:
    if loads is not None and os.environ.get("VINE_TEST_DRIVER_PID") == str(os.getpid()):
        (Path(loads) / str(os.urandom(4).hex())).touch()
    return Counted(total, loads)


def process(partition: Partition, resources: Any) -> Counted:
    uri, _, loads = partition.uri.partition("|")
    if uri == "segfault":
        os.kill(os.getpid(), signal.SIGSEGV)
    if uri == "count":
        (Path(loads) / str(os.getpid())).touch()
        raise ValueError("counted")
    if uri == "raise":
        raise ValueError(f"no entries in {partition.entry_start}")
    return Counted(partition.entry_stop - partition.entry_start, loads or None)


def combine(a: Counted, b: Counted) -> Counted:
    return Counted(a.total + b.total, a.loads)


def empty() -> Counted:
    return Counted(0, None)


def tasks(n: int, uri: str = "data", *, bad: str | None = None) -> tuple[Task, ...]:
    """``n`` tasks of 10 entries each, the first on ``bad`` when given."""
    return tuple(
        Task(i, Partition(bad if bad and i == 0 else uri, "Events", i * 10, (i + 1) * 10))
        for i in range(n)
    )
