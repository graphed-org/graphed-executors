"""Shared harness for the >64 KB-partial peer-reduction regressions (tests/extra — NOT frozen).

``PipeInbox.put_nowait`` is a *synchronous* pipe write, so any message past the OS pipe buffer
(~64 KB — every real reduction partial) parks the writer until the peer reads. Each scenario built
here runs a real ``ProcessPoolExecutor`` peer reduction inside a SPAWN CHILD under a hard timeout,
because the failure mode under test is a hang: the parent has to be able to give up and name the
scenario that wedged.

Payload size is the only variable between a scenario's control leg (20 KB, fits the pipe) and its
regression leg (160 KB, a blocking write) — which is what makes each test discriminating.
"""

from __future__ import annotations

import contextlib
import faulthandler
import multiprocessing as mp
import os
import signal
import sys
import tempfile
import time
from collections.abc import Callable, Container, Sequence
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
from graphed.core import Partition, Plan, Task
from graphed.core.execution import WorkerResources

BIG, SMALL = 20_000, 2_500  # float64 entries per partial: 160 KB vs 20 KB
TIMEOUT_S = 40.0


def _work(partition: Partition, n_floats: int) -> np.ndarray:
    time.sleep(partition.n_entries / 1000.0)  # the leaf weight, in ms — data-free stand-in for a read
    if partition.uri.startswith("boom"):  # a leaf marked by `plan(boom=...)` fails once its weight is up
        raise ValueError("kaboom")
    return np.ones(n_floats, dtype=np.float64)


def process_big(partition: Partition, resources: WorkerResources) -> np.ndarray:
    return _work(partition, BIG)


def process_small(partition: Partition, resources: WorkerResources) -> np.ndarray:
    return _work(partition, SMALL)


def combine(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return cast(np.ndarray, a + b)


def empty_big() -> np.ndarray:
    return np.zeros(BIG, dtype=np.float64)


def empty_small() -> np.ndarray:
    return np.zeros(SMALL, dtype=np.float64)


def plan(n_floats: int, weights: Sequence[int], boom: Container[int] = ()) -> Plan[np.ndarray]:
    """One leaf per weight (in ms); leaves in ``boom`` raise ``ValueError("kaboom")`` when they end."""
    process, empty = (process_big, empty_big) if n_floats == BIG else (process_small, empty_small)
    tasks = tuple(
        Task(k, Partition(f"{'boom' if k in boom else 'leaf'}{k}.root", "Events", 0, w))
        for k, w in enumerate(weights)
    )
    return Plan(process=process, combine=combine, empty=empty, tasks=tasks)


def _entry(target: Callable[..., None], args: tuple[Any, ...], dump_path: str) -> None:
    if sys.platform != "win32":  # own process group, so a wedged child's workers die with it
        with contextlib.suppress(OSError):
            os.setsid()
    # every thread's stack, shortly before the parent gives up: a wedge on a CI runner is otherwise
    # just "no exit after 40s" with nothing to say where the driver was parked
    dump = open(dump_path, "w")  # noqa: SIM115 — must outlive this frame for the watchdog thread
    faulthandler.dump_traceback_later(TIMEOUT_S - 5.0, file=dump)
    target(*args)


def _kill_tree(child: mp.process.BaseProcess) -> None:
    """POSIX: kill the child AND its worker processes — a wedged pool's workers outlive their driver.
    Windows has no process groups here, so it degrades to killing the child only and the orphaned
    workers are left to the OS."""
    assert child.pid is not None  # set once the child is started; killpg needs a real pid
    if sys.platform != "win32":
        with contextlib.suppress(OSError):
            os.killpg(child.pid, signal.SIGKILL)  # `_entry` made the child its own group leader
    child.kill()
    child.join(5)


def run_in_child(scenario: str, target: Callable[..., None], args: tuple[Any, ...]) -> None:
    """Run ``target(*args)`` in a spawn child; fail naming ``scenario`` if it does not finish."""
    fd, dump_path = tempfile.mkstemp(prefix="graphed-wedge-", suffix=".txt")
    os.close(fd)
    child = mp.get_context("spawn").Process(target=_entry, args=(target, args, dump_path))
    child.start()
    child.join(TIMEOUT_S)
    wedged = child.is_alive()
    if wedged:
        _kill_tree(child)
    stacks = Path(dump_path).read_text()
    with contextlib.suppress(OSError):
        os.unlink(dump_path)
    if wedged:
        pytest.fail(
            f"{scenario}: WEDGED — no exit after {TIMEOUT_S:.0f}s (a blocking peer pipe write)\n"
            f"child threads at {TIMEOUT_S - 5:.0f}s:\n{stacks}"
        )
    assert child.exitcode == 0, f"{scenario}: child exited {child.exitcode}"
