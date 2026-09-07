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
import multiprocessing as mp
import os
import signal
import time
from collections.abc import Callable, Container, Sequence
from typing import Any

import numpy as np
import pytest
from graphed.core import Partition, Plan, Task
from graphed.core.execution import LocalResources

BIG, SMALL = 20_000, 2_500  # float64 entries per partial: 160 KB vs 20 KB
TIMEOUT_S = 40.0


def _work(partition: Partition, n_floats: int) -> np.ndarray:
    time.sleep(partition.n_entries / 1000.0)  # the leaf weight, in ms — data-free stand-in for a read
    if partition.uri.startswith("boom"):  # a leaf marked by `plan(boom=...)` fails once its weight is up
        raise ValueError("kaboom")
    return np.ones(n_floats, dtype=np.float64)


def process_big(partition: Partition, resources: LocalResources) -> np.ndarray:
    return _work(partition, BIG)


def process_small(partition: Partition, resources: LocalResources) -> np.ndarray:
    return _work(partition, SMALL)


def combine(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return a + b


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


def _entry(target: Callable[..., None], args: tuple[Any, ...]) -> None:
    with contextlib.suppress(AttributeError, OSError):
        os.setsid()  # posix: own process group, so a wedged child's worker processes die with it
    target(*args)


def _kill_tree(child: mp.process.BaseProcess) -> None:
    """POSIX: kill the child AND its worker processes — a wedged pool's workers outlive their driver.
    Windows has no process groups here (``os.setsid``/``killpg`` raise inside the suppress), so it
    degrades to killing the child only and the orphaned workers are left to the OS."""
    with contextlib.suppress(AttributeError, OSError):
        os.killpg(child.pid, signal.SIGKILL)  # `_entry` made the child its own group leader
    child.kill()
    child.join(5)


def run_in_child(scenario: str, target: Callable[..., None], args: tuple[Any, ...]) -> None:
    """Run ``target(*args)`` in a spawn child; fail naming ``scenario`` if it does not finish."""
    child = mp.get_context("spawn").Process(target=_entry, args=(target, args))
    child.start()
    child.join(TIMEOUT_S)
    if child.is_alive():
        _kill_tree(child)
        pytest.fail(f"{scenario}: WEDGED — no exit after {TIMEOUT_S:.0f}s (a blocking peer pipe write)")
    assert child.exitcode == 0, f"{scenario}: child exited {child.exitcode}"
