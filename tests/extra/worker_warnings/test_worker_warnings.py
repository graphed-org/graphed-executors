"""A worker's warning reaches the driver, whose own warning hooks a run leaves alone."""

from __future__ import annotations

import concurrent.futures
import multiprocessing
import pickle
import queue
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import awkward as ak
import pytest
from graphed import Session
from graphed.aggregate import aggregate_plan
from graphed.awkward import AwkwardBackend, AwkwardForm
from graphed.core import Partition, Plan, chunk_label
from graphed.core.worker_warnings import Warned, keeping_warnings, process_id

import graphed_executors.local._peer as _peer
import graphed_executors.local.executors as _executors
from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor
from graphed_executors.local._pinned_pool import _pinned_loop, exit_with_driver
from graphed_executors.submit import SubmitRunner, ThreadBackend
from graphed_executors.submit.worker import RunContext, _leaf_task

DATA = ak.Array({"x": [1.0, 2.0, 3.0, 4.0], "y": [0.0, 1.0, 1.0, 1.0]})


@dataclass
class Events:
    data: ak.Array

    def __call__(self) -> ak.Array:
        return self.data

    def partitions(self, steps_per_file: int = 1) -> tuple[Partition, ...]:
        return tuple(
            Partition.blind("mem://events", "", s, steps_per_file) for s in range(steps_per_file)
        )

    def read_partition(self, partition: Partition, columns: Any, resources: Any) -> ak.Array:
        part = partition.resolve(len(self.data))
        return self.data[part.entry_start : part.entry_stop]


def as_list(values: list[object]) -> Any:
    return ak.to_list(values[0])


def concat(a: list[float], b: list[float]) -> list[float]:
    return [*a, *b]


def nothing() -> list[float]:
    return []


def _plan() -> Plan[list[float]]:
    form = AwkwardForm(ak.Array(DATA.layout.to_typetracer(forget_length=True)))
    ev = Session(AwkwardBackend()).source("events", form=form, data=Events(DATA))
    plan = aggregate_plan(
        ev.x / ev.y, reduce=as_list, combine=concat, empty=nothing, steps_per_file=2
    )
    assert isinstance(plan, Plan)
    return plan


DIVIDE = "divide by zero encountered in divide"
CARRIED = f"{DIVIDE} [first in {chunk_label(Events(DATA).partitions(2)[0], rows=2)}]"
RUNNERS: dict[str, tuple[Callable[[], Any], str]] = {
    "process-pool": (lambda: ProcessPoolExecutor(workers=2, comms=None), CARRIED),
    "process-peer": (lambda: ProcessPoolExecutor(workers=2), CARRIED),
    "pinned-peer": (lambda: PinnedPoolExecutor(workers=2), CARRIED),
    "http-peer": (lambda: ProcessPoolExecutor(workers=2, comms="http"), CARRIED),
    "in-driver-threads": (lambda: SubmitRunner(ThreadBackend(2)), DIVIDE),
}


@pytest.mark.parametrize(("make", "warned"), RUNNERS.values(), ids=RUNNERS.keys())
def test_a_tasks_warning_reaches_the_driver_whose_hooks_stay(
    make: Callable[[], Any], warned: str
) -> None:
    seen: list[str] = []
    with warnings.catch_warnings():
        warnings.showwarning = lambda message, *args, **kwargs: seen.append(str(message))
        warnings.simplefilter("always")
        shown, filters = warnings.showwarning, list(warnings.filters)
        with make() as runner:
            assert runner.run(_plan()).value == [float("inf"), 2.0, 3.0, 4.0]
        assert (warnings.showwarning, warnings.filters) == (shown, filters)
    assert seen == [warned]


def test_a_leaf_submitted_to_another_process_carries_its_warning() -> None:
    plan = _plan()
    args = (RunContext("run", None, None, lean=True), pickle.dumps(plan.process), "leaf")
    spawn = multiprocessing.get_context("spawn")
    with concurrent.futures.ProcessPoolExecutor(1, mp_context=spawn) as workers:
        partial = workers.submit(_leaf_task, *args, plan.tasks[0]).result()
    assert isinstance(partial, Warned)


def _stopped() -> queue.Queue[None]:
    calls: queue.Queue[None] = queue.Queue()
    calls.put(None)
    return calls


INITIALIZERS: dict[str, Callable[[str], None]] = {
    "exit-with-driver": exit_with_driver,
    "process-pool": lambda driver: _executors._proc_init(driver=driver),
    "process-peer": lambda driver: _peer.peer_pool_init({}, driver),
    "pinned-pool": lambda driver: _pinned_loop(
        lambda: None, (), _stopped(), queue.Queue(), driver
    ),
}


@pytest.mark.parametrize("init", INITIALIZERS.values(), ids=INITIALIZERS.keys())
def test_a_pool_initializer_run_in_its_driver_leaves_it_a_driver(
    init: Callable[[str], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_peer, "_peer_registry", None)
    monkeypatch.setattr(_executors, "_proc_resources", None)
    init(process_id())
    with warnings.catch_warnings(record=True) as seen, keeping_warnings() as caught:
        warnings.simplefilter("always")
        warnings.warn("task", RuntimeWarning, stacklevel=1)
    assert ([str(w.message) for w in seen], caught) == (["task"], {})
