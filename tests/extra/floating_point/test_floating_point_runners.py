"""``raise_on_floating_point`` and worker warnings mean the same on every runner."""

from __future__ import annotations

import sys
import warnings
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import awkward as ak
import pytest
from graphed import Session, raise_on_floating_point
from graphed.aggregate import aggregate_plan
from graphed.awkward import AwkwardBackend, AwkwardForm
from graphed.core import Partition, Plan, SequentialRunner
from graphed.debug import StageError

from graphed_executors.local import ProcessPoolExecutor, ThreadExecutor
from graphed_executors.submit import SubmitRunner, ThreadBackend

EVENTS = ak.Array({"x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], "y": [0.0, 1.0, 0.0, 1.0, 1.0, 1.0]})


@dataclass
class Events:
    def __call__(self) -> ak.Array:
        return EVENTS

    def partitions(self, steps_per_file: int = 1) -> tuple[Partition, ...]:
        return tuple(
            Partition.blind("mem://events", "", s, steps_per_file) for s in range(steps_per_file)
        )

    def read_partition(self, partition: Partition, columns: Any, resources: Any) -> ak.Array:
        part = partition.resolve(len(EVENTS))
        return EVENTS[part.entry_start : part.entry_stop]


def as_list(values: list[object]) -> Any:
    return ak.to_list(values[0])


def concat(a: list[float], b: list[float]) -> list[float]:
    return [*a, *b]


def nothing() -> list[float]:
    return []


def _plan() -> Plan[list[float]]:
    session = Session(AwkwardBackend())
    form = AwkwardForm(ak.Array(EVENTS.layout.to_typetracer(forget_length=True)))
    ev = session.source("events", form=form, data=Events())
    plan = aggregate_plan(
        ev.x / ev.y, reduce=as_list, combine=concat, empty=nothing, steps_per_file=3
    )
    assert isinstance(plan, Plan)
    return plan


class _Sequential(SequentialRunner):
    def __enter__(self) -> _Sequential:
        return self

    def __exit__(self, *exc: object) -> None:
        pass


IN_DRIVER: dict[str, Callable[[], Any]] = {
    "sequential": _Sequential,
    "thread-peer": lambda: ThreadExecutor(workers=2),
    "thread-hub": lambda: ThreadExecutor(workers=2, comms=None),
    "submit-thread": lambda: SubmitRunner(ThreadBackend(2)),
}
IN_WORKERS: dict[str, Callable[[], Any]] = {
    "process-peer": lambda: ProcessPoolExecutor(workers=2),
    "process-hub": lambda: ProcessPoolExecutor(workers=2, comms=None),
}


@pytest.fixture(scope="module")
def dask_runner_for() -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    from graphed_executors.dask_backend import dask_runner  # noqa: PLC0415

    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        yield lambda: dask_runner(client)


@pytest.fixture(scope="module")
def parsl_runner_for() -> Iterator[Any]:
    pytest.importorskip("parsl")
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m47"))
    from parsl_transport_harness import p2p_tpe_pool  # noqa: PLC0415

    from graphed_executors.parsl_backend import parsl_runner  # noqa: PLC0415

    with p2p_tpe_pool(max_threads=2) as tpe:
        yield lambda: parsl_runner(tpe)


@pytest.fixture(params=[*IN_DRIVER, *IN_WORKERS, "dask", "parsl"])
def make(request: pytest.FixtureRequest) -> Callable[[], Any]:
    if request.param in IN_DRIVER:
        return IN_DRIVER[request.param]
    if request.param in IN_WORKERS:
        return IN_WORKERS[request.param]
    factory: Callable[[], Any] = request.getfixturevalue(f"{request.param}_runner_for")
    return factory


@pytest.fixture(params=[*IN_WORKERS, "dask"])
def make_in_workers(request: pytest.FixtureRequest) -> Callable[[], Any]:
    if request.param in IN_WORKERS:
        return IN_WORKERS[request.param]
    factory: Callable[[], Any] = request.getfixturevalue(f"{request.param}_runner_for")
    return factory


def _ran(make: Callable[[], Any], plan: Plan[Any]) -> tuple[Any, list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as seen, make() as runner:
        warnings.simplefilter("always")
        return runner.run(plan).value, seen


def test_the_trap_raises_at_the_recording_line_on_every_runner(make: Callable[[], Any]) -> None:
    with make() as runner, pytest.raises(StageError) as raised:
        runner.run(raise_on_floating_point(_plan()))
    message = str(raised.value)
    assert "divide" in message
    assert "test_floating_point_runners.py" in message
    assert "mem://events" in message


def test_the_default_is_the_same_on_every_runner(make: Callable[[], Any]) -> None:
    value, _ = _ran(make, _plan())
    assert value == [float("inf"), 2.0, float("inf"), 4.0, 5.0, 6.0]


def test_a_worker_warning_reaches_the_driver_once(make_in_workers: Callable[[], Any]) -> None:
    _, seen = _ran(make_in_workers, _plan())
    carried = [w for w in seen if "[first in " in str(w.message)]
    assert [w.category for w in carried] == [RuntimeWarning]
    assert "divide by zero" in str(carried[0].message)
    assert "mem://events" in str(carried[0].message)
