"""A monitor with only ``on_task`` and ``ProgressBar`` run on every runner."""

from __future__ import annotations

import operator
import os
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from graphed import WorkerLostError
from graphed.core import (
    MonitorBase,
    Partition,
    Plan,
    RunControl,
    StopReason,
    Task,
    TaskEvent,
    TaskPhase,
)
from graphed.debug import ProgressBar

from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor
from graphed_executors.submit import SubmitRunner, ThreadBackend

N = 6

LOCAL: dict[str, Callable[..., Any]] = {
    "thread-peer": lambda **kw: ThreadExecutor(max_workers=2, **kw),
    "thread-hub": lambda **kw: ThreadExecutor(max_workers=2, comms=None, **kw),
    "thread-pooled": lambda **kw: ThreadExecutor(
        max_workers=2, comms=None, pooled_combines=True, **kw
    ),
    "process-peer": lambda **kw: ProcessPoolExecutor(max_workers=2, **kw),
    "process-hub": lambda **kw: ProcessPoolExecutor(max_workers=2, comms=None, **kw),
    "pinned-peer": lambda **kw: PinnedPoolExecutor(max_workers=2, **kw),
    "submit-thread": lambda **kw: SubmitRunner(ThreadBackend(2), **kw),
}


def ordered(part: Partition, _resources: object) -> tuple[int, ...]:
    return (part.entry_stop,)


def slow(part: Partition, _resources: object) -> tuple[int, ...]:
    time.sleep(0.05)
    return (part.entry_stop,)


def fail(part: Partition, _resources: object) -> tuple[int, ...]:
    raise ValueError("bad")


def die(part: Partition, _resources: object) -> tuple[int, ...]:
    os._exit(17)


def _plan(process: Callable[..., tuple[int, ...]] = ordered, n: int = N) -> Plan[Any]:
    return Plan(
        process=process,
        combine=operator.add,
        empty=tuple,
        tasks=tuple(Task(i, Partition("mem://p", "t", i, i + 1)) for i in range(n)),
    )


class OnlyOnTask:
    complete_events = True

    def __init__(self) -> None:
        self.finished = 0

    def on_task(self, event: TaskEvent) -> None:
        self.finished += event.phase is TaskPhase.FINISHED


class OverridesOnTask(OnlyOnTask, MonitorBase):
    pass


class RaisingFactory(OverridesOnTask):
    def worker_profiler_factory(self) -> Any:
        raise RuntimeError("factory")


class CancelAfterFirst(ProgressBar):  # type: ignore[misc]
    def __init__(self, control: RunControl) -> None:
        super().__init__()
        self.control = control

    def on_task(self, event: TaskEvent) -> None:
        super().on_task(event)
        if event.phase is TaskPhase.FINISHED:
            self.control.cancel()


@pytest.fixture(autouse=True)
def text_bar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "tqdm.auto", None)


@pytest.fixture(scope="module")
def dask_backend() -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    from graphed_executors.dask_backend import DaskBackend  # noqa: PLC0415
    from graphed_executors.dask_backend.plugin import GraphedWorkerPlugin  # noqa: PLC0415

    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        client.register_plugin(GraphedWorkerPlugin())
        yield lambda: DaskBackend(client)


@pytest.fixture(scope="module")
def parsl_backend() -> Iterator[Any]:
    pytest.importorskip("parsl")
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m47"))
    from parsl_transport_harness import p2p_tpe_pool  # noqa: PLC0415

    from graphed_executors.parsl_backend import ParslBackend  # noqa: PLC0415

    with p2p_tpe_pool(max_threads=2) as tpe:
        yield lambda: ParslBackend(tpe)


@pytest.fixture(params=[*LOCAL, "dask", "parsl"])
def make(request: pytest.FixtureRequest) -> Callable[..., Any]:
    if request.param in LOCAL:
        return LOCAL[request.param]
    backend = request.getfixturevalue(f"{request.param}_backend")
    return lambda **kw: SubmitRunner(backend(), **kw)


@pytest.mark.parametrize("monitor", [OnlyOnTask, OverridesOnTask, RaisingFactory])
def test_a_monitor_with_only_on_task_runs(make: Callable[..., Any], monitor: Any) -> None:
    watcher = monitor()
    with make(monitor=watcher) as runner:
        assert len(runner.run(_plan()).value) == N
    assert watcher.finished == N


def test_bar_reaches_total_and_leaves_the_result_alone(
    make: Callable[..., Any], capsys: pytest.CaptureFixture[str]
) -> None:
    with make() as runner:
        plain = runner.run(_plan())
    with make(monitor=ProgressBar()) as runner:
        watched = runner.run(_plan())
    assert watched.value == plain.value == tuple(range(1, N + 1))
    assert capsys.readouterr().err.endswith(f"\rtasks: {N}/{N}\n")


def test_bar_shows_the_whole_plan_under_a_control_window(
    make: Callable[..., Any], capsys: pytest.CaptureFixture[str]
) -> None:
    with make(monitor=ProgressBar(), control=RunControl()) as runner:
        assert len(runner.run(_plan(n=12)).value) == 12
    err = capsys.readouterr().err
    assert err.endswith("\rtasks: 12/12\n")
    assert not set(err.replace("\r", "\n").split("\n")) - {
        "",
        *(f"tasks: {done}/12" for done in range(13)),
    }


def test_a_cancelled_run_closes_the_bar(
    make: Callable[..., Any], capsys: pytest.CaptureFixture[str]
) -> None:
    control = RunControl()
    with make(monitor=CancelAfterFirst(control), control=control) as runner:
        result = runner.run(_plan(slow, n=40))
    assert result.stopped is StopReason.CANCELLED
    err = capsys.readouterr().err
    assert err.endswith("\n")
    assert "tasks: 40/40" not in err


def test_a_failed_run_closes_the_bar(
    make: Callable[..., Any], capsys: pytest.CaptureFixture[str]
) -> None:
    with make(monitor=ProgressBar()) as runner, pytest.raises(Exception, match="bad"):
        runner.run(_plan(fail))
    assert capsys.readouterr().err.endswith("\n")


@pytest.mark.parametrize("name", ["process-peer", "process-hub", "pinned-peer"])
def test_a_lost_worker_closes_the_bar(name: str, capsys: pytest.CaptureFixture[str]) -> None:
    with LOCAL[name](monitor=ProgressBar(), retries=0) as runner, pytest.raises(WorkerLostError):
        runner.run(_plan(die))
    assert capsys.readouterr().err == f"\rtasks: 0/{N}\n"


class _Order(OverridesOnTask):
    def __init__(self, calls: list[str]) -> None:
        super().__init__()
        self.calls = calls

    def on_run_start(self, n_tasks: int) -> None:
        self.calls.append("on_run_start")


def test_an_earlier_runs_events_drain_before_the_next_run_opens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    switch_in = ProcessPoolExecutor._switch_in

    def spy(self: ProcessPoolExecutor, monitor: Any) -> None:
        calls.append("switch_in")
        switch_in(self, monitor)

    monkeypatch.setattr(ProcessPoolExecutor, "_switch_in", spy)
    with ProcessPoolExecutor(max_workers=1, comms=None, monitor=_Order(calls)) as runner:
        runner.run(_plan())
    assert calls == ["switch_in", "on_run_start"]
