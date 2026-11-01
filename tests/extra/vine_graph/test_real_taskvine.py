"""Plans through a real TaskVine manager and workers, where a CCTools build has vine_graph."""

from __future__ import annotations

import collections
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from graphed.core import Plan
from graphed.core.execution import TaskPhase
from graphed.debug import StageError
from vine_plan_fns import combine, empty, process, tasks

from graphed_executors.vine_graph_backend import vine_graph_runner

vg = pytest.importorskip("ndcctools.taskvine.vine_graph")
PORT = 9823


@pytest.fixture(scope="module")
def executor(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    os.environ["VINE_TEST_DRIVER_PID"] = str(os.getpid())
    run_dir = tmp_path_factory.mktemp("vine")
    cwd = Path.cwd()
    os.chdir(run_dir)
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    worker = [shutil.which("vine_worker") or "vine_worker", "--cores", "2", "--memory", "4000"]
    worker += ["--disk", "4000", "--timeout", "120", "localhost", str(PORT)]
    logs = subprocess.DEVNULL
    workers = [subprocess.Popen(worker, env=env, stdout=logs, stderr=logs) for _ in range(2)]
    with vg.Executor(PORT, params={"progress-bar": 0, "max-retries": 2}) as ex:
        yield ex
    for proc in workers:
        proc.terminate()
        proc.wait()
    os.chdir(cwd)


class Recorder:
    """A monitor with no ``worker_monitor_factory``: its events must reach the driver."""

    def __init__(self) -> None:
        self.events: list[Any] = []

    def on_task(self, event: Any) -> None:
        self.events.append(event)

    def on_combine(self, *_: Any) -> None:
        pass

    def on_profile(self, *_: Any) -> None:
        pass

    def worker_profiler_factory(self) -> None:
        return None


def test_a_monitored_plan_leaves_partial_results_on_the_workers(
    executor: Any, tmp_path: Path
) -> None:
    monitor = Recorder()
    plan = Plan(process, combine, empty, tasks(8, f"data|{tmp_path}"))
    with vine_graph_runner(executor, monitor=monitor) as runner:
        result = runner.run(plan)
    assert result.value.total == 80
    phases = collections.Counter(event.phase for event in monitor.events)
    assert phases[TaskPhase.FINISHED] == 8
    assert phases[TaskPhase.STARTED] == 8
    # Only the root reaches the driver.
    assert len(list(tmp_path.iterdir())) == 1
    workers = {e.worker for e in monitor.events if e.phase is TaskPhase.FINISHED}
    assert len(workers) == 2, workers


@pytest.mark.parametrize("n", [1, 2, 5])
def test_a_killed_function_names_its_partition(executor: Any, n: int) -> None:
    plan = Plan(process, combine, empty, tasks(n, bad="segfault"))
    with vine_graph_runner(executor) as runner, pytest.raises(StageError) as raised:
        runner.run(plan)
    assert "segfault" in raised.value.partition
    assert "signal 11" in raised.value.cause_message


def test_a_raising_function_raises_its_exception_and_reports_it(executor: Any) -> None:
    monitor = Recorder()
    plan = Plan(process, combine, empty, tasks(3, bad="raise"))
    with (
        vine_graph_runner(executor, monitor=monitor) as runner,
        pytest.raises(ValueError, match="no entries in 0"),
    ):
        runner.run(plan)
    deadline = time.monotonic() + 30
    while not any(e.phase is TaskPhase.ERRORED for e in monitor.events):
        assert time.monotonic() < deadline, monitor.events
        time.sleep(0.05)


def test_a_function_that_raises_runs_once(executor: Any, tmp_path: Path) -> None:
    plan = Plan(process, combine, empty, tasks(1, bad=f"count|{tmp_path}"))
    with vine_graph_runner(executor) as runner, pytest.raises(ValueError, match="counted"):
        runner.run(plan)
    assert len(list(tmp_path.iterdir())) == 1
