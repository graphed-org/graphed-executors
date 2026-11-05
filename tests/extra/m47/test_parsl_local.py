"""``ParslRunner.local`` and ``stop_htex`` leave no process behind, within seconds."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from collections.abc import Callable
from pathlib import Path

import pytest
from graphed.core import Partition, Plan, RunControl, Task

from graphed_executors.parsl_backend import ParslRunner, parsl_runner, start_htex, stop_htex
from graphed_executors.parsl_backend import backend as backend_module
from graphed_executors.parsl_backend import launch as launch_module

pytest.importorskip("parsl")
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="parsl HTEX requires POSIX")

BOUND_S = 5.0
HERE = str(Path(__file__).parent)


def own_pid(part: Partition, res: object) -> set[int]:
    return {os.getpid(), os.getppid()}


def union(a: set[int], b: set[int]) -> set[int]:
    return a | b


def nothing() -> set[int]:
    return set()


def explode(part: Partition, res: object) -> set[int]:
    raise OSError("bad chunk")


def _plan(process: Callable[[Partition, object], set[int]] = own_pid) -> Plan[set[int]]:
    tasks = tuple(Task(i, Partition("f", "t", i, i + 1)) for i in range(4))
    return Plan(process=process, combine=union, empty=nothing, tasks=tasks)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _gone_within_bound(pids: set[int]) -> None:
    deadline = time.monotonic() + BOUND_S
    while (alive := {p for p in pids if _alive(p)}) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not alive


def _block_pids(executor: object) -> set[int]:
    return {job["remote_pid"] for job in executor.provider.resources.values()}  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def _workers_import_this_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([HERE, os.environ.get("PYTHONPATH", "")]))


def test_leaving_the_block_stops_the_pool(tmp_path: Path) -> None:
    control = RunControl()
    with ParslRunner.local(workers=2, run_dir=str(tmp_path), control=control) as runner:
        assert runner.control is control
        pids = runner.run(_plan()).value
        assert len(pids) >= 2, "a worker and its manager"
        assert all(_alive(p) for p in pids)
    _gone_within_bound(pids)


def test_a_failed_run_stops_the_pool(tmp_path: Path) -> None:
    with (
        pytest.raises(OSError, match="bad chunk"),
        ParslRunner.local(workers=2, run_dir=str(tmp_path), retries=0) as runner,
    ):
        pids = runner.run(_plan()).value
        runner.run(_plan(explode))
    _gone_within_bound(pids)


def test_stop_htex_stops_the_workers(tmp_path: Path) -> None:
    htex = start_htex(workers=2, run_dir=str(tmp_path))
    try:
        with parsl_runner(htex, retries=0) as runner:
            pids = runner.run(_plan()).value | _block_pids(htex)
        assert isinstance(runner, ParslRunner)
        assert all(_alive(p) for p in pids), "closing the runner leaves a caller's executor alone"
    finally:
        stop_htex(htex)
    _gone_within_bound(pids)


def test_an_unactivated_venv_runs(tmp_path: Path) -> None:
    script = textwrap.dedent(
        f"""
        from graphed_executors.parsl_backend import ParslRunner
        from test_parsl_local import _plan

        with ParslRunner.local(workers=1, run_dir={str(tmp_path)!r}, retries=0) as runner:
            print(len(runner.run(_plan()).value) > 0)
        """
    )
    env = {**os.environ, "PATH": "/usr/bin:/bin"}
    done = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=120
    )
    assert done.stdout.strip() == "True", done.stderr[-500:]


def test_no_interchange_anywhere_is_refused_by_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", "/nowhere")
    monkeypatch.setattr(sys, "executable", "/also/nowhere/python")
    with pytest.raises(RuntimeError, match=r"interchange\.py is not on PATH; activate the venv"):
        start_htex(workers=1, run_dir="unused")
    assert os.environ["PATH"] == "/nowhere"


def test_an_interchange_on_path_leaves_path_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", os.pathsep.join([str(Path(sys.executable).parent), "/usr/bin"]))
    before = os.environ["PATH"]
    launch_module._scripts_on_path()
    assert os.environ["PATH"] == before


def test_a_refused_runner_stops_its_pool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[object] = []

    def recording_start(**kw: object) -> object:
        started.append(start_htex(**kw))  # type: ignore[arg-type]
        return started[-1]

    monkeypatch.setattr(backend_module, "start_htex", recording_start)
    with pytest.raises(ValueError, match="max_in_flight"):
        ParslRunner.local(workers=1, run_dir=str(tmp_path), max_in_flight=0)
    _gone_within_bound(_block_pids(started[0]))
