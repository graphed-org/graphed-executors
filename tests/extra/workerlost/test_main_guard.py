"""An unguarded script is told about its missing ``__main__`` guard, once, on the driver."""

from __future__ import annotations

import multiprocessing
import operator
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from multiprocessing.context import SpawnProcess
from multiprocessing.managers import SyncManager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from graphed.core import MonitorBase, Partition, Plan, Task

from graphed_executors import ProcessPoolExecutor

SCRIPT = """
import operator
from graphed.core import Partition, Plan, Task
from graphed_executors import {executor}

def one(part, res):
    return 1

class Rec:
    def on_task(self, event): pass

plan = Plan(process=one, combine=operator.add, empty=int,
            tasks=tuple(Task(i, Partition("f", "t", i, i + 1)) for i in range(4)))
{body}
"""
UNGUARDED = "print({executor}(2, comms={comms}).run(plan).value)"
GUARDED = 'if __name__ == "__main__":\n    print({executor}(2, comms={comms}).run(plan).value)'


@pytest.fixture
def tmp() -> Iterator[Path]:
    # the scripts' temp dir too, so short: macOS caps the AF_UNIX paths multiprocessing puts there
    d = Path(tempfile.mkdtemp(dir=None if sys.platform == "win32" else "/tmp"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def run_script(
    tmp: Path, executor: str, comms: str, body: str
) -> subprocess.CompletedProcess[str]:
    script = tmp / "analysis.py"
    script.write_text(
        SCRIPT.format(executor=executor, body=body.format(executor=executor, comms=comms))
    )
    return subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "TMPDIR": str(tmp), "TEMP": str(tmp), "TMP": str(tmp)},
    )


@pytest.mark.parametrize(
    ("executor", "comms"),
    [
        ("ProcessPoolExecutor", "'ipc'"),
        ("ProcessPoolExecutor", "None"),
        ("ProcessPoolExecutor", "'http'"),
        ("ProcessPoolExecutor", "None, monitor=Rec()"),
        ("PinnedPoolExecutor", "'ipc'"),
    ],
)
def test_an_unguarded_script_is_told_the_fix(tmp: Path, executor: str, comms: str) -> None:
    out = run_script(tmp, executor, comms, UNGUARDED)
    assert out.returncode != 0
    assert 'if __name__ == "__main__":' in out.stderr.splitlines()[-1]
    assert out.stderr.count("re-imported your script") == 1
    assert "BrokenProcessPool" not in out.stderr
    assert not list(tmp.glob("graphed-in-flight-*"))


def test_a_guarded_script_is_unaffected(tmp: Path) -> None:
    out = run_script(tmp, "ProcessPoolExecutor", "'ipc'", GUARDED)
    assert (out.returncode, out.stdout.strip()) == (0, "4"), out.stderr


def test_python_dash_c_is_unaffected() -> None:
    code = (
        "import operator, os\n"
        "from graphed.core import Partition, Plan, Task\n"
        "from graphed_executors import ProcessPoolExecutor\n"
        "tasks = tuple(Task(i, Partition('f', 't', i, i + 1)) for i in range(4))\n"
        "plan = Plan(process=operator.is_, combine=operator.or_, empty=bool, tasks=tasks)\n"
        "print(ProcessPoolExecutor(2).run(plan).value)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120, check=False
    )
    assert (out.returncode, out.stdout.strip()) == (0, "False"), out.stderr


PLAN = Plan(
    process=lambda p, r: 1,
    combine=operator.add,
    empty=int,
    tasks=(Task(0, Partition("f", "t", 0, 1)),),
)


@pytest.mark.parametrize("delay", [0, 0.2])  # 0.2: the driver sees the death before start() raises
def test_a_manager_that_fails_otherwise_keeps_its_error(
    monkeypatch: pytest.MonkeyPatch, delay: float
) -> None:
    def start(self: Any) -> None:
        self._process = SimpleNamespace(exitcode=1, pid=0, join=lambda: None)
        time.sleep(delay)
        raise EOFError("manager died")

    monkeypatch.setattr(SyncManager, "start", start)
    with pytest.raises(EOFError, match="manager died"):
        ProcessPoolExecutor(2, comms=None, monitor=MonitorBase()).run(PLAN)


def test_a_manager_that_cannot_spawn_keeps_its_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def start(self: Any) -> None:
        raise OSError("spawn failed")

    monkeypatch.setattr(SpawnProcess, "start", start)
    with pytest.raises(OSError, match="spawn failed"):
        ProcessPoolExecutor(2, comms=None, monitor=MonitorBase()).run(PLAN)


@pytest.mark.parametrize(
    ("exitcode", "error", "match"),
    [(83, RuntimeError, "re-imported your script"), (1, EOFError, "exited with code 1")],
)
def test_a_manager_that_dies_without_answering_is_not_awaited(
    monkeypatch: pytest.MonkeyPatch, exitcode: int, error: type[Exception], match: str
) -> None:
    release = threading.Event()  # Windows: start() never returns once the server is gone

    def start(self: Any) -> None:
        self._process = SimpleNamespace(exitcode=exitcode, pid=0, join=lambda: None)
        release.wait()

    monkeypatch.setattr(SyncManager, "start", start)
    try:
        with pytest.raises(error, match=match):
            ProcessPoolExecutor(2, comms=None, monitor=MonitorBase()).run(PLAN)
    finally:
        release.set()


def test_a_worker_reimporting_main_exits_without_running(monkeypatch: pytest.MonkeyPatch) -> None:
    def exit_(code: int) -> None:
        raise SystemExit(code)

    ex = ProcessPoolExecutor(2)
    monkeypatch.setattr(
        multiprocessing, "current_process", lambda: SimpleNamespace(_inheriting=True)
    )
    monkeypatch.setattr(os, "_exit", exit_)
    with pytest.raises(SystemExit) as exited:
        ex.run(PLAN)
    assert exited.value.code == 83
    assert not os.path.exists(ex._in_flight)
