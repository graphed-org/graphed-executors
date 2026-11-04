"""An unguarded script is told about its missing ``__main__`` guard, once, on the driver."""

from __future__ import annotations

import operator
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator
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


def test_a_manager_that_fails_otherwise_keeps_its_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def start(self: Any) -> None:
        self._process = SimpleNamespace(exitcode=1, pid=0, join=lambda: None)
        raise EOFError("manager died")

    monkeypatch.setattr(SyncManager, "start", start)
    plan = Plan(
        process=lambda p, r: 1,
        combine=operator.add,
        empty=int,
        tasks=(Task(0, Partition("f", "t", 0, 1)),),
    )
    with pytest.raises(EOFError, match="manager died"):
        ProcessPoolExecutor(2, comms=None, monitor=MonitorBase()).run(plan)
