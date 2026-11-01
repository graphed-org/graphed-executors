"""An unguarded script is told about its missing ``__main__`` guard, once, on the driver."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = """
import operator
from graphed.core import Partition, Plan, Task
from graphed_executors import {executor}

def one(part, res):
    return 1

plan = Plan(process=one, combine=operator.add, empty=int,
            tasks=tuple(Task(i, Partition("f", "t", i, i + 1)) for i in range(4)))
{body}
"""
UNGUARDED = "print({executor}(2, comms={comms}).run(plan).value)"
GUARDED = 'if __name__ == "__main__":\n    print({executor}(2, comms={comms}).run(plan).value)'


def run_script(
    tmp_path: Path, executor: str, comms: str, body: str
) -> subprocess.CompletedProcess[str]:
    script = tmp_path / "analysis.py"
    script.write_text(
        SCRIPT.format(executor=executor, body=body.format(executor=executor, comms=comms))
    )
    return subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, timeout=120, check=False
    )


@pytest.mark.parametrize(
    ("executor", "comms"),
    [
        ("ProcessPoolExecutor", "'ipc'"),
        ("ProcessPoolExecutor", "None"),
        ("ProcessPoolExecutor", "'http'"),
        ("PinnedPoolExecutor", "'ipc'"),
    ],
)
def test_an_unguarded_script_is_told_the_fix(tmp_path: Path, executor: str, comms: str) -> None:
    out = run_script(tmp_path, executor, comms, UNGUARDED)
    assert out.returncode != 0
    assert 'if __name__ == "__main__":' in out.stderr.splitlines()[-1]
    assert out.stderr.count("re-imported your script") == 1
    assert "BrokenProcessPool" not in out.stderr


def test_a_guarded_script_is_unaffected(tmp_path: Path) -> None:
    out = run_script(tmp_path, "ProcessPoolExecutor", "'ipc'", GUARDED)
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
