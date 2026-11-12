"""A pool that lost a worker shuts down even when a survivor outlives the pool's SIGTERM."""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

SCRIPT = """
import operator, os, signal, sys, time
from graphed import WorkerLostError
from graphed.core import Partition, Plan, Task
from graphed_executors import ProcessPoolExecutor

def chunk(part, res):
    if part.entry_start == 0:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        with open({pidfile!r}, "w") as f:
            f.write(str(os.getpid()))
        time.sleep(300)
        return 1
    time.sleep(1.0)  # the other worker ignores SIGTERM by now
    os._exit(1)

plan = Plan(process=chunk, combine=operator.add, empty=int,
            tasks=tuple(Task(i, Partition("f", "t", i, i + 1)) for i in range(2)))
if __name__ == "__main__":
    try:
        ProcessPoolExecutor(2, comms={comms}).run(plan)
    except WorkerLostError as err:
        print("LOST", err.partition)
"""


@pytest.fixture
def tmp() -> Iterator[Path]:
    # short: macOS caps the AF_UNIX paths multiprocessing puts in the temp dir
    d = Path(tempfile.mkdtemp(dir=None if sys.platform == "win32" else "/tmp"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.parametrize("comms", ["None", "'ipc'", "'http'"])
def test_a_survivor_that_ignores_sigterm_does_not_hang_the_lost_worker_error(
    tmp: Path, comms: str
) -> None:
    pidfile = tmp / "ignorer.pid"
    script = tmp / "analysis.py"
    script.write_text(SCRIPT.format(pidfile=str(pidfile), comms=comms))
    try:
        out = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
            env={**os.environ, "TMPDIR": str(tmp), "TEMP": str(tmp), "TMP": str(tmp)},
        )
    finally:
        with contextlib.suppress(OSError, ValueError):
            os.kill(int(pidfile.read_text()), getattr(signal, "SIGKILL", signal.SIGTERM))
    assert out.returncode == 0, out.stderr
    lost = out.stdout.strip()
    assert lost.startswith("LOST"), out.stdout + out.stderr
    if sys.platform != "win32":  # where SIGTERM can be ignored, the killed survivor is no culprit
        assert lost == "LOST f:t (entries 1-2)", out.stdout + out.stderr
