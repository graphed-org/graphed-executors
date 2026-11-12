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

PRELUDE = """
import faulthandler, multiprocessing, operator, os, signal, sys, time
from concurrent.futures import FIRST_EXCEPTION, wait
from graphed import WorkerLostError
from graphed.core import Partition, Plan, Task
from graphed_executors import ProcessPoolExecutor
from graphed_executors.local import _process_peer

_process_peer._LOST_JOIN_S = 0.5  # the driver's wait on survivors; only an ignorer outlasts it
faulthandler.dump_traceback_later(45, exit=True)  # a hang names its frames, inside the 60 s run

def ignore():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with open({pidfile!r}, "w") as f:
        f.write(str(os.getpid()))
    time.sleep(300)

def crash():
    while not os.path.exists({pidfile!r}):  # the other worker ignores SIGTERM once it exists
        time.sleep(0.05)
    os._exit(1)
"""

SCRIPT = (
    PRELUDE
    + """
# the quick chunk's result wakes the pool's manager after both workers exist; a worker spawned
# after the manager's last wakeup has no sentinel there, so its crash alone would go unseen
def chunk(part, res):
    return (ignore, int, crash)[part.entry_start]()

plan = Plan(process=chunk, combine=operator.add, empty=int,
            tasks=tuple(Task(i, Partition("f", "t", i, i + 1)) for i in range(3)))
if __name__ == "__main__":
    try:
        ProcessPoolExecutor(2, comms={comms}).run(plan)
    except WorkerLostError as err:
        print("LOST", err.partition)
"""
)

CLOSE_SCRIPT = (
    PRELUDE
    + """
if __name__ == "__main__":
    pool = _process_peer._ReapingPool(2, mp_context=multiprocessing.get_context("spawn"))
    wait([pool.submit(int) for _ in range(2)])  # both workers exist and the manager sees them
    wait([pool.submit(ignore), pool.submit(crash)], return_when=FIRST_EXCEPTION)
    pool.shutdown()
    print("CLOSED")
"""
)


@pytest.fixture
def tmp() -> Iterator[Path]:
    # short: macOS caps the AF_UNIX paths multiprocessing puts in the temp dir
    d = Path(tempfile.mkdtemp(dir=None if sys.platform == "win32" else "/tmp"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _run(tmp: Path, source: str, **fmt: str) -> subprocess.CompletedProcess[str]:
    pidfile = tmp / "ignorer.pid"
    script = tmp / "analysis.py"
    script.write_text(source.format(pidfile=str(pidfile), **fmt))
    try:
        return subprocess.run(
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


@pytest.mark.parametrize("comms", ["None", "'ipc'", "'http'"])
def test_a_survivor_that_ignores_sigterm_does_not_hang_the_lost_worker_error(
    tmp: Path, comms: str
) -> None:
    out = _run(tmp, SCRIPT, comms=comms)
    assert out.returncode == 0, out.stderr
    lost = out.stdout.strip()
    assert lost.startswith("LOST"), out.stdout + out.stderr
    if sys.platform != "win32":  # where SIGTERM can be ignored, the killed survivor is no culprit
        assert lost == "LOST f:t (entries 2-3)", out.stdout + out.stderr


def test_a_pool_that_lost_a_worker_shuts_down_past_a_sigterm_ignorer(
    tmp: Path,
) -> None:
    out = _run(tmp, CLOSE_SCRIPT)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "CLOSED", out.stdout + out.stderr
