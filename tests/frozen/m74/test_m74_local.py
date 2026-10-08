"""m74 §4.2 E1-E3: a driver SIGKILLed after ``MIN_DONE`` stored tasks resumes on the local runners
(``ThreadExecutor(2)`` and ``ProcessPoolExecutor(2)`` with peer ipc, ``SubmitRunner(ThreadBackend(2))``),
executing only the tasks the store did not hold, bit-for-bit equal to an uninterrupted run."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from m74_harness import PROCESS_WORKERS, assert_resumed, crash_and_resume

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="kills a POSIX session")


@pytest.mark.parametrize("runner", ["thread", "process", "submit-thread"], ids=["E1", "E2", "E3"])
def test_a_killed_local_run_resumes_from_the_store(tmp_path: Path, runner: str) -> None:
    assert_resumed(crash_and_resume(tmp_path, runner), runner in PROCESS_WORKERS)
