"""m74 §4.2 E5 and E7 (parsl): a ``parsl_runner`` driver over ``start_htex(workers=2)`` resumes after its
session (interchange and LocalProvider workers included) is killed; a store left by a killed
``ThreadExecutor`` run resumes on ``parsl_run_plan``."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("parsl")

from m74_harness import PROCESS_WORKERS, assert_resumed, crash_and_resume

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="kills a POSIX session")


def test_e5_a_killed_parsl_run_resumes_from_the_store(tmp_path: Path) -> None:
    assert_resumed(crash_and_resume(tmp_path, "parsl"), "parsl" in PROCESS_WORKERS)


def test_e7_a_killed_thread_run_resumes_on_the_parsl_peer_transport(tmp_path: Path) -> None:
    assert_resumed(crash_and_resume(tmp_path, "thread", resume_on="parsl-peer"), process_workers=False)
