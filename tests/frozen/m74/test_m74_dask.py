"""m74 §4.2 E4 and E7 (dask): a ``dask_runner`` driver over a process ``LocalCluster(2)`` resumes after
its session is killed; a store left by a killed ``ThreadExecutor`` run resumes on ``transport_run_plan``."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("distributed")

from m74_harness import PROCESS_WORKERS, assert_resumed, crash_and_resume

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="kills a POSIX session")


def test_e4_a_killed_dask_run_resumes_from_the_store(tmp_path: Path) -> None:
    assert_resumed(crash_and_resume(tmp_path, "dask"), "dask" in PROCESS_WORKERS)


def test_e7_a_killed_thread_run_resumes_on_the_dask_peer_transport(tmp_path: Path) -> None:
    assert_resumed(crash_and_resume(tmp_path, "thread", resume_on="dask-peer"), process_workers=False)
