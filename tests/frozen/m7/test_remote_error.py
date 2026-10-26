"""A StageError raised in a worker process surfaces in the driver intact.

A StageError raised in a worker PROCESS surfaces in the driver intact, via M6's format_traceback
pointing at the user's analysis line — never an opaque worker traceback (plan M7 / A.3 #8).
"""

from __future__ import annotations

import analyses
import graphed.debug as gd
import pytest
from graphed.core import Partition, Plan, Task

from graphed_exec_local import ProcessExecutor


def test_remote_stage_error_round_trips_and_renders_user_source() -> None:
    plan = Plan(
        process=analyses.raise_stage_error,
        combine=analyses.add_int,
        empty=analyses.zero_int,
        tasks=[Task(0, Partition("f", "E", 0, 4))],
    )
    with pytest.raises(gd.StageError) as info:
        ProcessExecutor(max_workers=1).run(plan)
    err = info.value
    assert err.cause_type == "IndexError"  # the real failure, not a stringified blob
    out = gd.format_traceback(err)
    assert "analyses.py" in out  # the user's analysis frame
    assert "multiprocessing" not in out and "concurrent" not in out  # not a worker/pool traceback
