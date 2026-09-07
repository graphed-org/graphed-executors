"""An interrupted driver must still release its peer workers (tests/extra — NOT frozen).

A ``KeyboardInterrupt`` leaves ``_collect_peer`` without any worker error or root in hand. The
actors keep waiting for the driver's ``done``, and the pool shutdown that follows — the
non-persistent ``finally`` or the persistent discard after a failed run — joins them forever.
"""

from __future__ import annotations

import _thread
import threading
from pathlib import Path

import pytest
from _big_partial import BIG, plan, run_in_child

from graphed_executors.local import ProcessPoolExecutor

WEIGHTS = (4000, 4000)  # both workers are still computing when the interrupt lands


def _child(persistent: bool, out_path: str) -> None:
    executor = ProcessPoolExecutor(max_workers=2, steal=True, monitor=None, persistent=persistent)
    threading.Timer(1.0, _thread.interrupt_main).start()  # a SIGINT in the driver's main thread
    try:
        executor.run(plan(BIG, WEIGHTS))
        outcome = "no-error"
    except KeyboardInterrupt:
        outcome = "interrupted"
    executor.close()
    Path(out_path).write_text(outcome)  # last: absence means the shutdown wedged


@pytest.mark.parametrize("persistent", [False, True])
def test_interrupted_driver_releases_the_workers(persistent: bool, tmp_path: Path) -> None:
    out = tmp_path / f"interrupt-{persistent}.txt"
    run_in_child(f"interrupt persistent={persistent}", _child, (persistent, str(out)))
    assert out.read_text() == "interrupted"
