"""A worker error must still tear the run down when a >64 KB partial is parked in a pipe write
(tests/extra — NOT frozen).

Routing the actor's sends to a background thread keeps the actor reading only while it is *running*.
A worker that raises leaves its loop and never reads its inbox again, so a partial already parked in
its pipe can never complete — and it holds that inbox's cross-process write lock while it waits. Both
ends of the teardown then hang unless they are bounded:

* the PEER's exit path, if it joins its outbox thread — its future never completes and the driver
  blocks in ``pool.shutdown()``;
* the DRIVER's own ``done`` release, if it writes to the crashed worker's inbox on the collecting
  thread — the driver never reaches teardown at all, and the live workers are never released.

``comms="http"`` reaches the same teardown over real sockets, where the driver's release is queued for
a background POST instead: a close that stops the sender before it drains loses it just as surely.

DISCRIMINATION: each test first runs the identical scenario with a 20 KB partial (fits the pipe,
nothing ever parks) and only then the 160 KB one; payload size is the only variable.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from _big_partial import BIG, SMALL, plan, run_in_child

from graphed_executors.local import ProcessPoolExecutor

# leaf 1 is ODD at level 0, so its parent is owned by w0 and w1 ships its partial there on finishing.
# CRASH_FIRST: w0 is gone before that partial is sent — the parked write is w1's exit problem.
# CRASH_LAST:  w0 is still computing when the partial parks in its pipe, and only then raises — the
#              parked write now holds the lock the DRIVER's own `done` needs.
CRASH_FIRST = (0, 800)
CRASH_LAST = (500, 1)


def _child(n_floats: int, weights: tuple[int, ...], comms: str, out_path: str) -> None:
    """Spawn-child entry point: the error must surface AND the pool must shut down afterwards."""
    executor = ProcessPoolExecutor(max_workers=2, steal=True, monitor=None, comms=comms)
    try:
        executor.run(plan(n_floats, weights, boom={0}))
        outcome = "no-error"
    except ValueError as exc:
        outcome = str(exc)
    executor.close()  # joins the worker processes — a wedged peer never gets past this
    Path(out_path).write_text(outcome)  # written last: its absence means teardown never returned


def _run(scenario: str, n_floats: int, weights: tuple[int, ...], comms: str, tmp_path: Path) -> str:
    out = tmp_path / f"{scenario}-{comms}-{n_floats}.txt"
    run_in_child(f"{scenario} ({comms})", _child, (n_floats, weights, comms, str(out)))
    return out.read_text()


@pytest.mark.parametrize("comms", ["ipc", "http"])
def test_worker_error_tears_down_with_a_partial_larger_than_the_pipe(comms: str, tmp_path: Path) -> None:
    assert _run("crash-first control", SMALL, CRASH_FIRST, comms, tmp_path) == "kaboom"
    assert _run("crash-first", BIG, CRASH_FIRST, comms, tmp_path) == "kaboom"


@pytest.mark.parametrize("comms", ["ipc", "http"])
def test_worker_error_behind_an_already_parked_partial_tears_down(comms: str, tmp_path: Path) -> None:
    assert _run("crash-last control", SMALL, CRASH_LAST, comms, tmp_path) == "kaboom"
    assert _run("crash-last", BIG, CRASH_LAST, comms, tmp_path) == "kaboom"
