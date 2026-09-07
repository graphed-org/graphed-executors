"""A failed peer run must not poison the NEXT run of a persistent pool (tests/extra — NOT frozen).

The bounded outbox exit deliberately abandons a >64 KB partial parked in a crashed worker's pipe, and
the driver's own ``done`` is released off-thread — so both can land *after* the failing ``run()``
returns. With ``persistent=True`` the pool survives, and draining the inboxes at the start of the next
run happens too early to catch them: that run's worker takes the stale ``done`` as its first message
and leaves its actor without reducing anything, so no root ever forms and the driver hangs.

DISCRIMINATION: the identical scenario runs first with a 20 KB partial (fits the pipe, nothing parks)
and then with the 160 KB one — only the latter wedges. The discard witness holds on both legs.
"""

from __future__ import annotations

from pathlib import Path

from _big_partial import BIG, SMALL, plan, run_in_child

from graphed_executors.local import ProcessPoolExecutor

# w0 owns the 0.5 s leaf and raises at the end of it; w1's leaf 1 is ODD, so w1 ships its partial to
# w0 early and parks there mid-write for the rest of the run.
WEIGHTS = (500, 1)


def _child(n_floats: int, out_path: str) -> None:
    """Spawn-child entry point: fail a run, then get a CORRECT result out of the reused pool."""
    executor = ProcessPoolExecutor(max_workers=2, steal=True, monitor=None, persistent=True)
    try:
        executor.run(plan(n_floats, WEIGHTS, boom={0}))
        outcome = "no-error"
    except ValueError as exc:
        outcome = str(exc)
    discarded = executor._peer_pool is None  # the failed run's pool must not be reused
    value = executor.run(plan(n_floats, WEIGHTS)).value
    executor.close()
    correct = bool((value == float(len(WEIGHTS))).all())
    Path(out_path).write_text(f"{outcome}|{discarded}|{correct}")  # last: absence means it wedged


def _run(scenario: str, n_floats: int, tmp_path: Path) -> str:
    out = tmp_path / f"{scenario}-{n_floats}.txt"
    run_in_child(scenario, _child, (n_floats, str(out)))
    return out.read_text()


def test_persistent_pool_reuse_after_a_failed_run_with_a_big_partial(tmp_path: Path) -> None:
    assert _run("persistent-reuse control", SMALL, tmp_path) == "kaboom|True|True"
    assert _run("persistent-reuse", BIG, tmp_path) == "kaboom|True|True"
