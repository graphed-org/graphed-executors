"""m46 m42-parity Plan path — `SubmitRunner(ParslBackend(HTEX))` bit-for-bit vs `SequentialRunner`
(plan §1.2, §3 m46 `test_parsl_plan_parity`).

The engine is reused, not rebuilt, so parity is inherited — what THIS module witnesses is that the
parsl seam underneath it is real: leaves AND combines execute in HTEX worker processes (pid-span
provenance — a driver-fold matches values but fails the pid witness), the value is byte-identical
across two runs (determinism x2), `close()` does not tear down the caller-owned executor (the m42
ready-client-in precedent), and the §1.2 monitor piggyback delivers the per-leaf worker phases
(2·n worker events + n driver SUBMITTED) after completion, in phase order, carrying worker-side
identities.

Discriminates: a driver-side fold (leaf/combine pids == driver pid), a nondeterministic reduction
order (bit-for-bit x2 fails), a `close()` that shuts the executor down (the follow-on submit
dies), and a monitor path that drops or driver-stamps the worker-side STARTED/FINISHED events."""

from __future__ import annotations

import os
import pickle
import sys

import pytest
from graphed.core.execution import SequentialRunner, StopReason
from parsl_harness import (
    RecordingMonitor,
    concat_plan,
    double,
    events_by_key,
    expected_concat,
    htex_pool,
    make_parsl_backend,
    make_runner,
    pv_plan,
    run_bounded,
    wait_for,
)

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (parsl ships no Windows support; Windows keeps the base "
    "package only — plan §4 G8)",
)


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with htex_pool(workers=2) as ex:
        yield ex


def test_fixed_plan_matches_sequential_bit_for_bit_twice(htex) -> None:  # type: ignore[no-untyped-def]
    plan = concat_plan(9, "m46par")
    expected = SequentialRunner().run(plan)
    runner = make_runner(make_parsl_backend(htex))
    got1 = run_bounded(lambda: runner.run(plan))
    got2 = run_bounded(lambda: runner.run(plan))  # determinism x2 on the same runner
    for got in (got1, got2):
        assert pickle.dumps(got.value) == pickle.dumps(expected.value)
        assert got.value == expected_concat(9, "m46par")
        assert got.n_partitions == 9
        assert got.n_combines == 8
        assert got.stopped is StopReason.EXHAUSTED


def test_leaves_and_combines_run_in_worker_processes_and_span_two(htex) -> None:  # type: ignore[no-untyped-def]
    runner = make_runner(make_parsl_backend(htex))
    result = run_bounded(lambda: runner.run(pv_plan(12, "m46spread")))
    pv = result.value
    assert pv.payload == expected_concat(12, "m46spread")  # non-vacuity: the full run happened
    assert len(pv.leaves) == 12
    driver_pid = os.getpid()
    leaf_pids = {pid for _, pid, _ in pv.leaves}
    assert driver_pid not in leaf_pids, "a leaf ran in the DRIVER process — head-node compute"
    assert len({worker for _, _, worker in pv.leaves}) >= 2, (
        f"all 12 leaves ran on one worker: { ({worker for _, _, worker in pv.leaves}) } — the pool "
        "never spread (or the site witness is synthesized)"
    )
    assert pv.combine_sites, "no combine recorded a site — the tree reduction never ran as tasks"
    assert driver_pid not in {pid for pid, _ in pv.combine_sites}, (
        "a combine ran in the DRIVER process — combines must be submitted like any task "
        "(driver-side arg RESOLUTION is the §1.2 contract; driver-side COMPUTE is not)"
    )


def test_close_does_not_shut_down_the_callers_executor(htex) -> None:  # type: ignore[no-untyped-def]
    """`ParslBackend.close()` must leave the caller-owned executor alive (plan §1.2; the m42
    ready-client-in precedent) — witnessed by a successful follow-on submit on a FRESH backend
    over the SAME module-scoped pool."""
    with make_runner(make_parsl_backend(htex)) as runner:
        assert run_bounded(lambda: runner.run(concat_plan(4, "m46close"))).value == expected_concat(
            4, "m46close"
        )
    follow_on = make_parsl_backend(htex).submit(double, 21, key="graphed-m46par-close-probe")
    assert run_bounded(lambda: follow_on.result(timeout=120)) == 42, (
        "the pool died with backend.close() — close() must not shut down the caller's executor"
    )


def test_monitor_piggyback_delivers_worker_phases_after_completion(htex) -> None:  # type: ignore[no-untyped-def]
    """M37 through the seam (plan §1.2 monitor position/G10): per leaf exactly one driver-side
    SUBMITTED + worker-side STARTED + FINISHED (2·n worker events, completion-granularity
    delivery), no phantom keys, value unchanged by observation."""
    monitor = RecordingMonitor()
    runner = make_runner(make_parsl_backend(htex), monitor=monitor)
    value = run_bounded(lambda: runner.run(concat_plan(6, "m46mon"))).value
    assert value == expected_concat(6, "m46mon")
    wait_for(lambda: len(monitor.events) >= 18)
    per_key = events_by_key(monitor.events)
    assert set(per_key) == set(range(6)), "leaf keys only — no phantom tasks"
    for key in range(6):
        phases = [str(ev.phase) for ev in per_key[key]]
        assert sorted(phases) == ["finished", "started", "submitted"]
        assert phases.index("submitted") < phases.index("started") < phases.index("finished")
        for ev in per_key[key]:
            if str(ev.phase) in ("started", "finished"):
                assert ev.worker not in ("", "driver", "local"), (
                    f"a worker-side phase carries no worker identity: {ev!r} — the piggyback env "
                    "must stamp the POOL_ID:RANK worker, not a driver/default-env placeholder"
                )
