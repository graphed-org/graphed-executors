"""m43 carried follow-up F2a (m42 review, wf_20cb8bd3-42f APPROVE): an ADAPTIVE run with a monitor
attached must deliver every consumed task's STARTED/FINISHED events to the monitor BEFORE `run()`
returns — at parity with the fixed path's trailing-event drain (engine.py `_run_fixed` drains
`events_seen >= 2n` before unsubscribing; the adaptive path unsubscribes without any drain).

Engine-level and dask-free (runs in the MAIN matrix): the mechanism under test lives in
`graphed_executors.submit.engine.SubmitRunner`, shared by every backend. The harness
`DelayedEventBackend` models dask's ASYNCHRONOUS worker→driver event transport deterministically —
delivery lags emission by 0.5s and an unsubscribed topic never delivers (exactly dask's
`unsubscribe_topic` semantics). Against it, the current adaptive path returns microseconds after
the last future resolves, unsubscribes, and the final batch's events are dropped forever — a
DETERMINISTIC assertion failure, not a race (the delay is scenario construction; the assertions
are event-set counts, R0.10a).

The fixed-path CONTROL test runs the same wrapper and passes on current HEAD — proving delivery
through the delayed transport works and the adaptive failure is specifically the missing drain."""

from __future__ import annotations

from dask_shuffle_backends import (
    BatchFeed,
    DelayedEventBackend,
    EventRecordingMonitor,
    adaptive_leaf_value,
    add_ints,
    f2_events_by_key,
    f2_partitions,
    int_zero,
    make_submit_runner,
)
from graphed.core.execution import Plan, StopReason, Task, TaskPhase

from graphed_executors.submit import ThreadBackend

N = 6


def _assert_all_task_events_delivered(monitor: EventRecordingMonitor, n: int) -> None:
    per_key = f2_events_by_key(monitor.events)
    for key in range(n):
        phases = {ev.phase for ev in per_key.get(key, [])}
        assert TaskPhase.STARTED in phases and TaskPhase.FINISHED in phases, (
            f"task {key} events missing at run() return (got {sorted(str(p) for p in phases)}): "
            "the adaptive path unsubscribed without draining trailing worker events — F2a: it "
            "must wait for delivery like the fixed path's _wait_until(events_seen >= 2*n) drain"
        )


def test_adaptive_run_delivers_all_task_events_before_returning() -> None:
    tasks = [Task(i, p) for i, p in enumerate(f2_partitions(N, "f2a"))]
    feed = BatchFeed([tasks[:3], tasks[3:]])  # two batches; the FINAL batch is the one HEAD drops
    monitor = EventRecordingMonitor()
    backend = DelayedEventBackend(ThreadBackend(max_workers=2), delay_s=0.5)
    plan = Plan(process=adaptive_leaf_value, combine=add_ints, empty=int_zero, next_tasks=feed)

    with make_submit_runner(backend, monitor=monitor) as runner:
        result = runner.run(plan)

    # the run itself is correct and exhausted — only the telemetry contract is at stake
    assert result.stopped is StopReason.EXHAUSTED
    assert result.n_partitions == N
    assert result.value == sum(7 * i + 3 for i in range(N))

    _assert_all_task_events_delivered(monitor, N)


def test_fixed_run_control_delivers_under_the_same_delayed_transport() -> None:
    """CONTROL (passes on current HEAD): the fixed path drains before unsubscribing, so the same
    delayed transport delivers everything by return — the adaptive failure above is therefore the
    missing drain, not a harness artifact."""
    tasks = tuple(Task(i, p) for i, p in enumerate(f2_partitions(N, "f2a-fixed")))
    monitor = EventRecordingMonitor()
    backend = DelayedEventBackend(ThreadBackend(max_workers=2), delay_s=0.5)
    plan = Plan(process=adaptive_leaf_value, combine=add_ints, empty=int_zero, tasks=tasks)

    with make_submit_runner(backend, monitor=monitor) as runner:
        result = runner.run(plan)

    assert result.value == sum(7 * i + 3 for i in range(N))
    _assert_all_task_events_delivered(monitor, N)
