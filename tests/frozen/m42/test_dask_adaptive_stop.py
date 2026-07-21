"""m42 adaptive + stop: `next_tasks` batches are consumed, the `StopCondition` ends the run, the
engine CANCELS outstanding futures, refill never happens past the stop, and the fold equals the
running_fold oracle over exactly the consumed set (plan §1.2.4; blueprint §3
`test_dask_adaptive_stop`).

One deliberately slow leaf guarantees an outstanding future exists when the target is reached
(scenario construction, not an assertion — R0.10a). Discriminates: ignoring `StopCondition`
(EXHAUSTED instead of TARGET_EVENTS), never cancelling (the spy's cancel count stays 0),
refilling after the stop condition is satisfied (a Feed call observed at events_done >= target),
and a fold inconsistent with its own consumed-uri set."""

from __future__ import annotations

import pytest
from graphed.core.execution import Plan, StopCondition, StopReason, Task
from submit_backends import (
    Feed,
    SlowFoldProcess,
    SpyBackend,
    cluster_client,
    fold_combine,
    fold_empty,
    fold_oracle,
    make_dask_backend,
    make_runner,
    mem_partitions,
    register_worker_plugin,
)

pytest.importorskip("distributed")


@pytest.fixture(scope="module")
def client():
    with cluster_client(2, processes=True) as c:
        yield c


def test_stop_condition_cancels_outstanding_and_folds_consistently(client) -> None:
    parts = mem_partitions(6, "adstop")
    tasks = [Task(i, p) for i, p in enumerate(parts)]
    slow_uri = parts[0].uri
    feed = Feed([tasks])  # one batch of 6 (1 slow + 5 fast), then None
    target = 4
    plan = Plan(
        process=SlowFoldProcess(slow_uri, 5.0),
        combine=fold_combine,
        empty=fold_empty,
        next_tasks=feed,
        stop=StopCondition(target_events=target),
    )
    register_worker_plugin(client)
    spy = SpyBackend(make_dask_backend(client))
    with make_runner(spy) as runner:
        result = runner.run(plan)

    assert result.stopped is StopReason.TARGET_EVENTS  # the stop was honored, not EXHAUSTED
    fold = result.value
    assert target <= len(fold.uris) < 6  # stopped early: the slow leaf was never consumed
    assert slow_uri not in fold.uris
    assert result.n_partitions == len(fold.uris)  # n_done == consumed set, nothing double-counted
    assert fold.value == fold_oracle(fold.uris)  # running_fold correctness over the consumed set

    assert sum(spy.cancel_batches) >= 1, "stop left outstanding futures uncancelled"
    assert feed.calls, "next_tasks was never consulted"
    assert all(seen < target for seen in feed.calls), (
        f"next_tasks was called at events_done {feed.calls} — refill after the stop condition"
    )


def test_adaptive_exhaustion_needs_no_cancel(client) -> None:
    """Contrast run (non-vacuity for the cancel spy): batches exhaust normally — EXHAUSTED, every
    uri consumed, and NO cancels issued."""
    parts = mem_partitions(6, "adfull")
    tasks = [Task(i, p) for i, p in enumerate(parts)]
    feed = Feed([tasks[:3], tasks[3:]])
    plan = Plan(process=SlowFoldProcess("none", 0.0), combine=fold_combine, empty=fold_empty, next_tasks=feed)
    register_worker_plugin(client)
    spy = SpyBackend(make_dask_backend(client))
    with make_runner(spy) as runner:
        result = runner.run(plan)
    assert result.stopped is StopReason.EXHAUSTED
    assert result.value.uris == frozenset(p.uri for p in parts)
    assert result.value.value == fold_oracle(result.value.uris)
    assert sum(spy.cancel_batches) == 0
