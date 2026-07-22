"""m42 M37 on dask: worker task events reach the driver monitor over the run's own namespaced
topic, with the exact per-task phase contract, and observation NEVER perturbs the result (plan
§1.5.1; blueprint §3 `test_dask_monitor_events`).

Discriminates: emission riding the data path (payload inflation breaks the byte-identical
passivity gate), events dropped or duplicated per phase, worker events not attributed to real
workers, an un-namespaced or never-unsubscribed topic, and a run broken by a monitor that raises
(the emit-swallow contract)."""

from __future__ import annotations

import pickle

import pytest
from graphed.core.execution import Plan, Task, TaskPhase
from submit_backends import (
    RecordingMonitor,
    SpyBackend,
    cluster_client,
    concat_plan,
    empty_set,
    events_by_key,
    expected_concat,
    make_dask_backend,
    make_runner,
    mem_partitions,
    raise_value_error,
    register_worker_plugin,
    union,
    wait_for,
    worker_addresses,
)

pytest.importorskip("distributed")


@pytest.fixture(scope="module")
def client():
    with cluster_client(2, processes=True) as c:
        yield c


def test_exact_phase_contract_over_the_runs_namespaced_topic(client) -> None:
    n = 8
    monitor = RecordingMonitor()
    register_worker_plugin(client)
    spy = SpyBackend(make_dask_backend(client))
    plan = concat_plan(n, "mon")
    with make_runner(spy, monitor=monitor) as runner:
        value = runner.run(plan).value
    assert value == expected_concat(n, "mon")

    # the driver tapped the run's OWN topic, namespaced, and released it
    assert len(spy.topics) == 1 and spy.topics[0].startswith("graphed-monitor-")
    assert spy.unsubscribed == spy.topics

    wait_for(lambda: len(monitor.events) >= 3 * n)
    per_key = events_by_key(monitor.events)
    assert set(per_key) == set(range(n))  # leaf tasks only — no phantom keys
    workers = worker_addresses(client)
    for key in range(n):
        phases = [ev.phase for ev in per_key[key]]
        assert sorted(str(p) for p in phases) == ["finished", "started", "submitted"]
        started = next(ev for ev in per_key[key] if ev.phase is TaskPhase.STARTED)
        finished = next(ev for ev in per_key[key] if ev.phase is TaskPhase.FINISHED)
        assert started.worker in workers, "STARTED not attributed to a live dask worker"
        assert finished.worker in workers
        assert started.partition and finished.partition  # the dashboard label survives the trip
        assert finished.error is None


def test_errored_phase_carries_the_failure(client) -> None:
    monitor = RecordingMonitor()
    (partition,) = mem_partitions(1, "monerr")
    plan = Plan(process=raise_value_error, combine=union, empty=empty_set, tasks=(Task(0, partition),))
    with (
        make_runner(SpyBackend(make_dask_backend(client)), monitor=monitor, retries=0) as runner,
        pytest.raises(ValueError, match="broken partition"),
    ):
        runner.run(plan)
    wait_for(lambda: any(ev.phase is TaskPhase.ERRORED for ev in monitor.events))
    errored = [ev for ev in monitor.events if ev.phase is TaskPhase.ERRORED]
    assert len(errored) == 1
    assert errored[0].key == 0
    assert errored[0].error and "ValueError" in errored[0].error  # pre-rendered summary string
    assert not any(ev.phase is TaskPhase.FINISHED for ev in monitor.events)


def test_observation_is_passive_byte_identical_even_when_the_monitor_raises(client) -> None:
    plan = concat_plan(8, "monpass")
    with make_runner(SpyBackend(make_dask_backend(client))) as runner:
        detached = runner.run(plan).value
    with make_runner(SpyBackend(make_dask_backend(client)), monitor=RecordingMonitor()) as runner:
        attached = runner.run(plan).value
    with make_runner(SpyBackend(make_dask_backend(client)), monitor=RecordingMonitor(fail=True)) as runner:
        hostile = runner.run(plan).value  # a raising monitor must be swallowed, never break the run
    assert pickle.dumps(detached) == pickle.dumps(attached) == pickle.dumps(hostile)
    assert detached == expected_concat(8, "monpass")
