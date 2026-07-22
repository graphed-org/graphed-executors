"""m42 broadcast-once: the pickled `process` ships ONCE per run and is deserialized ONCE per
worker while every worker runs many tasks; keys are `graphed-`-namespaced and per-run nonced
(plan §1.2.2, §1.2.3; blueprint §3 `test_dask_broadcast_once`).

Discriminates: closure-per-task payload shipping (dask/dask#5503 — deserializations per pid ==
tasks per pid), driver-side per-task pickling loops, content-only keys without a run nonce
(run-1 ∩ run-2 keys non-empty), un-namespaced keys, and the dask/dask#9969 key-substitution footgun
(a task's string arg shaped like a key must never be swapped for a Future)."""

from __future__ import annotations

import os

import pytest
from graphed.core.execution import Partition, Plan, SequentialRunner, Task
from submit_backends import (
    SpyBackend,
    cluster_client,
    concat,
    concat_process,
    counting_plan,
    counts_snapshot,
    empty_text,
    expected_concat,
    leaf_text,
    make_dask_backend,
    make_dask_runner,
    make_runner,
    register_worker_plugin,
    sum_plan_over,
)

pytest.importorskip("distributed")


@pytest.fixture(scope="module")
def client():
    with cluster_client(2, processes=True) as c:
        yield c


def test_process_payload_deserializes_once_per_worker(client) -> None:
    before = dict(client.run(counts_snapshot))
    driver_before = counts_snapshot().get("process_unpickle", 0)
    with make_dask_runner(client) as runner:
        result = runner.run(counting_plan(16, "bcast"))
    prov = result.value
    after = dict(client.run(counts_snapshot))

    assert prov.payload == expected_concat(16, "bcast")
    tasks_per_pid: dict[int, int] = {}
    for _, pid, _ in prov.leaves:
        tasks_per_pid[pid] = tasks_per_pid.get(pid, 0) + 1
    assert sum(tasks_per_pid.values()) == 16
    assert max(tasks_per_pid.values()) > 1  # non-vacuity: some worker ran MANY tasks

    addr_pid = dict(client.run(os.getpid))  # worker address -> pid, for the per-pid delta map
    deltas = {
        addr_pid[addr]: after[addr].get("process_unpickle", 0) - before[addr].get("process_unpickle", 0)
        for addr in after
    }
    for pid, n_tasks in tasks_per_pid.items():
        assert pid in deltas, "a leaf ran in a process that is not a live worker"
        assert deltas[pid] == 1, (
            f"worker pid {pid} deserialized the process payload {deltas[pid]} times for "
            f"{n_tasks} tasks — broadcast-once + token cache not engaged"
        )
    assert counts_snapshot().get("process_unpickle", 0) == driver_before  # driver never unpickles


def test_keys_are_namespaced_unique_and_nonced_across_runs(client) -> None:
    register_worker_plugin(client)
    spy = SpyBackend(make_dask_backend(client))
    plan = sum_plan_over(8, "bkeys")
    expected = SequentialRunner().run(plan).value
    with make_runner(spy) as runner:
        assert runner.run(plan).value == expected
        first = list(spy.submitted_keys)
        first_bcasts = list(spy.broadcast_tokens)
        assert runner.run(plan).value == expected
    second = spy.submitted_keys[len(first) :]
    second_bcasts = spy.broadcast_tokens[len(first_bcasts) :]

    for keys in (first, second):
        assert len(keys) == 8 + 7  # leaves + plan_tree combines, nothing else submitted
        assert len(set(keys)) == len(keys)  # unique within a run
        assert all(k.startswith("graphed-") for k in keys)  # the collision-proof namespace
    assert not (set(first) & set(second)), "keys reused across runs — the run nonce is missing"
    assert len(first_bcasts) == len(second_bcasts) == 2  # process + combine, broadcast once each


def test_key_shaped_string_args_are_not_substituted(client) -> None:
    """dask/dask#9969: an argument string equal to a live key becomes that key's Future. The
    namespaced+nonced scheme makes user-string collision impossible — a partition uri crafted to
    look exactly like an engine key must round-trip as a plain string."""
    key_shaped = "graphed-deadbeefcafe-0badc0de-leaf-0"
    tasks = (Task(0, Partition(key_shaped, "", 0, 1)), Task(1, Partition("mem://bfoot/1", "", 1, 2)))
    plan = Plan(process=concat_process, combine=concat, empty=empty_text, tasks=tasks)
    with make_dask_runner(client) as runner:
        value = runner.run(plan).value
    assert value == "".join(leaf_text(t.partition) for t in tasks)
    assert key_shaped in value


def test_replicate_broadcast_knob_is_wired_and_harmless(client) -> None:
    """The coffea#1490 escape hatch (§1.2.2 mitigation 2): `replicate_broadcast=True` constructs
    and still produces the sequential value."""
    plan = sum_plan_over(6, "brep")
    expected = SequentialRunner().run(plan).value
    with make_dask_runner(client, replicate_broadcast=True) as runner:
        assert runner.run(plan).value == expected
