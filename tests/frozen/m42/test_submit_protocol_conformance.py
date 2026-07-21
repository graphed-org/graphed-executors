"""m42 seam-by-execution: the SAME fixed + adaptive + error scenarios green on BOTH backends
(`ThreadBackend` and `DaskBackend`), and the capability flags pinned per instance (plan §1.1,
§1.2.5, §1.3.2; blueprint §3 `test_submit_protocol_conformance`).

The M2/M39 discipline: a protocol is witnessed by EXECUTING two backends against one suite, not by
an import lint. Wrong implementations this module discriminates: an engine hard-coded to dask
semantics (ThreadBackend's wrapper-resolved futures break it), an all-True capability stub (the
pinned flag sets differ on five of seven flags), a submission-order fold (one scenario hands tasks
in reversed key order), and a backend whose broadcast handle does not resolve to raw bytes on the
worker. The ThreadBackend half runs in the MAIN CI matrix without dask installed."""

from __future__ import annotations

import dataclasses
import importlib.util
import pickle

import pytest
from graphed.core.execution import Plan, SequentialRunner, StopReason, Task
from graphed.debug import StageError
from submit_backends import (
    Feed,
    RecordingMonitor,
    cluster_client,
    concat_plan,
    double,
    events_by_key,
    expected_concat,
    fold_combine,
    fold_empty,
    fold_oracle,
    fold_process,
    make_dask_backend,
    make_dask_runner,
    make_runner,
    make_thread_backend,
    mem_partitions,
    payload_len,
    register_worker_plugin,
    stage_error_plan,
    submit_api,
    wait_for,
)

HAS_DISTRIBUTED = importlib.util.find_spec("distributed") is not None
BACKENDS = [
    "thread",
    pytest.param(
        "dask", marks=pytest.mark.skipif(not HAS_DISTRIBUTED, reason="needs graphed-executors[dask]")
    ),
]

# Pinned per-instance capability flag sets (plan §1.2.5 / §1.3.2) — the 7 protocol flags, no more.
FLAG_NAMES = (
    "peer_data_movement",
    "scatter_broadcast",
    "pin_to_worker",
    "per_task_retries",
    "per_task_resources",
    "cancel_running",
    "worker_file_cache",
)
THREAD_FLAGS = {
    "peer_data_movement": True,
    "scatter_broadcast": False,
    "pin_to_worker": False,
    "per_task_retries": False,
    "per_task_resources": False,
    "cancel_running": False,
    "worker_file_cache": False,
}
DASK_FLAGS = {
    "peer_data_movement": True,
    "scatter_broadcast": True,
    "pin_to_worker": True,
    "per_task_retries": True,
    "per_task_resources": True,
    "cancel_running": True,
    "worker_file_cache": False,
}


@pytest.fixture(scope="module")
def dask_client():
    if not HAS_DISTRIBUTED:
        yield None
        return
    # tier A (plan §3): in-process workers — protocol/engine logic, no serialization tier needed
    with cluster_client(2, processes=False) as client:
        yield client


def runner_for(kind: str, client) -> object:
    return make_runner(make_thread_backend(2)) if kind == "thread" else make_dask_runner(client)


def backend_for(kind: str, client) -> object:
    if kind == "thread":
        return make_thread_backend(2)
    register_worker_plugin(client)  # dask_runner registers it itself; direct backends need it here
    return make_dask_backend(client)


@pytest.mark.parametrize("kind", BACKENDS)
@pytest.mark.parametrize("n", [0, 1, 2, 5, 16])
def test_fixed_plan_matches_sequential_bit_for_bit(kind: str, n: int, dask_client) -> None:
    """Every fixed shape == SequentialRunner (value via pickle bytes), n_partitions == n,
    n_combines == n-1, EXHAUSTED. Discriminates: a fold that loses the identity-empty contract,
    wrong tree accounting, a non-EXHAUSTED fixed path."""
    plan = concat_plan(n, f"conf{n}")
    expected = SequentialRunner().run(plan)
    with runner_for(kind, dask_client) as runner:
        got = runner.run(plan)
    assert pickle.dumps(got.value) == pickle.dumps(expected.value)
    assert got.value == expected_concat(n, f"conf{n}")
    assert got.n_partitions == n
    assert got.n_combines == max(0, n - 1)
    assert got.stopped is StopReason.EXHAUSTED


@pytest.mark.parametrize("kind", BACKENDS)
def test_tasks_are_reduced_in_key_order_not_submission_order(kind: str, dask_client) -> None:
    """Tasks handed over in REVERSED key order still concat in key order (the engine sorts by
    ``Task.key``). Discriminates a submission-order fold."""
    plan = concat_plan(9, "confrev", reverse_tasks=True)
    with runner_for(kind, dask_client) as runner:
        assert runner.run(plan).value == expected_concat(9, "confrev")


@pytest.mark.parametrize("kind", BACKENDS)
def test_adaptive_exhaustion_folds_everything(kind: str, dask_client) -> None:
    """next_tasks batches -> None (DONE): all 12 leaves consumed, fold == oracle, EXHAUSTED.
    Discriminates an engine that ignores ``next_tasks`` or drops late batches."""
    parts = mem_partitions(12, "confadapt")
    tasks = [Task(i, p) for i, p in enumerate(parts)]
    feed = Feed([tasks[0:4], tasks[4:8], tasks[8:12]])
    plan = Plan(process=fold_process, combine=fold_combine, empty=fold_empty, next_tasks=feed)
    with runner_for(kind, dask_client) as runner:
        got = runner.run(plan)
    assert got.stopped is StopReason.EXHAUSTED
    assert got.n_partitions == 12
    assert got.value.uris == frozenset(p.uri for p in parts)
    assert got.value.value == fold_oracle(got.value.uris)


@pytest.mark.parametrize("kind", BACKENDS)
def test_worker_error_surfaces_as_intact_stage_error(kind: str, dask_client) -> None:
    """The M6 obligation on the seam itself: a StageError raised in ``process`` reaches the driver
    as a StageError (never a string / RuntimeError), on BOTH backends."""
    with runner_for(kind, dask_client) as runner, pytest.raises(StageError) as excinfo:
        runner.run(stage_error_plan(4, "conferr"))
    assert excinfo.value.op == "mul"
    assert excinfo.value.user_frame.filename == "user_analysis.py"


@pytest.mark.parametrize("kind", BACKENDS)
def test_direct_submit_seam_resolves_future_args_and_broadcast_bytes(kind: str, dask_client) -> None:
    """The portable protocol intersection, exercised directly: ``submit`` returns a
    ``SubmitFuture``; a future PASSED AS AN ARG arrives resolved; ``broadcast`` hands the task fn
    the RAW BYTES; capability-less hints (retries/priority/resources) are accepted and ignored;
    ``cancel([])`` and ``n_workers`` behave. Discriminates a backend that ships futures unresolved
    (ThreadBackend must resolve in its wrapper) or a broadcast returning a non-bytes handle."""
    api = submit_api()
    backend = backend_for(kind, dask_client)
    try:
        assert isinstance(backend, api.SubmitBackend)
        assert backend.n_workers() == 2
        f1 = backend.submit(double, 21, key="graphed-m42conf-direct-1")
        assert isinstance(f1, api.SubmitFuture)
        assert f1.result(timeout=60) == 42
        assert f1.done() and f1.exception(timeout=60) is None
        f2 = backend.submit(
            double, f1, key="graphed-m42conf-direct-2", retries=2, priority=5, resources={"GPU": 1.0}
        )
        assert f2.result(timeout=60) == 84  # the future ARG was resolved before fn ran
        backend.cancel([])  # a no-op cancel must not raise on any capability profile
        payload = b"m42-broadcast-payload"
        handle = backend.broadcast(payload, token="m42conf-token")
        f3 = backend.submit(payload_len, handle, key="graphed-m42conf-direct-3")
        assert f3.result(timeout=60) == len(payload)
    finally:
        if kind == "thread":
            backend.close()  # a DaskBackend close must not close the shared module client


def test_capability_flags_are_pinned_and_honestly_differ(dask_client) -> None:
    """The two instances' flag sets equal the plan's pins (§1.2.5 / §1.3.2) and differ on five of
    seven flags — an all-True (or copy-paste) capability stub fails. ``SubmitCapabilities`` is a
    frozen dataclass with exactly the seven protocol flags."""
    if not HAS_DISTRIBUTED:
        pytest.skip("needs graphed-executors[dask]")
    api = submit_api()
    thread = make_thread_backend(2)
    try:
        dask_b = make_dask_backend(dask_client)
        tcaps, dcaps = thread.capabilities, dask_b.capabilities
        assert isinstance(tcaps, api.SubmitCapabilities) and isinstance(dcaps, api.SubmitCapabilities)
        assert tuple(f.name for f in dataclasses.fields(api.SubmitCapabilities)) == FLAG_NAMES
        assert {f: getattr(tcaps, f) for f in FLAG_NAMES} == THREAD_FLAGS
        assert {f: getattr(dcaps, f) for f in FLAG_NAMES} == DASK_FLAGS
        assert sum(getattr(tcaps, f) != getattr(dcaps, f) for f in FLAG_NAMES) == 5
        with pytest.raises(dataclasses.FrozenInstanceError):
            tcaps.peer_data_movement = False
    finally:
        thread.close()


@pytest.mark.parametrize("kind", BACKENDS)
def test_monitored_run_emits_exact_phases_on_both_backends(kind: str, dask_client) -> None:
    """M37 through the seam on BOTH backends: per leaf exactly one SUBMITTED + STARTED + FINISHED,
    no other keys, and the value unchanged by observation. ThreadBackend's in-process
    subscribe_events/emit wiring is witnessed here by execution."""
    monitor = RecordingMonitor()
    plan = concat_plan(6, f"confmon-{kind}")
    if kind == "thread":
        runner = make_runner(make_thread_backend(2), monitor=monitor)
    else:
        runner = make_dask_runner(dask_client, monitor=monitor)
    with runner:
        value = runner.run(plan).value
    assert value == expected_concat(6, f"confmon-{kind}")
    wait_for(lambda: len(monitor.events) >= 18)
    per_key = events_by_key(monitor.events)
    assert set(per_key) == set(range(6))  # leaf keys only — no phantom tasks
    for key in range(6):
        phases = [str(ev.phase) for ev in per_key[key]]
        assert sorted(phases) == ["finished", "started", "submitted"]
        assert phases.index("submitted") < phases.index("started") < phases.index("finished")
