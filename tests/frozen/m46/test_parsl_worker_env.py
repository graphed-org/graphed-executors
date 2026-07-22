"""m46 worker seam — the `_ParslWorkerEnv` shim contract on HTEX (plan §1.2 `_shim.py`; §3 m46
`test_parsl_worker_env`).

Three mechanisms, each with its own counter/identity witness:

1. **Worker identity**: ``current_env().worker`` equals ``f"{PARSL_WORKER_POOL_ID}:
   {PARSL_WORKER_RANK}"`` recomputed IN-TASK from parsl's own env vars (measured present on
   2026.7.20 workers) — format-independent, exact; stable per worker pid across many tasks and
   distinct across worker pids.
2. **Per-worker-process resources**: ``open_once`` opens ONCE per worker process across many
   tasks (the process-global `LocalResources` cache keyed for file locality) — distinct
   pid-prefixed tokens count real opens; a fresh-env-per-task implementation mints one per task.
3. **Emit buffering**: events emitted through ``env.emit`` inside a task arrive at the driver's
   subscribed handler after completion, ALL of them, in emission order, carrying the worker pid
   (created worker-side, delivered driver-side — the completion-granularity piggyback, G10).

Discriminates: a driver-env leak ("local"/driver pid in results), a fresh-env-per-task shim
(open_once count), a synthesized worker id (mismatch vs the in-task env vars), and an emit path
that drops, reorders, or driver-stamps events."""

from __future__ import annotations

import os
import sys

import pytest
from parsl_harness import (
    emit_probe,
    htex_pool,
    make_parsl_backend,
    open_once_probe,
    run_bounded,
    wait_for,
    worker_identity_probe,
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


def test_worker_identity_is_pool_id_colon_rank_and_stable_per_worker(htex) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(htex)
    futs = [backend.submit(worker_identity_probe, i, key=f"graphed-m46env-id-{i}") for i in range(10)]
    results = [run_bounded(lambda f=f: f.result(timeout=120)) for f in futs]

    driver_pid = os.getpid()
    by_pid: dict[int, set[str]] = {}
    for pid, worker, expected in results:
        assert pid != driver_pid, "an identity probe ran in the DRIVER process"
        assert "<absent>" not in expected, "PARSL_WORKER_* env vars missing on the worker"
        assert worker == expected, (
            f"current_env().worker must be POOL_ID:RANK from parsl's own env vars — got "
            f"{worker!r}, in-task recomputation says {expected!r} (a synthesized/driver-side id)"
        )
        by_pid.setdefault(pid, set()).add(worker)
    for pid, workers in by_pid.items():
        assert len(workers) == 1, f"worker pid {pid} reported multiple identities: {workers}"
    assert len({next(iter(w)) for w in by_pid.values()}) == len(by_pid), (
        "distinct worker pids shared one identity — RANK never reached the id"
    )


def test_open_once_resources_are_cached_per_worker_process(htex) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(htex)
    uri = "mem://m46env/shared-resource"
    futs = [backend.submit(open_once_probe, uri, i, key=f"graphed-m46env-open-{i}") for i in range(12)]
    results = [run_bounded(lambda f=f: f.result(timeout=120)) for f in futs]

    tokens_by_pid: dict[int, set[str]] = {}
    tasks_by_pid: dict[int, int] = {}
    for pid, token in results:
        tokens_by_pid.setdefault(pid, set()).add(token)
        tasks_by_pid[pid] = tasks_by_pid.get(pid, 0) + 1
        assert token.startswith(f"{pid}:"), (
            f"the open_once token {token!r} was not minted in worker {pid} — the resource was "
            "opened somewhere else (driver-side or another process)"
        )
    for pid, tokens in tokens_by_pid.items():
        assert len(tokens) == 1, (
            f"worker {pid} ran {tasks_by_pid[pid]} tasks but opened the shared uri "
            f"{len(tokens)} times — the WorkerEnv resources are not cached per worker process "
            "(a fresh-env-per-task shim; file locality across tasks is the §1.2 contract)"
        )
    assert max(tasks_by_pid.values()) >= 2, (
        "scenario degenerate: no worker ran two tasks, the once-per-process claim was not exercised"
    )


def test_emit_buffering_delivers_all_events_in_order_after_completion(htex) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(htex)
    topic = "graphed-m46env-emit-topic"
    received: list[dict[str, object]] = []
    unsub = backend.subscribe_events(topic, received.extend)
    try:
        fut = backend.submit(emit_probe, topic, 7, key="graphed-m46env-emit-1")
        assert run_bounded(lambda: fut.result(timeout=120)) == 7
        wait_for(lambda: len(received) >= 7)
    finally:
        unsub()

    assert [ev["seq"] for ev in received] == list(range(7)), (
        f"emitted events lost or reordered: {[ev['seq'] for ev in received]} — the shim must "
        "buffer in emission order and the future must dispatch the whole buffer on completion"
    )
    driver_pid = os.getpid()
    assert all(ev["pid"] != driver_pid for ev in received), (
        "an emitted event carries the DRIVER pid — events must be created worker-side"
    )
