"""m46 `SubmitBackend` conformance over BOTH parsl executor classes — direct executor submit, no
DFK (plan §1.2; §3 m46 `test_parsl_submit_conformance`; the m42 seam-by-execution discipline).

Structural + behavioral: ``isinstance`` against the runtime-checkable protocols; submit→result on
HTEX and TPE; FUTURE-ARG COMPOSITION (``submit(g, fut_f)`` receives the RESOLVED value — the §1.2
driver-side resolution move, with g's execution site proving worker-side compute on HTEX);
broadcast hands the task fn RAW BYTES across the process boundary; capability-less hints are
accepted and ignored; ``cancel([])`` no-ops; ``n_workers()`` counts the real pool. The
resource_specification that actually reaches parsl is spied AT THE EXECUTOR SEAM: TPE must always
receive ``{}`` (measured: parsl's TPE raises ``InvalidResourceSpecification`` on anything else),
HTEX receives ``{"priority": p}`` iff p != 0 (the one spec key HTEX forwards).

Discriminates: a backend that ships futures unresolved (g crashes on a Future arg), one that
resolves-and-computes driver-side (fails the site pid witness), one that forwards a non-empty
spec to TPE (the armed rejection), one that drops the priority hook on HTEX, and a broadcast
returning a non-bytes handle."""

from __future__ import annotations

import os
import sys

import pytest
from parsl_harness import (
    double,
    double_with_site,
    executor_submit_recorder,
    htex_pool,
    make_parsl_backend,
    payload_len,
    run_bounded,
    tpe_pool,
)

from graphed_executors.submit.protocol import SubmitBackend, SubmitFuture

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


@pytest.fixture(scope="module")
def tpe():  # type: ignore[no-untyped-def]
    with tpe_pool(max_threads=2) as ex:
        yield ex


@pytest.fixture(params=["tpe", "htex"])
def backend(request, tpe, htex):  # type: ignore[no-untyped-def]
    return make_parsl_backend(tpe if request.param == "tpe" else htex)


def test_protocol_shape_and_direct_submit(backend) -> None:  # type: ignore[no-untyped-def]
    """The structural half + the simplest behavioral round trip, on both executor classes."""
    assert isinstance(backend, SubmitBackend)
    assert backend.n_workers() == 2
    fut = backend.submit(double, 21, key="graphed-m46conf-direct-1")
    assert isinstance(fut, SubmitFuture)
    assert run_bounded(lambda: fut.result(timeout=120)) == 42
    assert fut.done() and fut.exception(timeout=120) is None
    backend.cancel([])  # a no-op cancel must not raise on the all-False profile


def test_future_args_resolve_before_fn_runs(backend) -> None:  # type: ignore[no-untyped-def]
    """`args` may contain SubmitFutures from THIS backend; fn receives RESOLVED VALUES (§1.1).
    Hints without a capability (retries/priority/resources/workers) are accepted and ignored."""
    f1 = backend.submit(double, 21, key="graphed-m46conf-compose-1")
    f2 = backend.submit(
        double_with_site,
        f1,
        key="graphed-m46conf-compose-2",
        retries=2,
        priority=5,
        resources={"GPU": 1.0},
    )
    value, _pid, _worker = run_bounded(lambda: f2.result(timeout=120))
    assert value == 84  # the future ARG was resolved before fn ran


def test_future_arg_consumer_runs_worker_side_on_htex(htex) -> None:  # type: ignore[no-untyped-def]
    """The 3-move direct-submit integration is real remote execution: the composed task's site pid
    is a WORKER pid, not the driver's — a stub that resolves the future and computes the
    composition driver-side (returning the right value) fails specifically here."""
    backend = make_parsl_backend(htex)
    f1 = backend.submit(double, 8, key="graphed-m46conf-site-1")
    f2 = backend.submit(double_with_site, f1, key="graphed-m46conf-site-2")
    value, pid, worker = run_bounded(lambda: f2.result(timeout=120))
    assert value == 32
    assert pid != os.getpid(), "the composed task computed in the DRIVER process (head-node stub)"
    assert worker not in ("", "driver", "local"), f"worker identity missing at the task site: {worker!r}"


def test_broadcast_resolves_to_raw_bytes_on_the_worker(backend) -> None:  # type: ignore[no-untyped-def]
    """`broadcast` returns an arg-embeddable handle; the task fn receives the RAW BYTES (§1.1 —
    the degenerate reship form is fine on scatter_broadcast=False; on HTEX this crosses a real
    process boundary)."""
    payload = b"m46-broadcast-payload"
    handle = backend.broadcast(payload, token="m46conf-token")
    fut = backend.submit(payload_len, handle, key="graphed-m46conf-bcast-1")
    assert run_bounded(lambda: fut.result(timeout=120)) == len(payload)


def test_tpe_always_receives_an_empty_resource_specification(tpe) -> None:  # type: ignore[no-untyped-def]
    """MEASURED (plan §0.3): parsl's TPE raises InvalidResourceSpecification on ANY non-empty
    spec. The executor-seam spy proves ParslBackend sends ``{}`` there even when the caller passes
    priority/resources hints — and the successful result proves the rejection never fired."""
    with executor_submit_recorder(tpe) as records:
        backend = make_parsl_backend(tpe)
        fut = backend.submit(double, 3, key="graphed-m46conf-tpespec-1", priority=7, resources={"MEM": 2.0})
        assert run_bounded(lambda: fut.result(timeout=120)) == 6
    assert records, "the spy saw no executor submit — ParslBackend bypassed the executor seam"
    assert all(r["resource_specification"] == {} for r in records), (
        f"TPE must receive an empty resource_specification (measured InvalidResourceSpecification "
        f"otherwise), got: {[r['resource_specification'] for r in records]}"
    )


def test_htex_priority_is_forwarded_and_zero_is_elided(htex) -> None:  # type: ignore[no-untyped-def]
    """HTEX's resource spec speaks exactly {'priority'} (executor.py:393): a non-zero priority
    hint must arrive as ``{"priority": p}``; priority=0 must arrive as ``{}`` (plan §1.2 step 3)."""
    with executor_submit_recorder(htex) as records:
        backend = make_parsl_backend(htex)
        f1 = backend.submit(double, 5, key="graphed-m46conf-prio-1", priority=3)
        f2 = backend.submit(double, 6, key="graphed-m46conf-prio-2")
        assert run_bounded(lambda: f1.result(timeout=120)) == 10
        assert run_bounded(lambda: f2.result(timeout=120)) == 12
    specs = [r["resource_specification"] for r in records]
    assert {"priority": 3} in specs, f"priority=3 never reached HTEX's resource spec: {specs}"
    assert {} in specs, f"priority=0 must be elided to an empty spec (not {{'priority': 0}}): {specs}"
