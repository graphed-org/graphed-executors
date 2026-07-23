"""m47 self-rendezvous barrier — endpoints are minted IN-TASK on worker processes, no peer holds
the address book before ALL k hellos arrived, and over-asking k converts into a bounded attributed
error that FREES the slots (plan §1.4 rendezvous/G5 + barrier cleanup; blueprint §3
`test_parsl_rendezvous_barrier`; also the "exactly k hellos" third of the reshaped
anti-super-linear guard).

Witnesses (counters/pids, never clocks): per-peer ``endpoint_pid`` is a WORKER pid (never the
driver's) and the k pids are distinct; ``endpoint_port`` is a nonzero OS-assigned port, distinct
per peer; per-peer ``registry_size_at_receipt == k`` — a peer that saw a PARTIAL book (an
incremental, barrier-less rendezvous) records < k; the driver's ``recv_class_hello == k`` exactly;
over-ask arm (k+1 > slots): an attributed ``StageError`` naming ``hellos_seen/k`` ("2/3"),
returned BOUNDED, after which a plain submit on the SAME pool completes — every peer future
resolved and every slot was freed (the §1.4 cleanup: cancel the queued, marker-return the seated).

Discriminates: driver-assigned ports (driver pid provenance); an incremental registry broadcast
(size-at-receipt < k); a barrier-less design that starts sending before the k-th hello (ditto); a
hanging over-ask (hard timeout); leaked peer slots (the post-error submit never runs on a 2-slot
pool still held by zombie peers)."""

from __future__ import annotations

import os
import sys

import pytest
from graphed.debug import StageError
from parsl_transport_harness import (
    DRIVER,
    driver_row,
    make_p2p_plan,
    make_parsl_backend,
    p2p_double,
    p2p_htex_pool,
    peer_rows,
    plan_run,
    run_bounded,
)

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

N_LEAVES = 12
K = 2


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=K) as ex:
        yield ex


def test_endpoints_are_minted_in_task_and_no_peer_sees_a_partial_registry(htex) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(htex)
    res = run_bounded(lambda: plan_run(make_p2p_plan(N_LEAVES, "m47barrier"), backend), timeout_s=240.0)

    rows = peer_rows(res)
    assert sorted(rows) == ["w0", "w1"], f"pinned peer addresses w0..w{{k-1}} expected, got {sorted(rows)}"

    driver_pid = os.getpid()
    pids = {addr: int(c.get("endpoint_pid", 0)) for addr, c in rows.items()}
    ports = {addr: int(c.get("endpoint_port", 0)) for addr, c in rows.items()}
    for addr in rows:
        assert pids[addr] not in (0, driver_pid), (
            f"peer {addr}: endpoint_pid={pids[addr]} — the endpoint must be BOUND inside the "
            "worker task (self-minted (host, port)), never in the driver process"
        )
        assert ports[addr] > 0, f"peer {addr}: endpoint_port must be the OS-assigned in-task port"
    assert len(set(pids.values())) == K, "k peers must occupy k distinct worker processes"
    assert len(set(ports.values())) == K, "two live endpoints cannot share a port"

    for addr, c in rows.items():
        assert int(c.get("registry_size_at_receipt", 0)) == K, (
            f"peer {addr} received a registry of size {c.get('registry_size_at_receipt', 0)} != k={K} "
            "— the driver must broadcast the book only after ALL k hellos (the G5 barrier that "
            "subsumes pre-created inboxes); a partial book means sends can race a missing inbox"
        )

    hello = int(driver_row(res).get("recv_class_hello", -1))
    assert hello == K, (
        f"driver recv_class_hello={hello}, expected exactly k={K} — the barrier shape is the "
        "control third of the anti-super-linear guard (never per-task or per-partition hellos)"
    )
    assert DRIVER not in peer_rows(res), "the driver row must not be counted as a peer"


def test_over_asked_k_is_a_bounded_attributed_error_and_the_slots_are_freed(htex) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(htex)
    with pytest.raises(StageError) as excinfo:
        run_bounded(
            lambda: plan_run(
                make_p2p_plan(N_LEAVES, "m47overask"), backend, workers=K + 1, barrier_timeout_s=8.0
            ),
            timeout_s=180.0,
        )
    msg = str(excinfo.value)
    assert "2/3" in msg, (
        f"the barrier error must name hellos_seen/k ('2/3' on a {K}-slot pool asked for {K + 1}): {msg}"
    )

    # every peer future resolved and every slot was freed: a plain task runs on the SAME pool
    fut = backend.submit(p2p_double, 21, key="m47-barrier-liveness")
    assert run_bounded(lambda: fut.result(), timeout_s=120.0) == 42, (
        "the pool is dead after the barrier error — a peer slot leaked (a queued peer was not "
        "cancelled, or a seated peer never returned its barrier-expired marker)"
    )
