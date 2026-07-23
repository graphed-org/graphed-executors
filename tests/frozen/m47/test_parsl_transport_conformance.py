"""m47 transport conformance — ``EscalatingHttpTransport`` IS a graphed ``WorkerTransport``
(inside a real HTEX worker task), and its four §1.4 send/recv escalation arms are real mechanisms,
not the base class's silent counters (plan §1.4 send design + design-A/design-B; blueprint §3
`test_parsl_transport_conformance`; review N2 honored).

Witnesses: runtime_checkable isinstance IN-TASK on a worker pid != driver; send->True then a recv
round-trip carrying the true (sender, message); recv(timeout)->None and poll()->[] on empty;
send to self/unknown -> False with no POST; **flood**: an ``inbox_maxsize=1`` destination answers
503 -> ``send() == False`` (returned, never raised) + the sender's ``sends_rejected`` + the
receiver's ``recv_rejects``, with ``sends_retried == 0`` (a 503 is the destination's VERDICT — a
retry-then-raise-on-503 implementation fails here, review N2) and the one ACCEPTED message still
delivered; **exhaustion**: a registered-but-refused destination RAISES ``TransportDeliveryError``
(``send_failures`` bumped, ``sends_retried`` counted) — the drop->raise bridge over the base
class's ``drops += 1``; **idle deadline**: ``recv()`` raises ``TransportDeliveryError`` once
``idle_deadline_s`` passes with no traffic (design-A: the lost-``done`` backstop); **broadcast**:
attempts ALL peers then raises the aggregate — a live peer ORDERED AFTER a dead one still receives
(the base stop-at-first loop fails it deterministically).

Discriminates: a stub whose send always returns True (flood); a verbatim ``_Handler`` replica
(always-200 unbounded — cannot 503); a base-``HttpTransport`` delegation (send never raises,
recv never deadlines, broadcast never raises); a `.drops`-counting sender (wrong counter, no
raise); a retry-then-raise 503 handler (flood raises instead of returning False)."""

from __future__ import annotations

import os
import sys
import uuid

import pytest
from parsl_transport_harness import (
    closed_loopback_port,
    delivery_error_cls,
    make_endpoint,
    make_parsl_backend,
    p2p_conformance_probe,
    p2p_htex_pool,
    run_bounded,
    wire_endpoints,
)

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=2) as ex:
        yield ex


def test_endpoint_is_a_worker_transport_inside_an_htex_task(htex) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(htex)
    fut = backend.submit(p2p_conformance_probe, f"m47-conf-{uuid.uuid4().hex}", key="m47-conf-probe")
    obs = run_bounded(lambda: fut.result(), timeout_s=120.0)

    assert obs["pid"] != os.getpid(), "the conformance probe must run in a WORKER process"
    assert obs["is_transport"] is True, "the endpoint must satisfy the runtime_checkable WorkerTransport"
    assert obs["port_pa"] > 0 and obs["port_pb"] > 0 and obs["port_pa"] != obs["port_pb"], (
        "each endpoint must self-mint a distinct OS-assigned port in-task"
    )
    assert set(obs["peers_pa"]) == {"pb"}, "peers() must be the registry minus self"
    assert obs["empty_recv"] is None, "recv(timeout) on an empty inbox must return None, not block"
    assert obs["empty_poll"] == [], "poll() on an empty inbox must return an empty drain"
    assert obs["send_self"] is False and obs["send_unknown"] is False, (
        "send to self / an unregistered dest must return False without a POST"
    )
    assert obs["send_ok"] is True
    assert obs["round_trip"] == ("pa", ("hello", 1)), (
        f"round-trip lost the true sender or payload: {obs['round_trip']!r}"
    )


def test_flood_503_is_a_no_retry_false_with_both_counters() -> None:
    epoch = f"m47-flood-{uuid.uuid4().hex}"
    rx = make_endpoint("rx", epoch=epoch, inbox_maxsize=1)
    tx = make_endpoint("tx", epoch=epoch)
    try:
        wire_endpoints(rx, tx)
        assert tx.send("rx", ("m", 0)) is True, "a bounded inbox must still accept the first message"

        results = [tx.send("rx", ("m", i)) for i in range(1, 4)]  # nobody drains: the inbox stays full
        assert False in results, (
            "flooding a maxsize=1 inbox with no consumer must surface send()==False (the 503 "
            "verdict) — a transport that never refuses has an unbounded inbox (the verbatim "
            "_Handler replica the plan forbids)"
        )
        assert int(tx.sends_rejected) >= 1, "the sender must COUNT its 503 verdicts (sends_rejected)"
        assert int(rx.recv_rejects) >= 1, "the receiver must COUNT its full-inbox 503s (recv_rejects)"
        assert int(tx.sends_retried) == 0, (
            "a 503 is the destination's verdict, not a transient: it must be special-cased BEFORE "
            "the generic retry (review N2) — retrying into a full inbox only defers the same answer"
        )
        got = rx.recv(timeout=10.0)
        assert got is not None and tuple(got) == ("tx", ("m", 0)), (
            f"the one ACCEPTED message must survive the flood intact: {got!r}"
        )
    finally:
        rx.close()
        tx.close()


def test_exhausted_send_raises_transport_delivery_error() -> None:
    epoch = f"m47-exh-{uuid.uuid4().hex}"
    tx = make_endpoint("tx", epoch=epoch)
    try:
        tx.set_registry({"tx": (tx.host, tx.port), "gone": ("127.0.0.1", closed_loopback_port())})
        with pytest.raises(delivery_error_cls()):
            run_bounded(lambda: tx.send("gone", ("node", 1, 0, 42)), timeout_s=120.0)
        assert int(tx.send_failures) >= 1, "exhaustion must bump send_failures (the escalation witness)"
        assert int(tx.sends_retried) >= 1, (
            "the bounded retry must have engaged before the raise — a first-failure raise skips "
            "the at-least-once policy; a base-class delegation (drops += 1, return) never raises "
            "at all and fails the pytest.raises above"
        )
    finally:
        tx.close()


def test_idle_deadline_recv_raises_so_a_peer_future_always_resolves() -> None:
    epoch = f"m47-idle-{uuid.uuid4().hex}"
    ep = make_endpoint("solo", epoch=epoch, idle_deadline_s=0.3)
    try:
        wire_endpoints(ep)

        def drain_until_deadline() -> None:
            for _ in range(10_000):  # bounded loop, not a clock assertion: the raise ends it
                ep.recv(timeout=0.05)

        with pytest.raises(delivery_error_cls()):
            run_bounded(drain_until_deadline, timeout_s=60.0)
    finally:
        ep.close()


def test_broadcast_attempts_all_peers_then_raises_the_aggregate() -> None:
    epoch = f"m47-bcast-{uuid.uuid4().hex}"
    sender = make_endpoint("s", epoch=epoch)
    live = make_endpoint("live", epoch=epoch)
    try:
        # the DEAD entry is inserted BEFORE the live one: a stop-at-first-raise loop never
        # reaches "live", so the delivery below discriminates it deterministically (design-A
        # release netting: one dead peer cannot strand the live ones)
        sender.set_registry(
            {
                "s": (sender.host, sender.port),
                "dead": ("127.0.0.1", closed_loopback_port()),
                "live": (live.host, live.port),
            }
        )
        live.set_registry({"live": (live.host, live.port), "s": (sender.host, sender.port)})

        with pytest.raises(delivery_error_cls()):
            run_bounded(lambda: sender.broadcast(("done",)), timeout_s=120.0)
        got = live.recv(timeout=30.0)
        assert got is not None and tuple(got) == ("s", ("done",)), (
            "the live peer ordered AFTER the dead one never got the broadcast — the release loop "
            "stopped at the first raise instead of attempting all peers then raising the aggregate"
        )
    finally:
        sender.close()
        live.close()
