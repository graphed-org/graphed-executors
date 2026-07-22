"""m44 at-least-once safety — a retried send that DUPLICATES an already-landed reduction message
must not double-combine (plan §1.1 dedup layer, F10 respecified witness; blueprint §3
`test_transport_reduce_dedup`).

Mechanism: the §1.1 gated injection seam is armed for ONE message from the non-root worker
(deliver-then-raise): the handler delivers the message into the inbox, then raises, so the
sender's real at-least-once retry path re-sends a message that ALREADY landed — the exact
duplicate class m38's local transports never produce. Witnesses: the receiver's
``recv_duplicate_deliveries`` counter proves the same payload bytes were delivered at least twice
(content-digest keyed, F10 — the ``seen`` dedup inside the un-editable ``_peer.py`` exposes no
counter); the sender's ``sends_retried`` proves m44's OWN retry engaged; and the reduced value is
STILL bit-identical to ``SequentialRunner`` with zero epoch restarts.

Discriminates: a transport relying on exactly-once (no consumer dedup) double-combines and fails
the value equality; a transport that silently suppresses duplicates at the wire (digest-dedup)
breaks the at-least-once contract and fails the duplicate-delivery counter; an engine whose
"retry" is a fresh classifier-less resend never bumps ``sends_retried``."""

from __future__ import annotations

import pytest
from graphed.core.execution import SequentialRunner
from transport_harness import (
    build_dask_backend,
    install_transport_plugin,
    make_peer_plan,
    run_bounded,
    set_inject_recv_failures,
    sorted_worker_addresses,
    transport_cluster,
    transport_plan_run,
    tw_per_worker,
)

pytest.importorskip("distributed")

N_LEAVES = 12


def test_duplicate_delivery_is_witnessed_and_combined_exactly_once() -> None:
    oracle = SequentialRunner().run(make_peer_plan(N_LEAVES, "m44dedup"))
    with transport_cluster(2, processes=False) as client:
        install_transport_plugin(client)
        a0, a1 = sorted_worker_addresses(client)
        # ONE deliver-then-raise on the root owner for traffic from the non-root worker: the
        # message lands, the sender retries, the retry lands the SAME bytes again.
        set_inject_recv_failures(client, a0, a1, 1)

        outcome = run_bounded(
            lambda: transport_plan_run(make_peer_plan(N_LEAVES, "m44dedup"), build_dask_backend(client)),
            timeout_s=240.0,
        )
        assert "error" not in outcome, f"run with one injected transient failure raised: {outcome.get('error')!r}"
        res = outcome["result"]

        assert res.value == oracle.value, (
            "value diverged under a duplicated reduction message — the (level,pos) dedup did not "
            "suppress the double combine (at-least-once is UNSAFE in this engine)"
        )
        assert res.transport.epoch_restarts == 0, "one transient failure must ride the retry, not a restart"

        pw = tw_per_worker(res)
        assert pw[a1].get("sends_retried", 0) >= 1, (
            "the sender's own retry counter never advanced — the duplicate did not come from m44's "
            "retry path"
        )
        assert pw[a0].get("recv_duplicate_deliveries", 0) >= 1, (
            "the receiver never saw a duplicate payload delivery — either the injection seam is "
            "not wired, or the transport dedups at the wire (which would break at-least-once "
            "semantics for legitimately identical messages)"
        )
