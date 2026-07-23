"""m47 at-least-once safety — a retried send that DUPLICATES an already-landed reduction message
must not double-combine (plan §1.4 epoch/dedup via the reused ``(level,pos)`` guard; blueprint §3
`test_parsl_reduce_dedup`; the m44 F10 shape re-armed on the HTTP plane).

Mechanism: the pinned spec-carried injection seam (``inject_recv_failures``, the m44 plugin-seam
analog) is armed for ONE message from the non-root peer w1 at the root owner w0: the /msg route
DELIVERS the live-epoch message into the inbox, decrements the budget, then fails the HTTP
response — so w1's REAL inline retry re-sends bytes that already landed. Clock-free and
deterministic (budget-gated, not timing-gated).

Witnesses: w0's ``recv_duplicate_deliveries >= 1`` (content-digest keyed — the un-editable
``_peer.py`` dedup exposes no counter, so the plane must witness the duplicate itself); w1's
``sends_retried >= 1`` (the duplicate came from the REAL retry path, not a fresh classifier-less
resend); the reduced value STILL equals ``SequentialRunner`` bit-for-bit; zero epoch restarts.

Discriminates: an exactly-once assumption (no ``(level,pos)`` dedup) double-combines and fails
value equality; a wire-level digest-dedup transport (suppressing duplicates before delivery)
breaks at-least-once and fails the duplicate-delivery counter; an unwired injection seam fails
both counters."""

from __future__ import annotations

import sys

import pytest
from parsl_transport_harness import (
    W0,
    W1,
    make_p2p_plan,
    make_parsl_backend,
    p2p_htex_pool,
    p2p_seq_value,
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


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=2) as ex:
        yield ex


def test_duplicate_delivery_is_witnessed_and_combined_exactly_once(htex) -> None:  # type: ignore[no-untyped-def]
    oracle = p2p_seq_value(N_LEAVES, "m47dedup")
    backend = make_parsl_backend(htex)

    res = run_bounded(
        lambda: plan_run(make_p2p_plan(N_LEAVES, "m47dedup"), backend, inject_recv_failures={W0: {W1: 1}}),
        timeout_s=240.0,
    )

    assert res.value == oracle, (
        "value diverged under a duplicated reduction message — the (level,pos) dedup did not "
        "suppress the double combine (at-least-once is UNSAFE in this engine)"
    )
    assert res.transport.epoch_restarts == 0, (
        "one transient failure must ride the send retry, never a restart"
    )

    rows = peer_rows(res)
    assert int(rows[W1].get("sends_retried", 0)) >= 1, (
        "the sender's own retry counter never advanced — the duplicate did not come from the "
        "REAL inline retry path (§1.4)"
    )
    assert int(rows[W0].get("recv_duplicate_deliveries", 0)) >= 1, (
        "the receiver never witnessed a duplicate payload delivery — either the injection seam is "
        "unwired, or the transport dedups at the wire (which would break at-least-once semantics "
        "for legitimately identical messages)"
    )
