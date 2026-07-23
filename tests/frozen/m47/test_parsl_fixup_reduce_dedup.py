"""m47 FIXUP — the ``(level,pos)`` dedup MECHANISM witnessed, not just survived (plan §1.4
at-least-once + dedup; the adversarial-review ruling on the pre-fixup dedup test: ONE duplicated
hand-off can be absorbed by slot-keyed consumption without a value change, so value-parity alone
cannot distinguish 'dedup works' from 'the topology absorbed it').

Scenario: k=3 over 12 leaves. BOTH duplicates ride ONE edge — the deliver-then-fail seam is armed
for the first TWO deliveries from w1 at w0 (``{w0: {w1: 2}}``, budget 2 < SEND_RETRIES=5), so w1's
single hand-off ``send()`` delivers three times (two genuine at-least-once duplicates from the
REAL inline retry loop). Determinism by construction: the root owner w0 cannot complete until the
OTHER sibling's hand-off arrives, and w2's leaves each carry a compute delay — so w2's node lands
hundreds of ms AFTER w1's inline retries have fully settled (they resolve within w1's one
``send()`` call, in the first milliseconds of the run). A pure-duplicate retry can therefore never
race endpoint teardown — the failure mode that made the both-edges-into-the-root variant of this
scenario intermittently escalate a late retry into a legitimate epoch restart. w2's edge is clean
and serves as the negative control. (Ownership is the engine's contiguous leaf split — w2 owns the
top third — witnessed indirectly by the peer_sends/peer_recvs scenario guards below; work-stealing
is off in this engine, so the injected budget can only hit the node hand-off.)

Witnesses (exact, baseline-relative — a clean run on the same pool is measured first):
value bit-identical to ``SequentialRunner`` in BOTH runs; per-peer ``n_combines`` in the injected
run IDENTICAL to the clean run with the tree total Σ == N-1 == 11; w0's protocol-layer
``peer_recvs`` EXACTLY 2 in both runs (four plane deliveries, two protocol consumptions — the
dedup drop made observable: a dropped guard reads 4); Σ ``processed`` == N in both runs (no leaf
re-execution); w0 ``recv_duplicate_deliveries >= 2`` (BOTH duplicates genuinely landed and were
witnessed); w1 ``sends_retried >= 2`` (both duplicates came from the real §1.4 inline retry path);
w2 ``sends_retried == 0`` (the clean edge never retried — the injection stayed on its edge); zero
epoch restarts in both runs.

Discriminates (cheat killed per pin): a dropped ``(level,pos)`` dedup — ``peer_recvs`` at w0 reads
4 not 2 (the guard at the consumption boundary is exactly what this pin measures), and any
double-combine variant inflates ``n_combines`` past the pinned clean-run counts or diverges the
value (accumulate-style); a wire-level digest dedup that suppresses duplicates before delivery
(breaks at-least-once): ``recv_duplicate_deliveries`` reads < 2; an unwired injection seam:
``sends_retried`` stays 0 on w1; a re-execution 'recovery' masquerading as dedup: Σ ``processed``
inflates past N; an escalation instead of a retry: ``epoch_restarts`` reads > 0."""

from __future__ import annotations

import sys
import time

import pytest
from graphed.core.execution import Partition, Plan, Task
from parsl_transport_harness import (
    W0,
    W1,
    make_parsl_backend,
    p2p_add,
    p2p_htex_pool,
    p2p_leaf_value,
    p2p_partitions,
    p2p_seq_value,
    p2p_zero,
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
K = 3
W2 = "w2"  # the harness pins the "w0".."w{k-1}" scheme; k=3 adds this third address

#: leaves owned by w2 under the engine's contiguous 12/3 split — these gate root completion
_SLOW_LEAF_START = 8
#: per-leaf delay for w2's subtree: the root stays live >= 4x this after w1's retries settle
_SLOW_LEAF_S = 0.12


def p2p_leaf_value_w2_slow(partition: Partition, resources: object) -> int:
    """Same value as ``p2p_leaf_value`` (the oracle stays valid); w2-owned leaves sleep so the
    root's LAST needed hand-off arrives long after the injected edge's retries settled."""
    if int(partition.entry_start) >= _SLOW_LEAF_START:
        time.sleep(_SLOW_LEAF_S)
    return p2p_leaf_value(partition, resources)


def make_gated_p2p_plan(n: int, tag: str) -> Plan[int]:
    tasks = tuple(Task(i, p) for i, p in enumerate(p2p_partitions(n, tag)))
    return Plan(process=p2p_leaf_value_w2_slow, combine=p2p_add, empty=p2p_zero, tasks=tasks)


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=K) as ex:
        yield ex


def test_double_sibling_duplicates_are_deduped_with_exact_counters(htex) -> None:  # type: ignore[no-untyped-def]
    oracle = p2p_seq_value(N_LEAVES, "m47fixdedup")
    backend = make_parsl_backend(htex)

    clean = run_bounded(
        lambda: plan_run(make_gated_p2p_plan(N_LEAVES, "m47fixdedup"), backend),
        timeout_s=300.0,
    )
    assert clean.value == oracle, "clean k=3 baseline diverged from SequentialRunner"
    assert clean.transport.epoch_restarts == 0
    crows = peer_rows(clean)
    # scenario guard: the measured two-siblings-into-w0 topology must actually hold, else the
    # injection below is aimed at a hand-off that does not exist (a vacuous dedup scenario)
    assert int(crows[W1].get("peer_sends", 0)) >= 1 and int(crows[W2].get("peer_sends", 0)) >= 1, (
        f"k={K} topology drifted — both non-root peers must hand a node to the root owner: "
        f"{ {a: dict(c) for a, c in crows.items()} }"
    )
    assert int(crows[W0].get("peer_recvs", 0)) == 2, (
        "the root owner must consume exactly the two sibling nodes in the clean run"
    )
    assert int(crows[W0].get("recv_duplicate_deliveries", 0)) == 0, (
        "the clean baseline saw duplicate deliveries — the pool is polluted; the injected "
        "comparison below would be meaningless"
    )
    clean_combines = {a: int(c.get("n_combines", 0)) for a, c in crows.items()}
    assert sum(clean_combines.values()) == N_LEAVES - 1, (
        f"a {N_LEAVES}-leaf tree reduces in exactly {N_LEAVES - 1} combines: {clean_combines}"
    )

    inj = run_bounded(
        lambda: plan_run(
            make_gated_p2p_plan(N_LEAVES, "m47fixdedup"),
            backend,
            inject_recv_failures={W0: {W1: 2}},
        ),
        timeout_s=300.0,
    )
    assert inj.value == oracle, (
        "value diverged with w1's hand-off delivered three times — a duplicate was combined "
        "(at-least-once is UNSAFE in this engine)"
    )
    assert inj.transport.epoch_restarts == 0, (
        "two transient failures on one edge must ride the inline retry, never a restart — the "
        "root was still gated on w2's slow subtree, so no retry could race teardown"
    )

    irows = peer_rows(inj)
    inj_combines = {a: int(c.get("n_combines", 0)) for a, c in irows.items()}
    assert inj_combines == clean_combines, (
        f"per-peer combine counts changed under duplicated deliveries ({clean_combines} -> "
        f"{inj_combines}) — a dropped (level,pos) dedup combined a duplicate"
    )
    assert int(irows[W0].get("peer_recvs", 0)) == 2, (
        "the root owner's protocol layer consumed more than the two sibling nodes — duplicates "
        "leaked past the (level,pos) dedup into consumption (a dropped guard reads 4 here)"
    )
    assert sum(int(c.get("processed", 0)) for c in irows.values()) == N_LEAVES, (
        "leaf re-execution under duplicated hand-offs — a recompute masquerading as dedup"
    )
    assert int(irows[W0].get("recv_duplicate_deliveries", 0)) >= 2, (
        "fewer than TWO duplicate deliveries witnessed at w0 — either the injection seam did not "
        "arm both failures, or the transport dedups at the wire (which would break at-least-once "
        "for legitimately identical messages)"
    )
    assert int(irows[W1].get("sends_retried", 0)) >= 2, (
        "w1 retried fewer than twice — a duplicate did not come from the REAL inline retry "
        "path (§1.4)"
    )
    assert int(irows[W2].get("sends_retried", 0)) == 0, (
        "the CLEAN edge retried — the injected budget bled past its (receiver, sender) edge"
    )
