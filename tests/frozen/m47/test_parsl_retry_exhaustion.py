"""m47 non-loss arc (hard constraint 8) — the engine-level lifecycle of the §1.4
``EscalatingHttpTransport.send`` path: transient failures ride the bounded retry; exhaustion
RAISES and escalates to a whole-run fresh-epoch restart; an exhausted restart budget surfaces as
an attributed ``StageError``, never a hang and never a silent wrong result (plan §1.4 send design
+ epoch/restart; blueprint §3 `test_parsl_retry_exhaustion`; the m44-r3 silent-False bug class
re-armed against the base class's actual ``drops += 1`` behavior).

Three arms over the pinned deliver-then-fail injection seam (budget-gated per (dest, src), never a
clock), each on its OWN pool (a restart-scarred pool must not leak between arms):

(a) N=2 < SEND_RETRIES ⇒ the inline retry rides it out: w1 ``sends_retried >= 2``, value
    bit-identical to ``SequentialRunner``, zero restarts.
(b) N=8 >= SEND_RETRIES ⇒ the first send exhausts, ``TransportDeliveryError`` errors the peer,
    and with ``epoch_restarts_allowed=1`` the run RESTARTS under a fresh epoch and completes
    (``epoch_restarts == 1``, two distinct nonces, ``send_failures >= 1``); the leftover budget
    (8-5=3 < SEND_RETRIES) is absorbed by the rerun's own retries — retry and restart COMPOSE.
(c) N=8 with ``epoch_restarts_allowed=0`` ⇒ an attributed ``StageError`` NAMING
    TransportDeliveryError — never a raw transport/parsl exception, never a hang (hard timeout).

Discriminates: a base-``HttpTransport`` delegation (send returns after ``drops += 1``: arm (a)
loses the node — wrong value or hang, both surfaced — and arms (b)/(c) never raise); an engine
that swallows an errored peer future because a root happened to arrive (the injected deliveries DO
land — ``epoch_restarts == 1`` still must hold); raw-exception leakage to the user in (c)."""

from __future__ import annotations

import sys

import pytest
from graphed.debug import StageError
from parsl_transport_harness import (
    SEND_RETRIES,
    W0,
    W1,
    make_p2p_plan,
    make_parsl_backend,
    p2p_htex_pool,
    p2p_seq_value,
    peer_rows,
    plan_run,
    pw_total,
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
N_TRANSIENT = 2  # < SEND_RETRIES: the retry absorbs it
N_EXHAUST = 8  # >= SEND_RETRIES: first exhaustion fires; the leftover (3 < SEND_RETRIES) is survivable

assert N_TRANSIENT < SEND_RETRIES <= N_EXHAUST  # the arm arithmetic the frozen policy implies


def test_transient_failures_are_absorbed_by_the_inline_retry() -> None:
    with p2p_htex_pool(workers=2) as htex:
        res = run_bounded(
            lambda: plan_run(
                make_p2p_plan(N_LEAVES, "m47retry"),
                make_parsl_backend(htex),
                inject_recv_failures={W0: {W1: N_TRANSIENT}},
            ),
            timeout_s=300.0,
        )
        assert res.value == p2p_seq_value(N_LEAVES, "m47retry"), (
            "a transiently-failed send lost a reduction message"
        )
        assert res.transport.epoch_restarts == 0, "transient failures must ride the retry, not a restart"
        retried = int(peer_rows(res)[W1].get("sends_retried", 0))
        assert retried >= N_TRANSIENT, (
            f"sends_retried={retried} < {N_TRANSIENT}: the §1.4 inline retry never engaged — the "
            "base sender-thread path (or a first-failure raise) is standing in for it"
        )


def test_exhausted_retries_restart_the_epoch_and_complete() -> None:
    with p2p_htex_pool(workers=2) as htex:
        res = run_bounded(
            lambda: plan_run(
                make_p2p_plan(N_LEAVES, "m47retry"),
                make_parsl_backend(htex),
                inject_recv_failures={W0: {W1: N_EXHAUST}},
                epoch_restarts_allowed=1,
            ),
            timeout_s=300.0,
        )
        assert res.value == p2p_seq_value(N_LEAVES, "m47retry"), (
            "the restarted run diverged from the baseline"
        )
        assert res.transport.epoch_restarts == 1, (
            f"epoch_restarts={res.transport.epoch_restarts}: exhausted send retries must trigger "
            "exactly one whole-run restart under a fresh epoch — even though the injected "
            "deliveries landed, an errored peer future is run-fatal (never silently ignored)"
        )
        nonces = list(res.transport.epoch_nonces)
        assert len(nonces) == 2 and nonces[0] != nonces[1], f"expected two distinct nonces: {nonces}"
        assert pw_total(res, "send_failures") >= 1, (
            "no send_failures counted — the exhaustion raise (the drop->raise bridge) never fired"
        )


def test_exhausted_restart_budget_is_an_attributed_stage_error_never_a_hang() -> None:
    with p2p_htex_pool(workers=2) as htex:
        with pytest.raises(StageError) as excinfo:
            run_bounded(
                lambda: plan_run(
                    make_p2p_plan(N_LEAVES, "m47retry"),
                    make_parsl_backend(htex),
                    inject_recv_failures={W0: {W1: N_EXHAUST}},
                    epoch_restarts_allowed=0,
                ),
                timeout_s=300.0,
            )
        err = excinfo.value
        rendered = f"{getattr(err, 'cause_type', '')}: {err}"
        assert "TransportDeliveryError" in rendered, (
            f"the StageError must name the TransportDeliveryError exhaustion cause; got: {rendered}"
        )
