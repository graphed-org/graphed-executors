"""m44 non-loss retry/raise arc (plan §1.1 r2/r3, hard constraint 8; blueprint §3
`test_transport_retry_exhaustion`, added by review r3 to kill the exact pre-r2 bug: ``_asend``
returning a silent ``False`` on a transient comm failure — which the un-editable consumers ignore,
losing a reduction node forever).

Three arms, each on its own tier-A cluster, all driven through the §1.1 gated injection seam
(deliver-then-raise, budget decremented per handler invocation — clock-free, deterministic):

(a) N=2 < SEND_RETRIES transient failures ⇒ the send's own bounded retry rides them out:
    ``sends_retried >= 2``, result bit-identical to ``SequentialRunner``, zero restarts.
(b) N=8 >= SEND_RETRIES ⇒ the send exhausts, ``TransportDeliveryError`` escalates out of the
    pinned peer task, and with ``epoch_restarts_allowed=1`` the run RESTARTS under a fresh epoch
    and completes (``epoch_restarts == 1``, two nonces, old epoch purged). The leftover injection
    budget (8-5=3 < SEND_RETRIES) is absorbed by the rerun's own retries — proving retry and
    restart compose.
(c) N=8 with ``epoch_restarts_allowed=0`` ⇒ an attributed ``StageError`` NAMING
    TransportDeliveryError — never a hang (hard run_bounded timeout, F2) and never a silent wrong
    result.

Discriminates: the pre-r2 silent-False regression fails (a) via ``sends_retried == 0`` plus a lost
node (hang/wrong value, both surfaced); an engine without the exhaustion raise fails (b)/(c) by
completing silently or hanging; an engine that leaks the raw delivery error fails (c)'s
StageError type pin."""

from __future__ import annotations

import pytest
from graphed.core.execution import SequentialRunner
from graphed.debug import StageError
from transport_harness import (
    build_dask_backend,
    install_transport_plugin,
    make_peer_plan,
    probe_active_epochs,
    run_bounded,
    set_inject_recv_failures,
    sorted_worker_addresses,
    transport_cluster,
    transport_plan_run,
    tw_per_worker,
)

pytest.importorskip("distributed")

N_LEAVES = 12
N_TRANSIENT = 2  # < SEND_RETRIES: the retry absorbs it
N_EXHAUST = 8  # >= SEND_RETRIES: the first exhaustion fires; the leftover (< SEND_RETRIES) is survivable


def _oracle_value() -> int:
    return SequentialRunner().run(make_peer_plan(N_LEAVES, "m44retry")).value


def _mentions_transport_delivery_error(exc: BaseException | None) -> bool:
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if "TransportDeliveryError" in f"{type(exc).__name__}: {exc}":
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def test_transient_failures_are_absorbed_by_the_send_retry() -> None:
    with transport_cluster(2, processes=False) as client:
        install_transport_plugin(client)
        a0, a1 = sorted_worker_addresses(client)
        set_inject_recv_failures(client, a0, a1, N_TRANSIENT)

        outcome = run_bounded(
            lambda: transport_plan_run(make_peer_plan(N_LEAVES, "m44retry"), build_dask_backend(client)),
            timeout_s=240.0,
        )
        assert "error" not in outcome, f"transient-failure run raised: {outcome.get('error')!r}"
        res = outcome["result"]

        assert res.value == _oracle_value(), "a transiently-failed send lost a reduction message"
        assert res.transport.epoch_restarts == 0, "transient failures must ride the retry, not a restart"
        pw = tw_per_worker(res)
        assert pw[a1].get("sends_retried", 0) >= N_TRANSIENT, (
            f"sends_retried={pw[a1].get('sends_retried', 0)} < {N_TRANSIENT}: m44's OWN retry path "
            "never engaged — the pre-r2 silent-False bug (dask's base RPC retries nothing; "
            "comm.retry.count is 0 by default)"
        )


def test_exhausted_retries_restart_the_epoch_and_complete() -> None:
    with transport_cluster(2, processes=False) as client:
        install_transport_plugin(client)
        a0, a1 = sorted_worker_addresses(client)
        set_inject_recv_failures(client, a0, a1, N_EXHAUST)

        outcome = run_bounded(
            lambda: transport_plan_run(
                make_peer_plan(N_LEAVES, "m44retry"), build_dask_backend(client), epoch_restarts_allowed=1
            ),
            timeout_s=300.0,
        )
        assert "error" not in outcome, f"restart-allowed run raised: {outcome.get('error')!r}"
        res = outcome["result"]

        assert res.value == _oracle_value(), "the restarted run diverged from the baseline"
        assert res.transport.epoch_restarts == 1, (
            f"epoch_restarts={res.transport.epoch_restarts}: exhausted send retries must trigger "
            "exactly one whole-run restart under a fresh epoch (§1.5)"
        )
        nonces = list(res.transport.epoch_nonces)
        assert len(nonces) == 2 and nonces[0] != nonces[1]
        for _addr, epochs in client.run(probe_active_epochs).items():
            for nonce in nonces:
                assert nonce not in epochs, f"epoch {nonce} survived the run — purge did not happen"


def test_exhausted_restart_budget_is_an_attributed_stage_error_never_a_hang() -> None:
    with transport_cluster(2, processes=False) as client:
        install_transport_plugin(client)
        a0, a1 = sorted_worker_addresses(client)
        set_inject_recv_failures(client, a0, a1, N_EXHAUST)

        outcome = run_bounded(
            lambda: transport_plan_run(
                make_peer_plan(N_LEAVES, "m44retry"), build_dask_backend(client), epoch_restarts_allowed=0
            ),
            timeout_s=300.0,
        )
        assert "result" not in outcome, (
            "with epoch_restarts_allowed=0 an exhausted send must FAIL the run — a green result "
            "here means the delivery failure was swallowed"
        )
        err = outcome["error"]
        assert type(err) is StageError, (
            f"expected an attributed StageError, got {type(err).__name__}: the raw transport/dask "
            "failure must never leak to the user (§1.5, m42 precedent)"
        )
        assert _mentions_transport_delivery_error(err), (
            f"the StageError must name the TransportDeliveryError exhaustion cause; got: {err}"
        )
