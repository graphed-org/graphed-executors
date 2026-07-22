"""m44 peer reduction hosted on dask — ``transport_run_plan`` runs the UNCHANGED M38
``process_and_reduce`` protocol over ``DaskWorkerTransport`` endpoints (plan §1.3; blueprint §3
`test_transport_peer_reduce_dask`). Tier B: real worker processes, real sockets.

Witnesses (mechanism, never clocks): result equals ``SequentialRunner`` bit-for-bit, TWICE (fresh
epoch each run); the scheduler saw exactly k ``_dask_peer_main`` actor tasks through the m42
submit seam (leaves ride the broadcast payload — no per-leaf futures); at least one ``("node",…)``
hand-off genuinely crossed workers (the non-root owner's ``peer_sends`` and the root owner's
``recv_invocations``); ``sends_dropped == 0`` (reduction-class messages are never dropped, §1.1
layer 1); zero epoch restarts on the clean path.

Discriminates: a driver-side fold that matches the value but has zero cross-worker hand-offs and
the wrong task count; a per-leaf future graph (peer task count explodes); an engine that drops
reduction traffic and still reports green."""

from __future__ import annotations

import pytest
from graphed.core.execution import SequentialRunner
from transport_harness import (
    SpyDaskBackend,
    build_dask_backend,
    install_transport_plugin,
    make_peer_plan,
    run_bounded,
    sorted_worker_addresses,
    transport_cluster,
    transport_plan_run,
    tw_per_worker,
    tw_total,
)

pytest.importorskip("distributed")

N_LEAVES = 12


@pytest.fixture(scope="module")
def client():
    with transport_cluster(2, processes=True) as c:
        yield c


def _run_once(client) -> tuple[SpyDaskBackend, object]:  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    plan = make_peer_plan(N_LEAVES, "m44peer")
    spy = SpyDaskBackend(build_dask_backend(client))
    outcome = run_bounded(lambda: transport_plan_run(plan, spy), timeout_s=240.0)
    assert "error" not in outcome, f"clean peer run raised: {outcome.get('error')!r}"
    return spy, outcome["result"]


def test_peer_reduction_matches_sequential_twice_and_is_genuinely_distributed(client) -> None:  # type: ignore[no-untyped-def]
    a0, a1 = sorted_worker_addresses(client)
    oracle = SequentialRunner().run(make_peer_plan(N_LEAVES, "m44peer"))

    spy1, r1 = _run_once(client)
    assert r1.value == oracle.value, "peer reduction diverged from the SequentialRunner baseline"
    assert r1.n_partitions == oracle.n_partitions

    # scheduler shape: exactly k peer actors through the submit seam, no per-leaf futures
    names = spy1.submitted_fn_names()
    assert names["_dask_peer_main"] == 2, (
        f"expected exactly k=2 _dask_peer_main actor tasks, got {dict(names)} — leaves must ride "
        "the broadcast payload, never per-leaf futures"
    )

    # the reduction genuinely crossed workers: the non-root owner (sorted order, F8) must hand its
    # subtree top to the root owner over the transport
    pw = tw_per_worker(r1)
    assert pw[a1].get("peer_sends", 0) >= 1, (
        f"no accepted worker->worker send recorded on {a1} — a driver-side fold, not peer reduction"
    )
    assert pw[a0].get("recv_invocations", 0) >= 1, (
        f"the root-subtree owner {a0} never received a transport message"
    )

    # hard constraint 8: no reduction-class message may be dropped on the clean path
    assert tw_total(r1, "sends_dropped") == 0, "a dropped reduction message means a lost node"
    assert r1.transport.epoch_restarts == 0
    assert len(list(r1.transport.epoch_nonces)) == 1

    # determinism: a second run is bit-identical under a FRESH epoch
    spy2, r2 = _run_once(client)
    assert r2.value == oracle.value
    assert spy2.submitted_fn_names()["_dask_peer_main"] == 2
    assert list(r2.transport.epoch_nonces) != list(r1.transport.epoch_nonces), (
        "every run must mint a fresh epoch nonce (§1.5)"
    )
