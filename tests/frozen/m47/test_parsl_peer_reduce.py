"""m47 peer reduction hosted on parsl — ``parsl_run_plan`` runs the UNCHANGED M38
``process_and_reduce`` protocol (review N1) over in-task ``EscalatingHttpTransport`` endpoints on
real HTEX worker processes (plan §1.4 peer task shape step 3; blueprint §3
`test_parsl_peer_reduce`).

Witnesses (mechanism, never clocks): result equals ``SequentialRunner`` bit-for-bit, TWICE, each
run under a FRESH epoch nonce; the SubmitBackend spy saw exactly k ``_parsl_peer_main`` submits
(leaves ride the peer spec — no per-leaf futures); at least one ``("node", ...)`` hand-off
genuinely crossed workers (the non-root peer's ``peer_sends`` >= 1 AND the root owner w0's
``peer_recvs`` >= 1 — both from the reused ``process_and_reduce`` witness); the DRIVER inbox saw
only control classes — ``recv_class_root >= 1`` and ``recv_class_node == recv_class_leaf == 0``
(reduction data never routes through the submit host); zero epoch restarts on the clean path.

Discriminates: a driver-side fold that matches the value but has zero cross-worker hand-offs and
node/leaf traffic at the driver; a per-leaf future graph (submit count explodes past k); a relayed
"peer" reduction whose partials ride the driver inbox (recv_class_node > 0)."""

from __future__ import annotations

import sys
from collections import Counter

import pytest
from parsl_transport_harness import (
    W0,
    W1,
    SpyP2PBackend,
    driver_row,
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
K = 2


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=K) as ex:
        yield ex


def _run_once(htex) -> tuple[SpyP2PBackend, object]:  # type: ignore[no-untyped-def]
    spy = SpyP2PBackend(make_parsl_backend(htex))
    res = run_bounded(lambda: plan_run(make_p2p_plan(N_LEAVES, "m47peer"), spy), timeout_s=240.0)
    return spy, res


def test_peer_reduction_matches_sequential_twice_and_is_genuinely_distributed(htex) -> None:  # type: ignore[no-untyped-def]
    oracle = p2p_seq_value(N_LEAVES, "m47peer")

    spy1, r1 = _run_once(htex)
    assert r1.value == oracle, "peer reduction diverged from the SequentialRunner baseline"  # type: ignore[attr-defined]
    assert r1.n_partitions == N_LEAVES  # type: ignore[attr-defined]
    assert r1.transport.epoch_restarts == 0  # type: ignore[attr-defined]
    assert len(list(r1.transport.epoch_nonces)) == 1  # type: ignore[attr-defined]

    names = spy1.fn_names()
    assert names["_parsl_peer_main"] == K, (
        f"expected exactly k={K} _parsl_peer_main submits, got {dict(names)} — leaves must ride "
        "the peer spec, never per-leaf futures"
    )

    rows = peer_rows(r1)
    assert int(rows[W1].get("peer_sends", 0)) >= 1, (
        f"no worker->worker node hand-off recorded on {W1} — a driver-side fold, not peer reduction"
    )
    assert int(rows[W0].get("peer_recvs", 0)) >= 1, f"the root-subtree owner {W0} never consumed a peer node"

    drv = driver_row(r1)
    assert int(drv.get("recv_class_root", 0)) >= 1, "the root never arrived at the driver endpoint"
    assert int(drv.get("recv_class_node", 0)) == 0 and int(drv.get("recv_class_leaf", 0)) == 0, (
        f"the DRIVER inbox saw reduction data ({dict(drv)}) — partials must move peer-to-peer, "
        "never through the submit host (the head-node pathology the transport engine exists to end)"
    )

    spy2, r2 = _run_once(htex)
    assert r2.value == oracle, "second run diverged (determinism gate)"  # type: ignore[attr-defined]
    assert spy2.fn_names()["_parsl_peer_main"] == K
    assert list(r2.transport.epoch_nonces) != list(r1.transport.epoch_nonces), (  # type: ignore[attr-defined]
        "every run must mint a fresh epoch nonce (obligation 9)"
    )


def test_submit_shape_carries_no_engine_task_names(htex) -> None:  # type: ignore[no-untyped-def]
    """The reduce path must not smuggle relay/tasks-engine submits (a hybrid driver-fold)."""
    spy, _res = _run_once(htex)
    names: Counter[str] = spy.fn_names()
    for forbidden in ("_dask_map_write", "_dask_pick", "_dask_gather", "_dask_gather_join"):
        assert names[forbidden] == 0, f"reduce run submitted {forbidden}: {dict(names)}"
