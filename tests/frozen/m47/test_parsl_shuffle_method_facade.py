"""m47 parsl facade — ONE shared resolver, honest per-instance dispatch, loud refusals (plan
§1.5; blueprint §3 `test_parsl_shuffle_method_facade`).

Witnesses: **resolver identity** — ``parsl_backend.api.resolve_shuffle_method IS
dask_backend.api.resolve_shuffle_method`` (the same function object, re-exported from
``common/facade`` — a copied resolver fails ``is``); dispatch shapes at the spy seam — ``auto``
and explicit ``tasks`` on HTEX produce the RELAY Counter (maps+gathers, zero picks, zero peers)
with the head-node witness on the result, explicit ``transport`` produces the p2p Counter (k
``_parsl_peer_main``, zero relay names) with ``.transport`` on the result and hashes == local;
invalid method -> ValueError naming all three values with ZERO submits/broadcasts; a
transport-only knob explicitly set while resolution lands on tasks -> ValueError NAMING the knob
before any work ({fetch_budget_bytes, pull_timeout_s, epoch_restarts_allowed, workers,
on_unreachable} — the §1.5 additions included); **``peer_transport``** — True on the HTEX
instance, False on TPE, and explicit ``"transport"`` on TPE -> NotImplementedError naming
``"tasks"`` with zero submits (a loopback re-enactment of head-node routing adds mechanism and
removes honesty).

Discriminates: hardcoded-engine facades (both dispatch shapes present); a copied resolver
(identity); silent knob drops; a TPE "transport" that actually runs; a capability-field-based
transport gate (peer_transport is a backend attribute — the frozen 7-field vector is pinned
unchanged by m46)."""

from __future__ import annotations

import sys
from collections import Counter

import pytest
from parsl_transport_harness import (
    RefusingP2PBackend,
    SpyP2PBackend,
    facade_join,
    facade_repartition,
    htex_floor_caps,
    join_sides,
    make_parsl_backend,
    numpy_p2p_adapter,
    p2p_htex_pool,
    p2p_tpe_pool,
    papi,
    repartition_blocks,
    run_bounded,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

PARTS = 5
K = 2
TRANSPORT_ONLY_KNOBS = {
    "fetch_budget_bytes": 1,
    "pull_timeout_s": 30.0,
    "epoch_restarts_allowed": 1,
    "workers": 2,
    "on_unreachable": "fallback",
}


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=K) as ex:
        yield ex


def test_resolver_is_the_one_shared_function_object() -> None:
    import graphed_executors.dask_backend.api as dapi  # noqa: PLC0415  (frozen m45 anchor)

    assert papi().resolve_shuffle_method is dapi.resolve_shuffle_method, (
        "parsl_backend.api must re-export THE SAME resolve_shuffle_method object the dask facade "
        "uses (common/facade) — a copy can drift and un-share the frozen truth table"
    )


@pytest.mark.parametrize("method", ["auto", "tasks"])
def test_auto_and_tasks_dispatch_to_the_relay_shape_on_htex(htex, method: str) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    src = repartition_blocks(adapter)
    local = run_repartition(adapter.backend, src, PARTS)
    spy = SpyP2PBackend(make_parsl_backend(htex))

    res = run_bounded(
        lambda: facade_repartition(adapter.backend, src, PARTS, pbackend=spy, shuffle_method=method)
    )
    names: Counter[str] = spy.fn_names()
    assert names["_dask_map_write"] > 0 and names["_dask_gather"] == PARTS, (
        f"{method} on the all-False HTEX floor must run the RELAY shape: {dict(names)}"
    )
    assert names["_dask_pick"] == 0 and names["_parsl_peer_main"] == 0, (
        f"{method}: neither the m43 pick tier nor the p2p engine may engage: {dict(names)}"
    )
    assert res.dest_block_hashes == local.dest_block_hashes
    assert res.witness.head_node_routed is True, "the relay result must carry the head-node witness"
    assert not hasattr(res, "transport"), "a tasks-resolution result must not carry .transport"


def test_explicit_transport_dispatches_to_the_p2p_shape_on_htex(htex) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    src = repartition_blocks(adapter)
    local = run_repartition(adapter.backend, src, PARTS)
    spy = SpyP2PBackend(make_parsl_backend(htex))

    res = run_bounded(
        lambda: facade_repartition(adapter.backend, src, PARTS, pbackend=spy, shuffle_method="transport")
    )
    names: Counter[str] = spy.fn_names()
    assert names["_parsl_peer_main"] == K, f"explicit transport must seat k={K} peers: {dict(names)}"
    assert names["_dask_map_write"] == 0 and names["_dask_gather"] == 0, (
        f"the p2p engine must not smuggle relay submits: {dict(names)}"
    )
    assert res.dest_block_hashes == local.dest_block_hashes
    assert hasattr(res, "transport"), "the transport result must carry .transport (m45 observability)"


def test_invalid_method_raises_before_any_work() -> None:
    stub = RefusingP2PBackend(htex_floor_caps(), peer_transport=True)
    adapter = numpy_p2p_adapter()
    blocks = repartition_blocks(adapter)[:2]
    left, right = join_sides(adapter)

    with pytest.raises(ValueError, match="auto") as excinfo:
        facade_repartition(adapter.backend, blocks, 4, pbackend=stub, shuffle_method="p2p")
    msg = str(excinfo.value)
    assert "transport" in msg and "tasks" in msg, f"all three valid values must be named: {msg}"
    with pytest.raises(ValueError, match="auto"):
        facade_join(adapter.backend, left, right, 4, pbackend=stub, shuffle_method="p2p")
    assert stub.submit_attempts == 0 and stub.broadcast_attempts == 0, (
        "an invalid shuffle_method must be rejected before ANY work reaches the backend"
    )


@pytest.mark.parametrize("knob", sorted(TRANSPORT_ONLY_KNOBS))
def test_transport_only_knobs_are_rejected_on_tasks_resolution(knob: str) -> None:
    stub = RefusingP2PBackend(htex_floor_caps(), peer_transport=True)
    adapter = numpy_p2p_adapter()
    blocks = repartition_blocks(adapter)[:2]

    with pytest.raises(ValueError, match=knob):
        facade_repartition(
            adapter.backend,
            blocks,
            4,
            pbackend=stub,
            shuffle_method="auto",
            **{knob: TRANSPORT_ONLY_KNOBS[knob]},
        )
    assert stub.submit_attempts == 0, f"{knob}: the rejection must land before any submit"


def test_peer_transport_attribute_and_the_tpe_refusal() -> None:
    with p2p_tpe_pool() as tpe:
        tpe_backend = make_parsl_backend(tpe)
        assert tpe_backend.peer_transport is False, (
            "ParslBackend(TPE).peer_transport must be False — TPE tasks are driver-process "
            "threads; a 'peer exchange' there is loopback head-node routing with extra mechanism"
        )
        spy = SpyP2PBackend(tpe_backend)
        adapter = numpy_p2p_adapter()
        with pytest.raises(NotImplementedError, match="tasks"):
            facade_repartition(
                adapter.backend,
                repartition_blocks(adapter)[:2],
                4,
                pbackend=spy,
                shuffle_method="transport",
            )
        assert spy.fn_names() == Counter(), "the TPE refusal must fire before any submit"


def test_htex_backend_reports_peer_transport_true(htex) -> None:  # type: ignore[no-untyped-def]
    assert make_parsl_backend(htex).peer_transport is True, (
        "ParslBackend(HTEX).peer_transport must be True in m47 (the §1.4 plane runs on HTEX "
        "worker processes) — a parsl_backend-private attribute, never a SubmitCapabilities field"
    )
