"""m47 reachability probe — "the cluster decides, not the broker" made mechanical (plan §1.5 G2;
blueprint §3 `test_parsl_reachability_probe`). Worker<->worker dialability is a CLUSTER property
parsl never guarantees; the probe runs at rendezvous time, BEFORE any data-plane traffic, and the
verdict routes through ``on_unreachable``.

The negative arms use the pinned ``registry_rewrite`` poisoner (plan §3 harness row): one peer's
book entry is repointed at a just-closed loopback port — connection-refused is deterministic and
clock-free — while the driver's own control legs keep the true endpoints (release and attribution
still reach the peers, so the POOL SURVIVES every arm, asserted by a plain submit afterwards).

Witnesses: standalone ``probe_peer_reachability`` — healthy: ``ok is True`` + no failed pairs;
poisoned: ``ok is False`` + every failed pair names the poisoned destination. Facade explicit
"transport": poisoned + default ``on_unreachable`` ("error") -> an attributed ``StageError``
naming the failing pair, bounded; poisoned + ``"fallback"`` -> the RELAY result on the same
inputs — hashes byte-identical to a pure relay run, ``witness.head_node_routed is True``,
``witness.fallback_reason`` set, and the spy shows the relay Counter AFTER the k released peers
(automatic, observable, never silent).

Discriminates: a probe-less engine (data-plane traffic before the verdict — the poisoned run
would surface a delivery/pull error, not the pair-naming probe error); a silent fallback (the
witness field missing); a fallback that re-enters the p2p engine (the relay Counter absent); a
release that strands peers (the post-arm liveness submit hangs the 2-slot pool)."""

from __future__ import annotations

import sys
from collections import Counter

import pytest
from graphed.debug import StageError
from parsl_transport_harness import (
    W1,
    SpyP2PBackend,
    closed_loopback_port,
    facade_repartition,
    make_parsl_backend,
    make_runner,
    numpy_p2p_adapter,
    p2p_double,
    p2p_htex_pool,
    poison_one,
    probe_reachability,
    relay_repartition,
    repartition_blocks,
    run_bounded,
)

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

PARTS = 5
K = 2


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=K) as ex:
        yield ex


def _assert_pool_alive(backend) -> None:  # type: ignore[no-untyped-def]
    fut = backend.submit(p2p_double, 21, key="m47-probe-liveness")
    assert run_bounded(lambda: fut.result(), timeout_s=120.0) == 42, (
        "the pool is dead after the probe arm — the peers were never released (slots leaked)"
    )


def test_standalone_probe_healthy_and_poisoned(htex) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(htex)

    report = run_bounded(lambda: probe_reachability(backend, K), timeout_s=180.0)
    assert report.ok is True, f"loopback peers ARE mutually reachable; got {report!r}"
    assert not list(report.failed_pairs), "a healthy probe must report zero failed pairs"
    _assert_pool_alive(backend)

    poisoned = run_bounded(
        lambda: probe_reachability(backend, K, registry_rewrite=poison_one(W1, closed_loopback_port())),
        timeout_s=180.0,
    )
    assert poisoned.ok is False, "a poisoned book entry must fail the probe verdict"
    pairs = [(str(s), str(d)) for s, d in poisoned.failed_pairs]
    assert pairs, "the failing (src -> dst) pairs must be reported"
    assert all(dst == W1 for _src, dst in pairs), (
        f"every failed pair must name the unreachable destination {W1}: {pairs}"
    )
    _assert_pool_alive(backend)


def test_facade_default_on_unreachable_is_an_attributed_error_before_any_data(htex) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    backend = make_parsl_backend(htex)
    with pytest.raises(StageError) as excinfo:
        run_bounded(
            lambda: facade_repartition(
                adapter.backend,
                repartition_blocks(adapter),
                PARTS,
                pbackend=backend,
                shuffle_method="transport",
                registry_rewrite=poison_one(W1, closed_loopback_port()),
            ),
            timeout_s=240.0,
        )
    msg = str(excinfo.value)
    assert W1 in msg, (
        f"the attributed error must name the unreachable pair's destination ({W1}) — a pair-blind "
        f"error (or a data-plane delivery error from a probe-less engine) fails here: {msg}"
    )
    _assert_pool_alive(backend)


def test_facade_fallback_runs_the_relay_observably(htex) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    src = repartition_blocks(adapter)
    backend = make_parsl_backend(htex)
    relay_oracle = run_bounded(
        lambda: relay_repartition(adapter.backend, src, PARTS, runner=make_runner(backend))
    )

    spy = SpyP2PBackend(backend)
    res = run_bounded(
        lambda: facade_repartition(
            adapter.backend,
            src,
            PARTS,
            pbackend=spy,
            shuffle_method="transport",
            on_unreachable="fallback",
            registry_rewrite=poison_one(W1, closed_loopback_port()),
        ),
        timeout_s=240.0,
    )

    assert res.dest_block_hashes == relay_oracle.dest_block_hashes, (
        "the fallback result must be byte-identical to the relay engine on the same inputs"
    )
    assert res.witness.head_node_routed is True, "the fallback result must be labeled head-node-routed"
    reason = getattr(res.witness, "fallback_reason", None)
    assert reason, (
        "witness.fallback_reason must be set — the automatic as-tasks fallback is observable, "
        "never silent (§1.5)"
    )
    assert not hasattr(res, "transport"), (
        "a fallback result must be the RELAY engine's shape (no .transport) — the m45 "
        "resolution-observability pin"
    )

    names: Counter[str] = spy.fn_names()
    assert names["_parsl_peer_main"] == K, "the probe phase must have seated k peers first"
    assert names["_dask_map_write"] > 0 and names["_dask_gather"] == PARTS, (
        f"the fallback must genuinely run the relay shape after releasing the peers: {dict(names)}"
    )
    assert names["_dask_pick"] == 0, "zero pick submits (the relay shape, not the m43 T*P shape)"
    _assert_pool_alive(backend)
