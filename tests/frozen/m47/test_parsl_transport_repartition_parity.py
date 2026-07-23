"""m47 behavioral golden (m39 carried) — ``transport_run_repartition`` reproduces the LOCAL
engine's ``dest_block_hashes`` BYTE-IDENTICALLY, and the RELAY engine's too, on both real
backends, across two consecutive p2p runs under fresh epochs (plan §1.4/§2 m47; blueprint §3
`test_parsl_transport_repartition_parity`).

Witnesses: sha256 dest hashes equal to ``graphed_executors.local.shuffle.run_repartition`` AND to
the m46 relay engine on identical inputs (cross-ENGINE content addressing — the three engines
share the frozen route/merge/wire contract); per-dest key routing equals the golden-pinned
``partition`` oracle; row conservation; fresh epoch nonce per run. Any routing drift, merge-order
drift, or wire-format drift fails byte equality — the m39/m43 hash-gate discrimination, now
against the peer-exchange engine."""

from __future__ import annotations

import sys

import pytest
from parsl_transport_harness import (
    P2PAdapter,
    make_parsl_backend,
    make_runner,
    p2p_adapters,
    p2p_htex_pool,
    relay_repartition,
    repartition_blocks,
    result_dest_keys,
    route_oracle_dest_keys,
    run_bounded,
    total_result_rows,
    transport_repartition,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

ADAPTERS = p2p_adapters()
PARTS = 8


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=2) as ex:
        yield ex


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> P2PAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_dest_hashes_byte_identical_to_local_and_relay_and_across_runs(adapter: P2PAdapter, htex) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks(adapter)
    local = run_repartition(adapter.backend, src, PARTS)
    backend = make_parsl_backend(htex)

    r1 = run_bounded(lambda: transport_repartition(adapter.backend, src, PARTS, pbackend=backend))
    r2 = run_bounded(lambda: transport_repartition(adapter.backend, src, PARTS, pbackend=backend))
    relay = run_bounded(lambda: relay_repartition(adapter.backend, src, PARTS, runner=make_runner(backend)))

    assert r1.dest_block_hashes, "no dest blocks produced — scenario degenerate"
    assert r1.dest_block_hashes == local.dest_block_hashes, (
        "p2p-engine dest hashes diverged from the LOCAL engine on identical inputs — routing, "
        "merge order, or wire bytes drifted from the frozen m39 contract"
    )
    assert r1.dest_block_hashes == relay.dest_block_hashes, (
        "p2p and relay engines disagree on identical inputs — the two parsl engines must be "
        "interchangeable in content (the §1.5 fallback depends on it)"
    )
    assert r2.dest_block_hashes == r1.dest_block_hashes, "second p2p run diverged (determinism gate)"
    assert list(r1.transport.epoch_nonces) != list(r2.transport.epoch_nonces), (
        "every run must mint a fresh epoch nonce"
    )

    assert result_dest_keys(adapter, r1.value) == route_oracle_dest_keys(adapter, src, PARTS)
    assert total_result_rows(r1.value) == sum(len(b) for b in src), "rows were lost or invented"  # type: ignore[arg-type]


def test_salt_reroutes_deterministically(adapter: P2PAdapter, htex) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks(adapter)
    backend = make_parsl_backend(htex)
    plain = run_bounded(lambda: transport_repartition(adapter.backend, src, PARTS, pbackend=backend))
    s1 = run_bounded(lambda: transport_repartition(adapter.backend, src, PARTS, pbackend=backend, salt=3))
    s2 = run_bounded(lambda: transport_repartition(adapter.backend, src, PARTS, pbackend=backend, salt=3))

    assert s1.dest_block_hashes == s2.dest_block_hashes, "salted runs diverged (nondeterminism)"
    assert s1.dest_block_hashes != plain.dest_block_hashes, (
        "salt=3 produced the identical placement — salt never reached the pinned route"
    )
    assert total_result_rows(s1.value) == sum(len(b) for b in src)  # type: ignore[arg-type]
