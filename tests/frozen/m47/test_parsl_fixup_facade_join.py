"""m47 FIXUP — the parsl facade's ``run_join`` dispatch body, arm by arm (plan §1.5; the
adversarial-review dark-block ruling: the only pre-fixup frozen ``run_join`` facade call used an
invalid ``shuffle_method``, so the entire valid-method join dispatch — engine selection, the
many-positional-argument pass-through of ``on``/``how``/``broadcast``, and the per-arm result
shapes — ran unwitnessed).

Witnesses: explicit ``"transport"`` → exactly k ``_parsl_peer_main`` submits with ZERO relay/tasks
names, ``.transport`` present with the peer rows and the block-plane counters live
(``pull_requests_served``/``bytes_served`` — join fragments moved peer↔peer under the facade arm),
driver ``recv_class_hello == k`` (the rendezvous ran behind the facade), and bit-for-bit parity vs
the local engine (inner: byte-identical ``dest_block_hashes`` + the duplicating multiset oracle;
left: the null-aware multiset); ``"tasks"`` AND ``"auto"`` → the RELAY Counter
(``_dask_map_write`` + ``_dask_gather_join == parts``, zero peers, zero picks),
``witness.head_node_routed is True``, no ``.transport``, rows/hashes == the local engine at a
NON-default ``how`` (the m45-predicate: auto resolves to tasks on every parsl instance).

Discriminates: a facade whose transport-join branch silently reruns the relay (or local) engine —
the spy shape and the missing ``.transport`` land it; a join dispatch that drops or transposes the
positional/keyword hand-off of ``how``/``broadcast`` into either engine (row/hash parity at
``how="left"`` diverges); an ``auto`` that resolves to ``"transport"`` on parsl (peer submits
appear where the relay Counter is pinned); a transport arm that routes join fragments through the
driver (peer ``bytes_served``/``pull_requests_served`` stay 0).

pyarrow: the ``how="left"`` arms wire MASKED numpy blocks over Arrow (the m46 measured fact) —
importorskip'd loudly so CI can never silently skip them."""

from __future__ import annotations

import sys
from collections import Counter

import pytest
from parsl_transport_harness import (
    W0,
    W1,
    SpyP2PBackend,
    driver_row,
    dup_join_all,
    facade_join,
    join_sides,
    make_parsl_backend,
    nullable_join_multiset,
    numpy_p2p_adapter,
    observed_join_all,
    p2p_htex_pool,
    peer_rows,
    run_bounded,
)

from graphed_executors.local.shuffle import run_join

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)
pytest.importorskip(
    "pyarrow",
    reason="pyarrow is a measured runtime need of the non-inner numpy arms (masked blocks wire "
    "over Arrow) — pinned into the test-parsl CI job by the frozen m46 packaging pins",
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

PARTS = 8
K = 2


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=K) as ex:
        yield ex


def test_facade_transport_join_dispatches_the_p2p_engine_with_parity(htex) -> None:  # type: ignore[no-untyped-def]
    """Kills: a facade transport-join branch that reruns the relay/local engine driver-side (spy
    shape + missing ``.transport``); a driver-relayed 'peer' join (block-plane counters stay 0);
    a dispatch that corrupts the argument hand-off (hash/oracle parity)."""
    adapter = numpy_p2p_adapter()
    left, right = join_sides(adapter)
    local = run_join(adapter.backend, left, right, PARTS, how="inner", broadcast=False)
    spy = SpyP2PBackend(make_parsl_backend(htex))

    res = run_bounded(
        lambda: facade_join(
            adapter.backend,
            left,
            right,
            PARTS,
            how="inner",
            broadcast=False,
            pbackend=spy,
            shuffle_method="transport",
        ),
        timeout_s=240.0,
    )

    names: Counter[str] = spy.fn_names()
    assert names["_parsl_peer_main"] == K, (
        f"the facade's transport join must seat exactly k={K} peers: {dict(names)}"
    )
    for forbidden in ("_dask_map_write", "_dask_gather", "_dask_gather_join", "_dask_pick"):
        assert names[forbidden] == 0, (
            f"the transport-join branch smuggled a relay/tasks submit ({forbidden}): {dict(names)}"
        )

    assert res.dest_block_hashes == local.dest_block_hashes, (
        "facade transport join bytes diverged from the local engine — the run_join dispatch "
        "corrupted the argument hand-off into transport_run_join"
    )
    assert observed_join_all(adapter, res.value) == dup_join_all(adapter, left, right, PARTS), (
        "facade transport join rows diverged from the independent duplicating oracle (m40 §3.3)"
    )

    assert hasattr(res, "transport"), "the transport-arm result must carry .transport (m45 observability)"
    assert res.transport.epoch_restarts == 0
    assert len(list(res.transport.epoch_nonces)) == 1
    rows = peer_rows(res)
    assert set(rows) == {W0, W1}, f"expected exactly the k={K} pinned peer rows, got {sorted(rows)}"
    assert sum(int(c.get("pull_requests_served", 0)) for c in rows.values()) >= 1, (
        "no /pull request served on any peer — join fragments did not move over the block plane "
        "under the facade arm (a driver-relayed join)"
    )
    assert sum(int(c.get("bytes_served", 0)) for c in rows.values()) > 0, (
        "zero cross-peer bytes served — the facade's transport join moved no data peer-to-peer"
    )
    assert int(driver_row(res).get("recv_class_hello", 0)) == K, (
        f"the rendezvous barrier must run behind the facade arm (driver hellos != {K}): "
        f"{dict(driver_row(res))}"
    )


def test_facade_transport_join_left_rows_are_exact(htex) -> None:  # type: ignore[no-untyped-def]
    """Kills: a run_join dispatch that drops/transposes ``how`` (or ``broadcast``) on the
    transport branch — the many-positional-argument hazard: left-join parity vs the local engine
    diverges the null-aware multiset and the hashes; a sentinel-null carrier lands here too."""
    adapter = numpy_p2p_adapter()
    left, right = join_sides(adapter)
    local = run_join(adapter.backend, left, right, PARTS, how="left", broadcast=False)
    spy = SpyP2PBackend(make_parsl_backend(htex))

    res = run_bounded(
        lambda: facade_join(
            adapter.backend,
            left,
            right,
            PARTS,
            how="left",
            broadcast=False,
            pbackend=spy,
            shuffle_method="transport",
        ),
        timeout_s=240.0,
    )

    assert spy.fn_names()["_parsl_peer_main"] == K, "the left arm must still ride the p2p engine"
    assert hasattr(res, "transport"), "the left transport arm must carry .transport"
    assert nullable_join_multiset(res.value) == nullable_join_multiset(local.value), (
        "how='left' through the facade transport arm diverged from the local engine — the "
        "dispatch mangled the how/broadcast hand-off"
    )
    assert res.dest_block_hashes == local.dest_block_hashes, (
        "how='left': facade transport join hashes diverged from the local engine"
    )


@pytest.mark.parametrize("method", ["tasks", "auto"])
def test_facade_join_tasks_and_auto_resolve_to_the_relay_shape(htex, method: str) -> None:  # type: ignore[no-untyped-def]
    """Kills: an ``auto`` that resolves join to the p2p engine on parsl (the m45 predicate:
    auto→tasks on EVERY parsl instance — no vector has pin AND peer); a tasks-arm join that
    hardcodes the m43 pick tier; a relay result missing the head-node witness or leaking
    ``.transport``; a tasks dispatch that drops ``how`` (left-join parity vs local)."""
    adapter = numpy_p2p_adapter()
    left, right = join_sides(adapter)
    local = run_join(adapter.backend, left, right, PARTS, how="left", broadcast=False)
    spy = SpyP2PBackend(make_parsl_backend(htex))

    res = run_bounded(
        lambda: facade_join(
            adapter.backend,
            left,
            right,
            PARTS,
            how="left",
            broadcast=False,
            pbackend=spy,
            shuffle_method=method,
        ),
        timeout_s=240.0,
    )

    names: Counter[str] = spy.fn_names()
    assert names["_dask_map_write"] > 0 and names["_dask_gather_join"] == PARTS, (
        f"{method} on the all-False HTEX floor must run the RELAY join shape: {dict(names)}"
    )
    assert names["_parsl_peer_main"] == 0 and names["_dask_pick"] == 0, (
        f"{method}: neither the p2p engine nor the m43 pick tier may engage on a join: {dict(names)}"
    )
    assert res.witness.head_node_routed is True, "a tasks-resolution join must carry the head-node witness"
    assert not hasattr(res, "transport"), "a tasks-resolution join result must not carry .transport"
    assert nullable_join_multiset(res.value) == nullable_join_multiset(local.value), (
        f"{method}: relay join rows diverged from the local engine at how='left'"
    )
    assert res.dest_block_hashes == local.dest_block_hashes, (
        f"{method}: relay join hashes diverged from the local engine"
    )
