"""m44 F6 carried — broadcast-vs-shuffle is a PLAN property (``parts``-keyed
``broadcast_join_choice``), never a live-cluster recompute; the broadcast path never shuffles the
large side (plan §1.4 join; blueprint §3 `test_transport_join_plan_choice`).

The discrimination gap is built into the scenario (the m43 pattern): byte-comparable sides at
``parts=8`` make the pinned rule say SHUFFLE while the same rule keyed on a live
``n_workers()==1`` says BROADCAST — a live-recompute engine takes the broadcast path on the
1-worker cluster and fails the structural gates. The path taken is witnessed STRUCTURALLY through
the SpyDaskBackend fn-name counts (the pinned module-level task fns), never inferred from values:
shuffle ⇒ ``_transport_map_task > 0`` and zero ``_transport_broadcast_join_part``; broadcast ⇒
zero map tasks for EITHER side, one ``_transport_broadcast_join_part`` per probe block, ≥1
``backend.broadcast`` handle, ``broadcast_puts`` == the distinct workers that resolved the handle
(k), and ``large_side_blocks == 0``. Forced ``broadcast=True/False`` are honoured against the
model."""

from __future__ import annotations

import pytest
from graphed.shuffle import broadcast_join_choice
from transport_harness import (
    SpyDaskBackend,
    TransportAdapter,
    build_dask_backend,
    dup_join_all,
    install_transport_plugin,
    join_equal_sides,
    join_small_build_sides,
    observed_join_all,
    side_bytes,
    transport_adapters,
    transport_cluster,
    transport_join,
)

pytest.importorskip("distributed")

ADAPTERS = transport_adapters()
PARTS = 8


@pytest.fixture(scope="module")
def client1w():
    with transport_cluster(1, processes=False) as c:
        yield c


@pytest.fixture(scope="module")
def client2w():
    with transport_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> TransportAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _spy(client) -> SpyDaskBackend:  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    return SpyDaskBackend(build_dask_backend(client))


def test_auto_choice_is_parts_keyed_and_worker_count_invariant(adapter: TransportAdapter, client1w, client2w) -> None:  # type: ignore[no-untyped-def]
    left, right = join_equal_sides(adapter)
    bb, pb = side_bytes(adapter, left), side_bytes(adapter, right)
    # scenario sanity: the DISCRIMINATION GAP is real for these measured bytes
    assert broadcast_join_choice(bb, pb, PARTS) is False, "parts-keyed rule must say SHUFFLE here"
    assert broadcast_join_choice(bb, pb, 1) is True, "worker-keyed rule on 1 worker must say BROADCAST"

    hashes = []
    for client in (client1w, client2w):
        spy = _spy(client)
        r = transport_join(adapter.backend, left, right, PARTS, dbackend=spy)  # broadcast=None: auto
        assert r.witness.broadcast_chosen is False, (
            "auto choice flipped to broadcast — the F6 live-n_workers() recompute bug"
        )
        names = spy.submitted_fn_names()
        assert names["_transport_map_task"] > 0, "the shuffle path must submit stage-1 map tasks"
        assert names["_transport_broadcast_join_part"] == 0
        hashes.append(r.dest_block_hashes)
    assert hashes[0] == hashes[1], "1-worker vs 2-worker runs must be bit-for-bit identical"


def test_broadcast_path_never_shuffles_the_large_side(adapter: TransportAdapter, client2w) -> None:  # type: ignore[no-untyped-def]
    left, right = join_small_build_sides(adapter)
    assert broadcast_join_choice(side_bytes(adapter, left), side_bytes(adapter, right), PARTS) is True, (
        "scenario guard: the pinned rule must prefer BROADCAST for these sides"
    )
    spy = _spy(client2w)
    r = transport_join(adapter.backend, left, right, PARTS, dbackend=spy)  # auto -> broadcast
    assert r.witness.broadcast_chosen is True

    names = spy.submitted_fn_names()
    assert names["_transport_map_task"] == 0, (
        f"broadcast join must submit ZERO stage-1 map tasks (neither side shuffles): {dict(names)}"
    )
    assert names["_transport_broadcast_join_part"] == len(right), (
        "exactly one pinned probe-join task per probe block"
    )
    assert len(spy.broadcast_tokens) >= 1, "the build side must ship once via backend.broadcast"
    assert r.witness.broadcast_puts == 2, (
        f"broadcast_puts={r.witness.broadcast_puts}: must count the DISTINCT workers that resolved "
        "the build handle (k=2 here, with the P=3 probe tasks pinned across both)"
    )
    assert r.witness.large_side_blocks == 0, "the large side leaked into stage-1 shuffle blocks"
    assert observed_join_all(adapter, r.value) == dup_join_all(adapter, left, right, PARTS)


def test_forced_choice_is_honoured_both_ways(adapter: TransportAdapter, client2w) -> None:  # type: ignore[no-untyped-def]
    # forced shuffle where the model prefers broadcast
    left, right = join_small_build_sides(adapter)
    spy = _spy(client2w)
    r = transport_join(adapter.backend, left, right, PARTS, dbackend=spy, broadcast=False)
    assert r.witness.broadcast_chosen is False
    assert spy.submitted_fn_names()["_transport_map_task"] > 0, "forced broadcast=False was re-decided"
    assert observed_join_all(adapter, r.value) == dup_join_all(adapter, left, right, PARTS)

    # forced broadcast where the model prefers shuffle
    left2, right2 = join_equal_sides(adapter)
    spy2 = _spy(client2w)
    r2 = transport_join(adapter.backend, left2, right2, PARTS, dbackend=spy2, broadcast=True)
    assert r2.witness.broadcast_chosen is True
    assert spy2.submitted_fn_names()["_transport_map_task"] == 0, "forced broadcast=True was re-decided"
    assert observed_join_all(adapter, r2.value) == dup_join_all(adapter, left2, right2, PARTS)
