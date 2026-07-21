"""m43 F6 — broadcast-vs-shuffle is a PLAN property, never a live-cluster recompute (plan §1.3.4
"Broadcast-vs-shuffle frozen as a plan property"; blueprint §3 `test_dask_join_plan_choice`).

The discrimination gap is built into the scenario: with byte-comparable sides and ``parts=8`` the
pinned rule (`graphed.shuffle.broadcast_join_choice`, keyed on ``parts``) says SHUFFLE, while the
same rule keyed on a live ``n_workers()==1`` says BROADCAST — so an implementation that recomputes
the choice from the worker pool takes the broadcast path on a 1-worker cluster and fails the
future-class assertions. The 1-vs-3-worker runs must also agree bit-for-bit (topology invariance).

The path taken is witnessed STRUCTURALLY through the SpySubmitBackend's fn-name counts (the §2 m43
Implementation-Target task fns), never inferred from the value: shuffle ⇒ `_dask_map_write` > 0
and zero `_dask_broadcast_join_part`; broadcast ⇒ zero stage-1/pick/gather futures for either side
and exactly one `_dask_broadcast_join_part` per probe block (the large side is NEVER shuffled).

Discriminates: the F6 live-recompute bug, an executor that overrides a plan-recorded forced
choice, and a broadcast that secretly routes the large side through map-write."""

from __future__ import annotations

import pytest
from dask_shuffle_backends import (
    ShuffleJoinAdapter,
    SpySubmitBackend,
    build_dask_backend,
    dask_join,
    dup_join_all,
    install_worker_plugin,
    join_equal_sides,
    join_small_build_sides,
    make_submit_runner,
    observed_join_all,
    shuffle_adapters,
    shuffle_cluster,
    side_bytes,
)
from graphed.shuffle import broadcast_join_choice

pytest.importorskip("distributed")

ADAPTERS = shuffle_adapters()
PARTS = 8


@pytest.fixture(scope="module")
def client1():
    with shuffle_cluster(1, processes=False) as c:
        yield c


@pytest.fixture(scope="module")
def client3():
    with shuffle_cluster(3, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> ShuffleJoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _run_auto(adapter: ShuffleJoinAdapter, client) -> tuple[SpySubmitBackend, object]:  # type: ignore[no-untyped-def]
    left, right = join_equal_sides(adapter)
    install_worker_plugin(client)
    spy = SpySubmitBackend(build_dask_backend(client))
    res = dask_join(adapter.backend, left, right, PARTS, runner=make_submit_runner(spy))
    return spy, res


def test_auto_choice_is_parts_keyed_and_worker_count_invariant(adapter: ShuffleJoinAdapter, client1, client3) -> None:  # type: ignore[no-untyped-def]
    left, right = join_equal_sides(adapter)
    bb, pb = side_bytes(adapter, left), side_bytes(adapter, right)
    # scenario sanity: the DISCRIMINATION GAP is real for these measured bytes
    assert broadcast_join_choice(bb, pb, PARTS) is False, "parts-keyed rule must say SHUFFLE here"
    assert broadcast_join_choice(bb, pb, 1) is True, "worker-keyed rule on 1 worker must say BROADCAST"

    spy1, r1 = _run_auto(adapter, client1)
    names1 = spy1.submitted_fn_names()
    assert names1["_dask_map_write"] > 0, (
        "auto choice on a 1-WORKER cluster must still SHUFFLE (parts-keyed rule) — a broadcast "
        "here is the F6 live-n_workers() recompute bug"
    )
    assert names1["_dask_broadcast_join_part"] == 0
    assert r1.witness.broadcast_chosen is False

    spy3, r3 = _run_auto(adapter, client3)
    assert r3.witness.broadcast_chosen is False
    assert spy3.submitted_fn_names()["_dask_broadcast_join_part"] == 0
    assert r1.dest_block_hashes == r3.dest_block_hashes, (
        "the same logical join produced different content-addressed blocks under 1 vs 3 workers"
    )
    assert r1.dest_block_hashes, "no joined blocks produced — scenario degenerate"


def test_forced_broadcast_is_honoured_and_never_shuffles_the_large_side(adapter: ShuffleJoinAdapter, client3) -> None:  # type: ignore[no-untyped-def]
    left, right = join_equal_sides(adapter)
    bb, pb = side_bytes(adapter, left), side_bytes(adapter, right)
    assert broadcast_join_choice(bb, pb, PARTS) is False  # the model would shuffle: the force is real

    install_worker_plugin(client3)
    spy = SpySubmitBackend(build_dask_backend(client3))
    res = dask_join(adapter.backend, left, right, PARTS, runner=make_submit_runner(spy), broadcast=True)

    names = spy.submitted_fn_names()
    assert (
        names["_dask_map_write"] == 0
        and names["_dask_pick"] == 0
        and names["_dask_gather"] == 0
        and names["_dask_gather_join"] == 0
    ), "forced broadcast must retire stage-1 (and its gather-join) entirely — nothing is shuffled"
    assert names["_dask_broadcast_join_part"] == len(right), (
        "one broadcast-join future per probe block (the §1.3.4 graph shape)"
    )
    assert res.witness.broadcast_chosen is True
    assert observed_join_all(adapter, res.value) == dup_join_all(adapter, left, right, PARTS), (
        "broadcast result diverged from the duplicating relational oracle"
    )


def test_forced_shuffle_is_honoured_against_the_cost_model(adapter: ShuffleJoinAdapter, client3) -> None:  # type: ignore[no-untyped-def]
    parts = 4  # at 4 parts (measured: probe = 6x build bytes) the pinned rule says BROADCAST
    left, right = join_small_build_sides(adapter)
    bb, pb = side_bytes(adapter, left), side_bytes(adapter, right)
    assert broadcast_join_choice(bb, pb, parts) is True  # the model would broadcast: the force is real

    install_worker_plugin(client3)
    spy = SpySubmitBackend(build_dask_backend(client3))
    res = dask_join(adapter.backend, left, right, parts, runner=make_submit_runner(spy), broadcast=False)

    names = spy.submitted_fn_names()
    assert names["_dask_broadcast_join_part"] == 0, "forced shuffle must not take the broadcast path"
    assert names["_dask_map_write"] > 0
    assert res.witness.broadcast_chosen is False
    assert observed_join_all(adapter, res.value) == dup_join_all(adapter, left, right, parts)
