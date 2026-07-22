"""m45 dispatch pins — ``shuffle_method`` really selects the engine, witnessed STRUCTURALLY at
the submit seam by the two engines' pinned fn-name shapes, with results byte-identical to the
direct engine calls and the union result shape revealing the resolution
(m45-facade-plan §1/§1.3, §2 items 1/2/3/9).

Discriminates: a facade hardcoding either engine (the other method's fn-name shape never
appears), one that re-implements instead of forwarding (hash divergence), one that drops common
knobs (the salt arm), and one that normalizes the result (the ``.transport``/``.partitions``
engine markers disappear)."""

from __future__ import annotations

import pytest
from shuffle_method_harness import (
    BACKEND,
    PinlessView,
    SpyDaskBackend,
    build_dask_backend,
    facade_cluster,
    facade_repartition,
    install_transport_plugin,
    make_runner,
    repartition_blocks,
    run_bounded,
    total_result_rows,
)

from graphed_executors.dask_backend.shuffle import dask_run_repartition
from graphed_executors.dask_backend.transport_shuffle import transport_run_repartition

pytest.importorskip("distributed")

PARTS = 8


@pytest.fixture(scope="module")
def client():
    with facade_cluster(2) as c:
        yield c


def _common_triple(res) -> None:  # type: ignore[no-untyped-def]
    for attr in ("dest_block_hashes", "value", "witness"):
        assert hasattr(res, attr), f"union contract broken: result lacks .{attr}"


def test_auto_dispatches_to_the_transport_engine(client) -> None:  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    src = repartition_blocks()
    dbackend = build_dask_backend(client)
    oracle = transport_run_repartition(BACKEND, src, PARTS, dbackend=dbackend)

    spy = SpyDaskBackend(dbackend)
    outcome = run_bounded(lambda: facade_repartition(BACKEND, src, PARTS, dbackend=spy))
    assert "error" not in outcome, f"auto repartition raised: {outcome.get('error')!r}"
    res = outcome["result"]

    names = spy.fn_names()
    assert names["_transport_map_task"] > 0 and names["_transport_gather_task"] > 0, (
        f"auto on a full-capability DaskBackend must run the m44 transport engine: {dict(names)}"
    )
    assert names["_dask_map_write"] == 0 and names["_dask_pick"] == 0, (
        f"the m43 shape leaked into an auto->transport run: {dict(names)}"
    )
    assert res.dest_block_hashes == oracle.dest_block_hashes, "facade diverged from the direct engine"
    assert total_result_rows(res.value) == sum(len(b) for b in src)
    _common_triple(res)
    assert hasattr(res, "transport") and not hasattr(res, "partitions"), (
        "the transport result must carry .transport (and no .partitions) — the resolution-"
        "observability pin"
    )

    # determinism: the same auto call is byte-identical on a second run
    res2 = run_bounded(lambda: facade_repartition(BACKEND, src, PARTS, dbackend=spy))["result"]
    assert res2.dest_block_hashes == res.dest_block_hashes


def test_explicit_tasks_dispatches_to_the_m43_engine(client) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks()
    dbackend = build_dask_backend(client)
    oracle = dask_run_repartition(BACKEND, src, PARTS, runner=make_runner(dbackend))

    spy = SpyDaskBackend(dbackend)
    outcome = run_bounded(
        lambda: facade_repartition(BACKEND, src, PARTS, dbackend=spy, shuffle_method="tasks")
    )
    assert "error" not in outcome, f"tasks repartition raised: {outcome.get('error')!r}"
    res = outcome["result"]

    names = spy.fn_names()
    for fn in ("_dask_map_write", "_dask_pick", "_dask_gather"):
        assert names[fn] > 0, f"explicit tasks must run the m43 future graph ({fn} missing): {dict(names)}"
    assert names["_transport_map_task"] == 0, f"transport shape leaked into a tasks run: {dict(names)}"
    assert res.dest_block_hashes == oracle.dest_block_hashes
    _common_triple(res)
    assert hasattr(res, "partitions") and not hasattr(res, "transport"), (
        "the tasks result must carry .partitions (and no .transport)"
    )

    # common-knob forwarding: salt reroutes identically through facade and direct engine
    salted = run_bounded(
        lambda: facade_repartition(BACKEND, src, PARTS, dbackend=spy, shuffle_method="tasks", salt=3)
    )["result"]
    salted_oracle = dask_run_repartition(BACKEND, src, PARTS, runner=make_runner(dbackend), salt=3)
    assert salted.dest_block_hashes == salted_oracle.dest_block_hashes
    assert salted.dest_block_hashes != oracle.dest_block_hashes, (
        "salt=3 must reroute — a facade that drops the salt knob returns the salt=0 bytes"
    )


def test_auto_on_a_pinless_backend_degrades_to_tasks_and_runs(client) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks()
    dbackend = build_dask_backend(client)
    oracle = dask_run_repartition(BACKEND, src, PARTS, runner=make_runner(dbackend))

    spy = SpyDaskBackend(PinlessView(dbackend))
    outcome = run_bounded(lambda: facade_repartition(BACKEND, src, PARTS, dbackend=spy))
    assert "error" not in outcome, (
        f"auto on a pin-less backend must DEGRADE and run, not fail: {outcome.get('error')!r}"
    )
    res = outcome["result"]

    names = spy.fn_names()
    assert names["_dask_map_write"] > 0, f"the degraded path must run the m43 engine: {dict(names)}"
    assert names["_transport_map_task"] == 0, (
        f"auto picked transport on a backend without pin_to_worker: {dict(names)}"
    )
    assert res.dest_block_hashes == oracle.dest_block_hashes
    assert hasattr(res, "partitions") and not hasattr(res, "transport")
