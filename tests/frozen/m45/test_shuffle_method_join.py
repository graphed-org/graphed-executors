"""m45 join pins — facade joins are byte-identical to the direct engine calls on both methods
(inner AND outer), and join's ``mem_budget_bytes`` is a COMMON knob: explicit ``"tasks"`` +
``mem_budget_bytes`` SUCCEEDS with the m43 spill counters engaging (m45-facade-plan §1.2 r2
correction + §2 items 7/8 — the r1 draft would have raised a spurious ValueError on exactly this
frozen-m43-exercised usage; this module makes that bug unshippable).

Budget sizing mirrors frozen m43 ``test_dask_shuffle_bounded_memory`` (hot key, n*n duplicated
output, budget = output_bytes // 8)."""

from __future__ import annotations

import pytest
from shuffle_method_harness import (
    BACKEND,
    SpyDaskBackend,
    build_dask_backend,
    facade_cluster,
    facade_join,
    hot_key_sides,
    install_transport_plugin,
    join_sides,
    make_runner,
    run_bounded,
)

from graphed_executors.dask_backend.shuffle import dask_run_join
from graphed_executors.dask_backend.transport_shuffle import transport_run_join

pytest.importorskip("distributed")

PARTS = 8


@pytest.fixture(scope="module")
def client():
    with facade_cluster(2) as c:
        yield c


@pytest.mark.parametrize("how", ["inner", "outer"])
def test_join_parity_with_both_engines(client, how: str) -> None:  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    left, right = join_sides()
    dbackend = build_dask_backend(client)

    t_oracle = transport_run_join(
        BACKEND, left, right, PARTS, how=how, dbackend=dbackend, broadcast=False
    )
    res_t = run_bounded(
        lambda: facade_join(
            BACKEND, left, right, PARTS, how=how, dbackend=dbackend,
            shuffle_method="transport", broadcast=False,
        )
    )["result"]
    assert res_t.dest_block_hashes == t_oracle.dest_block_hashes, (
        f"how={how}: facade transport join diverged from the direct engine"
    )
    assert hasattr(res_t, "transport")

    k_oracle = dask_run_join(
        BACKEND, left, right, PARTS, how=how, runner=make_runner(dbackend), broadcast=False
    )
    res_k = run_bounded(
        lambda: facade_join(
            BACKEND, left, right, PARTS, how=how, dbackend=dbackend,
            shuffle_method="tasks", broadcast=False,
        )
    )["result"]
    assert res_k.dest_block_hashes == k_oracle.dest_block_hashes, (
        f"how={how}: facade tasks join diverged from the direct engine"
    )
    assert hasattr(res_k, "partitions")


def test_tasks_join_accepts_mem_budget_and_the_spill_engages(client) -> None:  # type: ignore[no-untyped-def]
    n = 200  # -> 40_000 duplicated output rows in one hot dest (the m43 sizing idiom)
    left, right = hot_key_sides(n)
    row_bytes = BACKEND.estimated_bytes(left[0]) // n + BACKEND.estimated_bytes(right[0]) // n
    budget = (n * n * row_bytes) // 8
    dbackend = build_dask_backend(client)
    oracle = dask_run_join(
        BACKEND, left, right, 4, runner=make_runner(dbackend), broadcast=False, mem_budget_bytes=budget
    )
    assert oracle.witness.join_spilled_partitions > 0, (
        "scenario guard misfired: this budget must engage the m43 spill on the direct engine"
    )

    spy = SpyDaskBackend(dbackend)
    outcome = run_bounded(
        lambda: facade_join(
            BACKEND, left, right, 4, dbackend=spy, shuffle_method="tasks",
            broadcast=False, mem_budget_bytes=budget,
        ),
        timeout_s=300.0,
    )
    assert "error" not in outcome, (
        f"tasks + mem_budget_bytes must SUCCEED (mem_budget_bytes is a COMMON join knob — the "
        f"r1-draft spurious ValueError, frozen unshippable here): {outcome.get('error')!r}"
    )
    res = outcome["result"]
    assert spy.fn_names()["_dask_map_write"] > 0, "the run must have taken the m43 path"
    w = res.witness
    assert w.join_spilled_partitions > 0, "the forwarded budget never engaged the m43 spill"
    assert w.peak_join_bytes <= budget, "the join working set exceeded the forwarded budget"
    assert w.join_output_rows == n * n
    assert res.dest_block_hashes == oracle.dest_block_hashes, "budgeted facade join diverged"
