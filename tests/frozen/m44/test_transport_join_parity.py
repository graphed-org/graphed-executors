"""m44 behavioral golden (m40 carried) — ``transport_run_join`` reproduces the local engine's
relational semantics on both backends (plan §1.4 join, DoD "join rows"; blueprint §3
`test_transport_join_parity`).

Witnesses: inner rows == the duplicating multiset oracle (routes via the golden-pinned
``partition``, never the join kernel) == the local ``run_join``; inner ``dest_block_hashes``
byte-identical to the local engine; left/outer rows == a null-aware ``pandas.merge`` oracle with
nulls OPTION-TYPED (the m40 ``-1``-sentinel trap re-armed: a sentinel impl fails the multiset
against real ``None`` rows); under ``mem_budget_bytes`` the reused ``_join_with_budget`` spill
counters engage and match the local engine exactly (``join_spilled_partitions ==
join_chunks_read``, F5), with the local run as an in-test scenario guard that the budget really
engages."""

from __future__ import annotations

import pytest
from transport_harness import (
    TransportAdapter,
    build_dask_backend,
    dup_join_all,
    install_transport_plugin,
    join_general_sides,
    join_hot_key_sides,
    nullable_join_multiset,
    observed_join_all,
    pandas_join_oracle,
    side_columns,
    transport_adapters,
    transport_cluster,
    transport_join,
)

from graphed_executors.local.shuffle import run_join

pytest.importorskip("distributed")
pytest.importorskip("pandas")

ADAPTERS = transport_adapters()
PARTS = 8


@pytest.fixture(scope="module")
def client():
    with transport_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> TransportAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _dbackend(client):  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    return build_dask_backend(client)


@pytest.mark.parametrize("how", ["inner", "left", "outer"])
def test_join_rows_match_local_engine_and_pandas_oracle(adapter: TransportAdapter, client, how: str) -> None:  # type: ignore[no-untyped-def]
    left, right = join_general_sides(adapter)
    local = run_join(adapter.backend, left, right, PARTS, how=how, broadcast=False)
    r = transport_join(
        adapter.backend, left, right, PARTS, how=how, dbackend=_dbackend(client), broadcast=False
    )

    assert nullable_join_multiset(r.value) == nullable_join_multiset(local.value), (
        f"how={how}: transport join rows diverged from the local engine"
    )
    lk, lv = side_columns(left, adapter, "lval")
    rk, rv = side_columns(right, adapter, "rval")
    assert nullable_join_multiset(r.value) == pandas_join_oracle(lk, lv, rk, rv, how), (
        f"how={how}: rows diverged from the independent pandas.merge oracle — unmatched rows must "
        "carry OPTION-TYPED nulls (None), never a sentinel value"
    )
    if how == "inner":
        assert r.dest_block_hashes == local.dest_block_hashes, "inner join bytes diverged from local"
        assert observed_join_all(adapter, r.value) == dup_join_all(adapter, left, right, PARTS)


def test_join_budget_spill_counters_engage_and_match_local(adapter: TransportAdapter, client) -> None:  # type: ignore[no-untyped-def]
    n_left, n_right = 200, 200  # -> 40_000 duplicated output rows in a single hot dest
    left, right = join_hot_key_sides(adapter, n_left, n_right)
    be: object = adapter.backend
    # bound the duplicated output from the input row widths (the m40/m43 sizing idiom), then squeeze
    row_bytes = be.estimated_bytes(left[0]) // n_left + be.estimated_bytes(right[0]) // n_right  # type: ignore[attr-defined]
    budget = (n_left * n_right * row_bytes) // 8

    local = run_join(adapter.backend, left, right, PARTS, broadcast=False, mem_budget_bytes=budget)
    assert local.witness.join_spilled_partitions > 0, (
        "scenario guard misfired: the budget must engage the LOCAL join spill on these inputs"
    )

    r = transport_join(
        adapter.backend, left, right, PARTS, dbackend=_dbackend(client),
        broadcast=False, mem_budget_bytes=budget,
    )
    w = r.witness
    assert w.join_spilled_partitions > 0, "the transport gather-join never engaged the spill"
    assert w.join_spilled_partitions == w.join_chunks_read, (
        "F5: every spilled radix partition must be read back exactly once"
    )
    assert w.join_spilled_partitions == local.witness.join_spilled_partitions, (
        "spill counter diverged from the local engine on identical inputs — _join_with_budget was "
        "not genuinely reused"
    )
    assert w.peak_join_bytes <= budget, "the join working set exceeded its budget (B5)"
    assert w.join_output_rows == n_left * n_right
    assert observed_join_all(adapter, r.value) == dup_join_all(adapter, left, right, PARTS)
