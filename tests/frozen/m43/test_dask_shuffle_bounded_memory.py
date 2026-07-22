"""m43 B5 analog — bounded join working set under OUTPUT-side duplication on dask (plan §1.3.4
point 3 witness counters; blueprint §3 `test_dask_shuffle_bounded_memory`; mirrors the frozen m40
`test_join_bounded_memory` semantics of the reused `_join_with_budget` kernel).

A single hot key routes every row to ONE dest; the inner join duplicates to ``n_left*n_right``
output rows, so the joined dest is ~8x larger than ``mem_budget_bytes``. The gather-join task must
radix-sub-partition the probe, SPILL each chunk, and STREAM — witnessed by the reused kernel
counters folded into the DaskShuffleWitness: ``peak_join_bytes <= budget`` (working set),
``join_spilled_partitions > 0`` (spill engaged, non-vacuous), ``join_output_rows == n_left*n_right``
(real duplication, no dedup), and the value equals the duplicating oracle. Counters, never
clocks/RSS (R0.10a — the m40 rationale carries over verbatim)."""

from __future__ import annotations

import pytest
from dask_shuffle_backends import (
    ShuffleJoinAdapter,
    build_dask_backend,
    dask_join,
    dup_join_all,
    install_worker_plugin,
    join_hot_key_sides,
    make_submit_runner,
    observed_join_all,
    shuffle_adapters,
    shuffle_cluster,
)

pytest.importorskip("distributed")

ADAPTERS = shuffle_adapters()

_N_LEFT = 200
_N_RIGHT = 200  # -> 40_000 duplicated output rows in a single dest
_PARTS = 4


@pytest.fixture(scope="module")
def client():
    with shuffle_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> ShuffleJoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_join_spills_and_streams_within_a_budget_smaller_than_the_output(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    be = adapter.backend
    left, right = join_hot_key_sides(adapter, _N_LEFT, _N_RIGHT)
    oracle = dup_join_all(adapter, left, right, _PARTS)
    assert sum(oracle.values()) == _N_LEFT * _N_RIGHT, "scenario must duplicate to n_left*n_right rows"

    # bound the duplicated output from the input row widths (the m40 sizing idiom), then squeeze
    row_bytes = be.estimated_bytes(left[0]) // _N_LEFT + be.estimated_bytes(right[0]) // _N_RIGHT
    output_bytes = _N_LEFT * _N_RIGHT * row_bytes
    budget = output_bytes // 8
    assert budget < output_bytes, "the budget must be smaller than the joined dest it covers"

    install_worker_plugin(client)
    runner = make_submit_runner(build_dask_backend(client))
    res = dask_join(
        adapter.backend, left, right, _PARTS, runner=runner, broadcast=False, mem_budget_bytes=budget
    )

    w = res.witness
    assert w.peak_join_bytes <= budget, (
        "the gather-join working set exceeded the budget — a full-RAM concat of the duplicated "
        "dest instead of the spill+stream kernel"
    )
    assert w.join_spilled_partitions > 0, "spill never engaged — the budget was never exercised"
    assert w.join_output_rows == _N_LEFT * _N_RIGHT, "relational duplication must emit n_left*n_right rows"
    assert observed_join_all(adapter, res.value) == oracle, (
        "spilled/streamed join content diverged from the duplicating oracle"
    )
