"""m42 determinism: the fixed `plan_tree` future graph is bit-for-bit vs `SequentialRunner`,
across two runs on one client, and invariant to worker count (plan §1.2.1, §1.2.3; blueprint §3
`test_dask_plan_tree_determinism`).

The payload is an associative but NON-commutative concat with staggered leaf delays (later keys
finish first): any arrival-ordered fold — the coffea-style batched reduction whose grouping varies
with worker count, exactly what `plan_tree` exists to avoid — concats out of key order and fails
loudly here. The re-execution witness (disjoint per-invocation marks across two runs) discriminates
content-only task keys without a run nonce: with reused keys the second run would return the first
run's cached partials instead of re-executing (plan §1.2.3)."""

from __future__ import annotations

import pickle

import pytest
from graphed.core.execution import SequentialRunner, StopReason
from submit_backends import (
    cluster_client,
    concat_plan,
    expected_concat,
    make_dask_runner,
    prov_plan,
)

pytest.importorskip("distributed")


@pytest.fixture(scope="module")
def client():
    # tier B (plan §3): real processes, real pickling — the tier determinism must hold on
    with cluster_client(2, processes=True) as c:
        yield c


def test_two_runs_are_bit_identical_and_equal_sequential(client) -> None:
    plan = concat_plan(16, "det", staggered=True)
    expected = SequentialRunner().run(plan)
    with make_dask_runner(client) as runner:
        r1 = runner.run(plan)
        r2 = runner.run(plan)  # SAME runner: the per-run() nonce must renew, not collide
    assert pickle.dumps(r1.value) == pickle.dumps(r2.value) == pickle.dumps(expected.value)
    assert r1.value == expected_concat(16, "det")
    assert r1.n_partitions == r2.n_partitions == 16
    assert r1.n_combines == r2.n_combines == 15  # the full plan_tree, every combine accounted for
    assert r1.stopped is r2.stopped is StopReason.EXHAUSTED


def test_result_is_invariant_to_worker_count(client) -> None:
    """The same plan on 1-worker and 3-worker clusters equals the 2-worker and sequential value —
    the topology-invariance kill for arrival/batch-grouped reductions (grouping fixed by leaf
    index, never by worker count or completion order)."""
    plan = concat_plan(16, "dettopo", staggered=True)
    expected = expected_concat(16, "dettopo")
    with make_dask_runner(client) as runner:
        assert runner.run(plan).value == expected
    for n_workers in (1, 3):
        with cluster_client(n_workers, processes=True) as other, make_dask_runner(other) as runner:
            assert runner.run(plan).value == expected, f"diverged on n_workers={n_workers}"


def test_second_run_reexecutes_instead_of_returning_cached_futures(client) -> None:
    """Two runs of a mark-carrying plan: payloads identical, but the per-invocation nonce sets are
    DISJOINT — every leaf and combine genuinely re-executed. A content-only key scheme (no run
    nonce) hands run 2 the cached run-1 partials: identical marks, caught here."""
    plan = prov_plan(16, "detre")
    with make_dask_runner(client) as runner:
        p1 = runner.run(plan).value
        p2 = runner.run(plan).value
    assert p1.payload == p2.payload == expected_concat(16, "detre")
    assert len(p1.marks) == len(p2.marks) == 16 + 15  # 16 leaves + 15 combines each run
    assert not (p1.marks & p2.marks), "second run returned cached partials (missing run nonce)"
