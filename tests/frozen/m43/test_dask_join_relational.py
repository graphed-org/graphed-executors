"""m43 relational correctness of `dask_run_join` on BOTH real backends (plan §1.3.4; blueprint §3
`test_dask_join_relational`).

- inner: per-dest multiset equality with the TEST-AUTHORED duplicating oracle (routes via the
  golden-pinned `partition`, never the join kernel) — a grouped/dedup impl emits a different
  multiset; per-DEST equality additionally pins co-partitioned routing.
- left/right/outer: whole-result equality with the independent `pandas.merge` oracle, null-aware —
  the m40 a3 trap re-armed: a `-1`-sentinel `take` that gathers the last row instead of emitting a
  null/option row produces a real (wrong) value, which the null-aware reader exposes as a multiset
  difference; the join key on a miss row must be COALESCED (present side), never null.
- cross-engine: `dest_block_hashes` equal to the LOCAL `run_join` on identical inputs for every
  `how` and for a salted run (the acceptance-criteria headline extended to joins).

Discriminates: dropped one-sided dests (m40 F1), per-block re-emission of unmatched build rows,
sentinel-gather nulls (a3), a salt that bypasses one side (co-partitioning broken), and any
keying/merge-order divergence from the local engine."""

from __future__ import annotations

from collections import Counter

import pytest
from dask_shuffle_backends import (
    ShuffleJoinAdapter,
    build_dask_backend,
    dask_join,
    dup_join_all,
    dup_join_per_dest,
    install_worker_plugin,
    join_general_sides,
    make_submit_runner,
    nullable_join_multiset,
    observed_join_all,
    observed_join_per_dest,
    pandas_join_oracle,
    shuffle_adapters,
    shuffle_cluster,
)

from graphed_executors.local.shuffle import run_join

pytest.importorskip("distributed")
pytest.importorskip("pandas")  # the independent non-inner oracle

ADAPTERS = shuffle_adapters()
PARTS = 4


@pytest.fixture(scope="module")
def client():
    # tier A: fast in-process workers — the serialization path is pinned by the hashes module
    with shuffle_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> ShuffleJoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _sides_rows(adapter: ShuffleJoinAdapter) -> tuple[list[int], list[int], list[int], list[int]]:
    left, right = join_general_sides(adapter)
    lrows = [r for b in left for r in adapter.side_rows(b, "lval")]
    rrows = [r for b in right for r in adapter.side_rows(b, "rval")]
    return [k for k, _ in lrows], [v for _, v in lrows], [k for k, _ in rrows], [v for _, v in rrows]


def test_inner_join_matches_the_duplicating_oracle_per_dest(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    left, right = join_general_sides(adapter)
    oracle = dup_join_per_dest(adapter, left, right, PARTS)
    assert oracle, "oracle degenerate — no matching rows"
    # non-vacuity: some probe row has MULTIPLE build matches — its (key, rval) appears in >1 output
    # row (with distinct lvals) — so a dedup/grouped impl emits fewer rows and fails
    probe_hits = Counter(
        (k, rv) for c in oracle.values() for (k, _lv, rv), n in c.items() for _ in range(n)
    )
    assert any(m > 1 for m in probe_hits.values()), "no probe row with multiple build matches"

    install_worker_plugin(client)
    runner = make_submit_runner(build_dask_backend(client))
    res = dask_join(adapter.backend, left, right, PARTS, runner=runner, broadcast=False)

    assert observed_join_per_dest(adapter, res.value) == oracle, (
        "inner join multiset diverged per dest — duplication, routing, or co-partitioning broken"
    )
    local = run_join(adapter.backend, left, right, PARTS, broadcast=False)
    assert res.dest_block_hashes == local.dest_block_hashes, (
        "dask and local run_join disagree on content-addressed joined blocks (inner)"
    )


@pytest.mark.parametrize("how", ["left", "right", "outer"])
def test_noninner_matches_the_pandas_oracle_null_aware(adapter: ShuffleJoinAdapter, how: str, client) -> None:  # type: ignore[no-untyped-def]
    left, right = join_general_sides(adapter)
    lk, lv, rk, rv = _sides_rows(adapter)
    oracle = pandas_join_oracle(lk, lv, rk, rv, how)
    # non-vacuity: the scenario really produces unmatched (None-bearing) rows for this how
    assert any(None in row for row in oracle), "scenario must produce unmatched rows (else vacuous)"

    install_worker_plugin(client)
    runner = make_submit_runner(build_dask_backend(client))
    res = dask_join(adapter.backend, left, right, PARTS, how=how, runner=runner, broadcast=False)
    observed = nullable_join_multiset(res.value)

    # (a3/F3) every emitted join key is coalesced from the present side — never null
    assert all(key is not None for (key, _, _) in observed), (
        f"how={how}: the join key must be COALESCED, never null"
    )
    assert observed == oracle, (
        f"how={how}: missing={dict(oracle - observed)} extra={dict(observed - oracle)}"
    )
    # cross-engine: the same how on the local engine yields the same content-addressed blocks
    local = run_join(adapter.backend, left, right, PARTS, how=how, broadcast=False)
    assert res.dest_block_hashes == local.dest_block_hashes, (
        f"how={how}: dask and local run_join disagree on content-addressed joined blocks"
    )


def test_salted_join_stays_co_partitioned_and_matches_local(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    """Both sides must receive the SAME salt (co-partitioning preserved ⇒ the join RESULT is
    salt-invariant) while the placement changes — and the salted hashes equal the local engine's
    salted hashes (the salt plumbing is cross-engine identical)."""
    left, right = join_general_sides(adapter)
    install_worker_plugin(client)
    runner = make_submit_runner(build_dask_backend(client))

    plain = dask_join(adapter.backend, left, right, PARTS, runner=runner, broadcast=False)
    salted = dask_join(adapter.backend, left, right, PARTS, runner=runner, broadcast=False, salt=5)
    local_salted = run_join(adapter.backend, left, right, PARTS, broadcast=False, salt=5)

    assert observed_join_all(adapter, salted.value) == observed_join_all(adapter, plain.value), (
        "the join RESULT changed under salt — one side missed the salt (co-partitioning broken)"
    )
    assert salted.dest_block_hashes == local_salted.dest_block_hashes, (
        "salted dask join diverged from the salted local join"
    )
    assert dup_join_all(adapter, left, right, PARTS) == observed_join_all(adapter, plain.value)
