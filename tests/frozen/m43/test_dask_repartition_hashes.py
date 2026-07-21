"""m43 headline determinism gate — CROSS-ENGINE bit-for-bit repartition (plan §1.3.4 point 3;
blueprint §3 `test_dask_repartition_hashes`).

`dask_run_repartition` run twice must produce identical `dest_block_hashes`, AND the same dict the
LOCAL two-phase engine (`graphed_executors.local.shuffle.run_repartition`) produces on identical
inputs — content-addressing across two INDEPENDENT engines is a stronger gate than either alone: it
pins worker-side sha256-of-`to_wire` hashing, the `_assign` contiguous-ascending producer split,
and the ascending-producer-task merge order all at once. On BOTH real backends (a2 discipline).

Also the witness-shape pin: the returned witness must carry the §1.3.4 dask counters and must NOT
carry the RETIRED announcement/manifest/steal counters (reusing the local `ShuffleWitness`
wholesale is the wrong implementation this discriminates).

Discriminates: any non-ascending-task concat or non-sha256 route (hash dicts diverge from local),
driver-side hashing of re-serialized blocks (bytes drift), a salt that never reaches the route,
and a witness that still names retired machinery."""

from __future__ import annotations

import pytest
from dask_shuffle_backends import (
    ShuffleJoinAdapter,
    build_dask_backend,
    dask_repartition,
    install_worker_plugin,
    make_submit_runner,
    repartition_blocks,
    result_dest_keys,
    route_oracle_dest_keys,
    shuffle_adapters,
    shuffle_cluster,
    total_result_rows,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip("distributed")

ADAPTERS = shuffle_adapters()
PARTS = 5


@pytest.fixture(scope="module")
def client():
    # tier B: real pickling of backends and blocks through worker processes
    with shuffle_cluster(2, processes=True) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> ShuffleJoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_two_dask_runs_and_the_local_engine_agree_bit_for_bit(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks(adapter)
    install_worker_plugin(client)
    runner = make_submit_runner(build_dask_backend(client))

    r1 = dask_repartition(adapter.backend, src, PARTS, runner=runner)
    r2 = dask_repartition(adapter.backend, src, PARTS, runner=runner)  # same runner: re-runnable
    local = run_repartition(adapter.backend, src, PARTS)

    assert r1.dest_block_hashes, "no dest blocks produced — scenario degenerate"
    assert r1.dest_block_hashes == r2.dest_block_hashes, "two dask runs diverged (nondeterminism)"
    assert r1.dest_block_hashes == local.dest_block_hashes, (
        "dask and the local engine disagree on content-addressed dest blocks — routing, merge "
        "order, or wire serialization drifted from the frozen m39 contract"
    )

    # content, not just hashes: assembled per-dest key sequences equal the partition-routed oracle
    assert result_dest_keys(adapter, r1.value) == route_oracle_dest_keys(adapter, src, PARTS)
    # row conservation
    assert total_result_rows(r1.value) == sum(len(b) for b in src)  # type: ignore[arg-type]


def test_salt_reroutes_deterministically(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    """`salt` must reach the pinned route: two salted runs agree with each other and differ from
    the unsalted run (content-neutral re-placement — the m40 skew-salt discipline on dask)."""
    src = repartition_blocks(adapter)
    install_worker_plugin(client)
    runner = make_submit_runner(build_dask_backend(client))

    plain = dask_repartition(adapter.backend, src, PARTS, runner=runner)
    s1 = dask_repartition(adapter.backend, src, PARTS, runner=runner, salt=3)
    s2 = dask_repartition(adapter.backend, src, PARTS, runner=runner, salt=3)

    assert s1.dest_block_hashes == s2.dest_block_hashes, "salted runs diverged (nondeterminism)"
    assert s1.dest_block_hashes != plain.dest_block_hashes, (
        "salt=3 produced the identical placement — salt never reached the route"
    )
    # content-neutral: the union of routed rows is conserved under the salt
    assert total_result_rows(s1.value) == sum(len(b) for b in src)  # type: ignore[arg-type]


def test_witness_names_dask_mechanisms_and_retires_local_ones(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks(adapter)
    install_worker_plugin(client)
    runner = make_submit_runner(build_dask_backend(client))
    w = dask_repartition(adapter.backend, src, PARTS, runner=runner).witness

    # the §1.3.4 counters that still name real mechanisms
    assert w.n_producer_tasks == min(2, len(src)) == 2  # T = min(n_workers, n_src)
    assert isinstance(w.blocks_per_producer_task, dict) and w.blocks_per_producer_task
    assert all(1 <= n <= PARTS for n in w.blocks_per_producer_task.values()), (
        "a producer task must coalesce to at most P dest blocks"
    )
    assert isinstance(w.peak_writer_buffer_bytes, int) and w.peak_writer_buffer_bytes >= 0
    assert isinstance(w.producer_sites, dict) and len(w.producer_sites) == w.n_producer_tasks
    assert isinstance(w.gather_sites, dict) and w.gather_sites

    # the RETIRED mechanisms must be gone — a reused local ShuffleWitness fails here (§1.3.4 (f))
    for retired in (
        "announcements_sent",
        "announcements_dropped",
        "manifest_put_attempts",
        "manifest_put_acks",
        "manifest_bytes",
        "steals",
        "stolen_tasks",
    ):
        assert not hasattr(w, retired), (
            f"witness still carries retired counter {retired!r} — announcements/manifests/steals "
            "do not exist under dask (the future graph IS completeness; the scheduler owns stealing)"
        )
