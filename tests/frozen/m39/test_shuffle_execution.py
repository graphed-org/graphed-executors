"""M39 — two-phase shuffle execution: determinism, coalescing, duplicate-delivery (themes a, b, d).

Run the map-write + gather over the real backends and witness the mechanism, not just the result:
- (a) deterministic hash-route: two fuzzed-arrival runs produce byte-identical gathered blocks
  (equal content-addressed dest_block_hashes IS the §7.1 determinism gate);
- (b) merge-to-large-blocks: <= P coalesced blocks PER PRODUCER-TASK (O(#producer-tasks*P)), NOT the
  O(#src_pid*P) tiny-fragment blowup a naive per-src_pid impl produces;
- (d) bit-for-bit content despite fuzzed arrival order AND duplicate deliveries.

Correctness is checked against a TEST-AUTHORED oracle (``expected_dest_keys``) that only fixes the
ascending-src_pid ASSEMBLY order (routing itself is pinned by the backend golden vectors), so the
executor cannot share a routing bug with the oracle.
"""

from __future__ import annotations

import pytest
from exchange_backends import (
    BackendAdapter,
    adapters,
    expected_dest_keys,
    observed_dest_keys,
    scenario_src_blocks,
)

from graphed_exec_local.shuffle import ShuffleFaults, run_repartition

ADAPTERS = adapters()


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> BackendAdapter:
    return request.param


def test_shuffle_matches_the_test_authored_oracle(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    src = scenario_src_blocks(adapter)
    res = run_repartition(adapter.backend, src, parts=4, workers=2, comms="ipc", store_root=tmp_path)
    assert observed_dest_keys(adapter, res.value) == expected_dest_keys(adapter, src, 4), (
        "each dest must gather exactly its routed rows in ascending-src_pid order"
    )


def test_two_fuzzed_runs_are_byte_identical(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # (a)/(d): racy arrival + duplicate deliveries must not change ANY gathered block's BYTES (§7.1).
    src = scenario_src_blocks(adapter)
    faults = ShuffleFaults(duplicate_all_announcements=True)
    r1 = run_repartition(
        adapter.backend, src, parts=4, workers=4, comms="ipc", store_root=tmp_path / "a", faults=faults
    )
    r2 = run_repartition(
        adapter.backend, src, parts=4, workers=4, comms="ipc", store_root=tmp_path / "b", faults=faults
    )
    assert r1.dest_block_hashes == r2.dest_block_hashes, "content-addressing is the determinism gate"
    assert observed_dest_keys(adapter, r1.value) == expected_dest_keys(adapter, src, 4)


def test_block_count_is_bounded_by_producer_tasks_times_P(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # (b) THE anti-MxR witness. 6 src_pids, 2 workers -> 2 producer-tasks each coalescing 3 src_pids.
    src = scenario_src_blocks(adapter)  # 6 source partitions
    parts = 4
    res = run_repartition(adapter.backend, src, parts=parts, workers=2, comms="ipc", store_root=tmp_path)
    w = res.witness
    assert w.n_producer_tasks <= 2, "T ~ W: producer-tasks track workers, not src_pids"
    assert w.n_producer_tasks < len(src), "coalescing is only witnessed if a task owns several src_pids"
    assert all(count <= parts for count in w.blocks_per_producer_task.values()), "<= P blocks per task"
    total_blocks = sum(w.blocks_per_producer_task.values())
    assert total_blocks <= w.n_producer_tasks * parts, "O(#producer-tasks * P)"
    assert total_blocks < len(src) * parts, "must NOT be the O(#src_pid * P) per-fragment blowup"


def test_producer_task_set_is_enumerable_a_priori(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # discovery is a pure function of the plan: every producer-task has a deterministic owner, and the
    # manifests cover the whole plan set (independent of any received message).
    src = scenario_src_blocks(adapter)
    res = run_repartition(adapter.backend, src, parts=4, workers=3, comms="ipc", store_root=tmp_path)
    assert set(res.witness.manifest_owner) == set(range(res.witness.n_producer_tasks))
