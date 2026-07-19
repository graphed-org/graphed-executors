"""M40 (e) — the join is bit-identical independent of arrival / duplicate deliveries / a stolen
producer-task (plan §4.2.1, §7.1). Mirrors M39 ``test_announcement_robustness`` + ``test_steal_shuffle``.

Gather completeness for a join is derived from the plan's producer-task set + the per-producer-task
manifests (per side), NOT from pushed announcements or the order blocks happen to arrive. So dropping
AND duplicating every announcement, and DETERMINISTICALLY stealing a producer-task (its blocks stay on
the thief, only the tiny manifest ships to ``owner(task)``), must leave every gathered joined block
BYTE-identical to an uninterrupted run — and equal to the relational oracle.

Witnesses (non-vacuous): the faults actually engaged (announcements dropped > 0; ``stolen_tasks``
non-empty), yet ``dest_block_hashes`` are identical and the content matches the oracle. An impl whose
per-dest merge order depends on arrival order, or on which node computed a stolen task, fails.
"""

from __future__ import annotations

import pytest
from join_backends import (
    JoinAdapter,
    adapters,
    expected_join_per_dest,
    general_sides,
    observed_join_per_dest,
)

from graphed_exec_local.shuffle import ShuffleFaults, run_join

ADAPTERS = adapters()


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> JoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_join_is_byte_identical_under_fuzzed_arrival_and_duplicates(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    left, right = general_sides(adapter)
    parts = 4
    oracle = expected_join_per_dest(adapter, left, right, parts)
    assert oracle, "the scenario must produce joined rows on >= 1 dest (else vacuous)"

    faults = ShuffleFaults(drop_all_announcements=True, duplicate_all_announcements=True)
    r1 = run_join(
        adapter.backend,
        left,
        right,
        parts,
        broadcast=False,
        workers=4,
        store_root=tmp_path / "a",
        faults=faults,
    )
    r2 = run_join(
        adapter.backend,
        left,
        right,
        parts,
        broadcast=False,
        workers=4,
        store_root=tmp_path / "b",
        faults=faults,
    )

    assert r1.witness.announcements_dropped > 0, "the drop fault must have engaged (else vacuous)"
    assert r1.dest_block_hashes == r2.dest_block_hashes, "content-addressing is the determinism gate"
    assert observed_join_per_dest(adapter, r1.value) == oracle, (
        "join content must equal the relational oracle"
    )


def test_join_survives_a_stolen_producer_task_bit_for_bit(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    left, right = general_sides(adapter)
    parts = 4
    oracle = expected_join_per_dest(adapter, left, right, parts)

    clean = run_join(
        adapter.backend, left, right, parts, broadcast=False, workers=3, store_root=tmp_path / "c"
    )
    stolen = run_join(
        adapter.backend,
        left,
        right,
        parts,
        broadcast=False,
        workers=3,
        store_root=tmp_path / "s",
        steal=True,
        faults=ShuffleFaults(force_steal=True),
    )
    w = stolen.witness
    assert w.stolen_tasks, "the force_steal hook must relocate >= 1 producer-task (else vacuous)"
    stolen_task = w.stolen_tasks[0]
    owner = w.manifest_owner[stolen_task]
    holders = set(w.block_holder.values())
    assert owner not in holders or len(holders) > 1, "a stolen task's block lives on the thief, not its owner"
    # bit-for-bit vs the uninterrupted run AND the oracle — steal moves WHERE, never the merge order.
    assert stolen.dest_block_hashes == clean.dest_block_hashes, (
        "a steal must not change any gathered joined block"
    )
    assert observed_join_per_dest(adapter, stolen.value) == oracle
