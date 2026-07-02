"""M39 (NB1, NB2) — stealing + the reliable manifest ship-back (plan §4.0, §4.3, §7.5).

A stolen producer-task leaves its coalesced blocks in the THIEF's Store (``holding_node=thief``) and
reliably pushes only the tiny manifest to ``owner(task)`` (retry-until-ack). The gather always dials
``owner(task)`` for the manifest, reads ``holding_node``, and pulls the block from the thief. So:

- (NB1) a gather succeeds after its producer-task was stolen — witnessed by the block being held on
  the thief while the manifest is journaled at the deterministic owner, and the content is correct;
- (NB2) dropping a fraction of the thief->owner manifest pushes still completes and does NOT deadlock
  — witnessed by retry-until-ack redelivering the manifest (attempts > acks).

Stealing is forced DETERMINISTICALLY (a relocation hook, not a timing race — per R0.10a the assert is
on structural counters, never a clock), so these tests are not flaky.
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


def test_gather_succeeds_after_a_producer_task_is_stolen(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # (NB1): force a steal; the block stays on the thief, the manifest lives at owner(task).
    src = scenario_src_blocks(adapter)
    res = run_repartition(
        adapter.backend,
        src,
        parts=4,
        workers=3,
        comms="ipc",
        store_root=tmp_path,
        steal=True,
        faults=ShuffleFaults(force_steal=True),
    )
    w = res.witness
    assert w.stolen_tasks, "the force_steal hook must relocate >= 1 producer-task (else vacuous)"
    stolen = w.stolen_tasks[0]
    owner = w.manifest_owner[stolen]
    # every block the stolen task wrote is held on a node OTHER than the task's owner (the thief)...
    stolen_holders = {node for h, node in w.block_holder.items() if h in w.blocks_of_task[stolen]}
    assert stolen_holders and owner not in stolen_holders, "a stolen task's block stays on the thief"
    # ...yet the gathered content is correct (bit-for-bit vs the uninterrupted oracle).
    assert observed_dest_keys(adapter, res.value) == expected_dest_keys(adapter, src, 4)


def test_lossy_manifest_shipback_still_completes(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # (NB2): drop the first 2 push attempts of each stolen manifest; retry-until-ack must redeliver.
    src = scenario_src_blocks(adapter)
    faults = ShuffleFaults(force_steal=True, manifest_push_drops=2)
    res = run_repartition(
        adapter.backend, src, parts=4, workers=3, comms="ipc", store_root=tmp_path, steal=True, faults=faults
    )
    w = res.witness
    assert w.stolen_tasks, "a task must have been stolen for the ship-back path to be exercised"
    # the retry witness: more push ATTEMPTS than ACKS (the dropped attempts were re-sent until acked).
    assert w.manifest_put_attempts > w.manifest_put_acks, "retry-until-ack must have re-sent dropped pushes"
    assert w.manifest_put_acks >= len(w.stolen_tasks), "every stolen manifest was eventually acked"
    # no deadlock + no corruption: the run finished and equals the uninterrupted oracle.
    assert observed_dest_keys(adapter, res.value) == expected_dest_keys(adapter, src, 4)
