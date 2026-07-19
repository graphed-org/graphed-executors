"""M40 (B5 honesty / F5) — a spilled partition must be READ BACK, not written-and-forgotten.

``_join_with_budget`` spills each duplicated output chunk to the node Store on disk
(``(obj_dir / digest).write_bytes(wire)``, ``shuffle.py:508``) and frees its wire from RAM — but the
result is still reconstructed from the ``joined`` objects it kept in RAM the whole time, so the disk
write has NO reader: dead, write-only I/O that never bounds anything (F5). The B5 budget gate is then
only as honest as its claim that the live working set was actually streamed off disk.

This pins the reader the fix must add: a ``join_chunks_read`` witness counting the chunks read BACK from
the Store, which must equal the number spilled (and be > 0). To keep the counter from being satisfiable
by an incremented-but-unused variable, it is triangulated against the spilled files that are physically
on disk AND against the relational result reconstructed by reading ONLY those files — so the reader
counts genuine reads of genuine payload.

Regression: the existing B5 gates (``peak_join_bytes <= budget``, ``join_spilled_partitions > 0``) are
kept, so a fix that reads back but blows the budget or stops spilling is still caught.
"""

from __future__ import annotations

import os
from collections import Counter

import pytest
from join_backends import (
    JoinAdapter,
    adapters,
    expected_join_all,
    observed_join_all,
    one_dest_sides,
)

from graphed_exec_local.shuffle import run_join

ADAPTERS = adapters()

_N = 120  # a single hot key -> one dest of _N*_N duplicated output rows, far larger than the budget


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> JoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _spill_files(res) -> dict[str, str]:  # type: ignore[no-untyped-def]
    """Map every object-store digest present on disk (across all node stores) to its path."""
    out: dict[str, str] = {}
    for store_dir in res.witness.node_store_dirs.values():
        obj_dir = os.path.join(store_dir, "objects")
        if os.path.isdir(obj_dir):
            for name in os.listdir(obj_dir):
                out[name] = os.path.join(obj_dir, name)
    return out


def _reconstruct_from_spill(adapter: JoinAdapter, res) -> Counter:  # type: ignore[no-untyped-def]
    """The join result rebuilt by reading ONLY the spilled dest files back off disk (never ``res.value``)."""
    on_disk = _spill_files(res)
    total: Counter = Counter()
    for digest in res.dest_block_hashes.values():
        with open(on_disk[digest], "rb") as fh:
            block = adapter.backend.from_wire(fh.read())
        total += Counter(adapter.joined_rows(block))
    return total


def test_spilled_partitions_are_read_back_not_dead_io(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    be = adapter.backend
    left, right = one_dest_sides(adapter, _N, _N)
    parts = 4
    oracle = expected_join_all(adapter, left, right, parts)
    assert sum(oracle.values()) == _N * _N, "the scenario must duplicate to a single over-budget dest"

    row_bytes = be.estimated_bytes(left[0]) // _N + be.estimated_bytes(right[0]) // _N
    output_bytes = _N * _N * row_bytes
    budget = output_bytes // 8
    assert budget < output_bytes, "the budget must be smaller than the joined dest it covers"

    res = run_join(
        be, left, right, parts, broadcast=False, workers=2, store_root=tmp_path, mem_budget_bytes=budget
    )
    w = res.witness

    # regression: the B5 budget gate + spill engagement are still enforced.
    assert w.peak_join_bytes <= budget, "the kernel working set must stay within the budget (spill+stream)"
    assert w.join_spilled_partitions > 0, "spill must actually engage (else the budget was never exercised)"

    # F5: each spilled chunk is READ BACK once — a real reader, not a write-only spill.
    assert w.join_chunks_read == w.join_spilled_partitions, (
        "every spilled chunk must be read back once (dead write-only I/O leaves chunks_read < spilled)"
    )
    assert w.join_chunks_read > 0, "the read-back path must actually run"

    # the read counter counts genuine reads of genuine payload: the same count of files is on disk AND
    # those files alone reconstruct the whole relational result.
    on_disk_dest = {d for d in _spill_files(res) if d in set(res.dest_block_hashes.values())}
    assert len(on_disk_dest) == w.join_spilled_partitions, "every spilled chunk must be a real file on disk"
    assert _reconstruct_from_spill(adapter, res) == oracle, "the spilled files must hold the full join result"
    assert observed_join_all(adapter, res.value) == oracle, "the returned value must equal the join result"
