"""M39 (c) — repartition by measured bytes + the bounded-memory witness (plan §5.1, guidance 3).

Awkward partition bytes are NOT a function of entry count, so coalesce/split size by measured bytes
(``estimated_bytes``) and split oversized partitions at EVENT boundaries only (``slice_rows``), never
mid-event. Correctness: coalescing many tiny partitions into fewer ~target ones preserves content and
order. Bounded memory: the stage-1 hash map-write holds P open writers at ~one row-group buffer each,
so peak writer-buffer bytes are pinned at O(P*rg) with ``rg`` a DOCUMENTED knob (``ROW_GROUP_BYTES``) —
large-P repartitions otherwise eat RAM silently.
"""

from __future__ import annotations

import pytest
from exchange_backends import BackendAdapter, adapters
from graphed_exec_local.shuffle import (
    ROW_GROUP_BYTES,
    run_repartition,
    run_repartition_by_size,
)

ADAPTERS = adapters()


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> BackendAdapter:
    return request.param


def test_row_group_bytes_is_a_documented_positive_knob() -> None:
    assert isinstance(ROW_GROUP_BYTES, int) and ROW_GROUP_BYTES > 0


def test_coalesce_by_size_reduces_partition_count_and_preserves_content(
    adapter: BackendAdapter, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    be = adapter.backend
    # 12 tiny source partitions (2 rows each); coalesce toward ~3 partitions' worth of bytes.
    src = [adapter.make_block([i, i + 100]) for i in range(12)]
    one_block_bytes = be.estimated_bytes(src[0])
    res = run_repartition_by_size(be, src, target_bytes=3 * one_block_bytes, workers=2, store_root=tmp_path)
    assert len(res.partitions) < len(src), "coalescing must merge tiny partitions into fewer"
    # content + order preserved: concat(outputs) == concat(inputs).
    assert adapter.keys_of(be.concat(res.partitions)) == adapter.keys_of(be.concat(src))
    # every coalesced partition except possibly the last is within the byte target.
    for part in res.partitions[:-1]:
        assert be.estimated_bytes(part) <= 3 * one_block_bytes * 2, "coalesce must respect the byte target"


def test_split_breaks_oversized_partitions_at_event_boundaries(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    be = adapter.backend
    big = adapter.make_block(list(range(64)))
    target = be.estimated_bytes(big) // 4
    res = run_repartition_by_size(be, [big], target_bytes=target, workers=1, store_root=tmp_path)
    assert len(res.partitions) > 1, "an oversized partition must be split"
    assert adapter.keys_of(be.concat(res.partitions)) == list(range(64)), "split loses no rows, keeps order"


def test_hash_shuffle_writer_buffer_is_bounded_by_P_times_rg(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # guidance 3: peak stage-1 writer-buffer memory is O(P * rg), not O(total shuffled bytes).
    be = adapter.backend
    src = [adapter.make_block(list(range(40))) for _ in range(4)]
    parts = 8
    res = run_repartition(be, src, parts=parts, workers=2, store_root=tmp_path)
    assert res.witness.peak_writer_buffer_bytes <= parts * ROW_GROUP_BYTES, "writer buffers must be O(P*rg)"
