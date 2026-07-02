"""M39 (B1) — correctness is independent of announcements (plan §4.2.1, §4.1).

The transport is best-effort and permanently drops on a full inbox; correctness must therefore NEVER
depend on an announcement arriving. Gather completeness is derived from the plan's producer-task set +
per-producer-task manifests, not from pushed announcements — which are demoted to a pure latency
optimization. This test DROPS AND DUPLICATES **every** announcement and asserts the gathered content
is unchanged and the full input set was fetched. An announcement-dependent impl silently loses a block.
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


def test_dropping_and_duplicating_every_announcement_is_correct(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    src = scenario_src_blocks(adapter)
    faults = ShuffleFaults(drop_all_announcements=True, duplicate_all_announcements=True)
    res = run_repartition(
        adapter.backend, src, parts=4, workers=3, comms="ipc", store_root=tmp_path, faults=faults
    )
    # witness the faults were actually injected (non-vacuous): announcements really were dropped.
    assert res.witness.announcements_dropped > 0, "the drop fault must have engaged (else vacuous)"
    # yet the result is complete and equals the test-authored oracle.
    assert observed_dest_keys(adapter, res.value) == expected_dest_keys(adapter, src, 4)


def test_every_nonempty_dest_is_produced_despite_lost_announcements(
    adapter: BackendAdapter, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    src = scenario_src_blocks(adapter)
    expected = expected_dest_keys(adapter, src, 8)
    res = run_repartition(
        adapter.backend,
        src,
        parts=8,
        workers=3,
        comms="ipc",
        store_root=tmp_path,
        faults=ShuffleFaults(drop_all_announcements=True),
    )
    assert set(observed_dest_keys(adapter, res.value)) == set(expected), "no dest partition may be lost"
