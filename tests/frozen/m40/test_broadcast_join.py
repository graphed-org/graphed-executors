"""M40 (b) — broadcast join == shuffle join, and the large side is NEVER shuffled (plan §4.2, risk #1).

Below the cost threshold a join broadcasts the SMALL build side to every node (``cluster.put`` on each)
and joins the LARGE probe side per-partition against the whole build side — the large side does NOT go
through the ``_stage1_map_write`` gather barrier. Two witnesses, not just the result:

- (correctness) the broadcast result equals the shuffle result equals the test-authored relational
  oracle — as a multiset over ALL output blocks (the partitioning differs between the two strategies,
  the JOIN RESULT does not);
- (mechanism) under broadcast ``witness.large_side_blocks == 0`` (the probe side wrote no shuffle
  blocks) AND ``witness.broadcast_puts == n_nodes`` (the build side really was replicated to every
  node). Under shuffle the same probe side writes ``large_side_blocks > 0`` — the contrast makes the
  broadcast==0 assertion non-vacuous.

An impl that routes the large side through ``_stage1_map_write`` reports ``large_side_blocks > 0`` under
broadcast and fails; an impl that never places the build side on the nodes reports ``broadcast_puts==0``.
"""

from __future__ import annotations

import pytest
from join_backends import (
    JoinAdapter,
    adapters,
    broadcast_sides,
    expected_join_all,
    observed_join_all,
)

from graphed_exec_local.shuffle import run_join

ADAPTERS = adapters()


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> JoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_broadcast_result_equals_shuffle_result_and_oracle(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    left, right = broadcast_sides(adapter)
    parts = 4
    oracle = expected_join_all(adapter, left, right, parts)
    assert sum(oracle.values()) > 0, "scenario must actually produce joined rows (else vacuous)"

    b = run_join(adapter.backend, left, right, parts, broadcast=True, workers=3, store_root=tmp_path / "b")
    s = run_join(adapter.backend, left, right, parts, broadcast=False, workers=3, store_root=tmp_path / "s")

    assert observed_join_all(adapter, b.value) == oracle, "broadcast join must equal the relational oracle"
    assert observed_join_all(adapter, s.value) == oracle, "shuffle join must equal the relational oracle"
    assert observed_join_all(adapter, b.value) == observed_join_all(adapter, s.value)


def test_broadcast_does_not_shuffle_the_large_side(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    left, right = broadcast_sides(adapter)
    parts, workers = 4, 3
    b = run_join(
        adapter.backend, left, right, parts, broadcast=True, workers=workers, store_root=tmp_path / "b"
    )
    s = run_join(
        adapter.backend, left, right, parts, broadcast=False, workers=workers, store_root=tmp_path / "s"
    )

    # (mechanism) broadcast: the large/probe side is not shuffled, the small/build side hits every node.
    assert b.witness.large_side_blocks == 0, (
        "the large side must NOT be routed through map_write under broadcast"
    )
    assert b.witness.broadcast_puts == workers, (
        "the build side must be replicated to every node (one put per node)"
    )
    assert b.witness.broadcast_chosen is True, "an explicit broadcast=True must be recorded as chosen"

    # contrast (non-vacuity): the SAME probe side IS shuffled when broadcast=False, so ==0 above is real.
    assert s.witness.large_side_blocks > 0, "the shuffle path must route the large side through map_write"
    assert s.witness.broadcast_puts == 0, "the shuffle path must not broadcast the build side"
    assert s.witness.broadcast_chosen is False
