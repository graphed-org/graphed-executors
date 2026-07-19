"""M40 (f) — a deterministic salt handles a skewed key bit-for-bit (plan theme f, trap 6).

A skewed key (here ~90% of rows share the single hot key 7) piles one dest sky-high under a plain
hash-route. A DETERMINISTIC salt (folded into the pinned ``sha256`` route, never Python ``hash()``)
re-places keys reproducibly. Two witnesses:

- (deterministic) two runs at the SAME salt produce byte-identical ``dest_block_hashes`` and the
  relational oracle — a ``hash()``-based or otherwise nondeterministic route drifts;
- (salt participates, content-neutral) across a set of salts the JOIN RESULT (multiset over all output
  blocks) is IDENTICAL — salt never changes which rows join — while at least one salt yields a
  different partitioning (``dest_block_hashes`` differ), proving salt is a live routing input. An impl
  that IGNORES salt gives the same partitioning for every salt and fails the "some salt differs" check;
  an impl where salt corrupts the join fails the "content identical" check.
"""

from __future__ import annotations

import pytest
from join_backends import (
    JoinAdapter,
    adapters,
    expected_join_all,
    expected_join_per_dest,
    observed_join_all,
    skewed_sides,
)

from graphed_exec_local.shuffle import run_join

ADAPTERS = adapters()


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> JoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_skewed_join_is_deterministic_and_correct(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    left, right = skewed_sides(adapter)
    parts = 8
    oracle = expected_join_all(adapter, left, right, parts)
    # non-vacuity: the scenario is genuinely skewed — one dest carries the overwhelming majority.
    dest_sizes = sorted(sum(c.values()) for c in expected_join_per_dest(adapter, left, right, parts).values())
    assert len(dest_sizes) > 1 and dest_sizes[-1] >= 2 * dest_sizes[-2], (
        "scenario must be skewed across >= 2 dests"
    )

    r1 = run_join(
        adapter.backend, left, right, parts, broadcast=False, salt=7, workers=3, store_root=tmp_path / "a"
    )
    r2 = run_join(
        adapter.backend, left, right, parts, broadcast=False, salt=7, workers=3, store_root=tmp_path / "b"
    )
    assert r1.dest_block_hashes == r2.dest_block_hashes, "salted routing must be deterministic across runs"
    assert observed_join_all(adapter, r1.value) == oracle, "the skewed join must be relationally correct"


def test_salt_is_a_live_content_neutral_routing_input(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    left, right = skewed_sides(adapter)
    parts = 8
    oracle = expected_join_all(adapter, left, right, parts)

    contents = []
    placements = []
    for salt in (0, 1, 2, 3, 5, 7):
        r = run_join(
            adapter.backend,
            left,
            right,
            parts,
            broadcast=False,
            salt=salt,
            workers=3,
            store_root=tmp_path / f"s{salt}",
        )
        contents.append(observed_join_all(adapter, r.value))
        placements.append(r.dest_block_hashes)
    assert all(c == oracle for c in contents), (
        "salt must be content-neutral (same joined rows for every salt)"
    )
    assert any(p != placements[0] for p in placements[1:]), (
        "salt must participate in routing (some salt re-places a key)"
    )
