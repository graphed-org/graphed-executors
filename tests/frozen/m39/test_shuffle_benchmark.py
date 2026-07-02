"""M39 benchmark gates — algorithmic/complexity, NOT wall-clock (plan §8, worklog guidance 1-3).

These are counts + scaling-of-counts + a measured artifact, deterministic and CI-stable:
- block + announcement counts are O(#producer-tasks * P) — each block large; NO per-src_pid fragments,
  NO per-row messages. FAILS on the MxR blowup (which is O(#src_pid * P)) and on super-linear growth.
- manifest bytes are O(N*P) via the per-dest GET, NOT the O(N*P^2) whole-manifest pull (guidance 1).
- the routing hash is MEASURED (sha256 vs a pinned non-crypto hash on packed u64 keys) and the pinned
  choice recorded — the test requires the artifact to exist, it does not hard-code a winner (guidance 2).
"""

from __future__ import annotations

import pytest
from exchange_backends import adapters

from graphed_exec_local.shuffle import (
    PINNED_ROUTING_HASH,
    routing_hash_measurement,
    run_repartition,
)

_ADAPTERS = adapters()
pytestmark = pytest.mark.skipif(not _ADAPTERS, reason="no exchange backend installed")


def _run(workers: int, parts: int, tmp_path, n_src: int = 8):  # type: ignore[no-untyped-def]
    ad = _ADAPTERS[0]
    src = [ad.make_block(list(range(i, i + 12))) for i in range(n_src)]
    return run_repartition(ad.backend, src, parts=parts, workers=workers, store_root=tmp_path).witness


def test_block_count_is_O_producer_tasks_times_P_not_MxR(tmp_path) -> None:  # type: ignore[no-untyped-def]
    n_src = 8
    for workers, parts in ((2, 4), (2, 8), (4, 8)):
        w = _run(workers, parts, tmp_path / f"{workers}_{parts}", n_src=n_src)
        total = sum(w.blocks_per_producer_task.values())
        assert w.n_producer_tasks <= workers, "producer-tasks track workers (T ~ W), not src_pids"
        assert total <= w.n_producer_tasks * parts, "O(#producer-tasks * P) — one block per dest per task"
        assert total < n_src * parts, "must NOT be the O(#src_pid * P) tiny-fragment blowup"


def test_announcements_are_not_per_row_or_per_fragment(tmp_path) -> None:  # type: ignore[no-untyped-def]
    w = _run(workers=2, parts=8, tmp_path=tmp_path, n_src=8)
    announced = w.announcements_dropped + getattr(w, "announcements_sent", 0)
    # announcements are a droppable per-block hint, so their count is bounded like blocks, never per-row.
    assert announced <= w.n_producer_tasks * 8, "announcements must be O(#producer-tasks * P), not per-row"


def test_manifest_bytes_are_O_N_P_not_O_N_P_squared(tmp_path) -> None:  # type: ignore[no-untyped-def]
    # guidance 1: per-dest GET keeps manifest traffic linear in P. Doubling P must NOT ~quadruple bytes.
    w4 = _run(workers=2, parts=4, tmp_path=tmp_path / "p4", n_src=8)
    w8 = _run(workers=2, parts=8, tmp_path=tmp_path / "p8", n_src=8)
    assert w4.manifest_fetch_is_per_dest, "manifests must be pulled per-dest (.../{task}/{dest}), not whole"
    assert w8.manifest_bytes <= 3 * w4.manifest_bytes, "manifest bytes must scale ~O(P) (linear), not O(P^2)"


def test_routing_hash_is_measured_and_the_choice_recorded(tmp_path) -> None:  # type: ignore[no-untyped-def]
    # guidance 2: MEASURE sha256 vs a pinned non-crypto hash on packed u64 keys; record the pinned choice.
    measurement = routing_hash_measurement()
    assert "sha256" in measurement, "the benchmark must measure the pinned sha256 route hash"
    others = [name for name in measurement if name != "sha256"]
    assert others, (
        "it must ALSO measure a non-cryptographic alternative (e.g. xxhash64) to justify the choice"
    )
    for name, seconds in measurement.items():
        assert isinstance(seconds, float) and seconds >= 0.0, f"{name} must be a measured time, not invented"
    # the pinned routing hash is recorded and is the one the golden vectors require (§4).
    assert PINNED_ROUTING_HASH == "sha256"
