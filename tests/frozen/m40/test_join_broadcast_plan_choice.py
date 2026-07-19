"""M40 (E5 / F6) — the broadcast-vs-shuffle choice is a PLAN property, invariant to the worker pool.

The pinned cost rule (broadcast IFF ``|build|*N < |build|+|probe|``) is computed at PLAN BUILD from the
typetracer/estimated form sizes and FROZEN into the plan; ``run_join`` HONOURS that recorded choice.
The current executor instead recomputes the auto (``broadcast=None``) choice from run-time bytes AND
the run-time ``workers`` count (``shuffle.py`` ``k = max(1, workers)`` → ``broadcast_join_choice(...,
k)``), so the SAME logical join flips broadcast↔shuffle merely because the operator resized the worker
pool — a racy, non-reproducible decision (F6).

This pins the invariant the fix must satisfy: the recorded choice must not depend on the runtime worker
count. The scenario STRADDLES the cost crossover so a workers-based rule provably flips between the two
worker counts, which is what makes the test DISCRIMINATING (an impl that ignores ``workers`` passes; the
current impl that keys off it fails). The join must also stay relationally correct at both counts, so
the invariance is not bought by breaking the join.

The complementary honour path (``broadcast=True/False`` recorded choice, byte-determinism of the auto
choice at a FIXED worker count) is already frozen in ``test_join_cost_model.py``; this file adds only the
missing worker-count-invariance guard.
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

from graphed_exec_local.shuffle import broadcast_join_choice, run_join

ADAPTERS = adapters()


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> JoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_auto_choice_is_invariant_to_the_runtime_worker_count(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    be = adapter.backend
    left, right = broadcast_sides(adapter)
    parts = 4

    # Two worker counts that STRADDLE the cost crossover: a workers-based rule flips between them, so a
    # choice that stays constant witnesses the decision is NOT keyed off the runtime pool (E5/F6).
    build_bytes = sum(be.estimated_bytes(b) for b in left)
    probe_bytes = sum(be.estimated_bytes(b) for b in right)
    hi = next(w for w in range(2, 256) if broadcast_join_choice(build_bytes, probe_bytes, w) is False)
    lo = hi - 1
    assert broadcast_join_choice(build_bytes, probe_bytes, lo) is True, "scenario must straddle the crossover"
    assert broadcast_join_choice(build_bytes, probe_bytes, hi) is False, "scenario must straddle the crossover"

    r_lo = run_join(be, left, right, parts, broadcast=None, workers=lo, store_root=tmp_path / "lo")
    r_hi = run_join(be, left, right, parts, broadcast=None, workers=hi, store_root=tmp_path / "hi")

    # F6: the recorded choice must be a PLAN property, not a function of the runtime worker count.
    assert r_lo.witness.broadcast_chosen == r_hi.witness.broadcast_chosen, (
        "the broadcast choice must be invariant to the runtime worker count (plan-recorded, not recomputed)"
    )

    # invariance must not be bought by breaking the join: both runs are the same duplicating oracle.
    oracle = expected_join_all(adapter, left, right, parts)
    assert observed_join_all(adapter, r_lo.value) == oracle, "the join must stay correct at every worker count"
    assert observed_join_all(adapter, r_hi.value) == oracle, "the join must stay correct at every worker count"
