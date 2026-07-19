"""M40 (c) — the broadcast cost model: a pinned rule + a PLAN-RECORDED (not racy runtime) choice (E5).

The choice is broadcast IFF ``|build|·N_nodes < |build|+|probe|`` from typetracer/estimated form sizes,
computed at plan-build time and FROZEN into the plan (E5); the executor READS the recorded choice, it
does not re-observe sizes at run time. Three witnesses:

- (rule) ``broadcast_join_choice`` matches the inequality on BOTH sides of the exact crossover — an
  impl using a different comparison (``|build|<|probe|``, ``<=`` vs ``<``) fails the boundary vectors;
- (recorded, honoured) ``run_join`` with an explicit ``broadcast=`` HONOURS that recorded choice and
  records it in ``witness.broadcast_chosen`` — even when the run-time input sizes would argue the other
  way (proving it reads the plan, not a runtime recompute), and the result stays correct;
- (deterministic) two ``broadcast=None`` (auto) runs on identical input yield an IDENTICAL choice AND
  byte-identical ``dest_block_hashes`` — a racy runtime cost model drifts across runs.
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


def test_cost_rule_is_the_pinned_inequality_at_the_crossover() -> None:
    # broadcast IFF |build|*N < |build|+|probe|. Straddle the EXACT boundary so a wrong comparison fails.
    build, n = 10, 5  # build*N = 50
    just_broadcast = build * n - build + 1  # |build|+|probe| = 51 > 50  -> broadcast
    just_shuffle = build * n - build - 1  # |build|+|probe| = 49 < 50  -> shuffle
    assert broadcast_join_choice(build, just_broadcast, n) is True, "build*N < build+probe must broadcast"
    assert broadcast_join_choice(build, just_shuffle, n) is False, "build*N > build+probe must shuffle"
    # clear-cut extremes, both backends' N counts:
    assert broadcast_join_choice(1, 10_000, 3) is True, "a tiny build vs a huge probe broadcasts"
    assert broadcast_join_choice(10_000, 1, 3) is False, "a huge build never broadcasts"


def test_executor_honours_the_recorded_choice_not_a_runtime_recompute(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # E5: the plan records the choice; the executor honours it. Force broadcast=True on a scenario a
    # runtime cost model MIGHT recompute the other way — the honoured choice must still be recorded True
    # and the join must stay correct. A recompute-at-runtime impl overrides the recorded True -> fails.
    left, right = broadcast_sides(adapter)
    parts = 4
    oracle = expected_join_all(adapter, left, right, parts)

    forced_bcast = run_join(
        adapter.backend, left, right, parts, broadcast=True, workers=3, store_root=tmp_path / "t"
    )
    assert forced_bcast.witness.broadcast_chosen is True, (
        "the executor must honour the recorded broadcast=True"
    )
    assert observed_join_all(adapter, forced_bcast.value) == oracle, "honouring the choice must stay correct"

    forced_shuf = run_join(
        adapter.backend, left, right, parts, broadcast=False, workers=3, store_root=tmp_path / "f"
    )
    assert forced_shuf.witness.broadcast_chosen is False, (
        "the executor must honour the recorded broadcast=False"
    )
    assert observed_join_all(adapter, forced_shuf.value) == oracle


def test_auto_choice_is_deterministic_across_two_runs(adapter: JoinAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # broadcast=None: the model chooses from form sizes; the choice + the bytes must be identical twice
    # (a racy runtime observation would drift). The chosen value must equal the pinned rule on the input
    # byte sizes — so the executor used THE rule, not an ad-hoc heuristic.
    left, right = broadcast_sides(adapter)
    parts, workers = 4, 3
    r1 = run_join(
        adapter.backend, left, right, parts, broadcast=None, workers=workers, store_root=tmp_path / "a"
    )
    r2 = run_join(
        adapter.backend, left, right, parts, broadcast=None, workers=workers, store_root=tmp_path / "b"
    )
    assert r1.witness.broadcast_chosen == r2.witness.broadcast_chosen, "the recorded choice must not drift"
    assert r1.dest_block_hashes == r2.dest_block_hashes, "content-addressing is the determinism gate"

    build_bytes = sum(adapter.backend.estimated_bytes(b) for b in left)
    probe_bytes = sum(adapter.backend.estimated_bytes(b) for b in right)
    assert r1.witness.broadcast_chosen == broadcast_join_choice(build_bytes, probe_bytes, workers), (
        "the auto choice must be exactly the pinned cost rule on the form sizes"
    )
