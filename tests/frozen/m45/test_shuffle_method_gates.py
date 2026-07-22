"""m45 gate + knob-honesty pins — the facade adds NO gate of its own (the engines' refusals
surface intact) and never silently ignores or mis-forwards a transport-only knob
(m45-facade-plan §1.2, §2 items 4/5/7). Runs in the MAIN CI matrix — the engine gates and the
facade's knob validation all fire BEFORE any submit, so refusing stubs suffice (no cluster).

Discriminates: a facade that swallows or re-words the m44 pin gate / m43 peer gate; one that
silently drops a transport-only knob on the tasks path (forward-and-ignore) or forwards it into a
TypeError; one that validates knobs BEFORE resolution (auto->tasks + knob must still raise); and
the r1-draft bug — treating join's COMMON ``mem_budget_bytes`` as transport-only — via the
knob-validation asserts here plus the success arm in the join module."""

from __future__ import annotations

import pytest
from shuffle_method_harness import (
    ALLFALSE_CAPS,
    BACKEND,
    FULL_CAPS,
    PINLESS_CAPS,
    RefusingBackend,
    facade_join,
    facade_repartition,
    join_sides,
    repartition_blocks,
)


def _zero_touch(stub: RefusingBackend) -> None:
    assert stub.submit_attempts == 0 and stub.broadcast_attempts == 0, (
        "the refusal must fire BEFORE any task or payload reaches the backend"
    )


def test_explicit_transport_on_pinless_surfaces_the_m44_gate() -> None:
    stub = RefusingBackend(PINLESS_CAPS)
    blocks = repartition_blocks()[:2]
    left, right = join_sides()

    with pytest.raises(NotImplementedError, match="pin") as rep_exc:
        facade_repartition(BACKEND, blocks, 4, dbackend=stub, shuffle_method="transport")
    assert "Phase 2" in str(rep_exc.value), "the m44 refusal must surface intact through the facade"
    with pytest.raises(NotImplementedError, match="pin") as join_exc:
        facade_join(BACKEND, left, right, 4, dbackend=stub, shuffle_method="transport")
    assert "Phase 2" in str(join_exc.value)
    _zero_touch(stub)


def test_auto_on_all_false_degrades_then_refuses_via_the_m43_gate() -> None:
    stub = RefusingBackend(ALLFALSE_CAPS)
    blocks = repartition_blocks()[:2]
    left, right = join_sides()

    with pytest.raises(NotImplementedError, match="peer data movement") as rep_exc:
        facade_repartition(BACKEND, blocks, 4, dbackend=stub)  # auto -> tasks -> m43 gate
    assert "Phase 2" in str(rep_exc.value), (
        "auto on an all-False backend must degrade to tasks and then surface the m43 peer-movement "
        "refusal — not a facade-invented error, not a silent fallback"
    )
    with pytest.raises(NotImplementedError, match="peer data movement"):
        facade_join(BACKEND, left, right, 4, dbackend=stub)
    _zero_touch(stub)


def test_transport_only_knobs_raise_on_explicit_tasks() -> None:
    # FULL caps: only the facade's own validation can raise a ValueError here — a facade that
    # forwarded instead would hit the stub's AssertionError (forward-and-ignore) or a TypeError.
    stub = RefusingBackend(FULL_CAPS)
    blocks = repartition_blocks()[:2]
    left, right = join_sides()

    for knob in (
        {"n_tasks": 2},
        {"fetch_budget_bytes": 1000},
        {"disk_budget_bytes": 1000},
        {"holder_budget_bytes": 1000},
        {"pull_timeout_s": 1.0},
        {"epoch_restarts_allowed": 0},
    ):
        (name,) = knob
        with pytest.raises(ValueError, match=name):
            facade_repartition(BACKEND, blocks, 4, dbackend=stub, shuffle_method="tasks", **knob)

    for knob in ({"holder_budget_bytes": 1000}, {"pull_timeout_s": 1.0}, {"epoch_restarts_allowed": 0}):
        (name,) = knob
        with pytest.raises(ValueError, match=name):
            facade_join(BACKEND, left, right, 4, dbackend=stub, shuffle_method="tasks", **knob)
    _zero_touch(stub)


def test_auto_resolution_to_tasks_still_validates_transport_knobs() -> None:
    # resolution-then-validate order (plan §1.2): auto on a pinless backend RESOLVES to tasks,
    # so an explicit transport knob must raise exactly as under explicit "tasks".
    stub = RefusingBackend(PINLESS_CAPS)
    with pytest.raises(ValueError, match="fetch_budget_bytes"):
        facade_repartition(
            BACKEND, repartition_blocks()[:2], 4, dbackend=stub, fetch_budget_bytes=1000
        )
    left, right = join_sides()
    with pytest.raises(ValueError, match="pull_timeout_s"):
        facade_join(BACKEND, left, right, 4, dbackend=stub, pull_timeout_s=2.0)
    _zero_touch(stub)
