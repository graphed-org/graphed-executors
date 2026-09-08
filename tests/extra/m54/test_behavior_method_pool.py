"""m54 §2 item 6 pool witness: a behavior METHOD with arguments, registered on the backend alone,
evaluates on spawned workers bit-identically to in-process — through the import-ref backend."""

from __future__ import annotations

from typing import Any

import awkward as ak
import pytest
from _m54_behavior import BEHAVIOR, EVENTS, CorpusEvents, add2, sum_reduce, zero
from graphed import Session
from graphed.aggregate import aggregate_plan
from graphed.awkward import AwkwardBackend, AwkwardForm, gak
from graphed.core.execution import SequentialRunner

from graphed_executors.local import ProcessPoolExecutor


def _plan(backend_ref: str | None) -> tuple[float, Any]:
    session = Session(AwkwardBackend(behavior=BEHAVIOR))
    form = AwkwardForm(ak.Array(EVENTS.layout.to_typetracer(forget_length=True)))
    jets = gak.with_name(session.source("events", form=form, data=CorpusEvents(EVENTS)).Jet, "Jet")
    out = gak.sum(jets.scaled(2.0, offset=1.0), axis=1)
    plan = aggregate_plan(
        out, reduce=sum_reduce, combine=add2, empty=zero, steps_per_file=4, backend=backend_ref
    )
    return float(ak.sum(ak.Array(session.materialize(out)))), plan


def test_a_backend_only_method_evaluates_on_pool_workers_bit_identically() -> None:
    assert ("*", "Jet") not in ak.behavior  # nothing global can resolve `scaled`
    expected, plan = _plan("_m54_behavior:make_backend")
    assert SequentialRunner().run(plan).value == [expected]
    assert ProcessPoolExecutor(max_workers=2, persistent=True).run(plan).value == [expected]


def test_a_worker_without_the_behavior_cannot_resolve_the_method() -> None:
    """The discriminating leg: the default worker backend carries no behavior dict."""
    _expected, plan = _plan(None)
    with pytest.raises(Exception, match="scaled"):
        SequentialRunner().run(plan)
