"""A worker-produced value stdlib pickle refuses comes back through cloudpickle; one it accepts never does (#65)."""

from __future__ import annotations

import functools
import pickle
from typing import Any

import awkward as ak
import cloudpickle
import pytest
from _hdna_payloads import EVENTS, Events, add, concat, empty_array, lambda_behavior_reduce, sum_all, zero
from graphed import Session
from graphed.aggregate import aggregate_plan
from graphed.awkward import AwkwardBackend, AwkwardForm, gak
from graphed.core.execution import SequentialRunner

import graphed_executors.local._portable as portable
import graphed_executors.local.executors as local_executors
from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor
from graphed_executors.local._portable import Portable

ROUTES: dict[str, tuple[Any, dict[str, Any]]] = {
    "hub": (ProcessPoolExecutor, {"comms": None}),
    "hub-pooled-combines": (ProcessPoolExecutor, {"comms": None, "pooled_combines": True}),
    "ipc": (ProcessPoolExecutor, {"comms": "ipc"}),
    "ipc-pinned": (PinnedPoolExecutor, {"comms": "ipc"}),
    "http": (ProcessPoolExecutor, {"comms": "http"}),
}


def _plan(reduce: Any, combine: Any, empty: Any) -> Any:
    session = Session(AwkwardBackend())
    form = AwkwardForm(ak.Array(EVENTS.layout.to_typetracer(forget_length=True)))
    ev = session.source("events", form=form, data=Events(EVENTS))
    return aggregate_plan(
        gak.sum(ev.Jet.pt, axis=1), reduce=reduce, combine=combine, empty=empty, steps_per_file=4
    )


def _loads_seen(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Count, in this (driver) process, which loader each arriving value names."""
    seen = {"from_pickle": 0, "from_cloudpickle": 0}
    for name in seen:
        real = getattr(portable, name)

        @functools.wraps(real)
        def counted(data: bytes, _real: Any = real, _name: str = name) -> Any:
            seen[_name] += 1
            return _real(data)

        monkeypatch.setattr(portable, name, counted)
    return seen


def _run(route: str, plan: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr(local_executors, "_PEER_ROOT_TIMEOUT_S", 60.0)
    cls, kwargs = ROUTES[route]
    return cls(max_workers=2, **kwargs).run(plan).value


@pytest.mark.parametrize("route", ROUTES)
def test_a_reduce_built_lambda_behavior_returns_through_cloudpickle(
    route: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = _plan(lambda_behavior_reduce, concat, empty_array)
    seen = _loads_seen(monkeypatch)
    got = _run(route, plan, monkeypatch)
    assert ak.to_list(got) == ak.to_list(SequentialRunner().run(plan).value)
    assert got.behavior["f"](7) == 7
    assert seen["from_cloudpickle"] > 0, seen


@pytest.mark.parametrize("route", ROUTES)
def test_a_stdlib_picklable_partial_never_uses_cloudpickle(
    route: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _loads_seen(monkeypatch)
    assert _run(route, _plan(sum_all, add, zero), monkeypatch) == float(ak.sum(EVENTS.Jet.pt))
    assert seen["from_cloudpickle"] == 0 and seen["from_pickle"] > 0, seen


def test_portable_tries_stdlib_pickle_first(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_a: Any, **_k: Any) -> bytes:
        raise AssertionError("cloudpickle called for a stdlib-picklable value")

    monkeypatch.setattr(cloudpickle, "dumps", refuse)
    value = {"a": [1.0, 2.0]}
    shipped = Portable(value)
    assert not shipped.cloud and shipped.data == pickle.dumps(value, pickle.HIGHEST_PROTOCOL)
    assert pickle.loads(pickle.dumps(shipped)) == value


def test_portable_falls_back_to_cloudpickle() -> None:
    shipped = Portable({"f": lambda x: x + 1})
    assert shipped.cloud
    assert pickle.loads(pickle.dumps(shipped))["f"](1) == 2


def test_the_process_pool_entries_return_portable_values(monkeypatch: pytest.MonkeyPatch) -> None:
    lam = lambda_behavior_reduce([ak.Array([1.0])])
    monkeypatch.setattr(local_executors, "_proc_task_shared", lambda _token, _task: lam)
    leaf = local_executors._proc_task_portable("token", None)  # type: ignore[arg-type]
    merged = local_executors._proc_combine_task(concat, lam, lam)
    assert isinstance(leaf, Portable) and leaf.cloud and isinstance(merged, Portable) and merged.cloud
    assert ak.to_list(pickle.loads(pickle.dumps(merged))) == [1.0, 1.0]
