"""m69a: the graphed translation (``examples/hgg/analysis.py``) gives the original's answers, part for
part, on coffea NanoEvents."""

from __future__ import annotations

import importlib
import re
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import hgg_harness as h
import pyarrow.parquet as pq
import pytest
import uproot
from coffea.nanoevents import NanoEventsFactory
from coffea.nanoevents._graphed import GraphedNanoArray
from coffea.processor import accumulate
from graphed.core import SequentialRunner
from graphed.errors import GraphedTypeError

from graphed_executors.submit import SubmitRunner, ThreadBackend

RANGES = [(0, 100), (100, 200)]
FIXTURES = {"mc": (h.MC_FIXTURE, "MC"), "data": (h.DATA_FIXTURE, "DataC_2024")}
UPROOT_USE = re.compile(r"import uproot|uproot\.")


@pytest.fixture(scope="module")
def analysis() -> ModuleType:
    return importlib.import_module("analysis")


@pytest.fixture(scope="module")
def oracle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[tuple[int, int], h.Part]]:
    out = tmp_path_factory.mktemp("oracle")
    return {k: h.oracle_parts(str(uri), ds, h.YEAR, RANGES, out / k) for k, (uri, ds) in FIXTURES.items()}


@pytest.fixture(params=["sequential", "thread-backend"])
def runner(request: pytest.FixtureRequest) -> Iterator[Any]:
    if request.param == "sequential":
        yield SequentialRunner()
        return
    submit = SubmitRunner(ThreadBackend(2))
    try:
        yield submit
    finally:
        submit.close()


@pytest.mark.parametrize("kind", FIXTURES)
def test_each_part_equals_the_originals_part(
    kind: str, runner: Any, analysis: ModuleType, oracle: dict[str, Any], tmp_path: Path
) -> None:
    uri, dataset = FIXTURES[kind]
    plan = analysis.plan(str(uri), ranges=RANGES, dataset=dataset, year=h.YEAR, out=str(tmp_path))
    value = runner.run(plan).value
    names = {f"{uri.stem}_Events_{s}-{e}.parquet": (s, e) for s, e in RANGES}
    assert sorted(value) == sorted(names)
    for name, rng in names.items():
        (path,) = tmp_path.rglob(name)
        assert h.compare_part(oracle[kind][rng], (value[name], pq.read_table(path))) == [], (kind, rng)
    assert analysis.totals(value) == accumulate([counters for counters, _ in oracle[kind].values()])


@pytest.fixture
def recorded_events(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Every events object a ``NanoEventsFactory`` hands out while the test runs (calls pass through)."""
    seen: list[Any] = []
    events = NanoEventsFactory.events

    def recording(self: NanoEventsFactory) -> Any:
        seen.append(events(self))
        return seen[-1]

    monkeypatch.setattr(NanoEventsFactory, "events", recording)
    return seen


@pytest.mark.parametrize("kind", FIXTURES)
def test_the_events_are_coffea_nanoevents(
    kind: str, analysis: ModuleType, recorded_events: list[Any], tmp_path: Path
) -> None:
    uri, dataset = FIXTURES[kind]
    analysis.plan(str(uri), ranges=RANGES, dataset=dataset, year=h.YEAR, out=str(tmp_path))
    assert recorded_events
    for events in recorded_events:
        assert isinstance(events, GraphedNanoArray)
        assert hasattr(events.Photon, "metric_table") and hasattr(events.Photon, "delta_r")
        assert not hasattr(events.Photon, "no_such_method_xyz")
        assert events.metadata["dataset"] == dataset
        assert {type(s).__name__ for s in events.session.sources().values()} == {"_GraphedTTreeSource"}
    # the same source node backs a raw uproot array, which has no NanoEvents collections
    raw = uproot.graphed({str(uri): "Events"})
    assert {type(s).__name__ for s in raw.session.sources().values()} == {"_GraphedTTreeSource"}
    with pytest.raises(GraphedTypeError):
        _ = raw.Photon
    # the analysis builds events through coffea, never by reading with uproot itself
    sources = sorted(h.EXAMPLE.rglob("*.py"))
    assert h.EXAMPLE / "analysis.py" in sources
    assert [p.name for p in sources if UPROOT_USE.search(p.read_text())] == []
    assert UPROOT_USE.search((h.DATA / "make_hgg_fixture.py").read_text())
