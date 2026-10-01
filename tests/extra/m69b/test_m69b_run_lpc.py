"""m69b ``run_lpc.py`` without a site: its manifests become each dataset's first ``--files`` files in
``--parts`` steps, the attached run asks for the LPC row narrowed to ``--placement`` with the analysis
shipped, the driverless run asks for a slot that holds the driver, its pilots and every server, and both
save the run's diagnostics. The runner is a stand-in that runs the plan on threads beside the driver.
Skips without coffea or higgs_dna unless ``GRAPHED_HGG_REQUIRED=1``, as ``hgg_harness`` does."""

from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import hgg_harness as h
import pytest
import uhi.io.json

from graphed_executors.submit import SubmitRunner, ThreadBackend


@pytest.fixture(scope="module")
def run_lpc() -> ModuleType:
    h.place_higgs_dna_data()
    return importlib.import_module("run_lpc")


def argv(tmp_path: Path, *extra: str) -> list[str]:
    manifest = tmp_path / "samples.json"
    manifest.write_text(json.dumps({"MC": [str(h.MC_FIXTURE), str(tmp_path / "never-read.root")], "x": []}))
    out = ["--datasets", "MC", "--files", "1", "--parts", "2", "--pilots", "2", "--year", h.YEAR]
    return [
        str(manifest),
        *out,
        "--out",
        str(tmp_path / "out"),
        "--histograms",
        str(tmp_path / "h.json"),
        *extra,
    ]


def threads(plan: Any, n: int) -> Any:
    with SubmitRunner(ThreadBackend(n)) as runner:
        return runner.run(plan).value


def saved(tmp_path: Path, run_lpc: ModuleType) -> None:
    parts = sorted(p.name for p in (tmp_path / "out" / "MC" / "nominal").glob("*.parquet"))
    assert parts == ["nano_hgg_v15_Events_0-100.parquet", "nano_hgg_v15_Events_100-200.parquet"]
    read = json.loads((tmp_path / "h.json").read_text(), object_hook=uhi.io.json.object_hook)
    assert set(read) == {"MC"} and set(read["MC"]) == set(run_lpc.analysis.DIAGNOSTICS)


def test_the_attached_run_asks_for_the_lpc_row_narrowed_to_its_placement(
    run_lpc: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    asked: dict[str, Any] = {}

    @contextmanager
    def runner(**kwargs: Any) -> Iterator[Any]:
        asked.update(kwargs)
        yield SimpleNamespace(run=lambda plan: SimpleNamespace(value=threads(plan, kwargs["n_pilots"])))

    monkeypatch.setattr(run_lpc, "htcondor_runner", runner)
    run_lpc.main(argv(tmp_path, "--placement", "driver"))
    assert asked["site"] == "lpc" and asked["service_hosts"] == ("driver",) and asked["n_pilots"] == 2
    assert asked["user_modules"] == [run_lpc.ANALYSIS] and run_lpc.ANALYSIS.name == "analysis.py"
    assert "nTot" in json.loads(capsys.readouterr().out.split("\n", 1)[1])["MC"]
    saved(tmp_path, run_lpc)


def test_the_driverless_run_asks_for_the_driver_its_pilots_and_every_server(
    run_lpc: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: dict[str, Any] = {}

    def submit(plan: Any, **kwargs: Any) -> Any:
        asked.update(kwargs, services=[spec.launch.resources["memory_mb"] for spec in plan.services])
        value = threads(plan, kwargs["n_pilots"])
        return SimpleNamespace(
            cluster=1, log_dir="d", wait=lambda: "completed", result=lambda: SimpleNamespace(value=value)
        )

    monkeypatch.setattr(run_lpc, "submit_driverless", submit)
    run_lpc.main(argv(tmp_path, "--driverless", "--pilot-mb", "1000", "--server-mb", "512"))
    assert asked["services"] and asked["pilots"] == "local" and asked["site"] == "lpc"
    assert asked["request_memory_mb"] == run_lpc.DRIVER_MB + 2 * 1000 + sum(asked["services"])
    saved(tmp_path, run_lpc)
