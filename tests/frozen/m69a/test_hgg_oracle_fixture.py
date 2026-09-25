"""m69a: the oracle is the owner's original script, on fixtures the builder reproduces byte for byte."""

from __future__ import annotations

import inspect
import subprocess
import sys
import uuid
from importlib import resources
from pathlib import Path

import hgg_harness as h
import pytest
import uproot

SOURCE_SHA256 = "0ee769872082459bf2ccbef08b916c26b3070b64969812aea4d75a0cfa006256"  # coffea tests/samples
FILE_UUID = str(uuid.UUID(int=20260925))
MC_COUNTERS = {"MC": {"nTot": 200, "nPos": 200, "nNeg": 0, "nEff": 200, "genWeightSum": 70033.9921875}}
DATA_ROWS = 23


def test_the_original_is_byte_identical_to_the_owners_file() -> None:
    assert h.sha256(h.ORIGINAL) == h.ORIGINAL_SHA256


def test_infer_nano_version_is_the_installed_higgs_dna() -> None:
    fn = h.original().infer_nano_version
    assert fn.__module__ == "higgs_dna.utils.misc_utils"
    assert Path(inspect.getfile(fn)).is_relative_to(Path(str(resources.files("higgs_dna"))))


def test_placed_files_are_data_and_are_what_the_original_reads(placed: dict[str, Path]) -> None:
    for name, target in placed.items():
        assert h.sha256(target) == h.sha256(h.DATA / name), name
    orig = h.original()
    golden = orig.golden_json_path(orig.HggInclusiveProcessor.GOLDEN_JSON["2024"])
    assert Path(golden) == placed["Cert_Collisions2024_378981_386951_Golden.json"]
    assert Path(orig.jme_json_path("2024_Summer24/jetid.json.gz")) == placed["jetid.json.gz"]


def test_placing_over_a_different_file_fails_naming_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(h, "PLACED", {"jetid.json.gz": "metaconditions/Era2022_v1.json"})
    with pytest.raises(AssertionError, match=r"Era2022_v1\.json"):
        h.place_higgs_dna_data()


def test_the_oracle_on_the_mc_fixture_in_one_chunk(tmp_path: Path) -> None:
    ((counters, table),) = h.oracle_parts(str(h.MC_FIXTURE), "MC", h.YEAR, [(0, 200)], tmp_path).values()
    assert counters == MC_COUNTERS
    assert [type(v) for v in counters["MC"].values()] == [int, int, int, int, float]
    assert (table.num_rows, table.num_columns) == (52, 145)
    assert table.column_names == sorted(table.column_names)
    assert table.schema.metadata == {b"sum_genw_presel": b"70033.99", b"sum_weight_central": b"18208.84"}
    (path,) = tmp_path.rglob("*.parquet")
    assert path.relative_to(tmp_path / "0-200").as_posix() == f"MC/nominal/{FILE_UUID}_Events_0-200.parquet"


def _event_index(uri: Path) -> dict[int, int]:
    events = uproot.open(uri)["Events"]["event"].array(library="np")
    return {int(e): i for i, e in enumerate(events)}


def test_on_the_data_fixture_only_events_0_to_29_survive(tmp_path: Path) -> None:
    ((_, mc),) = h.oracle_parts(str(h.MC_FIXTURE), "MC", h.YEAR, [(0, 200)], tmp_path / "mc").values()
    ((counters, data),) = h.oracle_parts(
        str(h.DATA_FIXTURE), "DataC_2024", h.YEAR, [(0, 200)], tmp_path / "data"
    ).values()
    assert counters == {
        "DataC_2024": {"nTot": 200, "nPos": 200, "nNeg": 0, "nEff": 200, "genWeightSum": 200.0}
    }
    assert data.schema.metadata == {b"sum_genw_presel": b"Data"}
    assert "genWeight" not in data.column_names and "genWeight" in mc.column_names
    mc_rows = sorted(_event_index(h.MC_FIXTURE)[e] for e in mc.column("event").to_pylist())
    data_rows = sorted(_event_index(h.DATA_FIXTURE)[e] for e in data.column("event").to_pylist())
    # the lumi mask is the only difference: the MC rows in the uncertified events 30-59 are gone
    assert any(30 <= i < 60 for i in mc_rows)
    assert data_rows == [i for i in mc_rows if i < 30]
    assert len(data_rows) == DATA_ROWS


@pytest.mark.parametrize(("uri", "dataset"), [(h.MC_FIXTURE, "MC"), (h.DATA_FIXTURE, "DataC_2024")])
def test_the_second_chunk_is_an_empty_selection(uri: Path, dataset: str, tmp_path: Path) -> None:
    parts = h.oracle_parts(str(uri), dataset, h.YEAR, [(0, 100), (100, 200)], tmp_path)
    assert parts[(0, 100)][1].num_rows > 0
    assert parts[(100, 200)][1].num_rows == 0


def test_a_rebuilt_fixture_is_byte_identical(tmp_path: Path) -> None:
    source = h.DATA / "nano_tt_v15.root"
    assert h.sha256(source) == SOURCE_SHA256
    for fixture, extra in [(h.MC_FIXTURE, []), (h.DATA_FIXTURE, ["--data"])]:
        rebuilt = tmp_path / fixture.name
        builder = [sys.executable, str(h.DATA / "make_hgg_fixture.py"), *extra, str(source), str(rebuilt)]
        subprocess.run(builder, check=True, capture_output=True)
        assert rebuilt.read_bytes() == fixture.read_bytes(), fixture.name
