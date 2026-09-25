"""m69a: the lumi-mask External plugin is coffea's ``LumiMask`` on the original's golden JSON."""

from __future__ import annotations

import importlib
import json

import awkward as ak
import hgg_harness as h
import numpy as np
import uproot
from coffea.lumi_tools import LumiMask
from graphed import Session
from graphed.awkward import AwkwardBackend, from_awkward
from graphed.core import GraphStore
from graphed.preserve.externals import sha256_bytes

GOLDEN = h.DATA / "Cert_Collisions2024_378981_386951_Golden.json"


def pairs() -> tuple[np.ndarray, np.ndarray]:
    """Each certified range's edges and their outside neighbours for three runs, an absent run, and the
    data fixture's own (run, lumi)."""
    golden = json.loads(GOLDEN.read_text())
    runs = sorted(golden, key=int)
    out = [(int(runs[0]) - 1, 1)]
    for run in (runs[0], runs[len(runs) // 2], runs[-1]):
        for first, last in golden[run]:
            out += [(int(run), first - 1), (int(run), first), (int(run), last), (int(run), last + 1)]
    fixture = uproot.open(h.DATA_FIXTURE)["Events"].arrays(["run", "luminosityBlock"], library="np")
    out += zip(fixture["run"].tolist(), fixture["luminosityBlock"].tolist(), strict=True)
    run, lumi = np.array(out, dtype=np.uint32).T
    return run, lumi


def test_the_plugin_equals_coffea_lumimask_and_carries_the_json_hash() -> None:
    analysis = importlib.import_module("analysis")
    run, lumi = pairs()
    expected = LumiMask(str(GOLDEN))(run, lumi)
    assert expected.any() and not expected.all()

    session = Session(AwkwardBackend())
    arr = from_awkward(session, "pairs", ak.Array({"run": run, "lumi": lumi}))
    mask = analysis.lumi_mask(arr.run, arr.lumi, h.YEAR)
    got = np.asarray(session.materialize(mask))
    assert got.dtype == np.bool_
    assert np.array_equal(got, expected)

    nodes = GraphStore.deserialize(session.serialized_ir(mask, optimize=False)).nodes()
    (external,) = [n for n in nodes if n["kind"] == "external"]
    assert external["descriptor"]["content_hash"] == sha256_bytes(GOLDEN.read_bytes())
