"""Build the m69a NanoAOD v15 diphoton fixtures from coffea's ``tests/samples/nano_tt_v15.root``.

``python make_hgg_fixture.py SRC DST`` writes the MC fixture: every branch of SRC, with two loose photons
and the mainAnalysis diphoton triggers injected into events 0-59. ``--data`` drops the generator branches
(``GenPart_*``, ``GenVtx_*``, ``LHE*``, ``genWeight``) and sets run/lumi from the 2024 golden JSON next to
this file: events 30-59 lie in uncertified lumis of a certified run, every other event is certified.
"""

import argparse
import datetime
import json
import pathlib
import uuid

import awkward as ak
import numpy as np
import uproot

SEED = 20260925
N_INJECTED = 60
UNCERTIFIED = range(30, 60)
HLT_PATHS = (
    "HLT_Diphoton30_18_R9IdL_AND_HE_AND_IsoCaloId",
    "HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass9",
)
GOLDEN = pathlib.Path(__file__).with_name("Cert_Collisions2024_378981_386951_Golden.json")
CREATED = datetime.datetime(2026, 9, 25, 12, 0, 0, tzinfo=datetime.UTC)


class _FixedClock(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        return CREATED


def read_collections(src):
    tree = uproot.open(src)["Events"]
    arr = tree.arrays()
    names = tree.keys()
    colls = sorted(
        {n[1:] for n in names if n.startswith("n") and any(k.startswith(n[1:] + "_") for k in names)}
    )
    out = {c: ak.zip({k[len(c) + 1 :]: arr[k] for k in names if k.startswith(c + "_")}) for c in colls}
    flat = [n for n in names if not n.startswith("n") and not any(n.startswith(c + "_") for c in colls)]
    # n* branches that count nothing; the jagged counters are rewritten from the collections
    flat += [n for n in names if n.startswith("n") and n[1:] not in colls and n[1:] not in names]
    for n in flat:
        out[n] = arr[n]
    return out, names


def inject_diphotons(out, names):
    ph = ak.flatten(out["Photon"])
    tmpl = ph[ak.argmax(ph.mvaID, axis=0, keepdims=False)]
    rng = np.random.default_rng(SEED)

    def photon(pt, eta, phi):
        p = tmpl
        fields = {
            "pt": pt,
            "eta": eta,
            "phi": phi,
            "mvaID": 0.95,
            "r9": 0.96,
            "hoe": 0.01,
            "sieie": 0.009,
            "electronVeto": True,
            "pixelSeed": False,
            "isScEtaEB": abs(eta) < 1.4442,
            "isScEtaEE": abs(eta) > 1.566,
            "pfChargedIsoPFPV": 0.1,
            "trkSumPtHollowConeDR03": 0.1,
            "pfPhoIso03": 0.5,
            "superclusterEta": eta,
            "mass": 0.0,
            "charge": 0,
        }
        for f, v in fields.items():
            if f in p.fields:
                prim = ak.type(ph[f]).content.primitive
                p = ak.with_field(
                    p, bool(v) if prim == "bool" else ak.values_astype(ak.Array([v]), prim)[0], f
                )
        return p

    pairs = []
    for _ in range(N_INJECTED):
        pt1, pt2 = 80 + 40 * rng.random(), 45 + 15 * rng.random()
        e1, e2 = rng.uniform(-1.3, 1.3), rng.uniform(-1.3, 1.3)
        f1 = rng.uniform(-np.pi, np.pi)
        f2 = (f1 + rng.uniform(2.6, 3.1) + np.pi) % (2 * np.pi) - np.pi
        pairs.append([photon(pt1, e1, f1), photon(pt2, e2, f2)])
    out["Photon"] = ak.concatenate([ak.Array(pairs), out["Photon"][N_INJECTED:]], axis=0)
    for path in [HLT_PATHS[0], *[k for k in names if k.startswith(HLT_PATHS[1])]]:
        out[path] = ak.concatenate([ak.Array([True] * N_INJECTED), out[path][N_INJECTED:]])


def make_data(out):
    for k in [k for k in out if k in ("GenPart", "GenVtx", "genWeight") or k.startswith("LHE")]:
        del out[k]
    golden = json.loads(GOLDEN.read_text())
    run = min(golden, key=int)
    first, last = golden[run][0]
    n = len(out["event"])
    uncertified = np.isin(np.arange(n), UNCERTIFIED)
    assert first > len(UNCERTIFIED) and last - first >= n
    lumi = np.where(uncertified, np.arange(n) - UNCERTIFIED.start + 1, first + np.arange(n))
    out["run"] = ak.values_astype(np.full(n, int(run)), ak.type(out["run"]).content.primitive)
    out["luminosityBlock"] = ak.values_astype(lumi, ak.type(out["luminosityBlock"]).content.primitive)


def write(dst, out):
    # a fixed clock and file UUID make the output a pure function of SRC
    datetime.datetime = _FixedClock
    fixed_uuid = uuid.UUID(int=SEED)
    with uproot.recreate(dst, uuid_function=lambda: fixed_uuid, compression=None) as f:
        f.mktree("Events", {k: ak.type(v).content for k, v in out.items()}, counter_name=lambda c: "n" + c)
        f["Events"].extend(out)
        f.mktree("Runs", {"run": np.int64})
        f["Runs"].extend({"run": np.array([1])})


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--data", action="store_true")
    args = ap.parse_args()
    out, names = read_collections(args.src)
    inject_diphotons(out, names)
    if args.data:
        make_data(out)
    write(args.dst, out)
    print("written", args.dst, "entries", uproot.open(args.dst)["Events"].num_entries)


if __name__ == "__main__":
    main()
