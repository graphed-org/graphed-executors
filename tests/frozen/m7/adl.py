"""ADL benchmark queries 1-8 re-expressed against graphed, plus the picklable executor glue (plan
M7). These are the corpus analyses recorded through the frontend; the M7 suite runs EACH of them
over partitions through BOTH executors and reduces to a histogram — a deeper integration than the
graphed-awkward (M3) per-`materialize` tests, since here the graph is recorded + executed per chunk
and the per-chunk histograms are tree-reduced across workers/processes.
"""

from __future__ import annotations

import numpy as np
from graphed import Array, Session
from graphed.awkward import AwkwardBackend, from_awkward, gak
from graphed.core import Partition, Task
from graphed_corpus import make_events
from graphed_corpus.histograms import hist1d

N_EVENTS = 6000
SEED = 1234


# ---- the 8 ADL queries (recorded through graphed; mirror graphed_corpus/analyses/adl.py) --------
def _delta_phi(a: Array, b: Array) -> Array:
    return (a - b + np.pi) % (2 * np.pi) - np.pi


def _delta_r(eta1: Array, phi1: Array, eta2: Array, phi2: Array) -> Array:
    return np.hypot(eta1 - eta2, _delta_phi(phi1, phi2))


def _pair_mass(o1: Array, o2: Array) -> Array:
    px = o1.pt * np.cos(o1.phi) + o2.pt * np.cos(o2.phi)
    py = o1.pt * np.sin(o1.phi) + o2.pt * np.sin(o2.phi)
    pz = o1.pt * np.sinh(o1.eta) + o2.pt * np.sinh(o2.eta)
    e = np.sqrt(o1.pt**2 * np.cosh(o1.eta) ** 2 + o1.mass**2) + np.sqrt(
        o2.pt**2 * np.cosh(o2.eta) ** 2 + o2.mass**2
    )
    return np.sqrt(np.maximum(e**2 - (px**2 + py**2 + pz**2), 0.0))


def q1(events: Array) -> Array:
    return events.MET.pt


def q2(events: Array) -> Array:
    return events.Jet.pt


def q3(events: Array) -> Array:
    return events.Jet[abs(events.Jet.eta) < 1.0].pt


def q4(events: Array) -> Array:
    njet = gak.num(events.Jet[events.Jet.pt > 40], axis=1)
    return events.MET.pt[njet >= 2]


def q5(events: Array) -> Array:
    mu = events.Muon
    pairs = gak.combinations(mu, 2, fields=["a", "b"])
    opp = pairs.a.charge != pairs.b.charge
    mass = _pair_mass(pairs.a, pairs.b)
    keep = gak.any((mass > 60) & (mass < 120) & opp, axis=1)
    return events.MET.pt[keep]


def q6(events: Array) -> Array:
    jets = events.Jet[gak.num(events.Jet, axis=1) >= 3]
    tri = gak.combinations(jets, 3, fields=["a", "b", "c"])
    a, b, c = tri.a, tri.b, tri.c
    px = a.pt * np.cos(a.phi) + b.pt * np.cos(b.phi) + c.pt * np.cos(c.phi)
    py = a.pt * np.sin(a.phi) + b.pt * np.sin(b.phi) + c.pt * np.sin(c.phi)
    tri_pt = np.sqrt(px**2 + py**2)
    pz = a.pt * np.sinh(a.eta) + b.pt * np.sinh(b.eta) + c.pt * np.sinh(c.eta)
    e = (
        np.sqrt(a.pt**2 * np.cosh(a.eta) ** 2 + a.mass**2)
        + np.sqrt(b.pt**2 * np.cosh(b.eta) ** 2 + b.mass**2)
        + np.sqrt(c.pt**2 * np.cosh(c.eta) ** 2 + c.mass**2)
    )
    mass = np.sqrt(np.maximum(e**2 - (px**2 + py**2 + pz**2), 0.0))
    best = gak.argmin(abs(mass - 172.5), axis=1, keepdims=True)
    return gak.flatten(tri_pt[best])


def q7(events: Array) -> Array:
    jets = events.Jet[events.Jet.pt > 30]
    leptons = gak.concatenate(
        [events.Muon[events.Muon.pt > 10], events.Electron[events.Electron.pt > 10]], axis=1
    )
    pair = gak.cartesian([jets, leptons], nested=True)
    j, lp = pair["0"], pair["1"]
    dr = _delta_r(j.eta, j.phi, lp.eta, lp.phi)
    isolated = gak.fill_none(gak.all(dr > 0.4, axis=2), True)
    return gak.sum(jets[isolated].pt, axis=1)


def q8(events: Array) -> Array:
    muons = gak.with_field(events.Muon, gak.zeros_like(events.Muon.pt, dtype="int64"), "flavor")
    eles = gak.with_field(events.Electron, gak.ones_like(events.Electron.pt, dtype="int64"), "flavor")
    lep = gak.concatenate([muons, eles], axis=1)
    lep = lep[gak.argsort(lep.pt, axis=1, ascending=False)]
    mask3 = gak.num(lep, axis=1) >= 3
    lep = lep[mask3]
    met = events.MET[mask3]
    idx = gak.local_index(lep, axis=1)
    pairs = gak.combinations(gak.zip({"lep": lep, "i": idx}), 2, fields=["a", "b"])
    ossf = (pairs.a.lep.charge != pairs.b.lep.charge) & (pairs.a.lep.flavor == pairs.b.lep.flavor)
    mass = gak.where(ossf, _pair_mass(pairs.a.lep, pairs.b.lep), np.inf)
    has_ossf = gak.any(ossf, axis=1)
    best = gak.argmin(abs(mass - 91.2), axis=1, keepdims=True)
    not_in_pair = (idx != gak.flatten(pairs.a.i[best])) & (idx != gak.flatten(pairs.b.i[best]))
    others = lep[not_in_pair]
    lead = gak.firsts(others)
    keep = has_ossf & (gak.num(others, axis=1) >= 1)
    met_k = met[keep]
    lead_k = lead[keep]
    dphi = _delta_phi(lead_k.phi, met_k.phi)
    return np.sqrt(2 * lead_k.pt * met_k.pt * (1 - np.cos(dphi)))


ADL = {"q1": q1, "q2": q2, "q3": q3, "q4": q4, "q5": q5, "q6": q6, "q7": q7, "q8": q8}
ADL_NAMES = [f"q{i}" for i in range(1, 9)]


# ---- executor glue (all picklable: module-level fns + partial of str/int/float) -----------------
def _load_dataset(uri: str) -> object:
    return make_events(n_events=N_EVENTS, seed=SEED)


def _counts(values: object, bins: int, start: float, stop: float, name: str) -> np.ndarray:
    return np.asarray(hist1d(values, bins=bins, start=start, stop=stop, name=name).values(), dtype=np.int64)


def adl_partial(qname: str, bins: int, start: float, stop: float, part: Partition, res: object) -> np.ndarray:
    chunk = res.open_once(part.uri, _load_dataset)[part.entry_start : part.entry_stop]  # type: ignore[attr-defined]
    s = Session(AwkwardBackend())
    ev = from_awkward(s, "events", chunk)
    values = s.materialize(ADL[qname](ev))  # record + execute the query ON THE CHUNK
    return _counts(values, bins, start, stop, qname)


def adl_full_counts(qname: str, bins: int, start: float, stop: float) -> np.ndarray:
    s = Session(AwkwardBackend())
    ev = from_awkward(s, "events", make_events(n_events=N_EVENTS, seed=SEED))
    return _counts(s.materialize(ADL[qname](ev)), bins, start, stop, qname)


def hist_zero_n(n: int) -> np.ndarray:
    return np.zeros(n, dtype=np.int64)


def hist_add(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return a + b


def adl_axis(qname: str) -> tuple[int, float, float]:
    from graphed_corpus.analyses import adl as corpus_adl  # noqa: PLC0415

    ax = corpus_adl.ADL_QUERIES[qname](make_events(n_events=400, seed=SEED)).axes[0]
    return int(ax.size), float(ax.edges[0]), float(ax.edges[-1])


def adl_partitions(n_chunks: int) -> list[Task]:
    edges = np.linspace(0, N_EVENTS, n_chunks + 1, dtype=int)
    return [
        Task(i, Partition("events://corpus", "Events", int(edges[i]), int(edges[i + 1])))
        for i in range(n_chunks)
    ]
