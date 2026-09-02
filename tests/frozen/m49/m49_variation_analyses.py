"""Module-level fixtures for the m49 cross-process variation anchors (plan §10/m49, executors half).

Everything a spawned worker must unpickle lives here at module scope: the `PartitionedSource`, the
histogram programs, and the poison. `graphed_histogram.plan` ships only the histograms' own
`_evaluators` — there is no `externals=` seam on that path — so the worker-side failure the §8.2
rendering anchors need is a plain USER arithmetic op recorded in the analysis, never a `.map`
payload (the m6 `numpy_mismatch_in_fused_stage` idiom, mismatched operand lengths).

The 15-reference matrix runs against the corpus's OWN dataset, so `graphed_corpus.ttbar_region` /
`ttgamma_region` recompute each reference in-process — the m7 house pattern (`tests/frozen/m7/adl.py`),
not a vendored JSON.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import cache
from typing import Any

import awkward as ak
import boost_histogram as bh
import graphed
import graphed_histogram as gh
import numpy as np
from graphed import Array, Session
from graphed.awkward import AwkwardBackend, AwkwardForm, gak
from graphed.core import Partition
from graphed.core.execution import Plan, WorkerResources
from graphed_corpus import make_events

#: the references' own dataset — `make_events()` at its defaults (20_000 events, seed 1234)
MATRIX_EVENTS = make_events()

#: a small dataset for the failure anchors: they never read a bin, only raise
POISON_EVENTS = make_events(n_events=400, seed=1949)

#: the cut inside the poison expression — an identity token in the captured sub-expression text
POISON_CUT = 20.0

MATRIX_PARTITIONS = 4

#: the full §10/m49 matrix: two ttbar regions and ttgamma, each over its five corpus variations
MATRIX: tuple[tuple[str, str], ...] = tuple(
    (f"ttbar_{region}", label)
    for region in ("4j1b", "4j2b")
    for label in ("nominal", "jes_up", "jes_down", "btag_up", "btag_down")
) + tuple(("ttgamma", label) for label in ("nominal", "jes_up", "jes_down", "pho_up", "pho_down"))


@dataclass
class CorpusEvents:
    """A `graphed.write.PartitionedSource` over an in-memory awkward array, counting its own reads.

    The count is per-PROCESS state: a worker mutates its unpickled copy, so the driver's copy stays
    empty exactly when the reads happened somewhere else.
    """

    data: ak.Array
    part_reads: list[tuple[int, int]] = field(default_factory=list)

    def __call__(self) -> ak.Array:
        raise AssertionError("the whole-dataset loader must never run during a plan")

    def partitions(self, steps_per_file: int = 1) -> tuple[Partition, ...]:
        return tuple(Partition.blind("corpus://events", "", s, steps_per_file) for s in range(steps_per_file))

    def read_partition(self, partition: Partition, columns: Any, resources: WorkerResources) -> ak.Array:
        part = partition.resolve(len(self.data))
        self.part_reads.append((part.entry_start, part.entry_stop))
        return self.data[part.entry_start : part.entry_stop]


def _session(events: ak.Array) -> tuple[Session, Any, CorpusEvents]:
    session = Session(AwkwardBackend())
    source = CorpusEvents(events)
    form = AwkwardForm(ak.Array(events.layout.to_typetracer(forget_length=True)))
    return session, session.source("events", form=form, data=source), source


def _stable(values: Array) -> Array:
    """The corpus's pre-fill 6-decimal rounding as a recorded ufunc (`np.round`'s own lowering)."""
    return np.rint(values * 1e6) / 1e6


# ---- the 15-reference matrix (§10/m49(ii)) ---------------------------------------------------
def matrix_plan(steps_per_file: int = MATRIX_PARTITIONS) -> tuple[Plan[Any], CorpusEvents]:
    """One plan carrying all 15 (output, label) slots: the JES shift re-runs the selection, the
    b-tag / photon scale factors re-weight it, and §2.4 keeps the two families siblings rather than
    a cross product."""
    _ses, events, source = _session(MATRIX_EVENTS)

    jets = graphed.vary(
        events.Jet,
        "jes",
        up=gak.with_field(events.Jet, events.Jet.pt * 1.05, "pt"),
        down=gak.with_field(events.Jet, events.Jet.pt * 0.95, "pt"),
    )
    good = jets[jets.pt > 25]
    at_least_four = gak.num(good, axis=1) >= 4
    n_b = gak.sum(good.btag > 0.7, axis=1)

    hists: dict[str, gh.boost.Histogram] = {}
    for region in ("4j1b", "4j2b"):
        selected = at_least_four & (n_b == 1) if region == "4j1b" else at_least_four & (n_b >= 2)
        sel_jets = good[selected]
        per_jet = 0.95 + 0.10 * sel_jets.btag
        btag = graphed.vary(
            gak.prod(per_jet, axis=1),
            "btag",
            up=gak.prod(per_jet * 1.03, axis=1),
            down=gak.prod(per_jet * 0.97, axis=1),
        )
        h = gh.boost.Histogram(bh.axis.Regular(40, 0, 800), storage=bh.storage.Double())
        h.fill(_stable(gak.sum(sel_jets.pt, axis=1)), weight=[btag])
        hists[f"ttbar_{region}"] = h

    photons = events.Photon[events.Photon.pt > 20]
    muons = events.Muon[events.Muon.pt > 30]
    good_jets = jets[jets.pt > 25]
    selected = (
        (gak.num(photons, axis=1) >= 1) & (gak.num(muons, axis=1) >= 1) & (gak.num(good_jets, axis=1) >= 2)
    )
    photon_pt = _stable(gak.drop_none(gak.firsts(photons[selected].pt)))
    pho = graphed.vary(
        gak.full_like(photon_pt, 0.98),
        "pho",
        up=gak.full_like(photon_pt, 1.01),
        down=gak.full_like(photon_pt, 0.95),
    )
    h = gh.boost.Histogram(bh.axis.Regular(30, 0, 300), storage=bh.storage.Double())
    h.fill(photon_pt, weight=[pho])
    hists["ttgamma"] = h

    return gh.plan(hists, steps_per_file=steps_per_file), source


@cache
def corpus_reference(output: str, label: str) -> Any:
    """The slot's reference recomputed IN-PROCESS from plain awkward + hist (`graphed_corpus`)."""
    from graphed_corpus import ttbar_region, ttgamma_region  # noqa: PLC0415

    if output == "ttgamma":
        return ttgamma_region(MATRIX_EVENTS, variation=label)
    return ttbar_region(MATRIX_EVENTS, region=output.removeprefix("ttbar_"), variation=label)


# ---- the §8.2 rendering fixtures -------------------------------------------------------------
def _poison(factor: Array) -> Array:
    """A user arithmetic op over operands of different lengths — the failure every rendering anchor
    raises. One spelling for all three, so a `variation` difference can only come from WHERE in the
    label topology the node sits."""
    return factor[factor > POISON_CUT] + factor


def poisoned_program(where: str) -> tuple[Any, Array]:
    """A varied value whose failing node sits in exactly one region of the label topology, and that
    node itself.

    ``jes_up`` puts it in one universe's chain; ``shared`` puts it in a node UPSTREAM of the fork
    that both varied members consume and the nominal member does not (§3.4's sharing shape);
    ``nominal`` puts it in the vary target itself, which only the nominal cone reaches.
    """
    _ses, events, _source = _session(POISON_EVENTS)
    scalar_pt = gak.sum(events.Jet.pt, axis=1)
    poison = _poison(scalar_pt)

    if where == "jes_up":
        return graphed.vary(scalar_pt, "jes", up=poison, down=scalar_pt * 0.95), poison
    if where == "shared":
        return graphed.vary(scalar_pt, "jes", up=poison + 1.0, down=poison * 3.0), poison
    if where == "nominal":
        return graphed.vary(poison, "jes", up=scalar_pt * 1.05, down=scalar_pt * 0.95), poison
    raise ValueError(where)  # pragma: no cover - the parametrization is closed


def poisoned_plan(where: str, *, steps_per_file: int = 2) -> Plan[Any]:
    varied, _poison_node = poisoned_program(where)
    h = gh.boost.Histogram(bh.axis.Regular(20, 0, 800), storage=bh.storage.Double())
    h.fill(varied)
    return gh.plan({"ht": h}, steps_per_file=steps_per_file)


def record_cone(array: Array) -> set[int]:
    """Every record node id reachable from ``array`` — the whole cone §8.2(i)'s producer walks."""
    reached: set[int] = set()

    def visit(node_id: int, *_rest: Any) -> None:
        reached.add(node_id)

    array.session.walk(array, source=visit, op=visit, external=visit)
    return reached


def healthy_plan(*, steps_per_file: int = 2) -> Plan[Any]:
    """The same shape with no poison: the control that the fixture's failures come from the poison
    and not from the varied lowering itself."""
    _ses, events, _source = _session(POISON_EVENTS)
    scalar_pt = gak.sum(events.Jet.pt, axis=1)
    varied = graphed.vary(scalar_pt, "jes", up=scalar_pt * 1.05, down=scalar_pt * 0.95)
    h = gh.boost.Histogram(bh.axis.Regular(20, 0, 800), storage=bh.storage.Double())
    h.fill(varied)
    return gh.plan({"ht": h}, steps_per_file=steps_per_file)
