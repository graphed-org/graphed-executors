"""M41 theme (d) — no data-path regression on the ADL ladder when the NEW budgeted/coalesced fetch
plane engages (plan §8 M41, R20.7; decomposition §3(d)).

A regression sentinel, made NON-VACUOUS by routing real ADL query output THROUGH the M41 fetch plane
with the budget deliberately SMALL so spill engages: each query's per-event values are repartitioned
(``run_repartition`` with a tiny ``fetch_budget_bytes``), gathered back through the spilling reader
plane, and re-histogrammed. Because repartition is row-conserving, the histogram must be BIT-FOR-BIT the
M7 golden (``adl.adl_full_counts``) — AND ``fetch_spill_count > 0`` must witness the plane actually ran.

Non-vacuity on HEAD: ``fetch_budget_bytes`` is not a parameter -> ``TypeError`` — the "budget engaged
during ADL" precondition cannot be met, so the setup fails (right reason: the plane is absent).
Post-T1 it discriminates a spill that corrupts / reorders / duplicates rows (the histogram diverges)
and refuses a no-op budget (``fetch_spill_count == 0`` -> the plane never engaged -> not a vacuous pass).

Queries: the reliably-dense ones (q1=6000, q2=21022, q3=4637, q7=6000 flat values) so a small budget
always forces a spill; the flatten-to-values extraction is proven == the golden before repartition, so a
divergence after repartition can only come from the fetch plane.
"""

from __future__ import annotations

import adl
import awkward as ak
import numpy as np
import pytest
from graphed import Session
from graphed.awkward import AwkwardBackend, from_awkward
from graphed_corpus import make_events
from graphed_corpus.histograms import hist1d

from graphed_executors.local.shuffle import run_repartition

_N_SRC = 8
_WORKERS = 4
_FETCH_BUDGET = 16_000  # << any dense query's single-dest gather -> the reader plane must spill
_QUERIES = ["q1", "q2", "q3", "q7"]


def _numpy_backend():  # type: ignore[no-untyped-def]
    from graphed.numpy import NumpyBackend  # noqa: PLC0415

    return NumpyBackend()


def _flat_values(qname: str) -> np.ndarray:
    s = Session(AwkwardBackend())
    ev = from_awkward(s, "events", make_events(n_events=adl.N_EVENTS, seed=adl.SEED))
    vals = s.materialize(adl.ADL[qname](ev))
    return np.asarray(ak.to_numpy(ak.flatten(vals, axis=None)), dtype=np.float64)


def _value_blocks(values: np.ndarray, n_src: int) -> list[np.ndarray]:
    # all __joinkey__ == 0 -> every row routes to ONE dest, so its gather exceeds the tiny budget and
    # the reader-plane spill engages; ``val`` rides along and must survive byte-for-byte.
    dt = np.dtype([("__joinkey__", np.uint64), ("val", np.float64)])
    out: list[np.ndarray] = []
    for chunk in np.array_split(values, n_src):
        block = np.zeros(len(chunk), dtype=dt)
        block["val"] = chunk
        out.append(block)
    return out


@pytest.mark.parametrize("qname", _QUERIES)
def test_adl_histogram_is_bit_for_bit_after_routing_through_the_spilling_fetch_plane(qname, tmp_path) -> None:  # type: ignore[no-untyped-def]
    bins, start, stop = adl.adl_axis(qname)
    golden = np.asarray(adl.adl_full_counts(qname, bins, start, stop), dtype=np.int64)

    values = _flat_values(qname)
    pre = np.asarray(hist1d(values, bins=bins, start=start, stop=stop, name=qname).values(), dtype=np.int64)
    assert np.array_equal(pre, golden), "extraction sanity: the flat values must histogram to the M7 golden"

    be = _numpy_backend()
    blocks = _value_blocks(values, _N_SRC)
    res = run_repartition(
        be,
        blocks,
        parts=8,
        workers=_WORKERS,
        comms="ipc",
        store_root=tmp_path,
        fetch_budget_bytes=_FETCH_BUDGET,  # HEAD: TypeError (knob absent) — precondition unmet, setup fails
    )

    # (the plane actually engaged — else this is a vacuous pass).
    assert res.witness.fetch_spill_count > 0, f"{qname}: the fetch-plane spill never engaged during the ADL run"

    # (result transparency) — the gathered values re-histogram BIT-FOR-BIT to the golden.
    gathered = np.concatenate([res.value[d]["val"] for d in sorted(res.value)])
    post = np.asarray(hist1d(gathered, bins=bins, start=start, stop=stop, name=qname).values(), dtype=np.int64)
    assert np.array_equal(post, golden), f"{qname}: the spilling fetch plane changed the histogram"
