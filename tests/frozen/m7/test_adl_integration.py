"""ADL queries 1-8 run end-to-end through BOTH executors (plan M7).

Each query is recorded through graphed + executed per partition, and the per-chunk histograms are
tree-reduced across workers/processes to a single histogram that must match the single-pass result
BIT-FOR-BIT. This is a fundamentally deeper integration than the graphed-awkward (M3) per-`materialize`
tests: it exercises the full record -> chunked execute -> tree-reduce path under real concurrency, for
the whole ADL functional ladder (column histogram, MET cuts, object selection, jet/lepton
combinatorics, dilepton+MET, tri-jet, jet-lepton isolation, 3-lepton mT)."""

from __future__ import annotations

import functools

import adl
import numpy as np
import pytest
from graphed.core import Plan

from graphed_exec_local import ProcessExecutor, ThreadExecutor


def _plan(qname: str, n_chunks: int) -> Plan:
    bins, start, stop = adl.adl_axis(qname)
    return Plan(
        process=functools.partial(adl.adl_partial, qname, bins, start, stop),
        combine=adl.hist_add,
        empty=functools.partial(adl.hist_zero_n, bins),
        tasks=adl.adl_partitions(n_chunks),
    )


@pytest.mark.parametrize("Ex", [ThreadExecutor, ProcessExecutor])
@pytest.mark.parametrize("qname", adl.ADL_NAMES)
def test_adl_query_via_executor_matches_single_pass(Ex: type, qname: str) -> None:
    bins, start, stop = adl.adl_axis(qname)
    full = adl.adl_full_counts(qname, bins, start, stop)
    r = Ex(max_workers=4).run(_plan(qname, n_chunks=6))
    assert np.array_equal(r.value, full), f"{qname} via {Ex.__name__}: chunked != single pass"
    assert r.n_combines == 5  # 6 chunks -> 5 tree combines


def test_adl_result_is_invariant_to_partition_count() -> None:
    # every query reduces to the identical histogram however the events are chunked
    for qname in adl.ADL_NAMES:
        bins, start, stop = adl.adl_axis(qname)
        full = adl.adl_full_counts(qname, bins, start, stop)
        for n_chunks in (1, 4, 13):
            r = ThreadExecutor(max_workers=4).run(_plan(qname, n_chunks))
            assert np.array_equal(r.value, full), f"{qname}: {n_chunks} chunks changed the histogram"


def test_the_adl_ladder_is_non_vacuous() -> None:
    # guard against a trivially-passing suite: the ladder must actually fill histograms
    nonempty = 0
    for qname in adl.ADL_NAMES:
        bins, start, stop = adl.adl_axis(qname)
        if int(adl.adl_full_counts(qname, bins, start, stop).sum()) > 0:
            nonempty += 1
    assert nonempty >= 7  # q1-q7 always populate; q8 (3-lepton) may be rare at this sample size
