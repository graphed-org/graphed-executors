"""m49/G1 — the full 15-reference systematics matrix through a real process-pool executor.

The executor-level end-to-end no frozen suite discharges today (§10/m49(ii)): one plan carrying two
ttbar regions and ttgamma over five variations each — a JES shift that re-runs the selection plus
the weight-only scale factors — reduced across worker PROCESSES and compared against
`graphed_corpus` recomputed in-process (the m7 house pattern), never materialize-then-fill-eagerly,
which exercises none of §4.2/§6.1/§6.2.

Every comparison rides a PLAN RUN. `Session.materialize` is partition-blind and cannot be the
oracle for anything the partitioning can change (§5.5a).
"""

from __future__ import annotations

from functools import cache
from typing import Any

import graphed_histogram as gh
import m49_variation_analyses as A
import pytest
from graphed_corpus.histograms import bin_values, fingerprint

from graphed_executors.local import ProcessPoolExecutor, ThreadExecutor


@cache
def _run(steps_per_file: int, threaded: bool = False) -> tuple[dict[str, Any], A.CorpusEvents, int]:
    plan, source = A.matrix_plan(steps_per_file)
    executor = ThreadExecutor(max_workers=2) if threaded else ProcessPoolExecutor(max_workers=2)
    result = executor.run(plan)
    return gh.unpack(result.value), source, result.n_combines


@pytest.mark.parametrize(("output", "label"), A.MATRIX)
def test_the_matrix_slot_reproduces_its_corpus_reference(output: str, label: str) -> None:
    unpacked, _source, _combines = _run(A.MATRIX_PARTITIONS)
    got = unpacked[output][label]
    reference = A.corpus_reference(output, label)
    assert bin_values(got) == bin_values(reference), f"{output}/{label}: bin contents drifted"
    assert fingerprint(got) == fingerprint(reference), f"{output}/{label}: fingerprint drifted"


def test_the_fifteen_references_are_pairwise_distinct() -> None:
    """Non-vacuity of the matrix above: an implementation that answered `nominal` for every label,
    or collapsed one family onto another, would satisfy 15 equalities against 15 references only if
    the references themselves agreed. They do not."""
    prints = {slot: fingerprint(A.corpus_reference(*slot)) for slot in A.MATRIX}
    assert len(set(prints.values())) == len(A.MATRIX)


def test_every_output_carries_exactly_its_own_five_labels() -> None:
    """§6.1a/§2.4: the two families stay siblings, so each output carries `1 + |S| + |W|` labels and
    no cross-product term, and no family leaks onto an output its `vary` never reached."""
    unpacked, _source, _combines = _run(A.MATRIX_PARTITIONS)
    assert sorted(unpacked["ttbar_4j1b"]) == ["btag_down", "btag_up", "jes_down", "jes_up", "nominal"]
    assert sorted(unpacked["ttbar_4j2b"]) == ["btag_down", "btag_up", "jes_down", "jes_up", "nominal"]
    assert sorted(unpacked["ttgamma"]) == ["jes_down", "jes_up", "nominal", "pho_down", "pho_up"]


def test_the_matrix_really_crossed_a_process_boundary() -> None:
    """The mechanism witness, with its own positive control: the source counts its reads in the
    process that performs them, so the driver's copy stays EMPTY under the process pool and fills
    under the thread pool. A process executor that silently degraded to in-process evaluation would
    populate both."""
    _unpacked, process_source, combines = _run(A.MATRIX_PARTITIONS)
    _unpacked_t, thread_source, _combines_t = _run(A.MATRIX_PARTITIONS, threaded=True)
    assert process_source.part_reads == []
    assert len(thread_source.part_reads) == A.MATRIX_PARTITIONS
    assert combines == A.MATRIX_PARTITIONS - 1


def test_the_matrix_is_invariant_to_the_partition_count() -> None:
    """§5.5a discipline: the compared quantities come from plan runs at two `steps_per_file` values,
    the only place the partitioning is observable at all."""
    fine, _source, _combines = _run(A.MATRIX_PARTITIONS)
    coarse, _source_c, _combines_c = _run(1)
    for output, label in A.MATRIX:
        assert bin_values(fine[output][label]) == bin_values(coarse[output][label]), f"{output}/{label}"
