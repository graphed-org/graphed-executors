"""m49/G2 — the labelled `StageError` across a REAL process boundary, and both §8.2 renderings.

Only this repo can run these: the label travels to a worker on `_PartitionReduce.variation_labels`,
the failure is attributed inside `evaluate_ir` there, and the `StageError` must arrive in the driver
pickled intact with the label AND the user's analysis line (§8.2 (i)-(iii), M6 contract extended).

All three placements raise the SAME user arithmetic op from the same source line, so the only thing
that differs between them is where that node sits in the label topology: one universe's chain, a
node upstream of the fork both varied members consume, and the vary target only the nominal cone
reaches. §7.4's dead-letter half rides the crossed error, because the descriptor's `error_message`
is `str(exc)` and no dead-letter edit is an m49 target.
"""

from __future__ import annotations

from functools import cache

import graphed
import graphed.debug as gd
import graphed_histogram as gh
import m49_variation_analyses as A
import pytest
from graphed.checkpoint import dead_letter_descriptor

from graphed_executors.local import ProcessPoolExecutor

#: where the poison sits -> the §8.2 rendering of the labels that key carries
RENDERINGS = (("jes_up", "jes_up"), ("shared", "jes_down,jes_up"), ("nominal", ""))

PLACEMENTS = tuple(where for where, _rendering in RENDERINGS)


@cache
def _crossing_failure(where: str) -> gd.StageError:
    with pytest.raises(gd.StageError) as info:
        ProcessPoolExecutor(max_workers=2).run(A.poisoned_plan(where))
    return info.value


@pytest.mark.parametrize(
    ("where", "reaching"),
    (("jes_up", {"jes_up"}), ("shared", {"jes_down", "jes_up"}), ("nominal", {"nominal"})),
)
def test_the_poison_sits_where_the_rendering_anchor_claims(where: str, reaching: set[str]) -> None:
    """The premise the renderings above turn on, measured rather than asserted in prose: which
    universes' record cones reach the failing node. If `shared` ever degenerated to a single
    universe the multi-label anchor would silently become a second single-label one."""
    varied, poison = A.poisoned_program(where)
    labels = graphed.labels(varied)
    assert {
        label for label in labels if poison.node_id in A.record_cone(graphed.universe(varied, label))
    } == reaching


def test_the_unpoisoned_program_runs_across_the_boundary() -> None:
    """The control for every failure below: the varied program's own lowering crosses the process
    boundary cleanly, so the errors those tests catch come from the poison and nothing else."""
    result = ProcessPoolExecutor(max_workers=2).run(A.healthy_plan())
    assert sorted(gh.unpack(result.value)["ht"]) == ["jes_down", "jes_up", "nominal"]


@pytest.mark.parametrize(("where", "rendering"), RENDERINGS)
def test_the_worker_failure_arrives_labelled_and_source_mapped(where: str, rendering: str) -> None:
    """§8.2: a failure inside one universe re-raises driver-side carrying the rendered label AND the
    user's line. `shared` pins the MULTI-LABEL rendering (sorted, comma-joined) that a
    pick-one-arbitrarily implementation fails; `nominal` pins the EMPTY-TUPLE rendering, the single
    encoding of nominal/unvaried — rendering it `"nominal"`, or skipping the wrap because its label
    tuple is empty, is red here."""
    err = _crossing_failure(where)
    assert err.variation == rendering
    assert err.cause_type == "ValueError"  # the real worker failure, not a stringified blob
    assert err.user_frame.function == "_poison"
    assert "POISON_CUT" in err.user_frame.source


@pytest.mark.parametrize("where", PLACEMENTS)
def test_the_crossed_error_renders_the_user_analysis_not_the_worker(where: str) -> None:
    """M6's contract is extended, not altered (§8.2): the rendered traceback still points at the
    analysis and never at the pool that ran it."""
    rendered = gd.format_traceback(_crossing_failure(where))
    assert "m49_variation_analyses.py" in rendered
    assert "multiprocessing" not in rendered and "concurrent" not in rendered


def test_a_shared_node_failure_names_both_labels_and_not_the_third() -> None:
    """The set-valued half of §8.2(i)'s key space, surviving the crossing: the poisoned node is
    upstream of the fork and consumed by BOTH varied members, so its key carries both labels — and
    not the nominal member, whose own chain never reaches it."""
    err = _crossing_failure("shared")
    assert sorted(err.variation.split(",")) == ["jes_down", "jes_up"]
    assert "nominal" not in err.variation


def test_the_label_reaches_the_dead_letter_descriptor() -> None:
    """§7.4: the guilty label is named on the dead-letter surface with no dead-letter edit, because
    the descriptor's `error_message` is `str(exc)` and §8.1 puts the variation into `summary()`.
    The structured half keeps its fixed key list and gains NO variation key — an absence whose
    positive control is the `error_message` assertion on the same descriptor."""
    err = _crossing_failure("jes_up")
    partition = A.poisoned_plan("jes_up").tasks[0].partition
    descriptor = dead_letter_descriptor("m49-jes-up", partition, err)
    assert "jes_up" in descriptor["error_message"]
    assert descriptor["error_type"] == "StageError"
    assert descriptor["stage_error"]["user_line"] == err.user_frame.lineno
    assert "variation" not in descriptor
    assert "variation" not in descriptor["stage_error"]
