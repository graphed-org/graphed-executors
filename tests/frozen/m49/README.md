# `tests/frozen/m49` — traceability (graphed-executors, Brief G)

The **cross-repo half** of m49: what only this repo can exercise. The wrap, the attribution, the
tie-break and the `variation_labels` payload are `graphed` / `graphed-histogram` source and are
anchored in those repos' own frozen suites (§B.3 diff coverage follows the source). What lives here
is a REAL process-pool boundary.

## Enabling wiring (not a test)

`graphed-histogram` joins the `dev` extra and gets a `HISTOGRAM` git-URL env var with its
`pip install` line in the two jobs that collect `tests/frozen` (`test`, `test-experimental`) —
`ci.yml`'s existing `CORPUS` shape. `test-dask` / `test-parsl` are milestone-scoped and collect
neither tree. Omitting the line fails **silently** here: `graphed-histogram` is on PyPI, so a
name-only dev-extra entry installs the stale `0.0.1` wheel and the job tests the wrong package.

## Anchor map

| Test | Plan anchor |
|---|---|
| `test_variation_reference_matrix.py::test_the_matrix_slot_reproduces_its_corpus_reference` | §10/m49 frozen anchor (ii) — the 15-reference matrix through a process-pool executor, against `graphed_corpus` recomputed in-process (m7 house pattern), exercising §4.2/§6.1/§6.2's varied-fill lowering rather than materialize-then-fill |
| `…::test_the_fifteen_references_are_pairwise_distinct` | non-vacuity of the matrix: §5.1/§5.5a — a label-collapsing implementation cannot satisfy 15 equalities against 15 distinct references |
| `…::test_every_output_carries_exactly_its_own_five_labels` | §6.1a / §2.4 — sibling lowering, no cross product, no family leak |
| `…::test_the_matrix_really_crossed_a_process_boundary` | §10/m49(ii) mechanism witness — the source counts reads in the process that performs them; the thread-pool run is the positive control |
| `…::test_the_matrix_is_invariant_to_the_partition_count` | §5.5a — the compared quantities come from PLAN RUNS at two `steps_per_file` values; `Session.materialize` is partition-blind and is never the oracle |
| `test_variation_crossing.py::test_the_poison_sits_where_the_rendering_anchor_claims` | §3.4 / §8.2(i) — the fixture premise: which universes' record cones reach the failing node |
| `…::test_the_unpoisoned_program_runs_across_the_boundary` | control for every failure below — the varied lowering itself crosses cleanly |
| `…::test_the_worker_failure_arrives_labelled_and_source_mapped` | §8.2 (i)+(ii)+(iii), §8.1 — the labelled `StageError` across a real process boundary, carrying the rendered label AND the user's line; the three parameters are §8.2's three renderings (single label, multi-label sorted and comma-joined, empty tuple → `""`) |
| `…::test_the_crossed_error_renders_the_user_analysis_not_the_worker` | §8.2 / plan §A.3 #8 — the M6 contract extended, not altered |
| `…::test_a_shared_node_failure_names_both_labels_and_not_the_third` | §8.2(i) — the set-valued key space surviving the crossing; §8.2's nominal-exclusion |
| `…::test_the_label_reaches_the_dead_letter_descriptor` | §7.4 — the label reaches the dead-letter surface with no dead-letter edit (`error_message` is `str(exc)`), and the structured half gains no variation key |

## Fixtures

`m49_variation_analyses.py` — module level so a spawned worker can unpickle everything. The poison
is a plain USER arithmetic op over operands of different lengths (the m6
`numpy_mismatch_in_fused_stage` idiom): `graphed_histogram.plan` forwards only the histograms' own
`_evaluators` and `aggregate_plan`'s `externals` defaults to `None`, so there is no seam through
which a `.map` payload could be poisoned on this path. One spelling serves all three placements, so
a difference in the rendered `variation` can only come from where the node sits in the label
topology.

## Not anchored here

The `variation_labels` payload's layout and population (`graphed-histogram`'s `tests/frozen/m49`),
the wrap's UNATTRIBUTED arm, the frame tie-break, §8.1's `__hash__` (`graphed`'s
`tests/frozen/debug/m49`), and §7.4's dead-letter MECHANISM (`graphed`'s
`tests/frozen/checkpoint/m49`).
