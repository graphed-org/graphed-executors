# m43 — Parity fixup (post-review)

Review-mandated FROZEN parity round (G1, after the 3× APPROVE): the join edge families previously
witnessed only in `tests/extra/m43` are pinned frozen here. One new module,
`test_dask_join_parity_edges.py` (5 test functions, 24 collected tests across how × both
backends) — added as a NEW file because `tests/frozen/m43/` is
frozen: `git diff freeze-m43 -- tests/frozen` shows ONLY added files (this note replaces an
in-place README edit for the same reason).

Same disciplines as the original suite: both real backends, R0.10a (hashes/multisets/counts,
never clocks), `importorskip("distributed")`/`importorskip("pandas")`, tier-A module cluster,
cross-engine oracle = the LOCAL `run_join` on identical inputs, pandas as the independent
relational oracle where the relation is defined. Scenario keys derive from the measured sha256
route at parts=4 (dest0 ← {3,7,9,18}, dest1 ← {2,6,8}, dest2 ← {0,1,5}, dest3 ← {4,12,14}),
re-verified in-test via the backend's own `partition` (non-vacuity guards).

## Tests → wrong implementation discriminated (concrete, mutation-ready)

| Test | Scenario | WRONG impl it FAILS |
|---|---|---|
| `test_broadcast_never_matched_build_tail_appears_exactly_once` (left/right/outer) | (a) whole build side matches nothing, 3 probe blocks, broadcast=True | re-emitting the unmatched build tail once PER PROBE BLOCK (count 3, not 1 — the m40 F2 N-fold bug resurfacing on dask); dropping the tail entirely (count 0); a tail whose hashes/keying diverge from local |
| `test_shuffle_one_sided_dests_null_fill_and_match_local` (left/right/outer) | (b) dests 1/2/3 pure one-sided, dest 0 two-sided with build duplication | skipping a one-sided dest (m40 F1 — the null-bearing oracle rows go missing); `-1`-sentinel `take` (a real value where the oracle has None); a null join key on a survivor row (coalescing lost); any hash divergence from local |
| `test_shuffle_partitionless_probe_side_keeps_build_rows` (left/outer) | (c) right=[] shuffle path | crashing on the schema-less side; silently dropping build rows (multiset ≠ input); emitting them other than exactly once; hash divergence from local |
| `test_broadcast_empty_build_side_returns_gracefully` (inner/outer) | (d) left=[] broadcast=True | crashing instead of the early-empty return; fabricating output (value non-empty while local's is `{}`) |
| `test_broadcast_empty_probe_side_no_crash_equals_local` (left/outer) | (e) right=[] + non-empty build, broadcast=True | the reviewed IndexError regression (probe schema-carrier indexed on an empty list); any divergence from local's documented emit-nothing ceiling |

Note on (e): the pin is deliberately **equality-to-local**, not relational content — the local
engine documents (ponytail ceiling in `_run_broadcast_join`) that this forced, cost-model-
unreachable combination leaves the unmatched build rows unemitted. If that ceiling is ever
lifted, BOTH engines must move together and this test still holds.

## Post-implementation sanity (differs from the pre-impl round by design)

- All 24 new tests PASS on current HEAD c800bf5 (the implementation exists; this is a coverage
  fixup, not a red suite). The original 38 tests are byte-untouched
  (`git diff freeze-m43 -- tests/frozen` = added files only).
- Collection deterministic: `pytest tests/frozen/m43 --co -q` exit 0, byte-identical twice.
- `ruff check tests/frozen/m43` clean.
- Frozen-only coverage (m42+m43 frozen, `.coveragerc-dask`): see the commit message / test-author
  report for the measured `dask_backend/shuffle.py` line+branch figure (goal ≥90% from frozen
  alone; measured after this fixup).
