# M41 Test Dispute — frozen-suite corrections (N1, N2, N3)

- **Raised by:** M41 adversarial review (task w35bi5nk9, VERDICT APPROVE with non-blocking notes N1–N3).
- **Independently verified by:** team-lead, against the live frozen suite @ `freeze-M41-0` (evidence below).
- **Scope:** three corrections inside `tests/frozen/m41/`. **None weakens discrimination** — one fixes an
  order-dependent measurement, one removes a provable tautology, one fixes a doc overclaim. The four
  Implementation Targets (T1–T4) and every real discriminator are untouched.
- **Resolver:** an **impl-blind** test-author (must NOT read `graphed_executors/local/shuffle.py` internals;
  the pinned public API in the m41 README is the only contract they need). Re-freeze after.

---

## N1 — `test_fetch_spill.py:97` tracemalloc ceiling is order-dependent (a TEST_SANITY defect)

**Defect.** The belt-and-suspenders `tracemalloc` net
`assert tm_peak <= 3*_DEST_BYTES + 4*_FETCH_BUDGET` (ceiling **1,792,000**) measures the WHOLE process's
peak Python allocation *since `tracemalloc.start()`*. On the FIRST such window in a process, that peak
includes one-time import/arena first-touch growth (~0.9 MB) that later windows do not.

**Evidence (measured, venv-m40, @ freeze).**
- In isolation (`pytest tests/frozen/m41/test_fetch_spill.py`): **FAILS** — `measured peak 2,693,523`
  exceeds the ceiling `1,792,000`.
- Full-dir (`pytest tests/frozen/m41/`): **PASSES** — but only because `test_adl_no_regression` sorts
  first alphabetically and warms the allocator before `test_fetch_spill` runs.

So the frozen suite is green in the grader's canonical per-subtree order but red in isolation / under a
different collection order (`pytest-randomly`, a single-file run). That contradicts §B.3 TEST_SANITY
("deterministic across two runs") and the project determinism ethos.

**Spec clause it contradicts.** §B.3 mechanical gate `determinism` + TEST_SANITY "deterministic across two
runs"; root CLAUDE.md "tests shall be … deterministic."

**Proposed correction (do NOT loosen the ceiling).** Make the measured window reflect the *operation's*
steady-state peak, not one-time process warm-up — e.g. run one throwaway `run_repartition(...)` (public
API) to warm imports/allocator BEFORE `tracemalloc.start()`, or `tracemalloc.reset_peak()` after a warm
call. The ceiling constant and every REAL discriminator (`peak_fetch_bytes <= budget+one_block`,
`fetch_spill_count/…_bytes > 0`, the independent on-disk `du`, the routing-oracle equality) stay exactly
as-is. **Acceptance:** the single test passes BOTH in isolation AND in canonical full-dir order, and the
warm-up must not swallow the discrimination (the ceiling still rejects a grossly many-copy impl).

---

## N2 — `test_incast.py:65` is a definitional tautology (vacuous assertion)

**Defect.**
```python
assert abs(w.bulk_fetch_count * w.bytes_per_fetch - w.bytes_transferred) <= w.bytes_transferred * 0.05
```
`bytes_per_fetch` is *defined* as `bytes_transferred / bulk_fetch_count` (the pinned contract, README
row `bytes_per_fetch`; impl computes it exactly so). Therefore
`bulk_fetch_count * bytes_per_fetch == bytes_transferred` holds **unconditionally**, for every impl,
including the very "batch the counter but transfer per-block" trap the comment claims it catches (that
trap sets a low count + a high total; the ratio times the count still reproduces the total). The
assertion can never fail → it discriminates nothing.

**Spec clause it contradicts.** §A.7 / root CLAUDE.md: "Tests … shall always be provably non-vacuous and
discriminating."

**Proposed correction.** Either (a) REMOVE the tautology (removing a provable tautology cannot weaken
discrimination — it has none) and reword the "(few LARGE streams)" comment to point at the assertions
that actually carry that meaning; or (b) replace it with a genuine coalescing-magnitude check that is not
implied by the existing `bulk_fetch_count < n_blocks` bound. The real T2 discriminators — line 52
(`bulk_fetch_count <= 2*K*K`), line 57 (`< n_blocks`), line 69 (`bytes_transferred >= result/2`), line 75
(routing oracle) — remain untouched. Impl-blind option (a) is the lowest-risk (it cannot over-constrain
an impl the resolver can't see).

---

## N3 — `README.md:58` overclaims the routable-sim endpoints

**Defect.** The file→theme table / sim block claims the spawned children serve
`GET /blob/{hash}, PUT /blob, GET+PUT /manifest/… — the §4.3 endpoints`. The actual sim child HTTP server
serves **only** `GET /blob/{hash}` and `PUT /blob` — there is no `/manifest` handler. (Verified against the
pinned public sim contract: `RoutableClusterSim` exposes `.put`/`.fetch` of blobs only; manifests in the
M39/M41 flow go through the in-process cluster, never this cross-process HTTP sim.)

**Spec clause it contradicts.** Root CLAUDE.md "base every claim on measured facts; no vacuous claims" —
the traceability README must describe what the suite actually exercises.

**Proposed correction.** Drop `GET+PUT /manifest/…` from the endpoint list on line 58 (and the parallel
mention if any), scoping it to the blob endpoints the sim genuinely serves. Documentation-only; no test
behaviour changes.

---

## Resolution log

**Resolved by:** impl-blind test-author (did NOT read `graphed_executors/local/shuffle.py` or any impl
source; contract = the pinned public API in `tests/frozen/m41/README.md` + the existing frozen tests).
All verification is black-box (pytest). No git-commit/tag — lead re-freezes.

### N1 — `test_fetch_spill.py` (order-dependent measurement fixed; ceiling constant untouched)
Inserted a throwaway warm-up over the SAME code path (public `run_repartition`, `fetch_budget_bytes`
engaged, its own `store_root=tmp_path/"warmup"`) BEFORE `tracemalloc.start()`, so the measured window
excludes one-time import/arena first-touch growth. Diff:
```
+    warm_root = tmp_path / "warmup"
+    warm_root.mkdir()
+    run_repartition(BACKEND, src, parts=_PARTS, workers=_WORKERS, comms="ipc",
+                    store_root=warm_root, fetch_budget_bytes=_FETCH_BUDGET)
+
     tracemalloc.start()
```
The ceiling `tm_peak <= 3*_DEST_BYTES + 4*_FETCH_BUDGET` (1,792,000) and every real discriminator
(`peak_fetch_bytes <= budget+one_block`, `fetch_spill_count/…_bytes > 0`, on-disk `du`, oracle
equality) are byte-for-byte unchanged.

**Order-robustness (measured, venv-m40):**
- isolation (cold): `pytest tests/frozen/m41/test_fetch_spill.py -p no:cacheprovider -q` → `1 passed`
- canonical full-dir: `pytest tests/frozen/m41/ -p no:cacheprovider -q` → `9 passed`
- reversed (fetch first, then incast): `... test_fetch_spill.py test_incast.py ...` → `2 passed`

**Non-vacuity PRESERVED (probe, not committed):** injecting a live `bytearray(3*_DEST_BYTES)` into the
measured window (a many-copy impl) yields `measured peak 2068752 > ceiling 1792000` → the ceiling
assertion fires. The warm-up removes only one-time growth; per-operation copies are re-allocated inside
the measured window and are still caught. Probe reverted; `git status` shows no probe artifacts.

### N2 — `test_incast.py` (provable tautology removed; real discriminators untouched)
Removed `assert abs(w.bulk_fetch_count * w.bytes_per_fetch - w.bytes_transferred) <= w.bytes_transferred
* 0.05`. **Why it was a tautology (one sentence):** the README pins `bytes_per_fetch =
bytes_transferred / bulk_fetch_count`, so `bulk_fetch_count * bytes_per_fetch` reduces algebraically to
`bytes_transferred` and the difference is identically ~0 (float rounding only) for every impl — it can
never fail. The comment was reworded to point at the assertions that actually carry "few large streams":
the `bulk_fetch_count` bounds (`<= 2*K*K`, `< n_blocks`) above and the completeness floor
(`bytes_transferred >= result/2`) + routing oracle below. `bytes_transferred > 0` kept. T2's four real
discriminators (`<= 2*K*K`, `< n_blocks`, `>= result/2`, oracle) are unchanged. `test_incast.py` →
`1 passed`.

### N3 — `README.md:58` (doc overclaim fixed; doc-only)
`(GET /blob/{hash}, PUT /blob, GET+PUT /manifest/… — the §4.3 endpoints)` →
`(GET /blob/{hash}, PUT /blob — the §4.3 blob endpoints)`. No test behaviour changes. (No parallel
`/manifest` sim-endpoint mention exists elsewhere in the README; the `manifest_bytes` row on line 44 is
an unrelated M39 witness field, left as-is.)

### Gates
- `ruff check tests/frozen/m41/` → `All checks passed!`
- No discriminator weakened, no budget/ceiling constant changed, no other test touched.
