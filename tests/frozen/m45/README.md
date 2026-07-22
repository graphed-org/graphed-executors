# m45 frozen suite — graphed-executors (unified shuffle facade, `shuffle_method`)

Milestone **m45** (`m45-facade-plan.md`, final r2). Freezes the single-entry-point facade
`graphed_executors.dask_backend.api` — `run_repartition`/`run_join` with
`shuffle_method="auto"|"transport"|"tasks"` dispatching over the frozen m43 (as-tasks) and m44
(transport) engines. Thin dispatch only: resolution is a pure capability function, transport-only
knobs are validated loudly after resolution, results are the engines' own objects (common triple
`dest_block_hashes`/`value`/`witness`; `.transport` vs `.partitions` reveal the resolution).
**Frozen — read-only after the m45 freeze tag.**

## Files → theme (plan §2; 13 tests)

| File | Theme | What it witnesses |
|---|---|---|
| `shuffle_method_harness.py` | harness | REPLICATED fixtures (plan §2 r2 — zero cross-suite imports): caps combos + `RefusingBackend` (FULL/PINLESS/ALLFALSE), `PinlessView` (real backend, pin-less caps — the degrade-and-RUN fixture), `SpyDaskBackend` fn-name seam tap, numpy scenario builders, `facade_cluster`, hard-timeout `run_bounded`; deferred `api()` accessor (right-reason `ModuleNotFoundError` pre-impl); direct imports of both ENGINE modules as parity oracles (frozen substrate) |
| `test_shuffle_method_resolution.py` | §1.1 + items 6/10/11 (MAIN matrix) | r2-corrected truth table (explicit methods stand under ALL 4 caps combos; auto→transport only on both-flags; peer-only/pin-only/neither → tasks; invalid raises under every combo, listing all three values); facade-level invalid method ⇒ ValueError with ZERO submits/broadcasts; subprocess probe: package + api import leave `distributed`/`dask` out of `sys.modules` |
| `test_shuffle_method_gates.py` | §1.2 + items 4/5/7 (MAIN matrix) | explicit transport on pin-less ⇒ the m44 gate intact ("pin" + "Phase 2"), zero touches; auto on all-False ⇒ degrade-then-refuse via the m43 gate ("peer data movement" + "Phase 2"), both ops; every transport-only knob (6 on repartition, 3 on join) + explicit "tasks" ⇒ ValueError naming the knob on a FULL-caps refusing stub (only the facade can raise ValueError there — forward-and-ignore hits the stub's AssertionError instead); auto→tasks (pin-less) + explicit knob raises too (resolution-then-validate order) |
| `test_shuffle_method_dispatch.py` | items 1/2/3/9 | auto on DaskBackend ⇒ m44 fn-name shape (`_transport_map_task`/`_transport_gather_task` > 0, zero `_dask_*`), hashes == direct `transport_run_repartition`, `.transport` present / `.partitions` absent, byte-identical ×2; explicit "tasks" ⇒ all three `_dask_*` present, zero transport names, hashes == direct `dask_run_repartition`, `.partitions`/no-`.transport`, salt=3 forwards (== direct salt=3, != salt=0); auto on `PinlessView(real)` ⇒ RUNS the m43 engine (no error), hashes == direct, tasks shape |
| `test_shuffle_method_join.py` | items 7/8 | inner AND outer: facade("transport") == direct `transport_run_join`, facade("tasks") == direct `dask_run_join` (byte-exact hashes, shape markers); **"tasks" + `mem_budget_bytes` SUCCEEDS** with the m43 spill engaging (`join_spilled_partitions > 0` == scenario-guarded direct run, `peak_join_bytes ≤ budget`, 40 000 duplicated rows; m43 bounded-memory sizing) — the r1-draft spurious-ValueError bug, frozen unshippable |

## Non-vacuity (TEST_SANITY evidence)

Pre-implementation all **13 tests fail** rooted in `ModuleNotFoundError:
...dask_backend.api` (11 raw; the 2 `run_bounded`-driven dispatch tests surface it inside their
assertion message — same right reason). Collection deterministic (md5-equal ×2, 13 collected).
Oracle/fixture validation was run against the LIVE engines at authoring time (branch HEAD): the
replicated spy sees exactly the two engines' pinned fn-name shapes, `PinlessView` delegation
executes the m43 engine for real, the two engines agree byte-for-byte on the parity scenarios
(inner + outer), salt=3 reroutes, and the mem-budget sizing engages the m43 spill
(`join_spilled_partitions > 0`, `peak ≤ budget`) on the direct engine. Witnesses are fn-name
counts, hashes, capability tables, and zero-touch counters — never clocks; every cluster call is
hard-timeout bounded.

Wrong-implementation discrimination: hardcoding either engine fails the other method's fn-name
shape; re-implementing instead of forwarding fails byte parity; dropping a common knob fails the
salt / mem-budget arms; forward-and-ignore of transport-only knobs hits the refusing stub's
AssertionError instead of the pinned ValueError; validate-before-resolve fails the auto→tasks
knob test; a facade-invented gate message fails the intact m43/m44 refusal pins; a normalizing
result wrapper fails the `.transport`/`.partitions` markers.
