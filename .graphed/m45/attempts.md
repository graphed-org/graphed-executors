# m45 implementer — attempts log

Milestone m45: the unified `shuffle_method` facade (`graphed_executors.dask_backend.api`) — one entry
point dispatching over the frozen m43 (as-tasks) and m44 (transport) dask shuffle engines.
Frozen suite: `tests/frozen/m45/` (13 tests, freeze tag `freeze-m45` = b8a2dca). Design authority:
`m45-facade-plan.md` (r2). Binding test-author interpretations: `m45-test-author-report.md` §4.

## Design (thin dispatcher — zero engine logic)

Read the plan + all 4 frozen modules + harness + README + test-author report before writing. Confirmed
the four engine signatures I forward to (measured, not assumed):
- `dask_run_repartition(backend, src, parts, *, runner, salt=0)` — m43, takes a `SubmitRunner`.
- `dask_run_join(backend, left, right, parts, *, on, how, runner, broadcast, salt, mem_budget_bytes)` — m43.
- `transport_run_repartition(backend, src, parts, *, dbackend, salt, n_tasks, fetch_budget_bytes,
  disk_budget_bytes, holder_budget_bytes, pull_timeout_s, epoch_restarts_allowed=1)` — m44.
- `transport_run_join(backend, left, right, parts, *, on, how, dbackend, broadcast, salt, mem_budget_bytes,
  holder_budget_bytes, pull_timeout_s, epoch_restarts_allowed=1)` — m44.
- `ShuffleResult`(dest_block_hashes/value/**partitions**/witness) vs
  `TransportShuffleResult`(dest_block_hashes/value/witness/**transport**) — the union + observability markers.

Facade (`api.py`, ~200 lines incl. docstrings):
1. `resolve_shuffle_method(method, caps)` — pure: transport/tasks stand; auto → transport iff
   `pin_to_worker AND peer_data_movement` else tasks; invalid → `ValueError` naming the three values.
2. `_reject_transport_only_knobs(resolved, {name: value})` — when resolved=="tasks", raise the FIRST
   explicitly-set (`value is not None`) transport-only knob, in declared order, naming it. No-op otherwise.
3. `run_repartition` / `run_join` — resolve → validate knobs → dispatch. Transport path forwards all
   transport knobs (mapping `epoch_restarts_allowed=None → 1`, the engine default); tasks path builds
   `SubmitRunner(dbackend)` and forwards only the common knobs. `mem_budget_bytes` is COMMON on join
   (forwarded on BOTH paths — the r1-draft spurious-ValueError bug the plan caught).

Key decisions / interpretations honored:
- **F13 dask-free at load**: only the two result dataclasses (+ `SubmitCapabilities` for the annotation)
  imported at top (all dask-free); engine-run fns + `SubmitRunner` deferred into function bodies.
- **No `runner=` knob** (plan r2): always `SubmitRunner(dbackend)` internally.
- **epoch_restarts_allowed explicit-set** (test-author §4.3): facade default None; `=0` is explicit and
  raises on tasks (detected by `is not None`, not truthiness).
- **Self-registration** (§4.1): the facade does NOT self-register the transport plugin — the suite
  pre-registers it (idempotent), and adding a register would be engine logic the plan calls out as
  optional. Left to the engine's own `ensure_engine_plugins`.
- **__init__**: eager re-export of `run_repartition`/`run_join`/`resolve_shuffle_method`; module docstring
  now LEADS with the facade as the front door (front-door discoverability, plan §1/§3).

## Iterations

### Iteration 1 — facade + wiring, all gates green first pass

- Wrote `api.py`, updated `__init__.py` (eager re-exports + front-door docstring), added the "Choosing an
  engine" subsection to `docs/design.rst`, wired `tests/frozen/m45` into the test-dask CI job (main matrix
  already runs the whole `tests/frozen` tree, so m45's dask-free resolution/gates tests auto-run there —
  same as m44's capability gate; the cluster dispatch/join tests importorskip out of the main matrix and
  run in test-dask, covering api.py under `.coveragerc-dask`).
- Frozen m45: **13/13 pass** first try (7 non-cluster resolution+gates, 6 cluster dispatch+join).
- F13 dask-free probe: `import graphed_executors.dask_backend.api` leaves distributed/dask out of
  sys.modules → CLEAN.
- ruff check clean · ruff format clean (28 files) · mypy --strict clean (24 files — the union return
  annotation typechecks) · sphinx -W 0 warnings.
- Frozen integrity: `git diff freeze-m45 -- tests/frozen` = 0 bytes.

## FINAL GATE TABLE (measured; evidence in /private/tmp/claude-501/m45-rem/)

| Gate | Result | Evidence |
|---|---|---|
| Frozen m45 | 13/13 (7 resolution+gates main-matrix, 6 dispatch+join cluster) | det1.out |
| Determinism (2 consecutive frozen-m45 runs) | 13/13 == 13/13; dispatch test also pins byte-identical `dest_block_hashes` ×2 | det1.out, det2.out |
| Wired dask coverage (m42+m43+m44+m45, `.coveragerc-dask`) | 205 passed; **api.py 100%** (37 stmt / 14 branch, 0 miss — covered by the FROZEN suite); scoped total 93.20% ≥ 90 | cov.out / cov.json |
| Whole frozen tree (`pytest tests/frozen`) | 516 collected, 515 passed / 1 skipped (pre-existing m37 perspective), 0 failures/errors, rc=0 | whole.out, whole.xml |
| F13 dask-free import | `import graphed_executors.dask_backend.api` leaves distributed/dask out of sys.modules (subprocess probe green) | test_shuffle_method_resolution |
| Main-matrix path (no distributed installed) | resolve + gate refusals fire before any distributed import (verified) | — |
| ruff check (`src tests`) | All checks passed | — |
| ruff format --check (`src tests`) | 28 files already formatted | — |
| mypy --strict (files=src, py3.12) | no issues in 24 files (union return `ShuffleResult \| TransportShuffleResult` typechecks) | — |
| sphinx -W | build succeeded (0 warnings) | m45-build/ |
| Integrity | `git diff freeze-m45 -- tests/frozen` EMPTY; no engine/local/protocol/frozen edits | — |

Src delta: `api.py` (new) + `__init__.py` (eager re-exports + front-door docstring). Non-code: `docs/design.rst`
("Choosing an engine" subsection), `.github/workflows/ci.yml` (m45 into the test-dask gated path).
