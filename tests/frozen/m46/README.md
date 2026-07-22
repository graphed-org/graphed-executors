# m46 frozen suite — graphed-executors (ParslBackend + relay engine + `common/` bootstrap)

Milestone **m46** (parsl-backend-plan §1.1-§1.3, §1.6, §2 m46, §3 m46 — plan FINAL at r3,
closure-verified). This suite freezes the `ParslBackend(SubmitBackend)` over DIRECT parsl executor
submit (no DFK — the three measured integration moves), per-INSTANCE capability vectors (HTEX =
the all-seven-False "parsl floor"; TPE = the ThreadBackend shape), the **relay (as-tasks/workflow)
shuffle engine** (T+P, zero picks, head-node routing honestly witnessed), and the `common/`
bootstrap (the m43 engine moved verbatim to `common/tasks_engine`; dask paths kept alive by
re-export shims — the UNTOUCHED m42–m45 frozen tree is the regression suite for that move).
**Frozen — read-only after the m46 freeze tag.**

## Files → theme (§3 m46)

| File | Theme | What it witnesses |
|---|---|---|
| `parsl_harness.py` | harness | planned-API accessors (deferred imports — right-reason MNFE pre-impl); `htex_pool`/`tpe_pool` fixtures over the PLANNED `start_htex`/`stop_htex`; the measured PYTHONPATH export (below); TWO spy seams — `SpyParslBackend` (SubmitBackend seam: RAW fn names + keys) and `executor_submit_recorder` (parsl-executor seam: the `resource_specification` parsl actually receives); numpy Shuffle/Join adapter + duplicating & null-aware oracles (route via the golden-pinned `partition`, never the join kernel); the independent `driver_relay_bytes` oracle |
| `test_parsl_no_cross_import.py` | packaging hygiene (MAIN matrix) | subprocess probes: `parsl_backend` imports pull neither parsl nor dask/distributed; `common` (tasks_engine + relay_engine) pulls none of the three; `local`/`submit` and `dask_backend` (incl. the m46 shim) stay parsl-free |
| `test_parsl_capabilities.py` | per-instance honesty | HTEX vector == all-False, TPE vector == peer-only-True, as DATACLASS EQUALITY on the frozen 7-field `SubmitCapabilities` (class-exact — no subclass, no new fields); unknown executor types (incl. the stdlib `ThreadPoolExecutor` name-trap) → loud TypeError naming both supported classes |
| `test_parsl_submit_conformance.py` | SubmitBackend conformance, BOTH executors | isinstance vs the runtime protocols; submit→result; future ARGS resolved before fn runs, with the composed task's site pid ∉ {driver} on HTEX; broadcast → RAW bytes across the process boundary; TPE receives `resource_specification == {}` (executor spy — the measured `InvalidResourceSpecification` armed); HTEX priority p≠0 → `{"priority": p}`, p=0 elided |
| `test_parsl_plan_parity.py` | m42-parity Plan path (HTEX) | `SubmitRunner(ParslBackend(HTEX))` == `SequentialRunner` bit-for-bit ×2; 12 leaves + combines all on worker pids, spanning ≥2 workers; `close()` leaves the caller's executor alive; monitor piggyback delivers 3 phases/leaf in order with worker-side identities (2·n worker events) |
| `test_parsl_relay_repartition.py` | §1.3 relay engine (HTEX) | hashes == local engine, ×2; routed content == `partition` oracle; row conservation; Counter EXACTLY `{_dask_map_write: T, _dask_gather: P}` with ZERO `_dask_pick` submits; counts row-count-independent (8× rows); map/gather sites on worker pids; `head_node_routed is True` + `driver_relay_bytes ==` the independent Σ-map-wire-sizes oracle; salt reroutes deterministically |
| `test_parsl_relay_join.py` | relay relational join (HTEX) | inner == duplicating oracle AND hashes == local `run_join`; left/right/outer == local, null-aware, with the unmatched key-13/key-17 rows present EXACTLY once each; budget arm: `peak_join_bytes <= budget`, `join_spilled_partitions > 0`, rows == n·n; gather-join sites on workers; relay witness fields on every arm |
| `test_parsl_relay_broadcast_join.py` | F6 + broadcast plane (HTEX ×{1,2} workers) | auto choice parts-keyed (SHUFFLE on a 1-worker pool where live-`n_workers()` says broadcast); 1-vs-2-worker hashes identical; forced broadcast: zero stage-1 submits, one `_dask_broadcast_join_part` per probe block; how=left unmatched tail == EXACTLY one extra part submit; forced shuffle honoured against the model |
| `test_parsl_m43_engine_on_tpe.py` | the reuse claim + honest refusal | `common.tasks_engine.dask_run_repartition/join` over `ParslBackend(TPE)`: parity + the FULL m43 Counters — repartition `{map: T, pick: T·P, gather: P}`; join `map: t_l+t_r`, **NONZERO pick t_l·P + t_r·P**, `gather_join: P` (the anti-relay signature); `ParslBackend(HTEX)` → `_require_peer` NotImplementedError naming peer data movement, ZERO submits/broadcasts, on both entry points |
| `test_parsl_worker_env.py` | the `_ParslWorkerEnv` shim seam (HTEX) | `current_env().worker` == `POOL_ID:RANK` recomputed IN-TASK from parsl's own env vars; stable per pid, distinct across pids; `open_once` == ONE token per worker process across 12 tasks (≥2 tasks/worker witnessed); emit buffering: all events, in order, worker-pid-stamped, delivered after completion |
| `test_parsl_failure_attribution.py` | §A.3 #8 | StageError raised in `process` surfaces INTACT (op + user frame) over HTEX and TPE; `describe_failure` maps SYNTHESIZED `WorkerLost`/`ManagerLost`-named exceptions (direct AND chained under `__cause__`) → a (key, worker) 2-tuple of strings; unrelated → None — the name-string contract, parsl-version-independent |
| `test_parsl_packaging_pins.py` | §1.6/§4 content pins (MAIN matrix) | the exact `parsl>=2026.7.20` extra (no marker); main omit += `*/common/*`, `*/parsl_backend/*`; `.coveragerc-parsl` == {parsl_backend, common.relay_engine} + fail_under 90 + branch (tasks_engine EXCLUDED — one gate per module); `.coveragerc-dask` += common.tasks_engine (relay/http_plane banned); test-parsl job: pyarrow installed, pytest step = bare `--cov` + `--cov-config=.coveragerc-parsl`, NO `--cov=`; dask step's path-valued `--cov=` flags GONE |

## Pinned execution contract (test-author restatement — the implementer builds exactly this)

```
graphed_executors.parsl_backend:
    ParslBackend(executor)   # a STARTED parsl HighThroughputExecutor or parsl ThreadPoolExecutor;
                             # capabilities derived from the instance's type (§1.1 table);
                             # any other executor type -> TypeError naming BOTH supported classes
    parsl_runner(executor) -> SubmitRunner
    # ParslBackend.submit resolves SubmitFuture args DRIVER-side, wraps fn in the module-level
    # shim AFTER any spy seam (witnesses see the raw fn name), sends resource_specification {} to
    # TPE always and {"priority": p} to HTEX iff p != 0; broadcast returns the payload (degenerate);
    # close() does NOT shut the caller's executor down; describe_failure matches
    # WorkerLost/ManagerLost BY CLASS NAME in the exception chain -> (key, worker) tuple, else None.
graphed_executors.parsl_backend.launch:
    start_htex(*, workers, run_dir, address=None, encrypted=False) -> HighThroughputExecutor
    stop_htex(executor) -> None
graphed_executors.common.tasks_engine:      # the §1.6 m46 move — m43 names VERBATIM
    dask_run_repartition / dask_run_join / _dask_map_write / _dask_pick / _dask_gather /
    _dask_gather_join / _dask_broadcast_join_part / DaskShuffleWitness / _require_peer / ...
graphed_executors.common.relay_engine:
    relay_run_repartition(backend, src_blocks, parts, *, runner, salt=0) -> ShuffleResult
    relay_run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",), how="inner",
                   runner, broadcast=None, salt=0, mem_budget_bytes=None) -> ShuffleResult
    # relay shape: T submits of _dask_map_write -> driver resolves + regroups by calling
    # _dask_pick LOCALLY (zero pick submits) -> P submits of _dask_gather/_dask_gather_join with
    # concrete wire args; broadcast path reuses _dask_broadcast_join_part + the once-only tail.
    RelayShuffleWitness      # the DaskShuffleWitness counters (n_producer_tasks,
                             # blocks_per_producer_task, peak_writer_buffer_bytes, broadcast_chosen,
                             # peak_join_bytes, join_spilled_partitions, join_output_rows,
                             # producer_sites, gather_sites) PLUS head_node_routed: bool (True on
                             # every relay result) and driver_relay_bytes: int (Σ wire sizes of all
                             # map payloads resolved at the driver barrier)
```

Capability vectors (frozen as dataclass equality): `ParslBackend(HTEX)` = all seven False;
`ParslBackend(TPE)` = `peer_data_movement=True`, rest False. NO eighth field, NO subclass — m47's
transport dispatch goes through a `parsl_backend`-private backend attribute, never capabilities.

## Traceability — test ↔ plan section ↔ Implementation Target ↔ wrong impl it kills

| Test | Plan | IT (m46) | WRONG impl it FAILS |
|---|---|---|---|
| no_cross_import | §0.4, §1.6 import rule, G9 | IT1, IT3 | eager `import parsl` anywhere under `parsl_backend/`; an engine import in `common/__init__`; a shim dragging parsl into `dask_backend` (or dask into `parsl_backend`); missing lazy exports |
| capabilities | §1.1 table + critique correction | IT3 | per-library flag stubs (TPE row); `peer_data_movement=True` on HTEX (the `_require_peer` blast radius); an 8-field capability subclass; `type(x).__name__`-based executor checks (stdlib-TPE trap) |
| submit_conformance | §1.2 backend.py, §0.3 measurements | IT3 | futures shipped unresolved (crash); driver-side compute (site pid); non-empty spec to TPE (armed rejection); dropped HTEX priority; `{"priority": 0}` noise; non-bytes broadcast handle |
| plan_parity | §1.2 (SubmitRunner reuse, G10 monitor) | IT3 | driver-side fold (pid span); nondeterministic reduction (×2); `close()` killing the caller's pool; dropped/driver-stamped worker phases; shim-less env ("local" worker ids) |
| relay_repartition | §1.3, design-M2 | IT2 (+IT1 bodies) | the m43 T·P shape (nonzero picks = ~P× driver amplification); per-row tasks (row ladder); driver-side kernels (sites); unlabeled relay (witness fields); stubbed `driver_relay_bytes` (independent Σ oracle); dropped salt |
| relay_join | §1.3 join mirror, F1, B5 | IT2 | dedup joins (multiset); dropped/duplicated unmatched rows (exact 13/17 counts); full-RAM concat under budget (peak); no spill (spilled==0); driver-side gather-join |
| relay_broadcast_join | §1.3 broadcast path, F6 | IT2 | live-`n_workers()` choice recompute (1-worker arm); forced-choice override; a "broadcast" that map-writes the large side; per-block unmatched re-emission (tail count > 1); missing once-only tail (multiset + count) |
| m43_engine_on_tpe | §1.1 consequences, §1.6 move, tests-B2 | IT1 | a relay-everywhere impl (zero picks fails the m43 Counter); renamed task fns (Counter keys); compute-without-submitting stubs (zero counts); an HTEX peer-flag lie (refusal arm runs work) |
| worker_env | §1.2 `_shim.py` | IT3 | fresh-env-per-task (open_once count); synthesized worker ids (in-task env-var recomputation); driver-env leak (pids, "local"); dropped/reordered/driver-stamped emits |
| failure_attribution | §1.2 describe_failure, r3 tests-(d) | IT3 | StageError re-wrapped/stringified across the boundary; a classifier `isinstance`-ing parsl classes (synthetic names fail; also the import-hygiene probe); chain-blind matching; attribute-everything (None arm) |
| packaging_pins | §1.6 items 1-3, §2 IT4/IT5, §4 | IT4, IT5 | drifted extra floor / smuggled marker; missing omit (main matrix red-by-construction); double- or zero-gated `common/` modules; path-valued `--cov=` overriding config sources (both jobs); a `--cov`-less silent-no-op step; a pyarrow-less parsl job (non-inner arms skip forever) |

## Fixture discipline + measured facts (all re-measured while authoring, parsl 2026.7.20, py3.12)

- **PYTHONPATH export is load-bearing**: HTEX workers launch via the `process_worker_pool`
  console script and do NOT inherit pytest's driver-side `sys.path` insertion. Measured: without
  the export, any harness-defined task fn fails on the worker with
  `ModuleNotFoundError: parsl_harness`; with `tests/frozen/m46` on `PYTHONPATH` (set by
  `ensure_harness_on_worker_path()` BEFORE `start_htex`) the round trip works. The export is
  idempotent and deliberately not restored.
- **Direct-HTEX recipe re-verified** (the plan's three moves: `run_dir`, `provider.script_dir`
  mkdir, `scale_out_facade(init_blocks)`): start ≈ 0.34 s, first result ≈ 1.0-1.3 s, 2 workers,
  `connected_managers()` shape as in plan §0.3. Module-scoped fixtures amortize this once per
  module (~8 HTEX startups across the suite ≈ 15 s of fixture cost).
- **Worker identity env vars measured**: `PARSL_WORKER_POOL_ID` (12-hex manager id),
  `PARSL_WORKER_RANK` ("0"/"1"), `PARSL_WORKER_COUNT`, `PARSL_WORKER_BLOCK_ID` all present
  in-task; the identity pin compares against an IN-TASK recomputation, so the test is
  format-independent.
- **TPE direct use measured**: `ThreadPoolExecutor(max_threads=2).start()` + submit < 1 ms;
  non-empty `resource_specification` raises `InvalidResourceSpecification` (the armed rejection
  behind the `{}` pin).
- **pyarrow is a runtime need of the non-inner numpy arms**: `graphed.numpy` wires MASKED blocks
  (every non-inner join output) over Arrow — measured `ImportError: install 'graphed[parquet]'`
  without it. The two affected tests `importorskip("pyarrow")` with a loud reason, and
  `test_parsl_packaging_pins` pins pyarrow into the test-parsl CI job (the same pairing the
  test-dask job already uses for pandas/pyarrow), so they can never silently skip in CI.
- **Scenario expectations pre-validated against the frozen LOCAL engine** (not invented): the F6
  choice gaps (`equal_sides`: shuffle@8/broadcast@1; `small_build`: broadcast@4), the unmatched
  counts (key 13 ×2 / key 17 ×2 per how), the budget arm (64×64 hot key, budget = output//8 →
  local peak 15360 ≤ 16384, spilled 7, rows 4096), and the `driver_relay_bytes` oracle machinery
  (1824 bytes on the 6-block/5-part scenario, salt=0).
- **Skip policy**: every parsl-touching module `importorskip("parsl")` with a visible reason (the
  main matrix is parsl-free by design) + a module-level win32 skip naming the POSIX constraint
  (plan §4 G8). The two no-parsl modules (no_cross_import, packaging_pins) run everywhere.
- All pool-touching calls run under `run_bounded` hard timeouts (a hang is a failure, never a
  hung CI job). All witnesses are counters, hashes, pids, worker identities, site provenance —
  never clocks; the single `sleep(0.05)` in `pv_process` and the emit/monitor `wait_for` polls
  are scenario construction / bounded fixture waits, never assertions (R0.10a).

## Non-vacuity evidence (TEST_SANITY)

- Pre-implementation, `pytest tests/frozen/m46` collects CLEAN and fails with right-reason
  errors only: `ModuleNotFoundError: graphed_executors.parsl_backend` / `.common` (every parsl/
  relay/engine test) and the six packaging pins failing on the exact missing m46 content
  (no `[parsl]` extra, omit not extended, `.coveragerc-parsl` absent, `.coveragerc-dask` without
  tasks_engine, no test-parsl job, dask step still carrying `--cov=` flags). Two consecutive runs
  produce IDENTICAL failsets (the m45 precedent).
- Two probes pass pre-implementation BY DESIGN and are documented as regression guards, not
  vacuous passes: `test_local_and_submit_leave_parsl_out` and `test_dask_backend_leaves_parsl_out`
  pin parsl-freeness of packages m46 edits (the shim) or must not touch — the m43 F2 precedent
  (a frozen test may pin existing behavior the milestone must preserve).
- Coverage wiring: `.coveragerc-parsl` is itself an Implementation Target (IT5) whose content
  this suite pins; the suite executes `parsl_backend` + `common.relay_engine` (and
  `common.tasks_engine`, gated on the dask job) through the deferred accessors, so
  `pytest tests/frozen/m46 --cov --cov-config=.coveragerc-parsl --cov-branch` produces a
  NON-EMPTY per-module report once the modules exist; the bare `--cov` requirement and the
  no-`--cov=` requirement are frozen in `test_parsl_packaging_pins` (pytest-cov 7.1.0 activation
  + override semantics, empirically verified twice upstream of this suite).

## Deliberate omissions (decisions, not gaps)

- No hinted-ImportError test for a parsl-less `ParslBackend(...)` construction (the m42
  `graphed-executors[dask]` analog): constructing a ParslBackend requires a parsl executor
  instance, so the no-parsl construction path has no unambiguous behavior to freeze (TypeError
  vs ImportError ordering is an implementation detail). The lazy-import discipline itself IS
  frozen (subprocess probes).
- No `peer_transport` attribute tests: the attribute is m47-only by plan design-M3 (m46's
  ParslBackend carries no such attribute; freezing its absence would be a freeze-trap).
- No facade tests: `parsl_backend/api.py` is deliberately m47 (§1.5 — a facade over one engine
  selects nothing).
- CI job-topology pins stop at the plan-blueprinted content (extra, omits, coveragerc sources,
  bare-`--cov` step shapes, pyarrow): timeout-minutes, python-version matrices and the
  3.13/3.14 exclusion policy are CI-run concerns the reviewer checks on the live workflow, not
  file-content pins this suite can hold stable across benign refactors.
