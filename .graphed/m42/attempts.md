# m42 attempts log

## TEST_AUTHORING (2026-07-21)

Frozen acceptance suite for the `SubmitBackend` protocol + dask Plan-path executor
(dask-parallel-backends-plan §2 m42, blueprint §3), authored implementation-blind on branch
`m42-dask-backend`. Spec: the plan (r2) + `graphed.core.execution` / `graphed.debug.errors` in the
consolidated core.

### Files (tests/frozen/m42/)

- `submit_backends.py` — harness (deferred impl accessors; tier-A/B `cluster_client`; provenance
  partials; unpickle-counted process; SpyBackend; RecordingMonitor; toy frontend backend/source)
- `test_submit_protocol_conformance.py` (21 tests) — both backends, one suite; pinned capability sets
- `test_dask_plan_tree_determinism.py` (3) — bit-for-bit, topology invariance, re-execution nonce
- `test_dask_worker_side_combines.py` (1) — off-driver combines + cross-worker input movement
- `test_dask_multi_worker_spread.py` (1) — coffea#1490 pinning tripwire
- `test_dask_broadcast_once.py` (4) — once-per-worker deserialization; namespaced/nonced keys; #9969
- `test_dask_stageerror_intact.py` (1) — M6 byte-equal StageError across tier-B workers
- `test_dask_killed_worker.py` (1) — attributed StageError, allowed-failures=1 dedicated cluster
- `test_dask_worker_death_reroute.py` (2) — die-once poison, allowed-failures=10, bit-for-bit
- `test_dask_adaptive_stop.py` (2) — stop honored, cancel witnessed, no refill-after-stop
- `test_dask_parity_surfaces.py` (6) — aggregate/External/to_parquet/parquet-source/open_once/plugin
- `test_dask_monitor_events.py` (3) — M37 exact phases over the run topic; passivity
- `test_submit_no_dask_import.py` (2) — MAIN-matrix packaging (no distributed import; hinted error)
- `README.md` — m40-template traceability (pinned contract, wrong-impl table, R0.10a, non-vacuity,
  deviations)

### TEST_SANITY evidence (venv: graphed+awkward+numpy compiled local, distributed 2026.7.1, pyarrow)

1. Collection: `pytest tests/frozen/m42 -q --co` twice → identical sets, 47 tests, zero errors.
2. Non-vacuity: full run pre-implementation → **47/47 FAILED**, and a scripted scan of all 47
   failure blocks shows every one cites `ModuleNotFoundError: No module named
   'graphed_executors.submit'` or `…'graphed_executors.dask_backend'` (the deferred-accessor
   design keeps collection clean while the failure lands in each test body). 0 passed, 0 skipped,
   no pytest internals errors.
3. Repo-wide: `pytest tests/frozen -q --co` collects 59 test files with no basename collisions
   (needed `pip install -e graphed-corpus` in the venv — a pre-existing env gap of m7/m41 ADL
   suites, unrelated to m42).
4. Lint: `uvx ruff@latest check tests/frozen/m42` → "All checks passed".
5. Scenario assumptions pre-validated against distributed 2026.7.1 directly (no impl involved):
   sys.path propagation to spawned nanny workers (harness fns unpickle by reference), KilledWorker
   under allowed-failures, die-once reroute, `log_event`/`subscribe_topic` roundtrip, and the
   awkward parquet aggregate/to_parquet determinism (pyarrow required — the `test-dask` CI job
   must install it).

### Notes for the implementer

- The README "Pinned execution contract" block is the full pin list (flag values per instance,
  key/broadcast counts, EXHAUSTED, monitor phase contract, plugin name/idempotent).
- `register_worker_plugin` is called by several tests BESIDE `dask_runner`'s own registration —
  idempotent re-registration must be a no-op.
- Add pyarrow to the dask CI job deps (parity parquet surfaces call `ak.to_parquet`).

---

## IMPLEMENTING (2026-07-21)

### Iteration 0 — design (implementer)

Read the full plan (§1.1–§1.6, §2 m42 targets 1–11), all 13 frozen files + harness + README,
core `execution.py` (Plan/Task/ExecResult/Monitor/TaskEvent/TaskPhase/LocalResources/emit_task/
partition_label/SequentialRunner), `debug/errors.py` (StageError.__reduce__), local
`executors.py` + `_reduce.py` (plan_tree/tree_reduce/running_fold — REUSED), pyproject, ci.yml.

Design decisions:
- `submit/engine.py` reuses `plan_tree`/`running_fold` from `graphed_executors.local._reduce`.
- Fixed path: submit leaves + all plan_tree combines up front as future-dep graph; await root.
  ThreadBackend resolves future-args via `.result()` in its wrapper — FIFO+lower-id-deps means no
  ThreadPool deadlock (lowest in-progress node always has complete deps).
- Broadcast token = f"{sha256(payload)[:12]}-{run_nonce}"; keys = f"graphed-{fp12}-{nonce8}-{kind}-{idx}".
- Worker seam: RunContext (picklable first arg) + WorkerEnv contextvar (resources+worker+emit);
  dask installs env via `_dask_task_shim`; ThreadBackend installs a thread-local env.
- Monitor: SUBMITTED driver-side (leaves only); STARTED/FINISHED/ERRORED worker-side via
  env.emit -> topic; DaskBackend.subscribe_events wraps Client.subscribe_topic (unwraps (ts,msg));
  runner drains until 2*n worker events before unsubscribing (finally), so nothing is lost.
- KilledWorker -> StageError via backend.describe_failure(exc) returning (failing_key, last_worker);
  engine maps key->task->partition_label. Ordinary exceptions (StageError/GraphedError/ValueError)
  re-raise intact (no translation).

### Iteration 1 — implementation complete (all gates green)

Files created:
- `src/graphed_executors/submit/{protocol,engine,threadpool,__init__}.py`
- `src/graphed_executors/dask_backend/{_lazy,plugin,_shim,backend,__init__}.py`
- `.coveragerc-dask`
Files edited: `pyproject.toml` (dask extra + mypy dask/distributed override + coverage omit
dask_backend), `.github/workflows/ci.yml` (test-dask job, py3.12+3.14, pyarrow, scoped coverage),
`docs/design.rst` (dask backend section + deployment recipes + checkpoint scope + 3.14t limitation).

Gate results (venv /private/tmp/claude-501/m42venv, distributed 2026.7.1):
- **frozen m42: 47 passed** (`pytest tests/frozen/m42`), twice (determinism), 19.9s.
- **whole frozen tree: exit 0, 0 FAILED** (`pytest tests/frozen`) — no m7..m41 regression.
- **coverage (scoped, .coveragerc-dask): 96.71%** ("Required 90.0% reached") — submit 96%,
  dask_backend ~99%. Misses are defensive branches or worker-process code coverage cannot trace.
- **ruff check src tests: All checks passed**; **ruff format --check src: 18 files already formatted**.
- **mypy (strict, files=[src]): Success, no issues in 18 files** (added dask.*/distributed.* override).
- **sphinx -W: build succeeded** (0 warnings; installed sphinx+furo into the venv).
- frozen integrity: `git diff freeze-m42 -- tests/frozen` empty; no tests/frozen edits.

Key design deviation (recorded for reviewer): `DaskBackend.submit` accepts but does NOT forward
`resources` to `client.submit`. The frozen seam test passes `resources={"GPU": 1.0}` (unsatisfiable
on a no-resource LocalCluster) and expects the result — dask treats resources= as a HARD constraint,
so forwarding it pins the task in no-worker forever, making correctness depend on a hint (§1.1
forbids exactly that). `per_task_resources=True` remains the honest capability of the dask library;
wiring it is a deployment-time opt-in on a resource-provisioned cluster. No m42 test relies on
resource enforcement. `_as_completed` (plan target 2) is implemented INLINE in `_run_adaptive`
(add_done_callback -> queue.Queue) rather than as a standalone helper, to keep the frozen suite
exercising every line (no dead helper).
