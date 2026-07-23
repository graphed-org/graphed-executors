# m46 implementer — attempts log

Milestone: **m46 — ParslBackend + relay engine + common/ bootstrap** (graphed-executors, branch `m46-parsl-backend`).
Frozen suite: `freeze-m46` @ 75efbfb, 51 tests in `tests/frozen/m46/`.

## Iteration 0 — reading the contract (in progress)

Read: plan (parsl-backend-plan.md, FINAL r3), frozen README + all 11 test modules + parsl_harness.py.

### Contract distilled (module paths / symbols the harness accessors demand)

- `graphed_executors.parsl_backend`:
  - `ParslBackend(executor)` — started parsl HTEX or parsl ThreadPoolExecutor; type-derived caps; other → TypeError naming both classes.
  - `parsl_runner(executor) -> SubmitRunner`
  - `.capabilities` (SubmitCapabilities, 7-field, class-exact); HTEX = all-False, TPE = peer_data_movement=True only.
  - `.submit(fn, /, *args, key, retries=0, priority=0, resources=None, workers=None)` → SubmitFuture. Resolves SubmitFuture/Future args DRIVER-side. Wraps fn in module-level shim AFTER the spy seam (spy sees raw fn.__name__). resource_specification: TPE always {}, HTEX {"priority":p} iff p!=0 else {}.
  - `.broadcast(payload, *, token)` → returns payload (degenerate; task gets raw bytes).
  - `.n_workers()` — HTEX Σ worker_count over active/non-draining managers (bounded wait ≥1); TPE thread count. Must ==2 in tests.
  - `.cancel(futures)` best-effort; cancel([]) no-op.
  - `.subscribe_events(topic, handler)` → unsub callable; driver-local registry.
  - `.describe_failure(exc)` → (key, worker) str 2-tuple if WorkerLost/ManagerLost by NAME in chain, else None.
  - `.close()` does NOT shut executor down.
- `graphed_executors.parsl_backend.launch`: `start_htex(*, workers, run_dir, address=None, encrypted=False) -> HTEX`, `stop_htex(executor) -> None`. Three measured moves: run_dir, provider.script_dir mkdir, scale_out_facade(init_blocks). MUST NOT scrub caller env (PYTHONPATH export must survive).
- `graphed_executors.common.tasks_engine`: the ENTIRE m43 engine MOVED verbatim from dask_backend/shuffle.py (names preserved: dask_run_repartition, dask_run_join, _dask_map_write, _dask_pick, _dask_gather, _dask_gather_join, _dask_broadcast_join_part, _WorkerStore, _MapOut/_GatherOut/_JoinOut, DaskShuffleWitness, _require_peer, _result/_skey/_site, ...).
- `graphed_executors.common.relay_engine`: `relay_run_repartition(backend, src_blocks, parts, *, runner, salt=0)`, `relay_run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",), how="inner", runner, broadcast=None, salt=0, mem_budget_bytes=None)`. RelayShuffleWitness = DaskShuffleWitness fields + head_node_routed:bool(True) + driver_relay_bytes:int.
- `dask_backend/shuffle.py` → re-export shim over common/tasks_engine (keeps frozen dotted paths + __name__s; must stay parsl-free).

### Shim seam (worker side)
- `_ParslWorkerEnv` (WorkerEnv shape): `.worker` = f"{POOL_ID}:{RANK}" from PARSL_WORKER_* env vars recomputed IN-TASK (fallback hostname:pid on TPE); `.resources` process-global cached LocalResources (open_once once/worker process); `.emit` buffers into a list.
- module-level `_parsl_task_shim(fn, shim_spec, *args)` installs env via set_worker_env, runs fn, returns (result, events). `_ParslFuture` unwraps + dispatches events to subscribers on completion.

### Coverage/packaging pins (test_parsl_packaging_pins.py — MAIN matrix)
- pyproject extras: `parsl = ["parsl>=2026.7.20"]` EXACTLY, no marker.
- [tool.coverage.run] omit += `*/common/*`, `*/parsl_backend/*` (already has `*/dask_backend/*`). fail_under stays 90.
- `.coveragerc-parsl`: sources = {parsl_backend, common.relay_engine}; branch=True; fail_under=90. NO tasks_engine, NO dask_backend.
- `.coveragerc-dask`: sources += common.tasks_engine (keep submit + dask_backend). No relay_engine/http_plane. fail_under=90.
- CI test-parsl job: installs pyarrow; pytest tests/frozen/m46 with bare `--cov` + `--cov-config=.coveragerc-parsl`, NO `--cov=path`.
- CI test-dask step: drop path-valued `--cov=` flags → bare `--cov`.

### Gate plan
1. pytest tests/frozen/m46 → 51 passed.
2. whole tests/frozen green (m43/m45 = shim regression).
3. ruff + ruff format --check repo-wide (prek -a --quiet).
4. mypy --strict per repo config (files=["src"]).
5. coverage ≥90% both .coveragerc-parsl and .coveragerc-dask (bare --cov each, run locally).
6. determinism ×2 on hash-witnessed modules.
7. integrity: git diff freeze-m46 --stat shows ZERO under tests/frozen/**.

Next: read source templates (shuffle.py = the move; backend.py/threadpool.py/protocol.py/engine.py = mirror), then get_symbols_overview on the submit engine WorkerEnv seam.

## Iteration 1 — implement + gate

### Files created/changed
- `src/graphed_executors/common/__init__.py` (new) — package doc, parsl/dask-free.
- `src/graphed_executors/common/tasks_engine.py` (new) — m43 engine MOVED verbatim from dask_backend/shuffle.py (only top docstring retitled). 233 stmts.
- `src/graphed_executors/dask_backend/shuffle.py` — reduced to a 3-stmt re-export shim over common.tasks_engine (re-exports dask_run_*, _WorkerStore, _dask_*, _MapOut/_GatherOut/_JoinOut, DaskShuffleWitness, _require_peer, _result/_site/_skey).
- `src/graphed_executors/common/relay_engine.py` (new) — relay_run_repartition/join + RelayShuffleWitness(DaskShuffleWitness) + head_node_routed/driver_relay_bytes. Driver-side map barrier + local _dask_pick regroup + P gathers; mirrors m43 driver exactly (byte-identical dest_block_hashes). ~124 stmts.
- `src/graphed_executors/parsl_backend/{__init__,_lazy,_shim,backend,launch}.py` (new). ParslBackend + _ParslFuture + parsl_runner; _parsl_task_shim + _ParslWorkerEnv (process-global LocalResources, in-task POOL_ID:RANK, buffering emit); start_htex/stop_htex (3 measured moves).
- `pyproject.toml` — parsl extra `["parsl>=2026.7.20"]`; omit += `*/common/*`,`*/parsl_backend/*`; mypy parsl.* override (follow_imports=skip, mirrors dask).
- `.coveragerc-parsl` (new) = {parsl_backend, common.relay_engine} branch fail_under 90.
- `.coveragerc-dask` — source += common.tasks_engine.
- `.github/workflows/ci.yml` — test-dask step → bare `--cov`; new test-parsl job (3.12 gate/3.13 continue-on-error, pyarrow, bare --cov + --cov-config=.coveragerc-parsl).

### Gate results (iteration 1)
1. pytest tests/frozen/m46 → **51 passed**, twice (determinism ×2 green incl. relay hash modules). PATH must include .venv/bin (HTEX interchange.py console script).
2. Whole tests/frozen tree → exit 0, reached 100%, zero FAILED/ERROR (m44 summary-line drop is cosmetic).
3. ruff check src tests/extra + ruff format --check → clean (fixed RUF002 ×→P-fold, RUF023 __slots__ sort, formatted 2 files).
4. mypy --strict → **Success, 32 files** (added parsl.* override + 1 cast in _ParslFuture.exception).
5a. .coveragerc-parsl → **91.59%** (relay_engine 93%, parsl_backend backend 90%, _shim 90%, launch 90%) — PASS.
5b. .coveragerc-dask → 88.45% LOCALLY, but this is a **pandas-missing artifact, NOT a regression**:
    - VERIFIED in a worktree at freeze-m46^ (a3497bc): pre-move dask cov = **88.43%** (shuffle.py engine 83%, transport.py 87%, transport_peer 83%, transport_shuffle 82%). My move is coverage-NEUTRAL (+0.02% via the 100% shim).
    - Root cause: m43 join-parity-edges/relational + m44 transport frozen tests use a `pd.merge` oracle → SKIP without pandas → join/transport branches uncovered. CI test-dask installs `pyarrow pandas`; local venv lacked pandas. Installed pandas 3.0.5; re-running to confirm ≥90 with the CI env. [pending]

### Determinism
- m46 suite run twice → identical (dot-for-dot). Relay hash modules (relay_repartition/join) assert dest_block_hashes equality ×2 internally too.

## Iteration 2 — dask-cov confirmation, docs, final gates

### Gate 5b RESOLVED (dask coverage): with pandas installed (CI env) → **93.21%**, `common.tasks_engine` **97%**.
Confirmed the local 88.45% was purely missing-pandas (m43 join-edges/relational + m44 transport frozen tests use a pd.merge oracle → skip). CI test-dask installs `pyarrow pandas`. Both coverage gates green in the CI dependency set.

### Docs (IT6) — all -W clean (built locally with sphinx+furo)
- `docs/parsl.rst` (new) — how-to; examples EXECUTED live (2-worker HTEX): Plan [700]/7/6, relay repartition {0:5,1:7,2:6,3:2} head_node_routed=True driver_relay_bytes=1088, relay join 16 rows, HTEX all-False caps, TPE peer-only caps.
- `docs/design.rst` — new `.. _design-parsl-backend:` section (direct submit / floor / worker seam / relay engine); Phase-2 parsl note updated.
- `docs/api.rst` — autosummary += common, parsl_backend.
- `docs/index.rst` — toctree += parsl + sentence.
- `docs/improvements.rst` — parsl transport/probe/facade, TaskVine, M37/TLS Phase-2 entries.
- `docs/conf.py` — autodoc_mock_imports += parsl.
- Fixed RST warning: `` ``__name__``s `` → reworded in tasks_engine module docstring.

### Final gate sweep (all GREEN)
1. pytest tests/frozen/m46 → **51 passed** (×2 determinism).
2. whole tests/frozen → exit 0, 100%, no FAILED/ERROR.
3. prek -a (ruff check + ruff format + mypy strict) → **all Passed** repo-wide.
4. mypy --strict → Success 32 files.
5. coverage: .coveragerc-parsl **91.59%** (all modules ≥90); .coveragerc-dask **93.21%** (tasks_engine 97%) — CI env (pandas+pyarrow).
6. determinism ×2 → identical.
7. integrity: `git diff freeze-m46 --stat -- tests/frozen/` = EMPTY (zero frozen edits); local/ + submit/ untouched.
8. Sphinx -W → build succeeded (docs/parsl.rst + design section).

### Deviations from plan (measured justification)
- None material. `_lazy.py` included (IT3) and used in ParslBackend.__init__ for the parsl type import (hinted ImportError + module stays parsl-free at load). `describe_failure` returns ("", str(exc)) — parsl exceptions carry no graphed task key; the runner's key_to_task supplies the partition (m47 worker-death is LENIENT). shim_spec dropped from `_parsl_task_shim(fn, *args)`: no frozen test pins the shim's internal signature and the WorkerEnv needs no driver-supplied spec (identity recomputed in-task, resources process-global, emit buffer per-task) — simpler + pickling-clean.

### Env note (for reviewer/CI)
HTEX interchange launches via the `interchange.py` console script → the venv `bin` MUST be on PATH to run the parsl suite locally (`export PATH="$PWD/.venv/bin:$PATH"`). CI's activated venv does this. pandas is a test-dask runtime need (installed in CI); a local venv without it under-reports dask coverage (frozen join/transport tests skip).

