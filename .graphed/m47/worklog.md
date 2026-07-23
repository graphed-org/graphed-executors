# m47 implementer worklog

Branch `m46-parsl-backend`, worktree /private/tmp/claude-501/graphed-exec-check. Freeze tag
`freeze-m47` (5609052). 54 frozen tests in tests/frozen/m47/. Goal: all pass, no weakening,
m7..m45 stay green.

## Architecture (from plan §1.4-1.6 + the 16 frozen modules + harness contract)

### common/ moves (mechanical; m44/m45 green = the proof)
- `common/transport_run.py` <- dask `_transport_run.py`: `TransportWitness`,
  `TransportShuffleResult`, `TransportExecResult`, `merge_counters`, `is_restart_worthy`(+`_in_chain`),
  `pick_attributable`, `build_stage_error`(+`_with_frames`). dask `_transport_run.py` KEEPS
  `require_pin`/`sorted_addresses`/`_counters_probe`/`_purge_probe`/`collect_and_purge`/`PLUGIN_NAME`/
  `DRIVER` and RE-EXPORTS the moved names (identity: `dtr.TransportWitness is ctr.TransportWitness`).
- `common/facade.py` <- dask `api.py`: `resolve_shuffle_method`, `_reject_transport_only_knobs`,
  `_VALID_METHODS`. dask `api.py` re-exports (identity pinned by m45 + m47 facade tests).
- `common/transport_tasks.py` <- dask `transport_shuffle.py` task BODIES, plane-parameterized:
  `_transport_map_task`/`_transport_gather_task`/`_transport_gather_join`/
  `_transport_broadcast_join_part`/`_pull_ordered` + `_MapFrag`/`_GatherOut`/`_JoinOut`/`_Sized`/
  `_SizingBackend`/`_SizingCluster`/`_replay_reader_plane`/`_collect_maps`/`_skey`. dask
  `transport_shuffle.py` keeps SAME-NAMED module-level wrappers binding the dask plane (m45 spy).

### common/http_plane.py (NEW, stdlib-only; subclass local/_transport.py; no edits to local/*)
- `TransportDeliveryError`, `PullTimeoutError` (Exception; matched by NAME by the reused classifier).
- `SEND_RETRIES = 5`.
- `EscalatingHttpTransport(HttpTransport)` — dual-route `_DualRouteServer(_InboxServer)` +
  `_DualRouteHandler`. /msg envelope `pickle.dumps((sender, epoch, message))`.
  - inline `send`: 503->False+`sends_rejected` (N2, before generic retry); unknown/self->False;
    exhaust comm failure->raise `TransportDeliveryError`+`send_failures`; retries counted `sends_retried`.
  - `recv`: idle-deadline raise (design-A). `broadcast`: attempt-all-then-raise-aggregate.
  - receiver /msg: `inbox_maxsize=None` unbounded; set->503+`recv_rejects`; wrong epoch->reject
    +`stale_epoch_rejects`; dup digest (per epoch)->deliver + `recv_duplicate_deliveries`.
  - /pull route: `pull_requests_served`, `bytes_served`, `serve_pid`; evict-after-serve;
    `store_blocks_at_return`. pull client raises `PullTimeoutError`.

### parsl_backend/transport_peer.py (NEW)
- `_parsl_peer_main(spec, *args)` module-level pinned peer actor. Lifecycle:
  mint endpoint (in-task, `endpoint_pid`/`endpoint_port`) -> hello -> wait registry
  (`registry_size_at_receipt`) -> [probe leg for shuffle/probe kinds] -> engine body -> return
  counters+provenance (+ result for shuffle). Kinds: reduce | repartition | join | probe.
- shared driver helpers: `_open_driver_ep`, `_barrier_hellos` (k hellos, `recv_class_hello`,
  barrier_timeout -> attributed StageError naming "hellos_seen/k", cancel queued, drain markers),
  `_assemble_and_broadcast_registry` (true reg for driver control; `registry_rewrite`'d reg to peers),
  `_collect_probes`, restart classification (`is_restart_worthy`/`build_stage_error` from common).
- `parsl_run_plan(plan, pbackend, *, monitor, workers, inbox_maxsize, epoch_restarts_allowed,
  barrier_timeout_s, inject_recv_failures)` -> TransportExecResult. reduce = process_and_reduce (N1),
  NO probe. Driver collects `("root",)` (recv_class_root). inject_recv_failures = file-backed
  per-(dest,src) budget dir PERSISTED across epochs (arm (b) leftover-budget composes).

### parsl_backend/transport_shuffle.py (NEW)
- `transport_run_repartition`/`transport_run_join` -> TransportShuffleResult. k peers do map(store)+
  gather(pull) in-task; probe leg first. reader-plane fetch budget = `_replay_reader_plane` (driver);
  join budget = real `_join_with_budget` (in `_transport_gather_join`). broadcast ignored (shuffle only).
  On probe fail raises `ProbeUnreachable(failed_pairs)` (facade decides error/fallback).

### parsl_backend/api.py (NEW)
- `run_repartition`/`run_join` facade (pbackend). Re-export `resolve_shuffle_method` from common.facade.
  tasks -> relay (HTEX) / m43 (TPE, peer_data_movement). transport -> peer_transport gate + probe.
  `probe_peer_reachability(pbackend, k, *, timeout_s, registry_rewrite) -> ProbeReport(ok, failed_pairs)`.
  `on_unreachable` error/fallback. transport-only knob rejection incl on_unreachable/workers/registry_rewrite.

### backend/launch/config
- `ParslBackend.peer_transport` = `_is_htex`. `start_htex(..., heartbeat_period=None)`.
- `.coveragerc-parsl` += common.http_plane. `.coveragerc-dask` += common.{transport_run,facade,transport_tasks}.
- ci test-parsl step += tests/frozen/m47 (bare --cov kept).

## Ownership pins (harness)
peer addrs "w0".."w{k-1}" (sorted==index). producer t = peer t = `_assign(n_src,k)[t]` contiguous.
gather owner dest d = peer d%k. holder of producer-t fragment = peer t. self-fragment (d%k==t) local
(no HTTP); cross (d%k!=t) pulled -> holder bytes_served. pull_requests_served <= k*k (coalesce /holder).

## Progress
- [reading done] full contract understood.

## Session 2 (continued) — Phases E–H complete, test validation

### Built
- **Phase E** `parsl_backend/transport_shuffle.py`: `_HttpBlockPlane` (self=take_local unbilled, cross=/pull),
  `_peer_shuffle_body` (map→manifest→assign→gather), `_gather_repartition` (coalesced ONE pull/holder
  for ≤k*k), `_gather_join` (per-dest gather_join_body), driver `_run_shuffle`+`_drive_shuffle_attempt`
  (barrier→probe→manifest→assign→gathered→done→harvest), `transport_run_repartition/join`,
  `_repartition_witness` (replay_reader_plane), `_shuffle_stage_error`.
- **Phase F** `parsl_backend/api.py`: run_repartition/run_join facade (shared resolve_shuffle_method IS
  common.facade), knob rejection, `_require_peer_transport` (TPE→NotImplementedError), integrated probe
  (ProbeUnreachable→on_unreachable error=StageError naming pair / fallback=relay+fallback_reason),
  `probe_peer_reachability`→ProbeReport, deferred relay/transport imports (G9).
- **Phase G** backend.ParslBackend.peer_transport property (=_is_htex); launch.start_htex(heartbeat_period).
- **Phase H** .coveragerc-parsl += common.http_plane; .coveragerc-dask += transport_run/facade/transport_tasks;
  ci.yml test-parsl runs tests/frozen/m47.

### PASS (validated)
imports(3), packaging(3), non-pool facade(7), peer_reduce, repartition_parity+join_parity(12),
budget_parity+retry_exhaustion(minus 1)+reachability_probe group, epoch_guard+conformance+task_shape.

### Bugs found & fixed
1. **retry arm(b) send_failures=0**: a raising peer's counters ride the task RESULT (now the exception)
   → lost. FIX: `_parsl_peer_main` attaches `exc.peer_counters={**base,**_sender_counters(ep)}` (survives
   pickle via BaseException.__reduce__ __dict__); `_drive_reduce`+shuffle `_harvest` merge it on error.
2. **over-ask barrier InvalidStateError**: pbackend.cancel() races parsl HTEX dispatch → cancelled future
   gets a result → parsl result-thread raises InvalidStateError → pytest escalates warning to failure.
   FIX: `_cleanup_after_barrier` no longer cancels; queued peer self-terminates via rendezvous timeout,
   freed seated slots suffice for the liveness probe.
3. **worker_death budget-0 DEATH_SIGNALS**: shared build_stage_error renders described death as
   cause_type="KilledWorker" (str(WorkerLost) has no class-name literal) → none of DEATH_SIGNALS match.
   FIX: `_shuffle_stage_error` uses cause_type=type(exc).__name__ (WorkerLost/PullTimeoutError/…).

### Pending validation
worker_death (2 arms), block_plane (re-run), shuffle_method_facade pool tests (re-run), then FULL m47 ×2
+ whole frozen tree + prek + mypy --strict + coverage both rcs + determinism + integrity.

## Session 3 — the parsl coverage gate (subprocess coverage)

### Root problem (measured)
Driver-only coverage of the parsl-rc source = **72.52%**. The §1.4 transport bodies
(`transport_peer._parsl_peer_main`/`_peer_shuffle_body`, `transport_shuffle` peer bodies,
`http_plane` block plane) run ONLY in HTEX WORKER subprocesses (exec'd fresh). The dask job never
hits this — its m44 suites run workers in-process (`processes=False`); the parsl transport has NO
in-process path (TPE→NotImplementedError by design). So the parsl-rc modules can only reach 90%
via **subprocess coverage**.

### Mechanism
`coverage.process_startup()` at interpreter start (a `.pth` hook) + `COVERAGE_PROCESS_START` +
`[run] parallel=true` makes each worker self-measure into its own `.coverage.<pid>` file.
- LOCAL: `.venv/.../a1_coverage.pth` (harness tooling, `slug="pth"`) provides the hook. **It is
  NOT in the repo** — a fresh CI venv would NOT have it, so subprocess coverage would silently
  measure driver-only and fail. CI must WRITE a portable hook: `graphed_subcov.pth` =
  `import coverage; coverage.process_startup()` (coverage.py "Measuring subprocesses").
- pytest-cov does NOT fold the `process_startup` worker files into its sessionfinish combine
  (measured: 119 leftover `.coverage.*` after `pytest --cov`). So the driver-only inline report
  (72–89%) is a FALSE red. Fix: `--cov-fail-under=0` on pytest (defer the gate), then
  `mv .coverage .coverage.driver` (fold the driver data back to a combinable parallel file — plain
  `coverage combine` merges only `.coverage.*`, so WITHOUT the mv it drops the driver and api.py→0%),
  `coverage combine`, then `coverage report --fail-under=90` on the true union.

### Measured union (clean run, no orphaned workers)
TOTAL = **90.49%** (Miss 79/1129 stmts, 44/290 br-part), GATE PASS. Per-module: http_plane 93.43%,
relay_engine 94%, transport_shuffle 90.96%, transport_peer 90.43%, backend 90.09%, launch 92%,
api.py **83.54%**.

### api.py gap (genuine, non-blocking)
Dark: `run_join` body 146-183 + `_relay_join` 281-283. Root: NO frozen test drives the facade
`run_join` with a VALID method — `facade_join` is called ONCE (`test_parsl_shuffle_method_facade`,
shuffle_method="p2p") which raises at `resolve_shuffle_method` (line 145) BEFORE the body. The join
ENGINE is fully tested via the DIRECT `transport_run_join`/`relay_run_join` harness wrappers
(join_parity, budget_parity); only the thin api FACADE wrapper for join is dark, and its logic
mirrors the fully-tested `run_repartition` facade. Cannot cover from frozen (can't add frozen
tests). Total still clears 90% because the other modules carry it. NOT a stub — fully implemented.

### Gotchas
- zsh background shell rejects `.coverage.*(N)` glob qualifier AND bare `.coverage.*` (no-match
  abort) — use `find . -maxdepth 1 \( -name '.coverage' -o -name '.coverage.*' \) -delete`.
- Orphaned HTEX workers from repeated local runs keep flushing late → inflate the union (saw
  90.0→91.47%). `pkill -f parsl` MISSES worker processes (cmdline is `process_worker_pool.py`, no
  "parsl") — must ALSO `pkill -f process_worker_pool`. Clean `.coverage*` before each measurement.
  In a CLEAN single run the file count stabilises after one 5s poll (workers die with their module pool).

### The reproducibility-run "failure" — POLLUTION ARTIFACT, not a bug
A clean-looking rerun failed `test_parsl_peer_reduce.py::test_submit_shape_carries_no_engine_task_names`
with `TransportDeliveryError: peer reduction completed with no captured root`. VERDICT: environment
pollution, NOT an implementation or suite bug. Evidence: (1) the test passes 3/3 in ISOLATION on a
quiet machine; (2) root cause found = a HUNG orphaned `pytest tests/frozen/m47` (PID 1604, elapsed
2:54) from an earlier run, spawning HTEX workers that interfered with THIS run's reduce root-capture
(7 stray parsl procs alive at failure time); (3) the "no captured root" guard is CORRECT — it RAISES
rather than defaulting a lost/withheld root to the identity value, so under real interference it fires
as designed. Same artifact class as the documented concurrent-HTEX ManagerLost/heartbeat starvation.

### CI mechanism finalised (ci.yml test-parsl step)
- `env: COVERAGE_PROCESS_START` / `COVERAGE_RCFILE` = `${{ github.workspace }}/.coveragerc-parsl`
  (ABSOLUTE — removes worker-CWD ambiguity resolving the config hook).
- run: write `graphed_subcov.pth` (`import coverage; coverage.process_startup()`) →
  `pytest … --cov --cov-config=.coveragerc-parsl --cov-branch --cov-fail-under=0` (collect; inline gate
  deferred) → `mv .coverage .coverage.driver` → `coverage combine` → `coverage report --fail-under=90`.
- Frozen `test_ci_parsl_step_…` still green (regex starts at `pytest tests/frozen/m46`; env + hook line
  precede it; no `--cov=` substring; `--cov` + `--cov-config` present).
- Tree note: team-lead ran `ruff --fix` + `ruff format` on src/ (incl. untracked new files) → api.py
  line refs SHIFTED (run_join 124→126, _relay_join 281→295). Semantic no-op; re-measure lines.
