# m42 frozen suite — graphed-executors (`SubmitBackend` protocol + dask Plan-path executor)

Milestone **m42** (dask-parallel-backends-plan §1.1–§1.5, §2 m42, §3). This repo gains the common
submit-backend seam for parallel-execution libraries — `SubmitCapabilities` / `SubmitFuture` /
`SubmitBackend`, the generic `SubmitRunner` Plan engine, the stdlib `ThreadBackend` conformance
backend — and the first real backend: `DaskBackend` over a READY `distributed.Client` used as a
dumb scheduler (`plan_tree` as the future graph, broadcast-once payloads, worker-side combines,
M6/M37 parity). **Frozen — read-only after `freeze-m42-0`.**

## Files → theme (plan §3 m42 table)

| File | Theme | What it witnesses |
|---|---|---|
| `submit_backends.py` | harness | deferred impl accessors; tier-A/B `cluster_client`; provenance partials (`Prov`/`FoldPart`/`AggPart`: pids, workers, moves, per-invocation marks); unpickle-counted `CountingProcess`; `SpyBackend`; `RecordingMonitor`; toy frontend backend/source; ALL worker-shipped fns module-level (spawn-safe) |
| `test_submit_protocol_conformance.py` | seam-by-execution | SAME fixed+adaptive+error+monitor scenarios green on BOTH backends; key-order (not submission-order) reduction; direct submit seam: future-args resolved, broadcast → raw bytes, hints ignored; capability flags pinned per instance and differing on 5/7 |
| `test_dask_plan_tree_determinism.py` | determinism | staggered NON-commutative concat: two runs on one runner == bit-for-bit == `SequentialRunner`; invariant across `n_workers` ∈ {1,2,3}; `n_combines == n-1`; re-execution witness (mark sets disjoint across runs) |
| `test_dask_worker_side_combines.py` | worker-side reduction | combine pids ⊆ worker pids ∌ driver pid; all 15 combines accounted (marks); ≥1 combine consumed an input materialized on a DIFFERENT worker (the m38 `test_peer_reduce` witness style) |
| `test_dask_multi_worker_spread.py` | coffea#1490 guard | fixed 2-worker tier-B cluster, 16 leaves: executed on ≥2 distinct workers, none on the driver |
| `test_dask_broadcast_once.py` | broadcast-once + keys | payload unpickles per worker pid == 1 while tasks per pid > 1, driver unpickles 0; submit keys = n+(n-1), `graphed-`-prefixed, unique, DISJOINT across runs; exactly 2 broadcasts/run; dask#9969 key-shaped-arg probe; `replicate_broadcast=True` knob harmless |
| `test_dask_stageerror_intact.py` | M6 | `StageError` from a tier-B worker: same type, `__dict__` byte-equal to the reference, still picklable, `format_traceback` names `user_analysis.py:42` + the sub-expression |
| `test_dask_killed_worker.py` | failure attribution | `os._exit(1)` leaf on a dedicated `allowed-failures: 1` cluster: driver gets `StageError` naming the partition uri, "KilledWorker", and a `tcp://` worker — never a raw `KilledWorker` |
| `test_dask_worker_death_reroute.py` | reroute | die-once poison leaf (atomic pid marker) under `allowed-failures: 10`: run completes bit-for-bit; marker pid ≠ successful pid (re-ran in another process); clean-twin control anchors the value |
| `test_dask_adaptive_stop.py` | adaptive + stop | `TARGET_EVENTS` honored; `SpyBackend.cancel` saw ≥1 outstanding future; no `next_tasks` call at/past the target; fold == oracle over exactly the consumed uri set; exhaustion contrast run cancels nothing |
| `test_dask_parity_surfaces.py` | parity §1.5 rows 1,3,4,5,11 | parquet 2-output `aggregate_plan` == sequential bit-for-bit + ≥2 workers; External resolved worker-side by content hash; missing evaluator → loud `needs an evaluator` from the worker; `to_parquet(executor=dask_runner)` paths+bytes == sequential; `open_once` handles-per-pid == 1 with tasks-per-pid > 1; plugin name/idempotent pins |
| `test_dask_monitor_events.py` | M37 | per leaf exactly one SUBMITTED+STARTED+(FINISHED\|ERRORED), in that per-key order, leaf keys only; STARTED/FINISHED attributed to live worker addresses; topic `graphed-monitor-*` subscribed AND unsubscribed; byte-identical value detached / attached / raising-monitor |
| `test_submit_no_dask_import.py` | packaging (MAIN matrix) | fresh interpreter imports both packages + every pinned public name with `distributed`/`dask` absent from `sys.modules`; hidden-distributed `DaskBackend` construction → `ImportError` naming `graphed-executors[dask]` |

## Pinned execution contract (test-author decisions — the implementer builds to this)

```
graphed_executors.submit:
  SubmitCapabilities(peer_data_movement, scatter_broadcast, pin_to_worker, per_task_retries,
                     per_task_resources, cancel_running, worker_file_cache)   # frozen dataclass,
                     # EXACTLY these 7 fields, in this order
  SubmitFuture (runtime-checkable Protocol): result(timeout=None) / done() / exception(timeout=None)
                     / add_done_callback(fn)
  SubmitBackend (runtime-checkable Protocol): capabilities; n_workers(); close();
      submit(fn, /, *args, key, retries=0, priority=0, resources=None) -> SubmitFuture
          # SubmitFuture args arrive RESOLVED; hints silently ignored without the capability
      broadcast(payload: bytes, *, token) -> handle   # the task fn receives the RAW BYTES
      subscribe_events(topic, handler) -> unsubscribe-callable
      cancel(futures)                                  # no-op-safe on cancel_running=False
  SubmitRunner(backend, *, monitor=None, retries=3): run(plan) -> ExecResult; close();
      context manager (__enter__ returns the runner)
  ThreadBackend(max_workers=N)          # ctor pin (plan §1.2.5 left it open)
  RunContext, WorkerEnv, set_worker_env, current_env   # re-exported (the §1.1 worker seam)

graphed_executors.dask_backend:
  DaskBackend(client, *, replicate_broadcast=False, default_retries=3)
  dask_runner(client, *, monitor=None, retries=3, replicate_broadcast=False) -> SubmitRunner
      # registers the worker plugin; close() must NOT close the caller's client
  plugin.GraphedWorkerPlugin: name == "graphed-worker", idempotent is True
      # re-registration is a no-op — several tests register it beside dask_runner's own
  importing graphed_executors.dask_backend leaves distributed/dask out of sys.modules;
  constructing DaskBackend without distributed -> ImportError containing "graphed-executors[dask]"

Engine behavior pins:
  fixed path: tasks sorted by Task.key; value bit-for-bit == SequentialRunner (identity empty);
      n_partitions == n; n_combines == max(0, n-1); stopped is StopReason.EXHAUSTED; n == 0 -> empty()
  capability flag VALUES pinned per instance:
      ThreadBackend:  peer=True,  scatter=False, pin=False, retries=False, resources=False,
                      cancel=False, file_cache=False
      DaskBackend:    peer=True,  scatter=True,  pin=True,  retries=True,  resources=True,
                      cancel=True, file_cache=False
  keys: every submitted key startswith "graphed-"; n + (n-1) submits per fixed run (nothing else);
      unique within a run; DISJOINT across two runs (per-run nonce); a key-shaped user string arg
      is never substituted (dask#9969)
  broadcast: exactly 2 per fixed run (process, combine); deserialized once per worker pid
      (token cache) while that pid runs > 1 task; the driver deserializes 0 times
  adaptive: events_done counts consumed partitions' n_entries; stop -> stopped == the StopReason,
      backend.cancel(outstanding) with >= 1 future, next_tasks never called at/past the target;
      exhaustion -> EXHAUSTED with zero cancels
  errors: StageError crosses tier-B workers type- and state-intact (__dict__ ==, repickleable);
      KilledWorker -> StageError with err.partition naming the chunk, text naming "KilledWorker"
      and the tcp:// last worker; ordinary exceptions (GraphedError, ValueError) re-raise as themselves
  monitor (M37): topic f"graphed-monitor-…" subscribed via the backend and unsubscribed by run end;
      per LEAF key exactly one SUBMITTED (driver-side) + STARTED + (FINISHED|ERRORED), per-key
      arrival order S < St < F; STARTED/FINISHED .worker ∈ live worker addresses; .partition
      non-empty; ERRORED .error carries the rendered cause; leaf keys ONLY (no combine/broadcast
      events on on_task); observation and a RAISING monitor are byte-identical no-ops on the value
  open_once: plan.open_once=True + plugin-held resources -> opens-per-worker == 1 across that
      worker's tasks; the opener runs IN the worker process (pid-prefixed handle witness)
```

Unpinned on purpose: `Monitor.on_combine`/`on_profile` emission, SUBMITTED's `worker` string,
`DaskBackend.describe_failure` (witnessed end-to-end via the KilledWorker test, not probed
directly), and any engine-internal module layout beyond the public names above.

## Traceability — per test, the WRONG impl it discriminates

| Test | WRONG impl it FAILS |
|---|---|
| `test_fixed_plan_matches_sequential_bit_for_bit` | non-identity empty folds; wrong combine count; a fixed path returning `stopped=None` |
| `test_tasks_are_reduced_in_key_order_not_submission_order` | folding in submission/arrival order |
| `test_adaptive_exhaustion_folds_everything` | ignoring `next_tasks`; dropping late batches |
| `test_worker_error_surfaces_as_intact_stage_error` | wrapping worker errors as strings/RuntimeError on EITHER backend |
| `test_direct_submit_seam_resolves_future_args_and_broadcast_bytes` | an engine/backend hard-coded to dask semantics (ThreadBackend must resolve future args in its wrapper); broadcast handles that don't resolve to raw bytes; hints that crash a capability-less backend |
| `test_capability_flags_are_pinned_and_honestly_differ` | an all-True (or copied) capability stub; a mutable or wrong-shaped `SubmitCapabilities` |
| `test_monitored_run_emits_exact_phases_on_both_backends` | ThreadBackend without the in-process subscribe/emit wiring; dropped or duplicated phases |
| `test_two_runs_are_bit_identical_and_equal_sequential` | any nondeterministic grouping/order; a second run diverging |
| `test_result_is_invariant_to_worker_count` | coffea-style arrival-BATCHED reduction (grouping varies with worker count) |
| `test_second_run_reexecutes_instead_of_returning_cached_futures` | content-only keys without a run nonce (run 2 returns cached partials → marks identical) |
| `test_combines_run_on_workers_with_cross_worker_inputs` | gather-to-driver folding (combine pids == driver); single-worker funnel with no cross-worker input movement |
| `test_leaves_spread_across_workers_on_a_fixed_size_cluster` | the coffea#1490 broadcast-future single-worker pinning |
| `test_process_payload_deserializes_once_per_worker` | closure-per-task payload shipping (deserializations == tasks per pid, dask#5503); driver-side unpickle loops |
| `test_keys_are_namespaced_unique_and_nonced_across_runs` | un-namespaced keys; per-task re-broadcast (submit count > n+(n-1) or broadcasts > 2); key reuse across runs |
| `test_key_shaped_string_args_are_not_substituted` | a key scheme a user string can collide with (dask#9969 substitution) |
| `test_replicate_broadcast_knob_is_wired_and_harmless` | a missing or result-corrupting `replicate_broadcast` |
| `test_stage_error_crosses_the_worker_boundary_byte_equal` | lossy/wrapping error transport (state or frames dropped) |
| `test_killed_worker_becomes_an_attributed_stage_error` | a raw `KilledWorker` escaping; attribution losing the partition or worker |
| `test_mid_run_worker_death_reroutes_and_completes_bit_for_bit` | a runner deadlocking on lost futures; results depending on which process recomputed a leaf; `retries`/reroute hard-wired off |
| `test_stop_condition_cancels_outstanding_and_folds_consistently` | ignoring `StopCondition`; never cancelling (spy == 0); refill-after-stop; a fold inconsistent with the consumed set |
| `test_adaptive_exhaustion_needs_no_cancel` | spurious cancels on a normal run (spy non-vacuity contrast) |
| `test_parquet_aggregate_multi_output_matches_sequential_and_spreads` | any frontend-plan divergence from `SequentialRunner`; driver-side execution of aggregate work |
| `test_external_payload_resolves_worker_side_by_content_hash` | External evaluation that needs driver state (must resolve from the shipped externals map) |
| `test_missing_external_fails_loudly_from_the_worker` | silent skip / opaque wrap of the missing-evaluator failure |
| `test_to_parquet_on_dask_matches_sequential` | path-order or content divergence in the write plan under the dask executor |
| `test_open_once_opens_each_uri_once_per_worker` | per-task (fresh-resources) opens; opens happening driver-side; a missing plugin resources seam |
| `test_worker_plugin_pins` | an anonymous / non-idempotent plugin (breaks late-joining workers and re-registration) |
| `test_exact_phase_contract_over_the_runs_namespaced_topic` | events on the data path or dropped; an un-namespaced or leaked topic subscription; worker events not attributed to workers |
| `test_errored_phase_carries_the_failure` | missing/duplicate ERRORED; FINISHED emitted for a failed task; a bare error object instead of the rendered string |
| `test_observation_is_passive_byte_identical_even_when_the_monitor_raises` | telemetry inflating/perturbing the reduced value; a raising monitor breaking the run |
| `test_import_and_public_surface_without_touching_distributed` | module-import-time `import distributed` anywhere on the two packages' import paths; a missing public re-export |
| `test_missing_distributed_raises_the_hinted_import_error` | a silent `None` backend / bare ImportError without the actionable extra name |

## Runtime / flakiness discipline (R0.10a)

Every gate asserts **structural counters, provenance sets, or content bytes** — pids, worker
addresses, unpickle counts, submit-key multisets, mark-set disjointness, pickle bytes — never a
clock. Sleeps exist only to CONSTRUCT scenarios (staggered completion order; one slow leaf holding
a future open across the stop; the 5 s margin dwarfs any CI jitter), and `wait_for` is a bounded
poll before counter asserts, not an assertion. Worker-death scenarios are in-band and
deterministic: the task kills its own process (single-task plan under `allowed-failures: 1` for
blame; atomic die-once marker under `allowed-failures: 10` for reroute) — no external kill races.
Cluster fixtures follow plan §3: module-scoped, tier A `processes=False` (conformance logic),
tier B `processes=True, n_workers=2, threads_per_worker=1` (serialization/death), all
`dashboard_address=":0"`, context-managed; death tests use dedicated clusters so restarts never
bleed across modules.

## Non-vacuity

Pre-implementation, `graphed_executors.submit` and `graphed_executors.dask_backend` do not exist.
The harness imports them ONLY inside the `*_api`/`make_*` accessors, so the suite **collects
cleanly (47 tests, twice, identical)** while **all 47 tests FAIL in their bodies with
`ModuleNotFoundError: No module named 'graphed_executors.submit'` (or `…dask_backend`)** —
verified and logged in `.graphed/m42/attempts.md`. Scenario engagement is asserted inside the
tests themselves: the death marker must exist, some worker must run >1 task, the mark count must
equal leaves+combines, the contrast runs (exhaustion-no-cancel, clean twin) anchor the witnesses.
A degenerate run-on-the-driver stub fails `worker_side_combines`/`multi_worker_spread`
specifically (pid witnesses), not generically.

## Deviations from plan §3 (recorded per the m42 brief)

1. **Deferred implementation imports** (harness accessors) instead of top-level imports: the
   TEST_SANITY brief requires BOTH clean collection AND per-test right-reason failures; top-level
   imports (the m41 style) would turn absence into collection errors.
2. `pytest.importorskip("distributed")` sits at module level AFTER the import block (E402-clean);
   nothing above it imports distributed (harness imports dask/distributed lazily inside
   `cluster_client`/`where`).
3. **Worker-death mechanics are in-band** (a task killing its own process; a die-once marker)
   rather than an external nanny kill: same witnesses (StageError attribution; leaves re-ran in a
   different process, result unchanged), zero timing races. `allowed-failures` is 1, not the
   blueprint's "0-ish" — 0 is quirk-prone upstream (gh#6078), and 1 exercises the same path.
4. **Pins the blueprint left open, chosen here:** `ThreadBackend(max_workers=N)` ctor; exactly 2
   broadcasts + n+(n-1) submits per fixed run; `stopped is EXHAUSTED` on the fixed path (matches
   the local executors); monitor event key set == leaf keys only; per-key event order.
5. The parity module's parquet surfaces need **pyarrow** at runtime (`ak.to_parquet`): the
   `test-dask` CI job must install it (flagged to the implementer; not a test-visible pin).
6. No `conftest.py` (repo convention): cluster fixtures are per-module wrappers over the harness
   `cluster_client` contextmanager — still module-scoped, still two-tier.
