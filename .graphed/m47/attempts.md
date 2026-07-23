# m47 — implementer attempts log

Milestone: **m47** — parsl HTTP self-rendezvous worker transport + reachability probe + `shuffle_method`
facade. Branch `m46-parsl-backend`. Frozen suite: `tests/frozen/m47/` (54 tests, tag `freeze-m47`).

Integrity: no `tests/frozen/**` file edited (git diff `freeze-m47 -- tests/frozen/` empty); `local/` and
`submit/` untouched; no gate relaxed. Detail per phase below.

## Iteration 1 — build the §1.6 `common/` move + the parsl engines

The plan splits m47 into a code MOVE (m44 bodies → `common/`) shared by dask+parsl, then the parsl
peer engines on top.

- **A. `common/` move (identity-preserving).** `common/transport_run.py` (witness/result dataclasses +
  classifier, matched by exception NAME so dask/parsl-agnostic), `common/facade.py`
  (`resolve_shuffle_method`/`_reject_transport_only_knobs`), `common/transport_tasks.py` (plane-
  parameterized m44 map/gather/gather-join bodies + `replay_reader_plane`). `dask_backend/_transport_run.py`
  and `dask_backend/api.py` rewritten as re-export shims (identity: `dtr.TransportWitness IS
  ctr.TransportWitness`, `dapi.resolve_shuffle_method IS cfacade.resolve_shuffle_method`).
  `merge_counters` kept EXACTLY as the dask original (no generalization) to avoid m44 counter drift.
- **B. `common/http_plane.py`** — `EscalatingHttpTransport` on a dual-route `ThreadingHTTPServer`
  (`/msg` inbox + epoch guard + optional `inbox_maxsize`→503; `/pull` block store, coalesced,
  evict-after-serve). Inline raising send (5 attempts; 503→False+no-retry per review N2; exhaustion→
  raise `TransportDeliveryError`). Idle-deadline recv backstop; attempt-all broadcast. File-backed
  per-(dest,src) `recv_failures` budget (the deliver-then-fail inject seam), persisted across epochs.
- **C. `parsl_backend/transport_peer.py`** — `_parsl_peer_main` (endpoint minted in-task → hello →
  barrier → registry → probe leg → `process_and_reduce` (UNCHANGED M38, review N1) | shuffle body).
  Driver primitives: `_barrier_and_registry` (exactly k hellos, registry_rewrite poison keeps the
  driver's TRUE legs), `_collect_probes`, `_release`, `_cleanup_after_barrier`, restart loop
  (`parsl_run_plan`).
- **D/E. `parsl_backend/transport_shuffle.py`** — `_peer_shuffle_body` (map→manifest→assign→gather),
  `_gather_repartition` (coalesced ONE pull/holder → ≤ k*k incast), `_gather_join` (per-dest
  `gather_join_body`), driver `_run_shuffle`; witness via `replay_reader_plane` (reader budget) + real
  `_join_with_budget` (join budget).
- **F. `parsl_backend/api.py`** — facade sharing the resolver by identity; knob rejection on tasks
  resolution; `peer_transport` gate (TPE→`NotImplementedError`); integrated probe (`ProbeUnreachable`→
  `on_unreachable` error=`StageError` naming the pair / fallback=relay + `witness.fallback_reason`);
  `probe_peer_reachability`→`ProbeReport`.
- **G.** `ParslBackend.peer_transport` property; `start_htex(heartbeat_period=…)`.
- **H.** `.coveragerc-parsl` += `common.http_plane`; `.coveragerc-dask` += the moved
  `transport_run`/`facade`/`transport_tasks`; ci.yml `test-parsl` runs `tests/frozen/m47`.

## Iteration 2 — three bugs surfaced by the frozen suite, fixed non-vacuously

1. **`retry_exhaustion` arm (b): `send_failures` aggregated to 0.** A peer that RAISES on send
   exhaustion returns its counters as the exception, not the result dict — the discarded epoch's
   `send_failures=1` never reached the driver. **Fix:** `_parsl_peer_main` attaches
   `exc.peer_counters` (survives pickling via `BaseException.__reduce__`'s `__dict__`); the reduce +
   shuffle harvest merge it on the error arc. Witness: the test now sees `send_failures>=1` from the
   exhausted-then-restarted run.
2. **`rendezvous_barrier` over-ask: `InvalidStateError` in parsl's result thread.** `pbackend.cancel()`
   raced HTEX dispatch — a cancelled future that still received a result crashed parsl's result-queue
   thread, which pytest escalated to a failure. **Fix:** `_cleanup_after_barrier` no longer cancels;
   the still-queued peer self-terminates via its bounded rendezvous wait, and the freed seated slots
   are enough for the liveness probe.
3. **`worker_death` budget-0: attributed error named no death signal.** The shared `build_stage_error`
   renders a described death as `cause_type="KilledWorker"`, and `str(parsl.WorkerLost)` carries no
   class-name literal, so none of `DEATH_SIGNALS` matched. **Fix:** the shuffle driver builds
   `_shuffle_stage_error` with `cause_type=type(exc).__name__` (WorkerLost/PullTimeoutError/…), still a
   `StageError` (no raw parsl exception leaks) — backend.describe_failure's own docstring anticipated an
   m47 driver layering context on the named signal.

Gates after iteration 2: mypy --strict clean (39 files); ruff clean; full `tests/frozen/m47` green
(see the freeze-verification + coverage runs recorded below). Note: a spurious `ManagerLost` was
observed only when TWO HTEX suites ran CONCURRENTLY on one machine (heartbeat starvation) — never in a
sequential run; the CI job runs the suite sequentially.
