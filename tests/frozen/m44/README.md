# m44 frozen suite — graphed-executors (WorkerTransport atop dask worker-to-worker comms)

Milestone **m44** (`m44-transport-plan.md` — final at r3; research: `m44-transport-research.md`).
This suite freezes graphed's M38 `WorkerTransport` Protocol implemented ATOP dask's worker-to-worker
comm layer (the P2P pattern: WorkerPlugin-registered handlers + `worker.rpc` pooled RPC), hosting
the M38 peer reduction and the M39–M41 shuffle/join engine on dask workers with an **O(T+P)**
scheduler graph — zero pick-shaped futures. The m43 future-graph engine and every earlier frozen
suite stay untouched and binding.
**Frozen — read-only after the m44 freeze tag.**

## Files → theme (plan §3)

| File | Theme | What it witnesses |
|---|---|---|
| `transport_harness.py` | harness | pinned execution contract (below); deferred impl accessors (right-reason `ModuleNotFoundError` pre-impl); awkward+numpy adapters (new names); duplicating + pandas relational oracles (route via the golden-pinned `partition`, never the join kernel); `SpyDaskBackend` submit-seam tap recording (fn, key, `workers=` pin); capability-gate stubs (FU2 `PinlessBackend` + all-False `NoPeerTransportBackend`); `GatedTransportBackend` death scenario (F9); pinned-micro-task + `client.run` plugin probes; `run_bounded` F2 hard-timeout driver |
| `test_transport_protocol_conformance.py` | Protocol conformance (tier B) | isinstance inside a worker task; send→True + (src,msg) round-trip; recv(None-on-empty); `inbox_maxsize=1` flood ⇒ some `send()==False` + receiver `sends_dropped>0`; driver endpoint (`"driver"`, peers, worker→driver channel, broadcast lands in every inbox) |
| `test_transport_peer_reduce_dask.py` | M38 reduction hosted unchanged (tier B) | value == `SequentialRunner` bit-for-bit, twice (fresh nonce each); exactly k `_dask_peer_main` through the submit seam; cross-worker `("node",…)` hand-off (`peer_sends` on the non-root, `recv_invocations` on the root owner); `sends_dropped == 0`; zero restarts |
| `test_transport_reduce_dedup.py` | at-least-once safety (F10) | one deliver-then-raise injection ⇒ the REAL retry duplicates a landed message: `recv_duplicate_deliveries ≥ 1` (content-digest) + `sends_retried ≥ 1` while the value stays bit-identical (the `(level,pos)` dedup suppressed the double combine) |
| `test_transport_retry_exhaustion.py` | §1.1 non-loss retry/raise arc (r3) | (a) N=2<`SEND_RETRIES` ⇒ `sends_retried ≥ 2` + bit-identical result, 0 restarts; (b) N=8 ⇒ exhaustion ⇒ `epoch_restarts == 1`, two nonces, purge, completion; (c) N=8 with `epoch_restarts_allowed=0` ⇒ `StageError` NAMING TransportDeliveryError — never a hang (F2 hard timeout) |
| `test_transport_repartition_parity.py` | m39 behavioral golden | `dest_block_hashes` **byte-identical** to local `run_repartition` AND across two dask runs (fresh nonces); per-dest keys == the golden-pinned `partition` route oracle; row conservation |
| `test_transport_join_parity.py` | m40 behavioral golden | inner/left/outer rows == local `run_join` == null-aware `pandas.merge` oracle (option-typed nulls — the `-1`-sentinel trap re-armed); inner `dest_block_hashes` == local; under `mem_budget_bytes` (m43 sizing, local-engine scenario guard): spill engaged, `join_spilled_partitions == join_chunks_read` (F5) == local's, `peak_join_bytes ≤ budget`, 40 000 duplicated rows == oracle |
| `test_transport_join_plan_choice.py` | F6 | auto choice `parts`-keyed: SHUFFLE on the 1-worker cluster where a live-`n_workers()` recompute says broadcast; 1w vs 2w bit-for-bit; broadcast path: ZERO `_transport_map_task`, one `_transport_broadcast_join_part` per probe block, ≥1 `backend.broadcast`, `broadcast_puts == 2` (distinct resolving workers, k=2), `large_side_blocks == 0`; forced True/False honoured |
| `test_transport_budget_parity.py` | m41 goldens + F12 | fetch plane: spill engaged + `fetch_spill_count`/`peak_fetch_bytes` EQUAL to local (workers=2, same budget), `peak ≤ budget+one_block`, `0 < bulk_fetch_count ≤ P·k`; disk plane: `disk_backpressure_events > 0` under skew + budget echo + worker-address keys (exact equality with local = recorded FU1 dispute candidate, NOT pinned); holder plane (F12): `holder_spill_count > 0` under a small `holder_budget_bytes`, `peak_holder_bytes ≤ budget+one_wire`; budgets never change bytes |
| `test_transport_task_complexity.py` | O(T+P) structural gate (F11) | per-class EXACT counts across {P,2P,4P} x {T,2T}: `_transport_map_task == T`, `_transport_gather_task == P`, control tail CONSTANT; `_dask_pick == 0` and no fn name contains "pick"; row-count independence (8x rows, same counts); every map/gather submit strict-pinned (single address; maps follow F8 `sorted_addrs[t % k]`), pin sequences identical across two runs |
| `test_transport_block_plane.py` | bytes never through the driver (tier B) | `bytes_served > 0` on BOTH workers; `serve_pid` == live worker pid ≠ driver pid; `cross_node_fetches > 0`; hashes == local; post-run NO epoch of the run active on any worker (evict + purge) |
| `test_transport_epoch_guard.py` | stale rejection (P2P run_id guard) | raw `graphed_transport_recv` RPC with a bogus nonce AND with a completed run's (purged) nonce both answer `{"accepted": False, "stale": True}`; `stale_epoch_rejects ≥ 2`; runs before/after stay byte-identical to local |
| `test_transport_worker_death.py` | §1.5 whole-run restart (tier B, F2/F9) | gated deterministic mid-gather kill on `allowed-failures=0` clusters; restart arm: bit-for-bit == local oracle, `epoch_restarts == 1`, two nonces, stage-1 re-ran (pmarks > n_src), full purge; budget-0 arm: `StageError` (type-pinned) naming the victim address — never raw `KilledWorker`, never a hang (thread-join hard timeout) |
| `test_transport_loop_discipline.py` | threading bridge (tier A for FU3 coverage) | `recv_on_loop == recv_invocations ≥ 1` (impl-recorded thread ids); `recv()` off-loop in the task thread; bounded flood rejected + counted without wedging the loop (fresh epoch round-trips after) |
| `test_transport_seam_canary.py` | §1.7 drift tripwire (FU4-softened) | `canary_ok` True per worker + the three `graphed_*` ops registered; `worker.handlers` frozen read-only BEFORE registration ⇒ `RuntimeError` naming the graphed-transport seam AT register time |
| `test_transport_capability_gate.py` | honest degradation (MAIN matrix, dask-free) | FU2 `PinlessBackend` (peer=True/pin=False — the frozen-m42 ThreadBackend shape) and all-False stub ⇒ `NotImplementedError` naming the pinning requirement + "Phase 2" for ALL THREE entry points, ZERO submits/broadcasts; subprocess probe: importing `transport_peer`+`transport_shuffle` leaves `distributed`/`dask` out of `sys.modules` (F13 — `transport.py` itself is import-dirty by design and is not probed) |

## Pinned execution contract (test-author decisions — full text in `transport_harness.py` docstring)

```
graphed_executors.dask_backend.transport:
  GraphedTransportPlugin      name="graphed-transport", idempotent=True (m42 plugin idiom);
                              handlers graphed_transport_recv(src, epoch, data) /
                              graphed_block_pull(epoch, digests) / graphed_transport_ping(nonce);
                              worker.extensions["graphed-transport"]; canary_ok; stale_epoch_rejects;
                              inject_recv_failures (deliver-then-raise test seam);
                              active_epochs(); counters(epoch) -> {recv_invocations, recv_on_loop,
                              recv_duplicate_deliveries, sends_dropped, bytes_served, serve_pid, ...}
  make_transport_spec(epoch, worker_addresses, *, inbox_maxsize=None, overlay=None)  # picklable;
                              overlay=None => full mesh + "driver" (engine passes its bounded overlay)
  open_endpoint(spec)         worker-task-side; idempotent per (worker, epoch) — state persists
  open_driver_endpoint(backend, spec)   address == "driver"; peers() == spec worker addresses
  recv reply: {"accepted": True} | {"accepted": False} | {"accepted": False, "stale": True}
  send policy: SEND_RETRIES = 5 total attempts; live accepted:False => immediate False (drop signal);
               comm-class failure => retry; exhaustion RAISES TransportDeliveryError (never silent False)

graphed_executors.dask_backend.transport_peer:
  transport_run_plan(plan, backend, *, monitor=None, inbox_maxsize=None, epoch_restarts_allowed=1)
graphed_executors.dask_backend.transport_shuffle:
  transport_run_repartition(backend, src_blocks, parts, *, dbackend, salt=0, n_tasks=None,
                            fetch_budget_bytes=None, disk_budget_bytes=None,
                            holder_budget_bytes=None, epoch_restarts_allowed=1)
  transport_run_join(backend, left, right, parts, *, on=("__joinkey__",), how="inner", dbackend,
                     broadcast=None, salt=0, mem_budget_bytes=None, epoch_restarts_allowed=1)

Results: shuffle -> .dest_block_hashes / .value / .witness (the imported ShuffleWitness names — every
m39/40/41 counter keeps naming the same mechanism) / .transport; plan -> ExecResult-shaped + .transport.
result.transport: .epoch_nonces (len == 1 + epoch_restarts), .epoch_restarts,
                  .per_worker[addr] ⊇ plugin counters + {sends_retried, peer_sends,
                  holder_spill_count, peak_holder_bytes}  (tests read .get(key, 0)).
Task fns (classified at the dbackend.submit seam, ALL strict-pinned single-address):
  _transport_map_task (T, sorted_addrs[t % k]) · _transport_gather_task (P) ·
  _transport_gather_join · _transport_broadcast_join_part · _dask_peer_main (k)
Capability gate: dbackend.capabilities.pin_to_worker AND .peer_data_movement checked FIRST;
  NotImplementedError names the pinning requirement + "Phase 2" before any submit/broadcast.
```

## Empirical pins recorded at authoring time (measured, distributed 2026.7.1)

- **F2 (worker-death exception type)**: a strict-pinned task (`workers=[addr]`,
  `allow_other_workers=False`, `retries=0`) whose worker is hard-killed raises
  `distributed.scheduler.KilledWorker` **promptly (<1 s)** with `exc.last_worker` = the victim's
  WorkerState — **only under `distributed.scheduler.allowed-failures: 0`**. Under the default
  config the future **hangs indefinitely** (observed 90 s with no error: the pin can never be
  satisfied by the nanny-restarted worker's new address). Hence: the death module pins
  `allowed_failures=0` on its dedicated clusters, and every hang-risk test drives the engine on a
  daemon thread with a HARD join timeout.
- Scenario guards: every budget/choice scenario was validated against the LOCAL engine (both
  adapters) before freezing — the fetch/disk/join budgets provably engage their mechanisms, the
  F6 discrimination gaps are real for the measured bytes, and the routing/relational oracles agree
  with the local engine on every `how`.

## Wrong-implementation discrimination

| Wrong implementation | Failing test / assert |
|---|---|
| Driver-side fold posing as peer reduction | peer_reduce: `_dask_peer_main != k`, zero `peer_sends`/`recv_invocations` |
| m43-shaped engine (T·P picks) delegated under the new names | task_complexity: gather count != P, pick-namespace gate |
| Per-row/per-fragment task creation | task_complexity: row-count independence |
| Driver-relay data plane (blocks through client process) | block_plane: `bytes_served == 0`, serve_pid provenance |
| `_asend` returns silent False on transient comm failure (pre-r2 bug) | retry (a): `sends_retried == 0` + lost node surfaces (bounded) |
| No exhaustion raise / raw error leak | retry (b/c), worker_death: restart witness / StageError type pin |
| Exactly-once transport (wire-level dedup) | reduce_dedup: `recv_duplicate_deliveries == 0` |
| No consumer dedup (double combine) | reduce_dedup: value diverges from SequentialRunner |
| No epoch guard / no purge | epoch_guard: stale replies accepted, counter 0; block_plane/death purge probes |
| Live-`n_workers()` broadcast recompute (F6) | join_plan_choice: 1-worker run flips to broadcast |
| Broadcast that secretly shuffles the large side | join_plan_choice: `_transport_map_task > 0`, `large_side_blocks != 0` |
| Whole-dest-resident gather (no fetch budget) | budget_parity: `peak_fetch_bytes` bound + local equality |
| Unbounded holder retention (F12) | budget_parity: `holder_spill_count == 0` under a small budget |
| Sync/blocking work inside handlers | loop_discipline: `recv_on_loop` mismatch or bounded calls time out |
| Fake canary (`canary_ok = True` hard-coded) | seam_canary drift arm: no RuntimeError on the frozen handlers dict |
| Silent fallback on pin-less backends | capability_gate: refusal + zero-touch asserts (FU2 stub isolates the NEW flag) |

## Non-vacuity (TEST_SANITY evidence)

Pre-implementation, all **47 collected tests fail in their bodies** with the right-reason
`ModuleNotFoundError` (`graphed_executors.dask_backend.transport` / `transport_peer` /
`transport_shuffle`), the capability-gate subprocess probe included; collection is clean on the
dask-free path (`pytest.importorskip("distributed")` in every dask-touching module; the harness
imports no dask at module level). All witnesses are counters, content hashes, pids, thread ids,
worker addresses, and file marks — **never wall-clock assertions** (R0.10a); recv timeouts, poll
loops, and thread-join bounds are hang guards / scenario construction only. Deliberately NOT
pinned (recorded): exact `disk_backpressure_events` equality with the local engine (FU1 dispute
candidate — engagement + echo + per-worker keys are pinned instead), `n_combines` accounting for
the peer runner, and the gather-task dest-owner formula (only strict single-address pinning +
run-to-run determinism are frozen).
