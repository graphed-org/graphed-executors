# m47 frozen suite — graphed-executors (parsl HTTP self-rendezvous transport + probe + facade)

Milestone **m47** (parsl-backend-plan §1.4-§1.6, §2 m47, §3 m47 — plan FINAL at r3; the two
r3-verify minors are honored in-suite: **N1** the reduce actor is `process_and_reduce`, **N2** the
flood arm discriminates a retry-then-raise-on-503 `send()`). This suite freezes the
**p2p-exchange engine** — k persistent peers minting `(host, port)` in-task on the
`EscalatingHttpTransport` dual-route (`/msg` + `/pull`) plane, driver-hosted rendezvous with a
barrier on k hellos, epoch/restart/attribution — plus the **runtime reachability probe** with
`on_unreachable={"error","fallback"}` and the **parsl facade** sharing ONE resolver object with
the dask facade. **Frozen — read-only after the m47 freeze tag.**

## Files → theme (§3 m47)

| File | Theme | What it witnesses |
|---|---|---|
| `parsl_transport_harness.py` | harness | the FULL pinned m47 contract (module docstring); deferred accessors (right-reason MNFE pre-impl); `p2p_htex_pool` (+ the `heartbeat_period` m47 `start_htex` extension), `p2p_tpe_pool`; `SpyP2PBackend` (raw fn names at the SubmitBackend seam); the stale-epoch injector (raw POST speaking the pinned `/msg` envelope); the registry-poisoner (`registry_rewrite` + closed-port factory — connection-refused, no clocks); the deliver-then-fail injection seam pin (`inject_recv_failures`); numpy+awkward adapters, `partition`-routed oracles, the cross-fragment byte oracle, `GatedP2PShuffleBackend` |
| `test_parsl_transport_conformance.py` | WorkerTransport obligations + the four §1.4 escalation arms | in-task isinstance + round-trip on a worker pid; empty recv/poll; self/unknown→False; flood: 503→`send()==False` + `sends_rejected` + `recv_rejects`, `sends_retried==0` (N2), accepted msg survives; exhaustion: RAISES `TransportDeliveryError` + `send_failures` (the drop→raise bridge); idle-deadline recv raise (design-A backstop); broadcast attempt-all-then-raise with a live peer ordered AFTER the dead one |
| `test_parsl_epoch_guard.py` | obligation 9 (MAIN-matrix POSIX — http_plane is stdlib-only) | wrong-nonce POST over the real wire: never delivered + `stale_epoch_rejects` counted; same message under the live nonce: delivered `(sender, message)` intact, not counted |
| `test_parsl_rendezvous_barrier.py` | §1.4 rendezvous, G5, + the exactly-k-hellos third of the reshaped guard | per-peer `endpoint_pid` = distinct worker pids ≠ driver, OS-assigned distinct ports; `registry_size_at_receipt == k` on every peer (no partial book before the k-th hello); driver `recv_class_hello == k`; over-ask (k+1 > slots): bounded `StageError` naming "2/3" + a post-error submit proves every slot was freed |
| `test_parsl_peer_reduce.py` | M38 hosted on parsl (N1) | == `SequentialRunner` bit-for-bit ×2 under fresh nonces; spy == k `_parsl_peer_main` (no per-leaf futures); w1 `peer_sends`≥1 + w0 `peer_recvs`≥1 (genuine cross-worker hand-off); driver `recv_class_node == recv_class_leaf == 0` with `recv_class_root ≥ 1`; zero engine-task names on the reduce path |
| `test_parsl_reduce_dedup.py` | at-least-once safety (F10) | budget-1 deliver-then-fail on w0←w1: `recv_duplicate_deliveries ≥ 1` (digest-keyed) + `sends_retried ≥ 1` + value bit-identical + zero restarts |
| `test_parsl_retry_exhaustion.py` | hard constraint 8 — the §1.4 inline-send arc | N=2<5: `sends_retried ≥ 2`, parity, zero restarts; N=8≥5 allowed=1: `epoch_restarts == 1`, 2 distinct nonces, `send_failures ≥ 1`, parity (retry+restart COMPOSE; an errored peer future is run-fatal even though the injected deliveries landed); allowed=0: `StageError` naming TransportDeliveryError; own pool per arm; hard timeouts |
| `test_parsl_transport_repartition_parity.py` | m39 golden, both adapters | hashes == LOCAL engine AND == the m46 relay engine, ×2 runs, fresh nonces; `partition`-oracle routing; row conservation; salt reroutes deterministically |
| `test_parsl_transport_join_parity.py` | m40 golden, both adapters | inner/left/right/outer null-aware multisets == local; unmatched key-13/17 counts EXACT per how (measured); inner == duplicating oracle + byte-identical hashes; pyarrow importorskip'd loudly (m46 measured fact + CI pin) |
| `test_parsl_block_plane.py` | §1.4 block plane + the in-task half of the reshaped guard (tests-M-c) | Σ holder `bytes_served` == the independent cross-fragment oracle (self-fragments never ride HTTP; driver-relay breaks equality); `serve_pid == endpoint_pid ≠ driver`; **`pull_requests_served` Σ ≤ k·k = 4 at parts=8>k=2** with the in-test guard `n_cross_frags = 7 > 4` (the BUILT counter — m44 has none to inherit); `store_blocks_at_return == 0` (evict); hashes == local |
| `test_parsl_budget_parity.py` | m41 goldens via the shared bodies | fetch: local scenario guard + `fetch_spill_count > 0` + `peak_fetch_bytes ≤ budget+one_block` + bytes unchanged; join: `join_spilled_partitions > 0` `== join_chunks_read` (F5), `peak_join_bytes ≤ budget`, rows == n², hashes == local |
| `test_parsl_task_shape.py` | the reshaped anti-super-linear submit-seam ladder (tests-M3) | `_parsl_peer_main == k` + zero engine-task names in every {parts,2·parts}×{T,2·T} cell; ONE constant total across cells (k + O(1) control); 8× rows change nothing |
| `test_parsl_reachability_probe.py` | G2 — the cluster decides | standalone probe: healthy ok/no-pairs, poisoned ok=False + pairs naming w1; facade default(=error): `StageError` naming w1, bounded; `"fallback"`: relay-hash parity + `head_node_routed` + `fallback_reason` set + relay Counter after k released peers + no `.transport`; pool alive after EVERY arm |
| `test_parsl_shuffle_method_facade.py` | §1.5 facade | resolver IDENTITY (`is` — parsl==dask==common); auto/tasks → relay shape + head-node witness, no `.transport`; explicit transport → k peers + `.transport` + hashes; invalid → ValueError naming three, zero submits; 5 transport-only knobs (incl. `on_unreachable`, `workers`) each rejected BY NAME on tasks resolution; `peer_transport` True(HTEX)/False(TPE); TPE explicit transport → NotImplementedError naming "tasks", zero submits |
| `test_parsl_worker_death.py` | obligation 17 | gated SIGKILL of a provably-blocked block-holding peer (marks+gate, no clocks; `heartbeat_period=2` fixture — measured 1.65 s WorkerLost); allowed=1: bit-for-bit vs the plain local oracle + `epoch_restarts == 1` + 2 nonces + pmark count > n_src (map re-ran in the fresh epoch); allowed=0: type-pinned `StageError`, LENIENT on the death signal |
| `test_parsl_transport_imports.py` | §1.6 import rule + G9 for the NEW modules (MAIN matrix) | subprocess: `common.{http_plane,transport_run,facade,transport_tasks}` import parsl/dask/distributed-free; `parsl_backend.{api,transport_peer,transport_shuffle}` additionally never pull `dask_backend`; shim identities: dask `resolve_shuffle_method`/`TransportWitness` et al. ARE the common objects |
| `test_parsl_transport_packaging.py` | §1.6 item 2 m47 deltas + IT5 (MAIN matrix) | `.coveragerc-parsl` += `common.http_plane` (moved modules stay OUT — one gate per module); `.coveragerc-dask` += `common.{transport_run,facade,transport_tasks}` (relay/http_plane banned); fail_under 90 + branch untouched in both; test-parsl step runs m47, keeps the bare `--cov`, no `--cov=` |

## Pinned execution contract

The FULL restatement (module paths, signatures, counter names, envelope, ownership rules, the
`"w0".."w{k-1}"` address scheme, per_worker/driver-row keys) lives in the
`parsl_transport_harness.py` module docstring — single source, kept next to the accessors that
enforce it. Highlights the implementer must not miss:

- `EscalatingHttpTransport(address, *, epoch, host, inbox_maxsize=None, idle_deadline_s=None)` —
  inline raising send (503-before-retry, N2), idle-deadline recv raise, attempt-all broadcast,
  `/msg` envelope `pickle.dumps((sender, epoch, message))`, counters
  `sends_retried`/`send_failures`/`sends_rejected`/`recv_rejects`/`stale_epoch_rejects`.
- `parsl_run_plan(plan, pbackend, *, monitor, workers, inbox_maxsize, epoch_restarts_allowed,
  barrier_timeout_s, inject_recv_failures)`; `transport_run_repartition(... , pbackend, salt,
  workers, fetch_budget_bytes, pull_timeout_s, epoch_restarts_allowed, barrier_timeout_s,
  registry_rewrite)`; `transport_run_join(..., on, how, pbackend, broadcast, salt,
  mem_budget_bytes, workers, pull_timeout_s, epoch_restarts_allowed, barrier_timeout_s)`.
- `parsl_backend.api.run_repartition/run_join` (dask-facade knob names + `workers` +
  `on_unreachable=None`≡"error" + `registry_rewrite`), `probe_peer_reachability(pbackend, k, *,
  timeout_s, registry_rewrite) -> ProbeReport(ok, failed_pairs)`, the SHARED resolver object,
  `ParslBackend.peer_transport` (m47-new), `start_htex(..., heartbeat_period=None)`.

## Traceability — test ↔ plan section ↔ Implementation Target ↔ wrong impl it kills

| Test | Plan | IT (m47) | WRONG impl it FAILS |
|---|---|---|---|
| transport_conformance | §1.4 send design, design-A/B, N2 | IT1 | always-True send stub (flood); verbatim `_Handler` replica (cannot 503); base-class delegation (`drops += 1`, no raise, no recv deadline, stop-at-raise broadcast); retry-then-raise on 503; `.drops`-counting tests would be vacuous — the pinned counters are the subclass's own |
| epoch_guard | §1.4 epoch guard, obligation 9 | IT1 | guard-less inbox (stale node delivered); count-less guard; a drifted `/msg` envelope (live-epoch arm fails delivery) |
| rendezvous_barrier | §1.4 rendezvous, G5, design-minor-2 | IT2 | driver-assigned ports (pid provenance); incremental/barrier-less registry (size-at-receipt < k); hanging over-ask (hard timeout); leaked slots (post-error submit starves on the 2-slot pool); per-task hellos (driver count ≠ k) |
| peer_reduce | §1.4 step 3 reduce (N1), §3 row | IT2 | driver-side fold (zero cross-worker hand-offs; node/leaf classes at the driver); per-leaf futures (spy count > k); relayed partials (driver `recv_class_node > 0`) |
| reduce_dedup | §1.4 at-least-once + (level,pos) dedup, F10 | IT1+IT2 | exactly-once assumption (double combine → value diverges); wire-level digest dedup (duplicate never delivered → counter 0); unwired seam (both counters 0) |
| retry_exhaustion | §1.4 send escalation + epoch restart | IT1+IT2 | the m44-r3 silent-False class (base delegation: arm (a) loses the node, (b)/(c) never raise); root-arrived-so-ignore-the-errored-peer (restart pin); raw delivery-error leakage (StageError type pin) |
| transport_repartition_parity | §1.4 engine, §2 acceptance | IT3 | any routing/merge/wire drift (byte equality vs local AND relay); dropped salt; nonce reuse |
| transport_join_parity | §1.4 join, F1 carriers | IT3 | dedup joins (multiset); dropped/duplicated unmatched carriers (exact 13/17 counts per how); sentinel nulls (option-typed reader) |
| block_plane | §1.4 block plane, tests-M2/M-c | IT1+IT3 | driver-relay (Σ bytes_served ≠ oracle; driver-pid provenance); per-fragment puller (7 > 4 trips ≤ k·k); /msg-piggybacked blocks (route-specific counters 0); no evict (`store_blocks_at_return`); re-pull-after-evict (oracle equality, F5) |
| budget_parity | §1.4 budgets via common/transport_tasks | IT1(common)+IT3 | unbounded gather (peak > budget+one_block); no spill under the guard-validated budget; `_join_with_budget` re-implementation (F5 equality breaks); budget-changed bytes |
| task_shape | §3 omissions item 3 (tests-M3) | IT2+IT3 | per-partition/per-src task engines (total varies across the ladder); relay-in-disguise (`_dask_*` names at the seam); per-row task creation (8× rows) |
| reachability_probe | §1.5 G2 + §3 row | IT4 (+IT2 probe leg) | probe-less engine (data before verdict → wrong error class); silent fallback (`fallback_reason` missing); fallback re-entering p2p (relay Counter absent); stranding release (liveness submit) |
| shuffle_method_facade | §1.5 facade + design-M3 | IT4 | copied resolver (`is`); hardcoded-engine facade (dispatch shapes); silent knob drops (5 named rejections); TPE "transport" that runs (honesty refusal); capability-field transport gates (attribute pin) |
| worker_death | §1.4 epoch/restart, §0.3 measured latency, r3 tests-(d) | IT2+IT3 | stale-block reuse (byte divergence); no restart (pmark count / hang); raw WorkerLost leakage; restart-budget ignore (allowed=0 green) |
| transport_imports | §0.4, §1.6 import rule, G9 | IT1 | eager parsl/dask imports in new modules; parsl→dask_backend coupling; copied resolver/witness classes (incomplete move) |
| transport_packaging | §1.6 items 2-3, §2 IT5 | IT5 | zero-gated `http_plane` (or double-gated moved modules); un-gated moved m44 engine (missing dask sources); a `--cov=`/`--cov`-less step regression; a frozen-but-never-run m47 suite (missing pytest path) |

## Test-author judgment calls — FLAGGED for the freeze ruling

These go beyond the plan's letter (the m46 pyarrow-precedent protocol). Each is the minimal pin
that makes a plan-mandated frozen witness realizable; none contradicts plan text.

1. **`pbackend` kwarg + `"w0".."w{k-1}"` peer addresses + ownership rules pinned.** The plan says
   "same knob names" as the dask facade but never names the backend kwarg or the address scheme.
   The injection seam, the per_worker counter reads, and the block-plane byte oracle all need
   addressable peers, so the harness pins `pbackend`, `f"w{i}"` addresses (sorted==index for the
   k ≤ 3 used here), producer = contiguous ascending split (the frozen m43 `_assign` contract),
   gather owner = `d % k` (the m44 F8 round-robin).
2. **`inject_recv_failures` as a spec-carried entry-point knob** (`parsl_run_plan` only). The
   plan's blueprint demands a "gated failing endpoint (refuses N times, then heals)" driving
   engine-level retry/exhaustion/dedup arms, and m44's proven mechanism is the plugin's
   deliver-then-fail budget — but parsl has no `client.run` side channel, so the seam must ride
   the spec. Framed exactly like m44's (deliver into the inbox, decrement, fail the response with
   a NON-503 comm-class failure).
3. **`registry_rewrite` as a test seam** on `probe_peer_reachability` / facade / p2p repartition.
   The plan's harness row names a "registry-poisoner (rewrites one entry to a closed port)";
   there is no external interposition point on a driver-internal book, so the rewrite hook is
   pinned, with the driver's own control legs keeping the true endpoints (release/attribution
   still work — which the pool-liveness assertions witness). An advertise-host blackhole was
   rejected: it would break the driver→peer registry broadcast itself (50 s of doomed retries
   before a NON-probe error).
4. **`barrier_timeout_s` spec-carried knob** — the over-ask arm needs a small barrier bound; the
   production default must exceed the 25 s send worst-case, so the knob (not a shrunken default)
   is the testable surface. Error message pinned to contain "2/3" (the plan's own
   "hellos_seen/k" notation).
5. **`start_htex(..., heartbeat_period=None)` extension** — the plan names `heartbeat_period` as
   the fixture-compression knob; MEASURED here (scratchpad `hb_probe.py`): `=2` → SIGKILL→
   `WorkerLost` in 1.65 s, pool survives, next submit 0.01 s. Ordinary pools call the frozen m46
   signature (no kwarg), so only the death module depends on the extension.
6. **Three files beyond the plan's 14-file table**: `test_parsl_task_shape.py` (the reshaped
   anti-super-linear ladder is plan-MANDATED but homeless in the table), `test_parsl_transport_imports.py`
   (the §1.6 "frozen-tested" import rule cannot be enforced on m47 modules by the m46 file,
   frozen before they existed), `test_parsl_transport_packaging.py` (the §1.6 m47 coverage
   deltas + IT5, per the m46 packaging-pins precedent).
7. **Witness-surface pins** (per_worker keys incl. `endpoint_pid`/`endpoint_port`/
   `registry_size_at_receipt`/`store_blocks_at_return`/`pull_requests_served`, the driver
   `recv_class_*` row, `ProbeReport(ok, failed_pairs)`, `witness.fallback_reason`): the plan
   demands these MECHANISMS be frozen-witnessed; the names are the minimal observable surface,
   read with `.get(key, 0)` so richer implementations stay green.
8. **Death-signal leniency widened by one**: `TransportDeliveryError` added to the plan's
   {PullTimeoutError, WorkerLost, barrier} — the driver's attempt-all release to a dead peer
   legitimately surfaces as a delivery error; same anti-race rationale as r3 tests-(d).
9. **Epoch guard frozen at the endpoint level** (where §1.4 places the mechanism): engine-level
   cross-epoch traffic is not synthesizable without timing races (fresh per-epoch ports make it
   "doubly improbable" — the plan's words), so the wire-level guard + counter is the frozen
   surface, exercised over a raw POST speaking the pinned envelope.

## Fixture discipline + measured facts (measured while authoring, parsl 2026.7.20, py3.12, macOS)

- `heartbeat_period=2`: SIGKILL→WorkerLost **1.65 s**, watchdog respawn in place, post-kill
  submit 0.01 s (`hb_probe.py`). The default-path ~29.7 s latency is why the death fixture
  compresses; `pull_timeout_s=20` keeps the survivor's dead-holder leg fast as well.
- Scenario oracles pre-validated against the FROZEN local engine (`scenario_probe.py`): fetch
  one_block=832/budget=416 → spill_count 7, peak 640, hashes unchanged; join 64×64 @ parts=8,
  budget=16384 → spilled 7 == chunks_read, peak 15360, rows 4096; cross-fragment oracle (6 srcs,
  parts=8, k=2): 13 fragments, **7 cross** (> k·k=4 — the per-fragment regression provably trips
  the pull bound), 1200 cross bytes; unmatched rows: inner 16 (no 13/17), left 18 (13×2), right
  18 (17×2), outer 20 (both).
- PYTHONPATH export before `start_htex` (HTEX workers don't inherit pytest's sys.path — the m46
  measured fact), replicated for THIS directory; PATH must include `.venv/bin` (interchange
  console script).
- Fixture scoping per plan §3: module-scoped shared pools for read-only conformance/parity/shape
  themes; the death module and each retry-exhaustion arm build DEDICATED pools (a killed or
  restart-scarred pool must not poison a shared one). ~10 HTEX startups ≈ 1.7 s each across the
  suite. Every pool-touching call runs under a hard-timeout driver; witnesses are counters,
  hashes, pids, site provenance — never clocks (R0.10a).
- The two MAIN-matrix modules (imports, packaging) need neither parsl nor a pool; `epoch_guard`
  needs only stdlib (http_plane's frozen import rule makes that an operational witness) and is
  win32-skipped with the G8 reason like every plane-touching module.

## Non-vacuity evidence (TEST_SANITY)

- Pre-implementation the suite COLLECTS cleanly and every test fails in-body for the right
  reason: `ModuleNotFoundError` on `graphed_executors.common.http_plane` / `.transport_run` /
  `.facade` / `.transport_tasks` / `parsl_backend.transport_peer` / `.transport_shuffle` /
  `.api`, `AttributeError` on the m47 `peer_transport` attribute, `TypeError` on the m47
  `start_htex(heartbeat_period=)` extension (the death module), and the packaging pins failing on
  the exact missing m47 content. Two consecutive runs produce IDENTICAL failsets (evidence in the
  authoring report).
- No test passes pre-implementation (unlike m46 there are no preservation-guard passes: every
  module touches a planned m47 surface).
- Coverage wiring (§1.6 m47 allocation): this suite is what executes `common.http_plane`
  (endpoint arms drive the 503/exhaustion/idle/stale/evict branches — the named coverage-risk
  module), `parsl_backend.transport_peer/transport_shuffle/api`, and `common.relay_engine`
  (fallback + facade arms) on the parsl job; the moved `common.transport_run/facade/
  transport_tasks` keep their gate on the dask job via the untouched m42–m45 suites. The
  packaging test freezes exactly that allocation plus the bare-`--cov` step shape.

## Deliberate omissions (decisions, not gaps)

- Engine-level stale-epoch injection, `inbox_maxsize` on engine runs, and drain-before-restart
  are not directly frozen (endpoint-level guard + the death/exhaustion restart pins cover the
  reachable mechanisms; the rest is untestable without timing races — judgment calls 2/9).
- The three m44 families the plan lists as reasoned omissions (seam canary, loop discipline,
  O(T+P) per-submit slope) are honored: their substitutes are the `start_htex` drift canary (every
  fixture), the flood/inline-send arms, and the reshaped k+O(1)/≤k·k/exactly-k guard trio.
- In-task kernel LOOP compute stays un-witnessed (the plan's stated reasoned omission — budget
  counters bound bytes, not loop counts).
- No monitor-events tests on transport paths (`emit=False` — the dask precedent, plan G10).
- No disk-budget (T3) arm: the plan's m47 budget blueprint pins fetch + join only; the parsl
  entry points pin no `disk_budget_bytes` knob (the facade rejection list matches).
