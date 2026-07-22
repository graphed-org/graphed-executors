# m43 frozen suite — graphed-executors (shuffle/join on dask + adaptive parity follow-ups)

Milestone **m43** (dask-parallel-backends-plan §1.3.4, §2 m43, §3 m43). This suite freezes
shuffle/join as a NATIVE dask future graph — T producer futures → T·P pick futures → P gather /
gather-join futures — reusing the local engine's per-task kernels and plan-level contracts while
RETIRING the announcement/manifest/steal machinery, PLUS the two m42-review follow-ups carried as
binding (F2a adaptive monitor drain, F2b adaptive KilledWorker attribution).
**Frozen — read-only after the m43 freeze tag.**

## Files → theme (§3 m43)

| File | Theme | What it witnesses |
|---|---|---|
| `dask_shuffle_backends.py` | harness | real `AwkwardBackend`+`NumpyBackend` adapters (m39/m40 style, new basenames); duplicating + pandas relational oracles (route via the golden-pinned `partition` / an independent engine — never the join kernel); `SpySubmitBackend` seam tap; `GatedShuffleBackend` death scenario; `DelayedEventBackend` async-transport model; `NoPeerBackend` gate stub |
| `test_dask_repartition_hashes.py` | cross-engine determinism | two dask runs AND the local `run_repartition` produce IDENTICAL `dest_block_hashes` on identical inputs (the headline gate); salt reroutes deterministically; row conservation; witness carries the §1.3.4 counters and NOT the retired ones |
| `test_dask_join_relational.py` | correctness | inner == duplicating oracle PER DEST; left/right/outer == null-aware `pandas.merge` oracle (a3 `-1`-sentinel trap re-armed; keys coalesced, never null); every `how` + a salted run bit-for-bit equal to local `run_join` |
| `test_dask_join_plan_choice.py` | F6 | auto choice is `parts`-keyed: SHUFFLE even on a 1-worker cluster where a live-`n_workers()` recompute says broadcast; 1-vs-3-worker runs bit-for-bit equal; forced `broadcast=True/False` honoured against the cost model; broadcast submits zero stage-1 futures and one `_dask_broadcast_join_part` per probe block |
| `test_dask_shuffle_locality.py` | peer movement | `producer_sites`/`gather_sites` all carry LIVE WORKER pids (cross-checked vs `client.run`), never the driver pid; ≥2 workers engaged; ≥1 (producer ≠ gather) worker pair — blocks crossed worker↔worker via future deps |
| `test_dask_shuffle_bounded_memory.py` | B5 analog | `peak_join_bytes <= mem_budget_bytes` while the duplicated output is 8× the budget; `join_spilled_partitions > 0`; `join_output_rows == n_left·n_right`; content == oracle |
| `test_dask_shuffle_structure.py` | complexity gates | submitted futures EXACTLY `{_dask_map_write: T, _dask_pick: T·P, _dask_gather: P}` across P ∈ {4, 8, 16}; counts row-count-independent (8× rows, same counts) — counts, never clocks |
| `test_dask_shuffle_worker_death.py` | recompute-on-loss | a producer-holding worker killed mid-gather: result bit-for-bit == local oracle; `pmark` count > n_src proves the lost producer RE-RAN (completeness is the graph, not a store) |
| `test_dask_shuffle_capability_gate.py` | honest scoping | `peer_data_movement=False` ⇒ `NotImplementedError` naming the capability AND "Phase 2", with ZERO submits/broadcasts reaching the stub; the shuffle module imports dask-free (subprocess probe) — runs in the MAIN matrix |
| `test_adaptive_monitor_drain.py` | F2a (carried) | adaptive+monitor: every consumed task's STARTED/FINISHED delivered BEFORE `run()` returns, under a deterministic delayed-delivery transport; fixed-path control passes on HEAD |
| `test_dask_adaptive_attribution.py` | F2b (carried) | adaptive worker death ⇒ `StageError` naming the `mem://…` partition, never the raw `graphed-…-leaf-N` dask key; cause names KilledWorker + the last worker |

## Pinned execution contract (test-author decisions — the implementer builds `graphed_executors.dask_backend.shuffle`)

```
dask_run_repartition(backend, src_blocks, parts, *, runner, salt=0) -> ShuffleResult
dask_run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",), how="inner",
              runner, broadcast=None, salt=0, mem_budget_bytes=None) -> ShuffleResult
# (plan §2 m43 Implementation Targets, verbatim.) runner is a graphed_executors.submit.SubmitRunner;
# EVERY task future is created through runner.backend.submit (the §1.1 seam — the structure suite
# counts there), with the §1.3.4 graph shape carried by the pinned MODULE-LEVEL task fns:
#   _dask_map_write   (T = min(n_workers, n_src) producer futures; worker-side to_wire + sha256)
#   _dask_pick        (T·P selector futures — a gather never pulls a producer's whole P-dict)
#   _dask_gather      (P gather futures; ascending-producer-task concat — the _assign contract)
#   _dask_gather_join / _dask_broadcast_join_part (join twins; one broadcast part per probe block)
# broadcast=None  ⇒ graphed.shuffle.broadcast_join_choice(build_bytes, probe_bytes, parts) —
#   parts-keyed, NEVER n_workers() (F6); True/False honour a plan-recorded choice.
# Capability gate: both entry points check runner.backend.capabilities.peer_data_movement FIRST and
#   raise NotImplementedError naming "peer data movement" and "Phase 2" before ANY submit/broadcast.

# ShuffleResult reused from graphed_executors.local.shuffle: value / dest_block_hashes keyed
# EXACTLY as the local engine keys them (dest under shuffle; probe-partition index under broadcast;
# budget sub-partitions from next_key up) — cross-engine dict equality is the determinism gate.

# NEW @dataclass DaskShuffleWitness in result.witness (§1.3.4 counters — only mechanisms that
# exist under dask):
#   n_producer_tasks: int                           # T
#   blocks_per_producer_task: dict[int, int]        # task -> non-empty dest blocks written (<= P)
#   peak_writer_buffer_bytes: int                   # the reused _coalesce_task streaming bound
#   broadcast_chosen: bool                          # the recorded/honoured F6 plan choice
#   peak_join_bytes / join_spilled_partitions / join_output_rows: int   # reused _join_with_budget
#   producer_sites: dict[int, tuple[int, str]]      # producer task -> (pid, worker address)
#   gather_sites: dict[int, tuple[int, str]]        # result block key -> (pid, worker address)
# RETIRED — must be ABSENT (asserted): announcements_sent/dropped, manifest_put_attempts/acks,
#   manifest_bytes, steals, stolen_tasks. Reusing the local ShuffleWitness wholesale FAILS.
# (Per .graphed/m42/disputes/frozen_readme_default_retries.md this suite pins NO DaskBackend
#  constructor defaults; the m42 pins stand.)
```

## Traceability — per test, the WRONG impl it discriminates

| Test | Mechanism witnessed | WRONG impl it FAILS |
|---|---|---|
| repartition_hashes | cross-engine content addressing | any non-ascending-task concat or non-sha256 route; driver-side re-serialization hashing; dropped salt plumbing; a witness still naming announcements/manifests/steals |
| join_relational | duplicating + null-preserving relation, cross-engine | grouped/dedup join (different multiset); dropped one-sided dests (m40 F1); `-1`-sentinel `take` gathering the last row instead of a null (a3); one-sided salt (co-partitioning broken) |
| join_plan_choice | F6 plan-stable choice + graph-shape per path | the live-`n_workers()` recompute (broadcasts on a 1-worker cluster where the parts-keyed rule shuffles); an executor overriding a forced choice; a "broadcast" that still map-writes the large side |
| shuffle_locality | worker-side kernels + peer block movement | **the rejected §1.3.4 option (i) duck-typed-cluster design** — kernels in a driver-side loop put the driver pid in every site; a single-worker pin; sites synthesized driver-side (fail the live-pid cross-check) |
| shuffle_bounded_memory | reused `_join_with_budget` streaming counters | a full-RAM concat of the duplicated dest (`peak_join_bytes` > budget); an impl that never spills; a dedup impl (fewer output rows) |
| shuffle_structure | exact T + T·P + P future counts at the submit seam | M×R fan-out; per-row tasks; a gather pulling whole producer dicts (no picks); hidden extra stages; driver-side empty-dest skipping (needs a retired manifest) |
| shuffle_worker_death | graph-owned recompute | an engine memoizing stage-1 output outside dask's graph (result lost or divergent after the death); a merge order depending on which worker recomputed |
| shuffle_capability_gate | pre-work refusal + dask-free import | a silent wrong answer on head-node backends; a gate firing after stage-1 submits; module-import-time `import distributed` |
| adaptive_monitor_drain (F2a) | trailing-event drain before unsubscribe | the CURRENT adaptive path (returns then unsubscribes, dropping the final batch's events); any fix that drains the fixed path only |
| dask_adaptive_attribution (F2b) | adaptive key→Task attribution | the CURRENT adaptive path (`_result(fut, {})` — partition degrades to the raw dask key); a translation losing the worker identity or the KilledWorker cause |

## Runtime / flakiness discipline (R0.10a)

Every gate asserts **counters, content hashes, pids, worker addresses, or file-mark counts** —
never a clock. The worker-death kill is gated on OBSERVED file marks (stage 1 complete + a
producer-holding worker provably blocked inside a gather) before the validated
`client.run(os._exit)` kill fires, so the death always lands mid-run. The F2a delayed transport
makes the missing-drain race DETERMINISTIC: delivery lags emission by 0.5 s while the undrained
driver path to unsubscribe is microseconds, and an unsubscribed topic never delivers (dask
semantics); the delay is scenario construction, the assertions are event-set counts. LocalCluster
fixture discipline follows the m42 harness verbatim: tier A `processes=False` for logic themes,
tier B `processes=True` (1 thread/worker) for pickling/locality/death themes, dedicated
per-test clusters wherever workers die, `dashboard_address=":0"`, context-managed, bounded polls.

## Non-vacuity (TEST_SANITY)

Pre-implementation, THREE distinct right-reason failure classes (no test passes vacuously):

1. **Shuffle themes** — `graphed_executors.dask_backend.shuffle` does not exist: every test body
   fails at the deferred `shuffle_api()` import with `ModuleNotFoundError` (the harness itself
   collects and its oracles compute today, so collection is clean and the failure is the missing
   implementation, nothing else). The capability-gate subprocess probe fails the same way inside
   the probe.
2. **F2a** — BEHAVIORAL failure on existing code: the adaptive path (engine.py `run`/`_run_adaptive`)
   unsubscribes without the fixed path's drain, so the delayed transport's undelivered events are
   dropped and the per-task STARTED/FINISHED completeness assertion fails. The fixed-path CONTROL
   test passes on the same transport on current HEAD — isolating the missing drain as the cause.
3. **F2b** — BEHAVIORAL failure on existing code: `_run_adaptive` resolves with an empty key→Task
   map, so the raised `StageError`'s partition is the raw `graphed-…-leaf-0` key and the
   `partition.uri in err.partition` assertion fails (the error itself already arrives — only the
   attribution is degraded, exactly the m42-review F2 finding).

Oracle deps: `numpy` (hard), `awkward` (adapter drops out if absent), `pandas`
(`pytest.importorskip` in the relational module only; installed into the m43 authoring venv —
the `test-dask` CI job must carry it). The suite touches nothing under any other `tests/frozen/**`.
