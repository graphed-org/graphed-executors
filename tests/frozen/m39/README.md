# M39 frozen suite — graphed-exec-local (two-phase shuffle execution + cluster-sim)

Milestone **M39**. This repo runs the shuffle: the two-phase map-write + gather executor, the routable
transport, the node-local content-addressed Store bulk `fetch`, and the reliable manifest push. It is
the integration home where the generic engine runs against BOTH real backends (the a-BI witness by
execution). **Frozen — read-only after `freeze-M39-0`.**

## Files → theme (§8 M39)

| File | Themes | What it witnesses |
|---|---|---|
| `exchange_backends.py`, `golden_route.py` | harness | real `AwkwardBackend`+`NumpyBackend` adapters; the frozen routing table |
| `test_backend_independence_exchange.py` | **(a-BI)** | ONE body, BOTH backends: golden route, concat, slice, wire — the seam is real by execution |
| `test_shuffle_execution.py` | **(a),(b),(d)** | bit-for-bit vs sequential oracle; two fuzzed+dup runs identical; blocks ≤ P per producer-task and NOT O(#src·P) |
| `test_routing_invariance.py` | **(B2)** | same key → same dest in two OS processes with different `PYTHONHASHSEED`, and = the golden dests |
| `test_advertise_host.py` | **(B4) pure-logic** | `is_routable_host`/`select_advertise_host` reject loopback/`0.0.0.0` — NEVER skips |
| `test_cluster_sim.py` | **(f),(B4) integration** | cross-process, distinct node Store dirs, cross-node fetch, non-loopback announced hosts; skips-with-reason only if no routable IP |
| `test_announcement_robustness.py` | **(B1)** | drop AND duplicate every announcement → bit-for-bit + no dest lost |
| `test_steal_shuffle.py` | **(NB1),(NB2)** | stolen task: block on thief, manifest at owner, bit-for-bit; lossy ship-back → retry-until-ack, no deadlock |
| `test_repartition_bytes.py` | **(c)** + guidance 3 | coalesce/split by measured bytes preserves content; writer buffer O(P·rg), `rg`=`ROW_GROUP_BYTES` a documented knob |
| `test_shuffle_benchmark.py` | gates + guidance 1,2 | counts O(#producer-tasks·P) not M×R; manifest bytes O(N·P) via per-dest GET not O(N·P²); routing hash MEASURED (sha256 vs a non-crypto alt), pinned choice recorded |

## Worklog guidance (2026-07-02) → strengthened tests

1. per-dest manifest GET + **manifest-bytes O(N·P)** → `test_shuffle_benchmark.py`
   (`witness.manifest_fetch_is_per_dest` + `manifest_bytes` scales ~linearly in P, not P²).
2. **measure** sha256 vs a pinned non-crypto hash (record the choice, don't hard-code a winner) →
   `test_shuffle_benchmark.py::test_routing_hash_is_measured_and_the_choice_recorded`.
3. bounded-memory pins the **O(P·rg)** writer-buffer term, `rg` a documented knob →
   `test_repartition_bytes.py::test_hash_shuffle_writer_buffer_is_bounded_by_P_times_rg`.

## Pinned execution contract (test-author decisions — the implementer builds `graphed_exec_local.shuffle`)

```
run_repartition(backend, src_blocks, parts, *, workers=1, comms="ipc"|"http", store_root=None,
                steal=False, faults=ShuffleFaults(), advertise_host=None) -> ShuffleResult
run_repartition_by_size(backend, src_blocks, *, target_bytes, workers=1, store_root=None,
                        row_group_bytes=ROW_GROUP_BYTES) -> ShuffleResult   # .partitions

# NOTE: the correctness oracle is TEST-AUTHORED (exchange_backends.expected_dest_keys /
# observed_dest_keys), NOT implementer-provided — so the executor cannot share a bug with the oracle.

ShuffleResult:  dest_block_hashes: dict[int,str]   # dest_pid -> sha256(gathered block wire bytes) — the
                                                   #   cross-run determinism key (content-addressing gate, §7.1)
                value: dict[int,object]            # dest_pid -> gathered backend block (content correctness)
                partitions: list[object]           # (run_repartition_by_size) coalesced/split blocks
                witness: ShuffleWitness

ShuffleWitness: n_producer_tasks:int; blocks_per_producer_task:dict[int,int]; blocks_of_task:dict[int,set[str]];
                manifest_owner:dict[int,str]; block_holder:dict[str,str]; node_store_dirs:dict[str,str];
                node_hosts:list[str]; announcements_dropped:int; manifest_put_attempts:int; manifest_put_acks:int;
                manifest_bytes:int; manifest_fetch_is_per_dest:bool; steals:int; stolen_tasks:tuple[int,...];
                cross_node_fetches:int; peak_writer_buffer_bytes:int

ShuffleFaults:  drop_all_announcements=False; duplicate_all_announcements=False;
                force_steal=False;           # DETERMINISTICALLY relocate >=1 producer-task to a non-owner node
                manifest_push_drops=0         # deterministically drop the first N push attempts per stolen manifest

graphed_exec_local._transport: is_routable_host(host)->bool; select_advertise_host(candidate=None)->str (ValueError on loopback/0.0.0.0)
graphed_exec_local.shuffle:   ROW_GROUP_BYTES:int; PINNED_ROUTING_HASH="sha256"; routing_hash_measurement()->dict[str,float]
```

## Runtime / flakiness discipline

Stealing is forced **deterministically** (a relocation hook, not a timing race) and manifest-push
drops are a deterministic count — so NB1/NB2 are not flaky (R0.10a: assert counters, never a clock).
Most themes run in-process (`comms="ipc"`); only `test_cluster_sim.py` spawns cross-process
(`comms="http"`), the slow tail — keep it small (workers≤3).

## Non-vacuity

Pre-implementation: `graphed_exec_local.shuffle` and `is_routable_host`/`select_advertise_host` are
absent → right-reason import failures; the primitive tests fail on `backend.partition` absent. The
control `test_at_least_two_backends_are_exercised` PASSES (guards that a-BI parametrization is never
silently one backend).
