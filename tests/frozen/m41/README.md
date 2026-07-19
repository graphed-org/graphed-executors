# M41 frozen suite — graphed-executors (cluster-runtime hardening + Phase-2 launch seam)

Milestone **M41** (plan §8 M41, §4.3, §6.1.3; decomposition `m41-decomposition.md`). This repo owns the
data-plane hardening — a RAM budget + spill on the **fetch/gather (reader) plane**, **incast-avoidance**
coalescing, **per-node disk budgeting** under skew — and the exec side of the Phase-2 **`ClusterExecutor`
launch seam** (a genuine cross-process routable-Store sim + mock launcher). The core-side seam CONTRACT
lives in `graphed-core`'s `tests/frozen/core/m41/`. **Frozen — read-only after `freeze-M41-0`.**

Backend-agnostic themes → one real backend (`NumpyBackend`, exact 16 B/row) is sufficient here; the
M39/M40 both-backend independence is already frozen. The cross-process forms are **env-gated** (skip
with a reason on a loopback-only runner); the never-skip contract is the `[core]` seam test.

## Files → theme / target (§8 M41)

| File | Theme / Target | What it witnesses | HEAD failure (right reason) |
|---|---|---|---|
| `m41_backends.py` | harness | numpy adapter, the M39 routing oracle (completeness/no-reorder), independent on-disk `du` (`store_bytes_on_disk`), `routable_ip` gate | — (helper) |
| `test_fetch_spill.py` | **(a) / T1** | one hot dest (512 000 B) gathered under a 64 000 B `fetch_budget_bytes`: `peak_fetch_bytes <= budget+one_block`, `fetch_spill_count/…_bytes > 0`, spill REALLY on disk (`du`), `tracemalloc` ceiling, gathered == oracle | `TypeError: … unexpected keyword argument 'fetch_budget_bytes'` |
| `test_incast.py` | **(b) / T2** | (N,P,K)=(48,12,4): `bulk_fetch_count <= 2*K*K` (=32) **and** `< blocks_written` (48) — coalesced, not per-block; `bytes_per_fetch` consistent with a low count + full `bytes_transferred`; gathered == oracle | `AttributeError: 'ShuffleWitness' object has no attribute 'bulk_fetch_count'` |
| `test_disk_budget.py` | **T3** | skew (one hot key → one node): every node's Store `<= disk_budget_bytes` (**hot node asserted specifically**, not aggregate) by independent `du`; `disk_backpressure_events > 0`; result complete | `TypeError: … unexpected keyword argument 'fetch_budget_bytes'` |
| `test_cluster_launch_sim.py` | **(c.2) / T4** | mock launcher spawns K CHILDREN; fetch through the core `NodeStore.fetch` seam: `served_pid ∈ child_pids` and `!= driver_pid`, `is_routable_host(served_host)`, `AddressTable.registered_by == child`, idempotent (content-keyed), blob streamed to consumer Store | `ImportError: cannot import name 'AddressTable' from 'graphed.core.execution'` |
| `test_bulk_throughput.py` | **(e)** | 2 MB moved through `fetch()`: `bytes_transferred == blob` (cross-checked vs consumer Store `du`), throughput finite+positive (**not** an MB/s gate — scope honesty), idempotent re-fetch adds 0 | `ImportError: cannot import name 'NodeStore' from 'graphed.core.execution'` |
| `test_adl_no_regression.py` | **(d)** | ADL q1/q2/q3/q7 values routed through the spilling fetch plane (`fetch_budget_bytes=16 000`): histogram **bit-for-bit == M7 golden** AND `fetch_spill_count > 0` (engaged, not vacuous) | `TypeError: … unexpected keyword argument 'fetch_budget_bytes'` (after extraction sanity == golden passes) |

## Pinned execution contract this milestone ADDS (test-author decisions — the implementer builds to it)

```
run_repartition(backend, src_blocks, parts, *, workers=1, comms="ipc", store_root=None,
                steal=False, faults=ShuffleFaults(), advertise_host=None,
                fetch_budget_bytes: int | None = None,   # NEW (M41): RAM budget on the FETCH/GATHER plane
                disk_budget_bytes:  int | None = None)   # NEW (M41): per-node Store DISK cap
    -> ShuffleResult
```

### NEW `ShuffleWitness` fields (implementer adds; mirror the M39/M40 counter style)

| Field | Plane / theme | Meaning + UNIT PIN | Distinct from |
|---|---|---|---|
| `peak_fetch_bytes: int` | reader / (a),(d) | peak live reader-buffer bytes (RAM), bounded by `fetch_budget_bytes` | `peak_writer_buffer_bytes` (writer, M39) |
| `fetch_spilled_bytes: int` | reader / (a),(d),T3 | **wire bytes written to the node Store on disk** by the fetch spill | M40 `join_spilled_partitions`; writer flush |
| `fetch_spill_count: int` | reader / (a),(d) | number of fetch-plane spills (spill engaged) | — |
| `bulk_fetch_count: int` | fetch coalescing / (b),(c.2),(e) | number of COALESCED fetch operations (few large streams); also the fetch-through-seam count in the cluster sim | `cross_node_fetches` (per-block, M39 — keep for cross-check) |
| `bytes_per_fetch: float` | fetch coalescing / (b) | mean bytes per coalesced fetch = `bytes_transferred / bulk_fetch_count` | — |
| `bytes_transferred: int` | bulk endpoint / (b),(e) | total UNIQUE payload bytes moved through `fetch()` (re-fetch of a present digest adds 0) | `manifest_bytes` (manifest overhead, M39) |
| `per_node_disk_bytes: dict[str,int]` | per-node Store / T3 | on-disk wire bytes per node addr (agrees with `du` of that node's Store dir) | `node_store_dirs` (paths only, M39) |
| `disk_budget_bytes: int` | per-node Store / T3 | the configured per-node cap (echoes the input) | — |
| `disk_backpressure_events: int` | per-node Store / T3 | times the per-node cap forced backpressure/redistribution | — |
| `served_pid: int` | cluster seam / (c.2) | `os.getpid()` of the CHILD that served the last fetch | `node_hosts` (in-process, M39) |
| `served_host: str` | cluster seam / (c.2) | the routable host that served the last fetch | — |

### NEW cross-process cluster sim + mock launcher (exec deliverable — the (c.2)/(e) frozen API)

```
launch_routable_cluster(n_nodes: int, *, store_root: str, advertise_host: str) -> RoutableClusterSim
#   spawns `n_nodes` CHILD processes (multiprocessing 'spawn'); each child:
#     - creates a node-local Store dir under store_root,
#     - starts an HTTP blob server bound to a ROUTABLE (non-loopback) `advertise_host` address
#       (GET /blob/{hash}, PUT /blob, GET+PUT /manifest/… — the §4.3 endpoints),
#     - REGISTERS (node_id -> (host, port), registered_by=<child os.getpid()>) into the shared
#       core AddressTable.
# RoutableClusterSim (returned handle):
#   .address_table : graphed.core.execution.AddressTable   # .lookup(nid)->(host,port); .registered_by(nid)
#   .driver_pid    : int                                   # os.getpid() of the driver
#   .child_pids    : tuple[int, ...]                        # the spawned children's announced pids
#   .node_ids      : tuple[...]                             # node identifiers (indexable)
#   .witness       : ShuffleWitness                         # served_pid/served_host/bytes_transferred/bulk_fetch_count
#   .consumer_store_dir : str                               # dir the fetched blobs stream INTO (du cross-check)
#   .put(node_id, data: bytes) -> str                       # PUT /blob to that child's Store -> content hash
#   .fetch(node_id, digest: str) -> bytes                   # GET /blob THROUGH core NodeStore.fetch; idempotent
#   .close() -> None
```

### Config knobs are TEST INPUTS (never values the impl may raise to pass)

`fetch_budget_bytes` (a/d/T3), `disk_budget_bytes` (T3), and the coalescing target (b) are inputs. The
budget scenarios are deliberately sized so a whole dest (512 000 B) is 8× the RAM budget and the hot
disk pile is > one node's cap — raising a budget to fit is not an option.

## Non-vacuity evidence (measured on HEAD @efcc566, venv-m40)

- **(b) HEAD baseline (measured):** at (N,P,K)=(48,12,4) HEAD issues **48** per-block `cluster.get`
  calls (== stage-1 blocks written; `cross_node_fetches`=36). Asserted bound `bulk_fetch_count <= 2*K*K`
  (=32) and `< blocks_written` (=48) → HEAD provably exceeds both; a node-pair/producer-task coalesced
  impl stays ≤ 16. `N*P`=576.
- All 9 exec tests FAIL/ERROR on HEAD for the intended reason (see the table's HEAD-failure column):
  4× `TypeError` (fetch/disk knobs absent), 1× `AttributeError` (`bulk_fetch_count` absent), 2×
  `ImportError` (the `ClusterExecutor`/`NodeStore` core seam + `launch_routable_cluster` absent), and the
  4 ADL params each `TypeError` after the extraction-sanity `== golden` passes.

## Determinism / §A.4 / scope

- Byte-identical comparisons key on content hashes / the routing oracle only; PID/port/ephemeral host
  are membership/routability-checked, never byte-compared (decomposition §5).
- The concrete Store/HTTP impl stays in `graphed_executors`, **behind** the core `NodeStore`/`AddressTable`
  Protocols (the `[core]` seam test enforces core purity via a `sys.modules` guard).
- **Scope honesty:** multi-host launch stays DEFERRED — (c.2)/(e) use a single-machine cross-process sim
  (spawned children announcing genuinely non-loopback addresses); throughput (e) is measured, never a
  wall-clock SLA.
