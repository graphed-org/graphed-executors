# M39 implementer attempts log

Milestone M39 — shuffle substrate + repartition (cluster-correct, cross-process sim).
Role: IMPLEMENTER. Make the frozen `tests/frozen/m39/` suites (six repos) pass without weakening them.

## Orientation (2026-07-02)

Read all frozen inputs. Key seams found (path:line):

### graphed-core (Rust+PyO3)
- `src/node.rs` `NodeKey` enum (L40): add `Exchange { scheme: ParamMap, inputs: Vec<NodeId> }`.
  - `inputs()` L71, `is_boundary()` L83 = `!matches!(Op)` → Exchange is a boundary AUTOMATICALLY.
  - `token()` L90: add `exch|<params>` prefix (non-`op|` → boundary in engine too).
  - `boundary_from_token` (engine.rs L54) = `!starts_with("op|")`. VERIFIED both paths agree.
  - `with_inputs()` L118, `label()` L149 need an Exchange arm.
- `src/serialize.rs`: add `T_EXCHANGE=5` (L24), writer arm (L120), reader arm (L281). MAGIC stays GIR1.
- `src/store.rs`: add `add_exchange(scheme, inputs)` (mirrors add_op, L96).
- `src/lib.rs`: PyO3 `add_exchange(inputs, params)` binding (NO name — §2.1 enum); `nodes()` arm
  kind="exchange" (L254); `params` = scheme map.
- `src/optimizer/incremental.rs` canonicalize L78 only special-cases Op → Exchange hash-conses. OK.
- `python/graphed_core/execution.py`: add `ShuffleBackend` Protocol (6 methods + identity), §A.4-clean.
- `python/graphed_core/plan.py`: add `DurablePlanV2` + `StageSpec`, format_version="graphed-plan/2"
  (string vs V1 int), task_id folds routing["backend_id"].
- `__init__.py` + `.pyi` stubs: export DurablePlanV2, StageSpec, ShuffleBackend.

### add_exchange PIN (test-author §7.1): `add_exchange(inputs, params) -> int`, NO name.
nodes()[i]["kind"]=="exchange". Test: test_exchange_ir.py asserts node["inputs"]==[xchg-1],
node["params"]["scheme"]=="hash" etc.

Baseline (pre-impl, from task): core 9F/1P/2E, graphed 5F/1E, awkward 9F, numpy 8F,
exec-local 13F/1P/7E, checkpoint 4F/1E — every failure a named absent target.

## Iteration 1 — graphed-core (DONE, 2026-07-02)

Implemented: Exchange NodeKey variant (node.rs: variant + inputs/token `exch|`/with_inputs/label;
is_boundary automatic), T_EXCHANGE=5 codec (serialize.rs), store.add_exchange, PyO3 add_exchange +
nodes() `kind="exchange"` arm, fixed optimizer/mod.rs test-eval match (Exchange=identity passthrough),
ShuffleBackend Protocol (execution.py, Index_co covariant phantom for M40 join half), DurablePlanV2 +
StageSpec (plan.py, format_version="graphed-plan/2" string, task_id folds routing incl backend_id),
exports + both .pyi stubs.

Gates: ruff clean; ruff format clean; mypy --strict clean; cargo fmt clean; cargo clippy -D warnings
clean; cargo test 26/26 (needs PYO3_PYTHON=venv + DYLD_FALLBACK_LIBRARY_PATH=/Users/lgray/miniforge3/lib);
m1/m4/m8/m10 regression 98/98 pass; m39 21/22 pass.

**DISPUTE FILED** (graphed-core/.graphed/M39/disputes/test_exchange_blob_roundtrips_byte_identically.md):
`test_exchange_serialize.py::test_exchange_blob_roundtrips_byte_identically` line 43
`assert back.to_dot() == g.to_dot()` compares MARKED deserialized store vs UNMARKED builder (g never
marked; serialize(outputs=) is read-only per M22). Differ only by output doublecircle. The M8 analogue
(test_ir_serialization.py:41) deliberately compares deserialize==deserialize. Proposed 1-line fix:
`back.to_dot() == GraphStore.deserialize(blob).to_dot()`. Did NOT cheat (no to_dot degrade, no serialize
side-effect). Other 2 asserts in that test + all other 21 core m39 tests pass.

Build cmds:
- rebuild ext: `cd graphed-core && env -u CONDA_PREFIX VIRTUAL_ENV=<venv> <venv>/bin/maturin develop`
- rust gates: BIN=/opt/homebrew/Cellar/rust/1.96.0/bin; `$BIN/cargo-fmt --check`; `env -u CONDA_PREFIX $BIN/cargo-clippy --all-targets -- -D warnings`; `env -u CONDA_PREFIX PYO3_PYTHON=<venv>/bin/python DYLD_FALLBACK_LIBRARY_PATH=/Users/lgray/miniforge3/lib $BIN/cargo test`

## Iteration 2 — backends + frontend (DONE, 2026-07-02)

- **graphed-numpy**: `shuffle.py` (route sha256, partition/concat/slice_rows/estimated_bytes/wire via
  np.save); NumpyBackend gains `identity="graphed-numpy/0"` + 6 delegate methods + op_form("exchange")=identity.
  pyproject mypy override extended to `graphed_numpy.shuffle` (array-boundary, same policy as `graphed_numpy`).
  Gates: m39 8/8, full frozen 336 pass, cov 90.53% (shuffle.py 100%), mypy/ruff clean.
- **graphed-awkward**: `shuffle.py` (route + ak.to_buffers wire); AwkwardBackend gains identity + 6 delegates
  + op_form("exchange"). override extended to `graphed_awkward.shuffle`. m39 9/9, full 260 pass, cov 94.91%.
- **graphed**: `Session.record_exchange`, `Array.repartition` (physical, delegates), `shuffle.py`
  (repartition verb + shuffle_plan builder over DurablePlanV2). m39 8/8, full 211 pass, cov 94.34%, mypy/ruff clean.
  DECISION: generic block ENGINE lives in exec-local (frozen-covered there), NOT graphed/shuffle.py — graphed's
  frozen suite records/plans but never executes blocks, so hosting the engine here would leave it uncovered by
  graphed's fail_under=90 gate. ShuffleBackend seam keeps it backend-neutral either way (documented in shuffle.py).

## Iteration 3 — graphed-exec-local (DONE)

- `_transport.py`: `is_routable_host` (ipaddress: reject loopback/unspecified), `select_advertise_host` (+auto-detect).
- `shuffle.py`: two-phase executor. T=min(workers,n_src) producer-tasks, contiguous ascending src_pid chunks
  → ≤P blocks/task (anti-MxR). Coalescing writer with O(P*rg) peak (flush at ROW_GROUP_BYTES=1MiB). Deterministic
  ascending-task gather → byte-identical dest_block_hashes. Announcements = droppable hints (gather derives from
  manifests). Per-dest manifest GET (O(N*P) via _MANIFEST_ENTRY_BYTES). Steal: task0→thief=1, block on thief,
  manifest at owner; reliable push retry-until-ack (drop first N). routing_hash_measurement (sha256 vs crc32, MEASURED).
  run_repartition_by_size: split-at-row-boundary + greedy coalesce.
  **Cluster-sim (comms="http")**: in-process K nodes, each a REAL ThreadingHTTPServer on the routable advertise_host
  serving GET /block/{hash}; cross-node fetch = real urllib GET over the routable socket. DEVIATION from literal
  "multi-process": nodes are threads not PIDs (robustness/R0.10a; every asserted witness — distinct Store dirs,
  cross_node_fetches>0, routable node_hosts, correctness — is genuinely met over real sockets). Flagged in §6.5
  CLAUDE.md amendment; true separate-PID/host launch is M41.
  Gates: m39 47/47 (cluster_sim RAN both backends, not skipped; routing_invariance subprocess PASSED),
  full frozen 254 pass, cov 93.05% (shuffle.py 97%), mypy/ruff clean.

## Iteration 4 — graphed-checkpoint (DONE)

- `store.py`: JournalEntry +stage +deps; `Store(root, node=None)` (node→journal.<node>.log, default journal.log
  byte-identical to M8); record_done(..., *, stage="", deps=()) writes stage/deps only when set (M8 lines untouched);
  completed() replays UNION of journal*.log.
- `runner.py`: `run_shuffle_resumable(plan_v2, store, *, resources, _kill_after) -> ShuffleResumeResult(.value,.report)`.
  Two-phase: stage payloads flow as `inputs` to dependent stages; each block content-addressed by V2 task_id,
  journaled with stage+deps; skip on resume. value = gather stage's block hashes.
  Gates: m39 8/8, full frozen 43 pass (M8 intact), cov 96.01%, mypy/ruff clean.

- **§6.5 CLAUDE.md amendments**: graphed-exec-local (cluster-correct seam scope) + graphed-checkpoint
  (per-node journal-per-writer) both amended.

## Plan of attack (bottom-up)
1. graphed-core: Exchange IR + serialize + ShuffleBackend + DurablePlanV2 (foundation).
2. graphed-awkward + graphed-numpy: exchange primitives (golden route).
3. graphed: repartition verb + shuffle_plan + generic engine.
4. graphed-exec-local: two-phase executor + transport + cluster-sim.
5. graphed-checkpoint: multi-stage journal + resume.
6. §6.5 CLAUDE.md amendments (exec-local + checkpoint).

> **freeze-M39-1 (2026-07-02):** owner-sanctioned refreeze resolving both M39 disputes.
> (1) graphed-core `test_exchange_serialize.py:43` → deserialize-vs-deserialize comparison (the M8
> pattern); the marked-vs-unmarked form was unsatisfiable under the M22 read-only-serialize pin.
> Core m39 now 22/22. (2) exec-local: I001 blank-line/grouping fix applied to the six frozen files
> and the per-file-ignore REMOVED — no gate remains relaxed ("ruff check --no-cache ." clean).
> 47/47 green after. Both sanctioned explicitly by the project owner; no assertion weakened.
> Applied by the orchestrator session (roles were shut down), recorded here per B.6.
