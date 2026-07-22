# m43 — attempts log

## TEST_AUTHORING (test-author, implementation-blind) — 2026-07-21

Frozen suite authored to dask-parallel-backends-plan §1.3.4 / §2 m43 / §3 m43 + the two carried
m42-review follow-ups (F2a/F2b, wf_20cb8bd3-42f APPROVE). 10 modules + harness + README under
`tests/frozen/m43/` (38 tests; every basename repo-wide unique, verified by find over `tests/`).

**Pinned contract decisions** (full block in `tests/frozen/m43/README.md`):
- Entry points verbatim from plan §2 m43: `dask_run_repartition(backend, src_blocks, parts, *,
  runner, salt=0)` / `dask_run_join(..., on=("__joinkey__",), how="inner", runner, broadcast=None,
  salt=0, mem_budget_bytes=None)`, both -> the local `ShuffleResult` shape with local-identical
  value/hash keying (cross-engine dict equality is the headline gate).
- New `DaskShuffleWitness`: n_producer_tasks, blocks_per_producer_task, peak_writer_buffer_bytes,
  broadcast_chosen, peak_join_bytes, join_spilled_partitions, join_output_rows, producer_sites,
  gather_sites. Retired counters (announcements_*/manifest_*/steals/stolen_tasks) asserted ABSENT.
- Future-graph shape pinned by fn NAME at the `runner.backend.submit` seam (SpySubmitBackend):
  exactly `{_dask_map_write: T, _dask_pick: T·P, _dask_gather: P}` for repartition;
  `_dask_broadcast_join_part == len(probe_blocks)` and zero stage-1 futures under broadcast.
- Capability gate: NotImplementedError naming "peer data movement" + "Phase 2", BEFORE any
  submit/broadcast reaches the backend (stub counts attempts); shuffle module imports dask-free
  (subprocess probe, m42 idiom).
- Per the m42 dispute record (`.graphed/m42/disputes/frozen_readme_default_retries.md`) NO
  DaskBackend constructor defaults are pinned here.

**Scenario calibration measured before freezing** (never invented, R0.11):
- equal-sides join: measured 256/256 bytes both backends -> broadcast_join_choice = False@parts=8,
  True@n=1 (the F6 live-recompute discrimination gap is real).
- small-build join: 48/288 bytes -> True@parts=4 (forced-`broadcast=False` runs against the model);
  the first draft used parts=8 where the rule says False — caught by the sanity run and repinned.
- worker-kill idiom `client.run(os._exit, 1, workers=[victim], on_error="ignore")` + nanny restart
  validated standalone against distributed 2026.7.1 (`wait=False` + workers= is unsupported there).
- local `run_repartition` has NO salt parameter — salted-repartition determinism is pinned
  dask-vs-dask + inequality vs salt=0; salted CROSS-ENGINE equality is pinned via `run_join(salt=5)`
  (which the local engine does expose).

**TEST_SANITY evidence** (venv `/private/tmp/claude-501/m42venv`, HEAD fc648e2):
- Collection: `pytest tests/frozen/m43 --co -q` exit 0, byte-identical across two runs (38 tests).
- Full pre-impl run: **37 failed, 1 passed in ~4 s** — every failure right-reason:
  - 34 tests fail at `ModuleNotFoundError: No module named
    'graphed_executors.dask_backend.shuffle'` (deferred import inside the test body);
  - the capability-gate subprocess probe fails with the same ModuleNotFoundError in its stderr;
  - F2a fails BEHAVIORALLY: `task 0 events missing at run() return (got ['submitted'])` — the
    adaptive path unsubscribes without the fixed path's drain;
  - F2b fails BEHAVIORALLY: `assert 'mem://f2b/0' in 'graphed-…-leaf-0'` — the raw dask key leaks
    as the StageError partition (empty key_to_task at engine.py `_run_adaptive`).
  - The single pass is `test_fixed_run_control_delivers_under_the_same_delayed_transport` — the
    DELIBERATE F2a control: it proves the delayed transport delivers when the drain exists (fixed
    path, present on HEAD), isolating the adaptive failure to the missing drain. Not vacuous: it
    fails if the fixed drain is ever removed.
- Neighbor suites: `tests/frozen/m42` still 47/47 (exit 0); whole `tests/` tree collects exit 0.
- `ruff check tests/frozen/m43` clean (repo config).
- Oracle dep: **pandas 3.0.3 installed into /private/tmp/claude-501/m42venv** for the independent
  non-inner oracle (`pytest.importorskip("pandas")` in the relational module — the m40 pattern);
  the `test-dask` CI job must install it.

## IMPLEMENTING (implementer) — 2026-07-21

### Iteration 1 — first-pass green

**F2 engine fixes** (`src/graphed_executors/submit/engine.py`, `_run_adaptive`), zero regressions:
- **F2a** — pass the run's `events_seen` counter into `_run_adaptive` and drain trailing worker
  events in a `finally` (`_wait_until(events_seen >= 2*n_done, _DRAIN_TIMEOUT_S)`), guarded by
  `monitor_topic is not None` — parity with the fixed path. Guard means F2b (no monitor) never
  blocks on the drain, and a monitored adaptive death waits at most the bounded 30 s off-path.
- **F2b** — build `key_to_task[dask_key] = task` in `refill()` and pass the real map to
  `self._result(fut, key_to_task)` (was `{}`), so a KilledWorker attributes to the partition uri
  via the existing `_translate` path (unchanged).

**shuffle.py** (`src/graphed_executors/dask_backend/shuffle.py`, NEW, ~430 lines): native dask
future graph reusing the local kernels verbatim (`_assign`/`_coalesce_task`/`_join_with_budget`/
`_sha256_hex` + `graphed.shuffle.broadcast_join_choice`), worker identity via `current_env()` (no
dask import — the capability-gate subprocess probe passes). Producer `_dask_map_write` -> `_MapOut`
(payload dict + site/blocks/peak metadata); `_dask_pick` scopes gather deps; `_dask_gather` /
`_dask_gather_join` concat in ascending-producer-task order (== ascending-src -> cross-engine
byte-identical, worker-count-invariant). `_dask_broadcast_join_part` broadcasts the build once and
joins each probe block; the once-only unmatched-build tail mirrors `_run_broadcast_join` F2. Budget
spill reuses `_join_with_budget` via a minimal `_WorkerStore` (worker-local tempdir; F5 read-back).
`DaskShuffleWitness` carries the §1.3.4 counters only (retired ones absent); cast into the reused
local `ShuffleResult`. Capability gate `_require_peer` fires first in both entry points.

`dask_backend/__init__.py`: lazy `__getattr__` re-export of the two entry points (dask-free import).

**First full m43 run: 38/38 PASS, 0 skips** (venv m42venv). No frozen edits (`git diff freeze-m43 --
tests/frozen` empty). m42 still 47/47; whole frozen tree 395 passed / 1 pre-existing skip.

**Gates:** DoD coverage cmd (`pytest tests/frozen/m42 tests/frozen/m43 --cov=…submit
--cov=…dask_backend --cov-branch --cov-config=.coveragerc-dask`) = **91.18% ≥ 90%**. `ruff check src
tests` clean; `ruff format --check src` clean; `mypy` strict clean (19 files); `sphinx -W` builds.
m43 twice back-to-back = identical (determinism). CI `test-dask` extended (+`tests/frozen/m43`,
+pandas). docs `design.rst` gains the "Shuffle and joins on dask" subsection (retired-mechanism
table, cross-engine determinism contract, preemption interplay).

**Non-frozen coverage note:** three correctness-parity paths (broadcast left/right/outer, pure
one-sided shuffle dests, a partitionless side) mirror the local `run_join` F1/F2 contracts but are
not reached by the frozen scenarios (broadcast-inner + mixed-dest shuffle). Added
`tests/extra/m43/test_join_parity_extra.py` (16 tests) validating each against the local `run_join`
oracle on identical inputs with disjoint-dest keys verified via the backend's own `partition` —
lifting shuffle.py to 97% (combined). tests/extra is a local witness (not CI-wired, consistent with
tests/extra/m41); the DoD frozen-only coverage number stands at 91.18%.

## REVIEW / REMEDIATION — 2026-07-21

**Review verdict** (wf_699eaa05-384): **3× APPROVE, 0 blockers**; all mutations killed. Coverage
concern ruled ACCEPT-WITH-FOLLOWUP (gate honest, `.coveragerc-dask` untouched, 91.18% from frozen;
§B.3 frozen-hit intent strained on shuffle.py 83% frozen-only — parity paths only in tests/extra).
Three follow-ups: G3 (real bug) + G2 (interim CI) to the implementer; **G1 frozen parity fixup to
the impl-blind test-author — NOT mine.**

**G3 — real bug (mutation lens), FIXED.** `_broadcast_join` dropped the local engine's empty-probe
guard: local `_run_broadcast_join` gates the never-matched-build tail with `if unmatched and
right_blocks:` (`local/shuffle.py:821`); my port checked only `if unmatched:` then indexed
`right_blocks[0]` → **IndexError** on `dask_run_join(left=nonempty, right=[], how='left'|'outer',
broadcast=True)` where local degrades gracefully (empty result — the tail has no probe schema
carrier). Root cause = one dropped conjunct; fix = restore `and right_blocks` (shuffle.py:538),
matching local semantics exactly. Witness `test_broadcast_empty_probe_matches_local_no_crash`
(tests/extra/m43, parametrized left/outer × both backends) asserts dask == local (both empty) and
does not raise. **Non-vacuity PROVEN:** reverted to `if unmatched:` → all 4 witness cases FAIL with
IndexError; restored → 4/4 pass.

**G2 — interim CI, DONE.** Added a NON-GATING `pytest tests/extra/m43 -q` step to the `test-dask`
job AFTER the coverage-gated invocation (the gate command is untouched), so the 20 parity witnesses
run continuously until the frozen fixup lands.

**Deferred to the test-author round (G1, NOT done here):** the FROZEN parity fixup (broadcast
non-inner tail, one-sided carrier null-fill, partitionless side, empty-build/probe) lifting
shuffle.py ≥90% frozen-only + a fresh freeze tag.

**Post-remediation gates:** frozen m43 38/38; frozen m42 47/47; `git diff freeze-m43 -- tests/frozen`
empty; tests/extra/m43 20/20; ruff check src tests clean; ruff format --check src clean; mypy strict
clean (19 files).
