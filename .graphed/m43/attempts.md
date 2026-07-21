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
