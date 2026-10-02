# m69b — attempts log (executors)

Milestone **m69b**, executors unit: `service_hosts` placement, schedulability (engine driver-memory check, condor
match + start-based timeout), the H→γγ diagnostics on histserv servers, `run_lpc.py`, ci, docs. Branch `m69b` at
7afad04 (freeze-m69b ed08da6 + upstream/main 8c299f7). Plan `plan-services.md` §5.2 (+ D2, D8, §6–§8);
constraints `reviews/m69b-exit-items.md`. Venvs: `.venv-m69b` (macOS py3.12; graphed a0638719, histogram 3830aca,
histserv 0.2.1), `.venv-m69b-hgg` (+ coffea fork b2612ab, uproot ca3a8a2, higgs_dna d179305 --no-deps).

## Baseline (7afad04, macOS)
- frozen m69b + m68b: 8 fail — m69b narrowing refusal, the five driver-memory rows, m68b `idle`/`spooling`
  (timeout from first `JobStatus 2`). Live pool rows skip (no bindings); hgg rows skip (no coffea).

## Iteration 1 — commit 1: `service_hosts` narrowing
- `HTCondorBackend(service_hosts=)` / `htcondor_runner(service_hosts=)`: subset of the row's hosts (attached: the
  profile's; in a driver job: `("driver",)`), kept in the row's order; a host not offered → `ValueError` naming
  the offered tuple, before `TaskServer`. `host_service` attaches iff `"cluster"` survives the narrowing.
- `tests/extra/m69b/test_m69b_placement.py` (8): fail 8/8 on the unchanged src; a mutant that accepts and ignores
  the kwarg fails 5/8.
- Frozen narrowing refusal passes.

## Iteration 2 — commit 2: schedulability
- Engine (`submit/services.py`): a driver-hosted managed server must fit `backend.driver_memory_mb`, else the
  host's physical memory (`os.sysconf` pages; Windows `GlobalMemoryStatusEx`), summed over the set's
  driver-hosted servers; a misfit goes to `host_service` with `detail` naming both numbers, or is refused
  (`legs["managed"]`) when there is none. A backend's refusal merges with the earlier legs.
- Condor: one collector query before submit; after submit the job ad `symmetricMatch`es slot copies with the
  partitionable `TotalSlot*` as `Memory/Cpus/GPUs`; no match → removed unrun, `ServiceUnavailable` naming the
  requests and the largest slot memory. `_await_announce`: idle/spooling has no deadline, logs key + state every
  `IDLE_LOG_S`; `timeout_s` counts from the first `JobStatus == 2`. A driver job's `driver_memory_mb` = slot
  `Memory` from `$_CONDOR_MACHINE_AD`.
- Finding: `ctypes.c_ulong` is 8 bytes on LP64, so the Windows struct uses `c_uint32` DWORDs (64 bytes on
  every OS; the extra test pins `dwLength == 64`).
- Mutants: driver check always fits → 2 extra + 6 frozen managed fail; deadline from submit → 2 extra +
  m68b `idle`/`spooling` fail. Restored tree: 85 passed (extra m69b, frozen managed, m68b job tests).
- Linux pool (`m69b-impl-pool`, CI test-htcondor equivalent): 481 passed, 3 skipped, 1 failed —
  `tests/extra/m68a/test_m68a_services.py::test_a_child_that_exits_after_its_check_passed` (a 1 s window for a
  child to exit; passes 3/3 alone, with and without coverage). Per-file ≥98% (submit/services.py 99.15%);
  diff-cover 100% (100 lines).

## Merge — upstream/main b0dfd2e (#42 m68c, #43 atomic write_secret)
- `git merge upstream/main` at 4759ba8: no conflicts (4ecd444). ci.yml GRAPHED arrived at 7e048bf; lane venvs and
  the pool container upgraded to graphed 7e048bf.
- Linux pool after the merge (+ frozen/extra m68c): see Iteration 4. macOS: frozen m68a/m68c/m69b + extra
  m68c/m69b 191 passed, 16 skipped (pool and hgg rows).

## Iteration 3 — commit 3: H→γγ diagnostics
- `analysis.DIAGNOSTICS` (seven, Weight, weight=`weight`), `diagnostics(record, context)` (gh.boost, or
  histserv on a context), `HggReduce`/`HggCombine`/`HggEmpty` composing `Counters`/`accumulate` with
  `pieces.reduce`/`combine`/`empty`; `dataset_plan`/`plan(..., context=None)` → `pieces.serve(aggregate_plan(...))`.
  `run_local.report` prints the counters and saves the diagnostics as UHI JSON.
- test-hgg equivalent (`.venv-m69b-hgg`, graphed 7e048bf, `GRAPHED_HGG_REQUIRED=1`): frozen m69a + m69b
  `test_hgg_*` + extra m69b: 57 passed, 0 skipped.
- Mutants (frozen diagnostics + extra hgg): combine drops b's histograms → no-context row + empty-identity fail
  (served rows pass: every chunk's receipt names the same server histogram); reduce without
  `resolve_services` → all three served/no-context rows fail; empty without `"diagnostics"` → empty-identity fails.

## Iteration 4 — commit 4: LPC runner, ci, docs
- `examples/hgg/run_lpc.py`: manifests → each dataset's first `--files` files in `--parts` steps (entry counts via
  `run_local.fileset`); `htcondor_runner(site="lpc", service_hosts=(--placement,), user_modules=[analysis.py])`, or
  `--driverless` via `submit_driverless(pilots="local", request_memory_mb=driver + pilots + every server)`;
  `Context(memory_mb=--server-mb, workers=--pilots, ports=lpc service_ports)`; parts to EOS by default; UHI JSON.
  Not run at a site. `tests/extra/m69b/test_m69b_run_lpc.py` (runner stand-ins): mutants placement ignored /
  servers left out of the slot / one file too many each fail.
- ci.yml (§6): HISTOGRAM = `graphed-histogram[histserv]` @ 3830aca (test-experimental strips the extra: no cp314t
  grpcio); test-htcondor adds frozen m69b `test_histserv_*.py` + extra m69b; test-hgg runs `-rs` m69a + m69b
  `test_hgg_*` + extra m69b. GRAPHED stays 7e048bf (from the merge).
- Docs: htcondor.rst timeout wording, "Schedulability", "Histograms on histserv servers", "An H→γγ run",
  `service_hosts` row; hgg.rst "Diagnostics"; README; changelog. The histserv example ran (printed output
  matches); the hgg.rst fragment ran on the MC fixture; sphinx -W clean.
- Gates at the head: macOS main job 1136 passed, 116 skipped (bindings/coffea/dask/parsl/Triton only), per-file
  min 98.33% (submit/threadpool.py), diff-cover 100% (44 lines). Linux pool (merge state; src unchanged since):
  503 passed, 5 skipped (dask/parsl absent, no Triton container), htcondor scope min 98% (backend.py), diff-cover
  100% (98 lines); extra m69b in the container 25 passed, 2 skipped (no coffea). test-hgg equivalent 59 passed.

## Iteration 5 — PR #44 round 2: the test-dask diff-cover
- CI (d3f2820): test-dask py3.12/3.14 diff-cover 65.9% on `submit/services.py` (missing 176,178-179,191-195,466-469,
  483-484,499): the job runs no m69b file. Reproduced verbatim (job's pytest list + `.coveragerc-dask` + its
  diff-cover include) in `.venv-m69b-dask` (the job's installs): 384 passed, same 15 lines, 65%.
- `tests/extra/m69b/test_m69b_dask_driver_check.py` (dask runner, LocalCluster 2×1): a 64 MiB service under
  `driver_memory_mb=64` starts beside the driver and every task dials it from a dask worker; 1024 MiB is refused
  before it starts naming 1024 and 64; with no limit, physical+1 is refused naming the physical size. The job's
  list adds it and `test_m69b_schedulable.py` (the Windows branch, the fall to a cluster host, the merged refusal).
  Same command: 404 passed, diff-cover 100% (44 lines). Include globs untouched.
- test-parsl: its include (`parsl_backend/**`, `common/relay_engine.py`, `common/http_plane.py`) meets none of the
  PR's changed src files (`htcondor_backend/{backend,services}.py`, `submit/services.py`): no exposure.

## Iteration 6 — PR #44 round 2: run_lpc parts from pilot scratch
- Site run (driver, `lanes/htcondor/probes/site-lpc/m69-hgg*.txt`): the EOS `--out` failed the first pilot write
  (`ArrowInvalid: Unrecognized filesystem type in URI: root://…`; graphed's `_ArrowParquet` hands the path to
  `pq.write_table`); a local `--out` passed and stayed in pilot scratch. Plan §9's fallback:
  `--out` defaults to `output_inclusive` in each pilot's scratch; pilots carry `transfer_output_files=<out>` and
  `output_destination=<--destination>` (default `root://cmseos.fnal.gov//store/user/<user>/hgg/`) through
  `extra_submit`; a `://` `--out` is refused via `parser.error` naming pyarrow. Driverless returns `<out>` beside
  `result.pkl,driver.log` instead (an `output_destination` on the driver job would take `result.pkl` too).
- Tests: the attached and driverless submit keys, the refusal before any runner. Mutants (no
  `output_destination`; driverless takes the pilot keys; root `--out` accepted) each fail one test.

## Iteration 7 — review r1 (REJECT) round: M1, M2, m1, m2
- Hygiene: a zsh `rm .coverage*` glob had deleted the three `.coveragerc-*` files (restored by the coordinator);
  coverage data is cleaned with `.coverage .coverage.*` only.
- M1: `HTCondorRunner.close()` (and `HTCondorBackend.close()`) call `backend.stop_waiting()` first; a service wait
  still idle or spooling (no deadline yet) raises `RuntimeError` naming the key at its next poll (≤ `POLL_S`), its
  ExitStack removes the job and forgets the key. A started job keeps its `timeout_s`; the no-deadline wait stays
  (owner ruling). Docs: the false "never left waiting" sentence rewritten (waits while the scheduler offers no
  room, own pilots included; key logged every 30 s; `close()` ends it).
  Test `test_close_ends_a_wait_for_a_slot_removing_the_job` (stand-in pool, every job idle): fails on d536f04
  ("close() still waits for the slot"), fails with the runner's `stop_waiting()` call removed. Reviewer's
  self-starve probe in the pool: close returned (~13 s after the 90 s watch), run raised naming the key, queue 0.
- M2: `_as_whole` also takes `TotalSlotDisk` → `Disk`. Dict row fails on d536f04 (`Disk 524288 != 2097152`);
  classad2 row `test_a_job_waiting_only_on_a_busy_node_s_disk_matches` passes in the pool; the reviewer's disk
  probe now prints "match as shipped: True".
- m1: `run_lpc` logs through `ServerTimes`, which appends each status's `started_at`/`ready_at`. Row fails on
  d536f04 (the stand-in status line carries no times).
- m2: the empty-ads guard moves into `match_refusal`; row `test_a_pool_whose_collector_lists_no_slot_submits_and_waits`
  (collector lists nothing; a stand-in classad2) reaches the announce; with the guard deleted it fails with
  `ValueError: max() iterable argument is empty`.
- Gates: macOS main job 1138 passed / 120 skipped, per-file min 98.33 %, diff-cover 100 %; test-dask job 406
  passed, diff-cover 100 %; pool test-htcondor job 506 passed / 10 skipped, per-file min 99.79 %, diff-cover 100 %
  (109 lines); sphinx -W, ruff, mypy src+tests clean.

## Iteration 8 — 40470e6, 84e6fb7: run_lpc writes its parts to EOS again; CI's graphed at a51bee4
- 40470e6 reverts d536f04's output route (parts left in pilot scratch and shipped by `output_destination`, a
  `root://` `--out` refused): no LPC slot offers the root file-transfer plugin. Each job again writes its parts to
  the `root://` `--out` through graphed's parquet writer; df4d059's `ServerTimes` status lines stay. The
  root-`--out` refusal row left with the revert (review r2: test-hgg 62 passed).
- 84e6fb7 moves CI's `GRAPHED` from 7e048bf to a51bee4, whose writers open fsspec URLs, as that default `--out`
  needs; the docs put fsspec-xrootd beside the analysis. Until this pin, 40470e6's "now opens root://" did not hold
  at CI's graphed (review r2 exit item 5).

## Iteration 9 — §5.2 "Ordering", commit 2: a run's service jobs go before its pilots
- `CondorPilots.prepare(url, secret)` is `start` without the pilots' submit, once; `start` = `prepare` + submit;
  `stop()` waits for no pilot when none was submitted. `HTCondorBackend` over `CondorPilots` with `host_service`
  (the `"cluster"` hosts, or `announced`) prepares when built and submits its pilots at the first `n_workers()`,
  `submit()` or `wait_for_pilots()`; the first `n_workers()`/`submit()` waits once for `min_pilots`
  (`HTCondorRunner` sets it; 1 bare), an explicit `wait_for_pilots(n >= min_pilots)` counting as that wait.
  `HTCondorRunner.run` no longer waits. `driver.py`'s start-up line says the pilots go at the first need.
- Extra: `test_m66_ports.py` intercepts `prepare` (the url is now first seen there).
- Frozen recorder rows 2–4 pass; rows 1 and 5 wait on commit 3.

