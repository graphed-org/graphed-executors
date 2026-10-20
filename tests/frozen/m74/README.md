# m74 frozen suite — a killed run resumes from the content-addressed store

This suite freezes the executors half of the m74 acceptance tests (`lanes/ckpt-resume/plan.md` §4.2):
`graphed.checkpoint.resumable(plan, store, ...)` makes any fixed-task plan resumable, and a run whose
driver is SIGKILLed mid-run is rerun to completion executing only the tasks the store does not hold.
**Frozen — read-only after the `freeze-m74` tag.**

Run it with `pytest tests/frozen/m74`. On main every file fails at collection: `m74_harness` and
`test_m74_htcondor.py` import `resumable`, `StoreUnavailable` and `EnvironmentChanged` from
`graphed.checkpoint`, which do not exist there.

## Files

| File | Plan item | What it shows | A wrong implementation it fails |
|---|---|---|---|
| `m74_harness.py` | §4.2 harness | T=16 leaves (`Leaf`: ~0.2 s, a marker file per execution, float64 `[1e16 if i == 0 else 1.0, i]`), `CountingCodec` (a file per decode, named by pid), `drive` (the driver entry for every runner, logging `graphed.checkpoint` at INFO to its log), session kill (`ps -A -o pid=` + `os.getsid`, `wait()` the driver, poll until no member is left), `crash_and_resume`/`assert_resumed`, the fake `zzfake-<v>.dist-info`, and the m67 driverless pieces (`RecordingSchedd`, `FakeHTCondor`, `record_bindings`, `job_dir`, `prepared_job`) plus the m69b in-memory source for a `gh.plan` | — |
| `test_m74_local.py` | E1, E2, E3 | `ThreadExecutor(2)`, `ProcessPoolExecutor(2)` (peer ipc) and `SubmitRunner(ThreadBackend(2))`: killed after ≥ 4 stored tasks, `0 < done < T`; the rerun executes exactly `T - done` tasks, its value bytes equal an uninterrupted run's on the same runner, its INFO line reports `reused == done`; on E2 no decode ran on the driver | a resume that records nothing (`done == 0`), recomputes done tasks, serves wrong values, logs no or a wrong count, or decodes stored partials on the driver |
| `test_m74_dask.py` | E4, E7 (dask) | E4: `dask_runner` over `LocalCluster(n_workers=2, processes=True)`, the same four checks; E7: a store left by a killed `ThreadExecutor(2)` run resumes on `transport_run_plan` | as above; a key that differs across runners (E7 reuses nothing) |
| `test_m74_parsl.py` | E5, E7 (parsl) | E5: `parsl_runner` over `start_htex(workers=2)` (interchange and LocalProvider workers are in the driver's session), the same four checks; E7: the killed `ThreadExecutor` store resumes on `parsl_run_plan` | as above |
| `test_m74_htcondor.py` | E6, E8 | E6: the driver entry run from a job dir of a recorded `submit_driverless(store=...)` with two local pilots, killed and rerun in the same dir as a retry would: `T - done` executions, value equal to an uninterrupted job's, `driver.log`'s last `driver pid=` section reports `reused == done`. E8: `run.json` carries `store`/`storage_options`/`salt`/`accept_environment`; a store whose environment record was made under `zzfake 1.0` (by `resumable` in a subprocess with the run's salt) makes the driver exit 3 with `(False, EnvironmentChanged)` naming `zzfake` in `result.pkl`, no pilots line and no leaf executed; a histserv-backed `gh.plan` with a store is refused with a `TypeError` naming `checkpointable` before any bindings call and accepted without one; an unbacked `gh.plan` with a store is accepted; `_exit_code` of a `StoreUnavailable` that crossed `pickle` is 1 | a driver that ignores `run.json`'s store, resumes before its environment check, retries an environment change (exit 1), starts pilots before refusing, a submit that refuses late (after a bindings call) or not at all, a `StoreUnavailable` treated as a plan error (exit 3) |

## The contract these tests pin

Beyond the names in plan §4.2 and the addenda's `resumable` signature, the tests rely on these readings:

- "Done" is `len(Store(root).completed())` on the directory store the run was given.
- The reused INFO line matches `(\d+) of (\d+) tasks reused from ` with the total `T`, once in the
  driver's log; the HTCondor driver writes the same text to `driver.log`.
- `submit_driverless` takes `store`, `storage_options`, `salt`, `accept_environment` keywords and writes
  them to `run.json` under those names.
- `check_resumable`'s refusal of an undeclared process with services names `checkpointable` (plan §2.5).
- `StoreUnavailable(message)` and `EnvironmentChanged` are importable from `graphed.checkpoint`.

## Where CI runs each file

`pytest tests/frozen` in the main matrix collects every file: E1–E3 run there (skipped on Windows,
like every kill leg), the dask and parsl files importorskip out, and the HTCondor file needs no
bindings. `test_m74_dask.py` is on the test-dask job's path list, `test_m74_parsl.py` on test-parsl's,
`test_m74_htcondor.py` on test-htcondor's (where `htcondor_backend/` coverage is gated).
`tests/frozen/m74` is on pytest's `pythonpath`; every driver subprocess gets it on `PYTHONPATH`.
