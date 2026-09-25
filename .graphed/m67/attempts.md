# m67 implementer — attempts log

Milestone **m67**: driverless runs on the HTCondor backend, branch `m67` on `lane/htcondor` 613aaa7, frozen suite
`freeze-m67` @ 713a624 (`tests/frozen/m67/`). Plan: `lanes/htcondor/plan-services.md` §2, D4–D6.
Baseline at 613aaa7 (`pytest tests/frozen tests/extra`, macOS py3.12): 711 passed, 11 skipped, 0 failed.

## Iteration 1 — commit 1, driver entry + schedd_locate + D4 site rows
- `sites.py`: `service_ports`/`worker_ports` after `driver_ports` (default `None`), `service_hosts` derived
  (`"driver"` iff service_ports, `"cluster"` iff worker_ports); D4 rows lpc (10000,10100)/(10001,10100), lxplus
  (10000,10100)/None, generic both (10000,10100). No `services` field (m68).
- `launch.py`: `CondorPilots(schedd_locate=(pool, name))` → `_choose` returns
  `Schedd(Collector(pool).locate(Schedd, name))`; `submit_description(url, n, base)` lets a caller replace base
  keys before the site keys and `extra_submit`; `_stage` (job script + env.tgz) and `_submit` (submit + spool)
  split out of `start` for reuse; `collectors(htc, param)` shared. The default-schedd name is now `SCHEDD_HOST`
  else the local schedd ad's `Name` (was `socket.getfqdn()`): a later session locates the schedd by that name.
- `driver.py`: `main([dir])` reads `run.json` + `plan.pkl`, builds `LocalPilots(python=sys.executable,
  pythonpath=[dir])` on loopback (`worker_ports`, else an ephemeral port) or `CondorPilots(schedd_locate=…)`
  dialled at `Machine` of `$_CONDOR_MACHINE_AD` on `worker_ports`; waits for `min_pilots` before `run`;
  writes `result.pkl` `(ok, ExecResult | exception)` and `driver.log` on every exit; 0 / 3 (raised in
  `run`) / 1 (anything else). An unpicklable outcome comes back as a `RuntimeError` naming it.
- `tests/extra/m67/test_m67_driver.py`: host fallback, unpicklable outcome, default-schedd name, service_hosts.
- Expected on this commit alone: the m67 frozen files fail at `submit_driverless` (commit 2), per the plan's order.
- Gates at 6efd4a4: `pytest tests/frozen tests/extra` → the 34 non-live m67 frozen tests fail at the missing
  `submit_driverless`/`RunHandle` accessor (commit 2); zero other failures (baseline 711 passed). precommit `--fast
  --no-coverage` ok. `driver.py` per-file coverage needs the frozen driver tests, so it is measured at commit 2.

## Iteration 2 — commit 2, submit_driverless + RunHandle
- `driverless.py`: `submit_driverless` refuses (pilots mode, un-importable process/combine/empty/next_tasks/stop,
  sandbox/image/env via `CondorPilots._refuse`, `pilots="condor"` without `worker_ports`, lxplus condor `log_dir`
  outside `/afs`) before any bindings call; then writes `plan.pkl`, `driver.sh` (+`env.tgz`), `run.json` and submits
  ONE job: base keys via `submit_description(..., base)` with `executable=<abs driver.sh>`, `arguments=.`,
  `transfer_output_files=result.pkl,driver.log`, `max_retries=2`, `retry_until=3`, `graphed-driverless-<nonce>`,
  then site keys, then `extra_submit`. `run.json.log_dir = <log_dir>/pilots` (inner pilots' submit dir),
  `schedd_locate = [COLLECTOR_HOST, chosen schedd]` for condor pilots.
- `RunHandle`: every call locates the schedd by name through the site's collectors (`Collector()` where the site
  has no `schedd_query`); status from one projected query, history when the job left the queue; `wait` bounded;
  `result` refuses unless done/failed, retrieves a spooled job still in the queue, re-raises `(False, exc)`;
  `remove`, `logs`, `save`/`load`.
- Every bindings call is `launch._htcondor()` looked up per call (a `from .launch import _htcondor` escaped the
  frozen recorder; fixed before any gate).
- `tests/extra/m67/test_m67_driverless.py`: collector failover + none-found error, forgotten job, JobStatus 6 and
  4-without-ExitCode, unbounded wait, `logs()`, unknown pilots mode. Discrimination: one-edit mutants (failover
  catch narrowed, Machine parse bypassed, default schedd name hard-coded, unpicklable fallback narrowed) each fail
  their extra test.
- Gates at commit 2: `pytest tests/frozen tests/extra` → 758 passed (711 baseline + 35 m67 frozen + 12 extra),
  0 failed; the live m67 module skips without bindings. Per-file coverage (m66+m67 frozen + extra, subprocesses
  measured with an absolute `COVERAGE_FILE`): driver.py 100%, driverless.py 100%, launch.py 95%, sites.py 100%,
  every file ≥ 90% (`scripts/coverage_gate.py`). Frozen m67 alone: driver.py 93%, driverless.py 93%.
