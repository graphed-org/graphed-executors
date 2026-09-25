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
