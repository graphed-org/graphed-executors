# m66 implementer — attempts log

Milestone **m66**: direct HTCondor backend (`graphed_executors.htcondor_backend`), branch `lane/htcondor`,
frozen suite `freeze-m66` @ 8dd983b (50 tests in `tests/frozen/m66/`). Plan: `lanes/htcondor/plan.md`.

## Iteration 1 — commit 1, the CI leg
- `test-htcondor` job (ubuntu-latest, py3.12): get.htcondor.org minicondor under sudo, `systemctl enable --now condor`
  falling back to `condor_master`, a bounded wait for a schedd and a startd, then `pytest tests/frozen/m66` as the
  runner user (non-root) with bare `--cov --cov-config=.coveragerc-htcondor`, combine, per-file + diff gates.
  `ci-required` needs it. `[htcondor]` extra `htcondor>=25.13`; main omit and main diff-cover exclude gain
  `*/htcondor_backend/*`.
- Expected on this commit: the m66 tests (except the 5 packaging pins) fail at the accessor with
  `ModuleNotFoundError: graphed_executors.htcondor_backend`, in the main matrix and in `test-htcondor`. The job
  still reports the installer/pool-start steps on a real runner, which is what this commit measures.
- Local: `pytest tests/frozen/m66/test_htcondor_packaging_pins.py` → 5 passed.
- CI measured on 5cfc5b8 (run 36103619305, test-htcondor): the get.htcondor.org installer itself runs
  `systemctl enable condor` + `systemctl start condor` on the runner; `condor_version` 25.14.1, one partitionable
  slot (4 CPUs, 15989 MB) Unclaimed within ~25 s of the install; both live-pool tests pass `require_pool()` and
  fail at the accessor, like the rest of m66, as expected.

## Iteration 2 — commit 2, site profiles + launcher
- `sites.py` (SiteProfile, SITES, schedd_weight, choose_schedd, counts_as_alive), `launch.py` (`_htcondor`,
  PilotLauncher, LocalPilots, CondorPilots: submit_description, refusals before `_htcondor()`, secret file 0600
  as hex text, `pilot.sh`, `env.tgz`, collector failover node by node + `Collector.locate`, spool + retrieve +
  remove on `(schedd name, ClusterId)`).
- `tests/extra/m66/test_m66_launcher.py`: collector failover and the no-collector error on a fake bindings
  namespace; the default `log_dir` is a fresh dir under the sandbox root. The test-htcondor pytest command now
  includes `tests/extra/m66` (the test-parsl precedent runs its extra dir in the scoped job).
- Local: test_htcondor_sites.py + packaging pins + extra → 26 passed. The rest of m66 still fails at the
  accessor for the server/backend names (commit 3).
