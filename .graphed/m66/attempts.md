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
