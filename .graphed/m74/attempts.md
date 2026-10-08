# m74 implementer attempts (graphed-executors)

## Iteration 1 — plan commits 5 and 6
- Base (freeze-m74 094e19c): 7 passed (E1-E5, E7 x2), 6 failed (every test_m74_htcondor.py test: no `store=` on
  `submit_driverless`, `_exit_code(StoreUnavailable) == 3`).
- `submit_driverless(store=, storage_options=, salt=, accept_environment=)`: `check_resumable(plan)` first (before
  `_require_plan_importable`, which pickles the process), the four fields in `run.json`.
- `driver.main`: `resumable(plan, run["store"], ...)` after loading the plan, before pilots, on every try;
  `EnvironmentChanged` -> exit 3; `StoreUnavailable` from `runner.run` -> exit 1 in `_exit_code`; the
  `graphed.checkpoint` logger joins `graphed_executors` on the driver.log handler, so the reused INFO line lands there.
- CI: `GRAPHED` pinned to graphed da9c20c (ckpt-m74-resume); the m74 path lists were already added by the freeze commit.
- Result: m74 frozen 13/13 on macOS; htcondor diff coverage 100% (18 lines) over the CI htcondor path list run locally.
- Added before commit 5: `submit_driverless` runs `json.dumps` on `storage_options` before the bindings (run.json
  must carry them); tests/extra/m74 pins it (mutant without the line fails: bindings touched).
- Local gates: main-matrix command 1567 passed, 8 failed + 5 errors = the same ids as at base 582d3dc (venv lacks
  grpcio-health-checking and histserv, which CI installs); test-dask list 424 passed + the grpc one; test-parsl list
  138 passed, per-file gates ok; Linux (docker python:3.12): m74 htcondor + local, extra m74, m67 all pass.

## Iteration 2 — plan commit 6 (docs)
- design.rst "Resuming a killed run" (example executed twice: reused 0 then 8, same value), htcondor.rst "Resuming
  on a retry", dask/parsl "When things fail" paragraphs, improvements/limitations bullets replaced; sphinx -W ok.
- Trap: a parsl coverage run's late HTEX worker data files landed in the repo root after cleanup and were combined
  into the next precommit run (84%, parsl files reported); deleting `.coverage.*` and rerunning gave 98%.
