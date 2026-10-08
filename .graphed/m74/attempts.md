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
