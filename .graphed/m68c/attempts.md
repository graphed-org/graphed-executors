# m68c executors implementer — iteration log

Freeze: freeze-m68c (e41032a suite). `git diff freeze-m68c -- tests/frozen/` must stay empty.
Graphed under test: editable m68c-graphed at 95b1b7b (G's feat(runners) tip).

## Iteration 1 — plan-executors.md §4/§5 as one pass, split into the §7 commits
- submit: `SubmitRunner.run` takes a `DurablePlanV2` (overloads); `_run_scoped` branches only on the
  early CANCELLED value (`None`) and the body dispatch to `_run_stages`; the ServiceSet prologue,
  bind, cancel-before-release callback and resolve are the V1 lines. `_run_stages`: every task
  through `_RunTasks`; keys `<kind>.<s>`/task index and `pick.<s>.<t>`/dest; per-stage barrier on its
  inputs' tasks, then `control.wait()`, then that stage's picks and tasks; stage process broadcast
  once per stage (`_fingerprint`/token); map tasks return `graphed.shuffle.split`; the (map, dest) edge
  is a `pick` task when `peer_data_movement`, else one driver fetch per map and a driver-side `pick`;
  all stage inputs are top-level positional args; the first failed future in a barrier raises via
  `_result`; a completion that finds the control CANCELLED returns `stopped=CANCELLED`, `value=None`;
  value `plan.value(last)`, counts as SequentialRunner (no-input stages / the rest; picks uncounted).
- refusals: `common.refuse_staged` first statement in `_BaseExecutor.run`, `transport_run_plan`,
  `parsl_run_plan` (before `require_bound`).
- htcondor: one `_require_plan_importable` for `HTCondorRunner.run` and `submit_driverless`
  (V1 roles as before; V2 each stage's resolved process as `stages[i].process`).
- ci: m68c files added to the test-dask, test-parsl and test-htcondor steps; `.coveragerc-htcondor`
  header. The `GRAPHED` pin bump waits on graphed's m68c merge SHA (none exists yet).
- tests/extra/m68c: a failed map task raises at its barrier before any gather/pick submit; a cancel
  during the last stage returns `value=None`, CANCELLED, counts (3, 1), control reset.
- docs: design.rst "Running a join or repartition plan" (executed two-file example, output matches),
  improvements.rst ceiling + refusal list, changelog bullet.
- Finding (not changed, plan's stdlib `_fingerprint` idiom kept): a V2 plan whose callables live in
  `__main__` fails `SubmitRunner` even on ThreadBackend (`PicklingError: not the same object as
  __main__.scale`), unlike V1: builder `OpSpec`s are cloudpickled, so `resolve()` returns copies of
  `__main__` functions that stdlib pickle cannot name. The docs say so; the cause-level fix (builders
  keep the original as `live`, or a by-value broadcast) is a decision for the lead.

Mutants on the real implementation (each restored, `cmp` against the saved post file): no barrier
exception check → extra failed-map test FAILS; no final-settle cancel check → extra last-stage cancel
FAILS; no CANCELLED check in the barrier → frozen mid-run cancel FAILS; ignoring `wait()`'s result →
frozen stage-boundary FAILS; stage tasks via `backend.submit` → frozen cancel-before-release FAILS;
ignoring `peer_data_movement` → frozen peer edge FAILS; key without the stage index → thread peer edge
passes, frozen dask join FAILS (as the README states).
