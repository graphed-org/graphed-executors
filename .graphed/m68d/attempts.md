# m68d — attempts log (executors)

Milestone **m68d**, executors unit: driverless service liveness (C stop + classify, B restart + rerun, A fail-fast,
the port race). Branch `m68d` from freeze-m68d 60288ee (main 582d3dc + the frozen suite). Plan
`lanes/htcondor/m68d/plan.md` §2/§4/§5 with `addenda.md`; constraints `ta/m68d/sanity-check/README.md`.
Venv `~/vibe-coding/cloud/.venv-m68d` (macOS py3.12, graphed a51bee4, histogram 3830aca).

## Iteration 1 — commit 1: the fixed path stops at its first failed task
- `_run_fixed` puts every leaf and combine future on a done-queue and raises through `_result` at the first
  one with an exception; the drain waits for the events of the leaves taken off the queue (all `n` once the
  root resolved).
- Frozen C1/C1b (thread, htcondor): 4 passed.
- `tests/extra/m68d/test_m68d_engine.py`: a failed combine raises while two gated leaves still hold their
  pilots. Mutants: no loop (root-only wait) and leaves-only registration both fail it (HARD TIMEOUT 50 s).

## Iteration 2 — commit 2: a task whose service fails its check raises ServiceUnreachable
- `_service_checked(checks, fn, *args)` (module level, pickled by reference): on an `Exception` it runs
  `check_ready(endpoint, check, PROBE_CHECK_S)` per service on the worker, and the first failing check
  raises `ServiceUnreachable(name, endpoint, host_identity(), f"{why}; the task raised {exc!r}")` from it;
  else the exception re-raises unchanged. `_RunTasks(backend, checks)` submits through it when `checks`
  is non-empty; `_run_scoped` builds `checks` from the endpoints bound for `plan.services`.
- Frozen C1–C6 (13): passed.
- Extras: the first failing service among several is named (mutant: first check only → fails); a
  `RunControl`-windowed run classifies too (mutant: no wrapper → fails).
