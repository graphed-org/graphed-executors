# m63 — implementer attempts log

Milestone **m63** (R19.6 throughput, part a): `submit(plan)` pipelining. Branch `lane/throughput`.
Frozen suite `tests/frozen/m63/` (tag `freeze-m63`). Plan: lane `plan-A.md` (A1–A4).

## Iteration 1 — A1: one run at a time per local executor

`_BaseExecutor.run` holds an instance `threading.Lock`. `test_m63_run_serialized.py`: the five
run-beside-run / persistent-pool legs pass; the submit leg waits on A3 (`submit` missing).

## Iteration 2 — A2: `SubmitRunner.monitor` public, read once per run

`self.monitor` replaces `self._monitor`; `run` reads it once and passes it to `_run_fixed`,
`_run_adaptive` and `_profiler_payload`. Probe: `r.monitor = m; r.run(fixed_plan(7))` on
`ThreadBackend(2)` gives 6 `SUBMITTED` (was 0 on main).

## Iteration 3 — A3: `submit(plan)` behind a bounded in-flight queue

`_plan_queue.PlanQueue`: stdlib one-thread `ThreadPoolExecutor` driver calling the host's `run`,
`BoundedSemaphore(max_in_flight)` released by the future's done-callback (fires on cancel too).
Hosts: `_BaseExecutor` (+ `_ProcessExecutorBase` forwards), `SubmitRunner`; `dask_runner` /
`parsl_runner` forward `max_in_flight`; `close()` drains first. CI: the m63 dask/parsl files join
the `test-dask`/`test-parsl` pytest lines. Whole frozen m63 suite (thread, process, pinned,
ThreadBackend, dask, parsl, serialized) passes, first attempt.
