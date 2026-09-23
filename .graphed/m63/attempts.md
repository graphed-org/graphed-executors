# m63 — implementer attempts log

Milestone **m63** (R19.6 throughput, part a): `submit(plan)` pipelining. Branch `lane/throughput`.
Frozen suite `tests/frozen/m63/` (tag `freeze-m63`). Plan: lane `plan-A.md` (A1–A4).

## Iteration 1 — A1: one run at a time per local executor

`_BaseExecutor.run` holds an instance `threading.Lock`. `test_m63_run_serialized.py`: the five
run-beside-run / persistent-pool legs pass; the submit leg waits on A3 (`submit` missing).
