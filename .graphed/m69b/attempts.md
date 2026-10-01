# m69b — attempts log (executors)

Milestone **m69b**, executors unit: `service_hosts` placement, schedulability (engine driver-memory check, condor
match + start-based timeout), the H→γγ diagnostics on histserv servers, `run_lpc.py`, ci, docs. Branch `m69b` at
7afad04 (freeze-m69b ed08da6 + upstream/main 8c299f7). Plan `plan-services.md` §5.2 (+ D2, D8, §6–§8);
constraints `reviews/m69b-exit-items.md`. Venvs: `.venv-m69b` (macOS py3.12; graphed a0638719, histogram 3830aca,
histserv 0.2.1), `.venv-m69b-hgg` (+ coffea fork b2612ab, uproot ca3a8a2, higgs_dna d179305 --no-deps).

## Baseline (7afad04, macOS)
- frozen m69b + m68b: 8 fail — m69b narrowing refusal, the five driver-memory rows, m68b `idle`/`spooling`
  (timeout from first `JobStatus 2`). Live pool rows skip (no bindings); hgg rows skip (no coffea).

## Iteration 1 — commit 1: `service_hosts` narrowing
- `HTCondorBackend(service_hosts=)` / `htcondor_runner(service_hosts=)`: subset of the row's hosts (attached: the
  profile's; in a driver job: `("driver",)`), kept in the row's order; a host not offered → `ValueError` naming
  the offered tuple, before `TaskServer`. `host_service` attaches iff `"cluster"` survives the narrowing.
- `tests/extra/m69b/test_m69b_placement.py` (8): fail 8/8 on the unchanged src; a mutant that accepts and ignores
  the kwarg fails 5/8.
- Frozen narrowing refusal passes.
