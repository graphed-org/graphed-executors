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

## Iteration 2 — commit 2: schedulability
- Engine (`submit/services.py`): a driver-hosted managed server must fit `backend.driver_memory_mb`, else the
  host's physical memory (`os.sysconf` pages; Windows `GlobalMemoryStatusEx`), summed over the set's
  driver-hosted servers; a misfit goes to `host_service` with `detail` naming both numbers, or is refused
  (`legs["managed"]`) when there is none. A backend's refusal merges with the earlier legs.
- Condor: one collector query before submit; after submit the job ad `symmetricMatch`es slot copies with the
  partitionable `TotalSlot*` as `Memory/Cpus/GPUs`; no match → removed unrun, `ServiceUnavailable` naming the
  requests and the largest slot memory. `_await_announce`: idle/spooling has no deadline, logs key + state every
  `IDLE_LOG_S`; `timeout_s` counts from the first `JobStatus == 2`. A driver job's `driver_memory_mb` = slot
  `Memory` from `$_CONDOR_MACHINE_AD`.
- Finding: `ctypes.c_ulong` is 8 bytes on LP64, so the Windows struct uses `c_uint32` DWORDs (64 bytes on
  every OS; the extra test pins `dwLength == 64`).
- Mutants: driver check always fits → 2 extra + 6 frozen managed fail; deadline from submit → 2 extra +
  m68b `idle`/`spooling` fail. Restored tree: 85 passed (extra m69b, frozen managed, m68b job tests).
- Linux pool (`m69b-impl-pool`, CI test-htcondor equivalent): 481 passed, 3 skipped, 1 failed —
  `tests/extra/m68a/test_m68a_services.py::test_a_child_that_exits_after_its_check_passed` (a 1 s window for a
  child to exit; passes 3/3 alone, with and without coverage). Per-file ≥98% (submit/services.py 99.15%);
  diff-cover 100% (100 lines).
