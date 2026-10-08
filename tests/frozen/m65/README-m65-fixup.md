# m65 frozen suite fixup: event-bounded timing windows (tag `freeze-m65-fixup`)

Frozen and read-only after `freeze-m65-fixup`. Owner ruling 2026-10-08: "You can refreeze the flaky m65 timing tests."

Shape repaired: each test let a fixed wall-clock budget (a sleep before sampling, a task's sleep, a bound after a trigger) stand in for an event — "no task has finished yet", "key 0 is still running", "every task handed out before the pause has started" — so a CPU-starved worker crossed the budget on a correct run. The repairs hold tasks on a file gate until the event the assertion needs has been seen, and compare against the events' own `perf_counter` stamps.

| File / test | Contract | What it witnesses |
|---|---|---|
| `m65a_probe.py` | helper | `wait_until(predicate, timeout_s)` (the poll `_await_file` now uses); `Probe.hold_all` holds every key on the `entered_path`/`release_path` gate, as `hold_key` holds one |
| `test_m65a2_control_routes.py::test_pause_stops_dispatch_then_resume_completes`, `m65a3_bodies.pause_stops_dispatch_then_resume_completes` (submit and dask legs) | 14 | pause at the first FINISHED; STARTED counted by the workers' stamps, `s1` before `pause + PAUSE_SETTLE_S` (2.0 s) and `s2` before the resume 0.6 s later: `s1 == s2 < 40`; 40 SUBMITTED at the first FINISHED; resume completes EXHAUSTED |
| `test_m65a3_control_submit.py::test_window_floors_at_one_and_widens_on_reread` | 19, A3-1 | `task_slots()` 0 then 4, every task held until 4 have STARTED: exactly 4 STARTED before the first FINISHED; adaptive `next_tasks.calls == n_partitions + 1` |
| `…submit::test_timed_wake_sees_resume_and_new_slots` | 19b, A3-1 | widen: key 0 held, slots 1 → 8 at `t_flag`; the second STARTED lands at or after `t_flag` and before key 0's FINISHED. Resume: key 0 held; exactly 2 STARTED before the resume, and one after it before key 0's FINISHED; EXHAUSTED |
| `test_m65a3_control_dask.py::test_window_is_the_dask_task_slots` | 18 slot leg | `replicate_broadcast=True`, 8 tasks held until 4 have STARTED: exactly 4 STARTED before the first FINISHED (a window of `n_workers()` gives 2) |
