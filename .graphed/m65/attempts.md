# m65 — implementer iterations (graphed-executors)

Debug lane (plans in `graphed-workdir/lanes/debug/`). One section per sub-plan.

## A2 (plan-A2.md, frozen `freeze-m65a2` = `27ce729`)

Run: `python -m pytest tests/frozen/m65 tests/extra/m65 -p no:cacheprovider`.

### Iteration 1 — A2.1 hub and adaptive routes (hub/pooled/adaptive 53/53; peer routes still fail)

`control=` on both constructors, public `control`. `run()` holds the entry check (before every route's
`n == 0` return) and the CANCELLED reset in `finally`. One `_Window` is the dispatch point of the three
hub routes: `take()` reads the state once, submits only while RUNNING, drops held work on CANCELLED;
`wait()` blocks in `control.wait()` when nothing runs, and waits 50 ms when `take()` saw PAUSED, so a
resume refills free slots while a task still runs. Fixed hub: a separate `_run_fixed_windowed` over
`LazyReducer` (the uncontrolled `_run_fixed` is untouched: a `wait()` over every future per completion
would make it O(n²)). Pooled and adaptive: one loop each, a control-less `_Window` passes everything
through. SUBMITTED is emitted when tasks become known. The cancel fold is `LazyReducer.frontier()` /
the pooled `ready` nodes by first leaf, through one `_folded`. Adaptive: `t0` at window submission; a
timeout wake calls neither `next_tasks` nor `stop`; once a check sees CANCELLED the loop drains without
the stop exit. Extra tests (`tests/extra/m65/test_a2_hub_window.py`) fail on a mutant without the timed
wake (next STARTED 0.74 s after resume) and on one stamping `t0` at batch time (1.66 s duration).
