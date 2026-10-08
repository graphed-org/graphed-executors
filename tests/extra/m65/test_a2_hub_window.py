"""m65 A2.1: a paused hub window refills on resume, and held time never counts as run time."""

from __future__ import annotations

import dataclasses
import threading
import time
from pathlib import Path

import m65a_probe as mp
from graphed.core import ExecContext, RunControl, StopReason, Task, TaskPhase

from graphed_executors.local import ThreadExecutor

HOLD_S = 2.0


def test_resume_refills_while_a_task_is_still_running(tmp_path: Path) -> None:
    release = str(tmp_path / "release")
    ctl = RunControl()
    rec = mp.Recorder(on_first_finished=ctl.pause)
    ex = ThreadExecutor(max_workers=2, comms=None, monitor=rec, control=ctl)
    probe = mp.Probe(sleep_s=0.05, hold_key=0, entered_path=str(tmp_path / "entered"), release_path=release)
    run = mp.Background(lambda: ex.run(mp.make_plan(probe)))
    assert rec.first_finished.wait(10)
    time.sleep(0.2)
    resumed = time.perf_counter()
    ctl.resume()
    mp.wait_until(lambda: rec.count(TaskPhase.STARTED) > 2)
    mp.touch(release)
    res = run.result()
    later = [e.t for e in rec.events if e.phase is TaskPhase.STARTED and e.t > resumed]
    key0_done = min(e.t for e in rec.events if e.phase is TaskPhase.FINISHED and e.key == 0)
    assert res.stopped is StopReason.EXHAUSTED
    assert later and min(later) < key0_done  # key 0 is held until the release: only the timed wake refills


def test_held_time_is_not_run_time() -> None:
    ctl = RunControl()
    seen: list[ExecContext] = []
    batch = [*mp.tasks()]

    def next_tasks(ctx: ExecContext) -> list[Task] | None:
        seen.append(ctx)
        return batch if len(seen) == 1 else None

    def pause_for_the_hold() -> None:
        ctl.pause()
        threading.Timer(HOLD_S, ctl.resume).start()

    rec = mp.Recorder(on_first_finished=pause_for_the_hold)
    ex = ThreadExecutor(max_workers=2, comms=None, monitor=rec, control=ctl)
    plan = dataclasses.replace(mp.make_plan(mp.Probe(sleep_s=0.05), adaptive=True), next_tasks=next_tasks)
    res = ex.run(plan)
    assert res.stopped is StopReason.EXHAUSTED and res.n_partitions == mp.N
    durations = seen[-1].last_durations
    assert len(durations) == mp.N
    assert max(durations.values()) < HOLD_S  # a held task timed from its batch would include the whole hold
