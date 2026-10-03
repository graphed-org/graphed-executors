"""m68d engine paths the frozen suite does not reach: a fixed run raising at a failed combine, not only at a
failed leaf."""

from __future__ import annotations

import sys
from pathlib import Path

from graphed.core.execution import Partition, Plan, Task
from m68d_extra_parts import failing_combine, gated_leaf, no_leaves

from graphed_executors.htcondor_backend import HTCondorBackend, HTCondorRunner, LocalPilots

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import raised_by, run_bounded

HERE = str(Path(__file__).resolve().parent)


def test_a_fixed_run_raises_at_a_failed_combine_while_other_leaves_still_run(tmp_path: Path) -> None:
    gate = tmp_path / "gate"
    tasks = tuple(Task(i, Partition(f"mem://m68d/{i}", str(gate), i, i + 1)) for i in range(4))
    plan = Plan(process=gated_leaf, combine=failing_combine, empty=no_leaves, tasks=tasks)
    # three pilots: leaves 2 and 3 hold two of them on the gate, the third runs combine(0, 1)
    backend = HTCondorBackend(LocalPilots(pythonpath=[HERE]), 3, host="127.0.0.1", port_range=(0, 0))
    runner = HTCondorRunner(backend, min_pilots=3)
    try:
        run_bounded(runner.wait_for_pilots, 120.0)
        err = raised_by(lambda: runner.run(plan), 50.0)  # within GATE_S: the gated leaves have not returned
    finally:
        gate.touch()
        run_bounded(runner.close, 120.0)
    assert type(err) is ValueError and str(err) == "m-combine-of-leaf-zero-failed", repr(err)
