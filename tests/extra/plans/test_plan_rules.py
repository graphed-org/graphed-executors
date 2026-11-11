"""Which plans may hold lambdas and ``__main__`` functions, and what a hand-built one gets."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from graphed.core import Plan

from graphed_executors import ProcessPoolExecutor
from graphed_executors.parsl_backend.backend import _explained

MAIN_ATTR = (
    "Can't get attribute 'count' on <module '__mp_main__' from '/v/bin/process_worker_pool.py'>"
)


def _launch(code: str, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    script = tmp_path / "launch.py"
    script.write_text(code)
    return subprocess.run(
        [sys.executable, str(script)], cwd=tmp_path, capture_output=True, text=True, timeout=300
    )


def test_a_graphed_built_plan_with_lambdas_runs_on_a_process_pool(tmp_path: Path) -> None:
    ak = pytest.importorskip("awkward")
    pytest.importorskip("pyarrow")
    import graphed  # noqa: PLC0415
    from graphed import Session  # noqa: PLC0415
    from graphed.awkward import AwkwardBackend, from_parquet  # noqa: PLC0415

    data = tmp_path / "events.parquet"
    ak.to_parquet(ak.Array({"pt": [[40.0, 25.0], [55.0], [30.0, 60.0, 20.0], [80.0]]}), data)
    events = from_parquet("events", str(data), steps_per_file=2, session=Session(AwkwardBackend()))
    plan = graphed.aggregate_plan(
        events.pt,
        reduce=lambda pt: float(ak.sum(pt)),
        combine=lambda a, b: a + b,
        empty=lambda: 0.0,
    )
    assert isinstance(plan, Plan)
    with ProcessPoolExecutor(workers=2) as pool:
        assert pool.run(plan).value == 310.0


HAND_BUILT_LAMBDA = """
from graphed.core import Partition, Plan, Task
from graphed_executors import ProcessPoolExecutor

if __name__ == "__main__":
    tasks = tuple(Task(i, Partition("data", "", i, i + 1)) for i in range(2))
    plan = Plan(process=lambda p, r: 1, combine=max, empty=int, tasks=tasks)
    try:
        ProcessPoolExecutor(workers=2).run(plan)
    except Exception as exc:
        print(type(exc).__name__, exc)
"""


def test_a_hand_built_plan_with_a_lambda_does_not_pickle_for_a_process_pool(
    tmp_path: Path,
) -> None:
    out = _launch(HAND_BUILT_LAMBDA, tmp_path)
    assert out.stdout.startswith("PicklingError Can't pickle <function <lambda>"), (
        out.stdout + out.stderr
    )


def test_a_launch_script_function_is_named_with_its_fix() -> None:
    raw = AttributeError(MAIN_ATTR)
    explained = _explained(raw)
    assert type(explained) is AttributeError and explained.__cause__ is raw
    assert str(explained).startswith("count is defined in your launch script (__main__)")
    assert "PYTHONPATH" in str(explained)


@pytest.mark.parametrize(
    "exc",
    [AttributeError("'NoneType' object has no attribute 'pt'"), ValueError(MAIN_ATTR)],
    ids=["other-attribute-error", "not-an-attribute-error"],
)
def test_any_other_error_comes_back_unchanged(exc: Exception) -> None:
    assert _explained(exc) is exc


HAND_BUILT_IN_MAIN = """
from graphed.core import Partition, Plan, Task

def count(partition, resources):
    return 1

if __name__ == "__main__":
    from graphed_executors import ParslRunner

    tasks = tuple(Task(i, Partition("data", "", i, i + 1)) for i in range(2))
    plan = Plan(process=count, combine=max, empty=int, tasks=tasks)
    with ParslRunner.local(workers=1) as runner:
        try:
            runner.run(plan)
        except AttributeError as exc:
            print(exc)
"""


def test_parsl_names_a_hand_built_plans_launch_script_function(tmp_path: Path) -> None:
    pytest.importorskip("parsl")
    out = _launch(HAND_BUILT_IN_MAIN, tmp_path)
    assert out.stdout.startswith("count is defined in your launch script (__main__)"), (
        out.stdout + out.stderr
    )
