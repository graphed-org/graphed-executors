"""m68c: a ``join_plan`` whose ``reduce``/``combine`` live in ``__main__`` (a notebook's) runs through
``SubmitRunner(ThreadBackend)`` to ``SequentialRunner``'s value."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import graphed
import pytest
from graphed import Session
from graphed.awkward import AwkwardBackend
from graphed.core import SequentialRunner

from graphed_executors.submit import SubmitRunner, ThreadBackend

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68c"))
from m68c_harness import LEFT, RIGHT, _source, cat, no_rows, rows


def test_a_main_reduce_join_plan_runs_through_submit(monkeypatch: pytest.MonkeyPatch) -> None:
    def main_rows(values: list[Any]) -> Any:
        return rows(values)

    def main_cat(a: list[str], b: list[str]) -> Any:
        return cat(a, b)

    for fn in (main_rows, main_cat):
        fn.__module__, fn.__qualname__ = "__main__", f"m68c_{fn.__name__}"
        monkeypatch.setattr(sys.modules["__main__"], fn.__qualname__, fn, raising=False)
    s = Session(AwkwardBackend())
    joined = graphed.join(
        _source(s, "left", "mem://m68c-main/l", LEFT, None),
        _source(s, "right", "mem://m68c-main/r", RIGHT, None),
        on=["k"],
    )
    plan = graphed.join_plan(joined, steps_per_file=2, reduce=main_rows, combine=main_cat, empty=no_rows)
    want = SequentialRunner().run(plan).value
    assert want
    with SubmitRunner(ThreadBackend(2)) as runner:
        assert runner.run(plan).value == want
