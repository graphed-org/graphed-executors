"""m74 ``submit_driverless(store=...)`` refusals the frozen suite does not reach."""

from __future__ import annotations

import operator
from pathlib import Path

import pytest
from graphed.core.execution import Partition, Plan, Task

from graphed_executors.htcondor_backend import launch, submit_driverless


def test_storage_options_json_cannot_hold_are_refused_before_the_bindings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    touched: list[str] = []
    monkeypatch.setattr(launch, "_htcondor", lambda: touched.append("_htcondor"))
    plan = Plan(
        process=operator.add,
        combine=operator.add,
        empty=float,
        tasks=(Task(0, Partition("mem://m74/0", "", 0, 1)),),
    )
    log_dir = tmp_path / "submit"
    with pytest.raises(TypeError, match="not JSON serializable"):
        submit_driverless(
            plan,
            request_memory_mb=1024,
            log_dir=log_dir,
            store=str(tmp_path / "store"),
            storage_options={"client": object()},
        )
    assert touched == []
    assert not log_dir.exists()
