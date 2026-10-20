"""The driver passes ``run.json``'s store fields to ``resumable`` and leaves its loggers as it found them."""

from __future__ import annotations

import json
import logging
import operator
import pickle
from pathlib import Path
from typing import Any

import pytest
from graphed.core.execution import Partition, Plan, Task

from graphed_executors.htcondor_backend import driver
from graphed_executors.htcondor_backend.launch import PLAN_FILE, RUN_FILE


def test_main_forwards_the_store_fields_and_restores_its_loggers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = Plan(
        process=operator.add,
        combine=operator.add,
        empty=float,
        tasks=(Task(0, Partition("mem://m74/0", "", 0, 1)),),
    )
    (tmp_path / PLAN_FILE).write_bytes(pickle.dumps(plan))
    fields = {"store": "file:///m74", "storage_options": {"auto_mkdir": True}, "salt": "s"}
    (tmp_path / RUN_FILE).write_text(json.dumps({**fields, "accept_environment": True}))
    seen: dict[str, Any] = {}

    def fake(plan: Any, store: str, **kwargs: Any) -> Any:
        seen.update(kwargs, store=store)
        raise RuntimeError("stop before the pilots")

    monkeypatch.setattr(driver, "resumable", fake)
    loggers = [logging.getLogger(name) for name in ("graphed_executors", "graphed.checkpoint")]
    before = [(logger.level, list(logger.handlers)) for logger in loggers]
    for logger, level in zip(loggers, (logging.WARNING, logging.ERROR), strict=True):
        monkeypatch.setattr(logger, "level", level)
    assert driver.main([str(tmp_path)]) == driver.EXIT_FAILED
    assert seen == {**fields, "accept_environment": True}, seen
    after = [(logger.level, list(logger.handlers)) for logger in loggers]
    assert after == [(logging.WARNING, before[0][1]), (logging.ERROR, before[1][1])], after
