"""m74 ``submit_driverless(store=...)`` refusals and store paths the frozen suite does not reach."""

from __future__ import annotations

import json
import operator
import sys
from pathlib import Path
from typing import Any

import pytest
from graphed.core.execution import Partition, Plan, Task

from graphed_executors.htcondor_backend import launch, submit_driverless

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m74"))
from m74_harness import HARNESS_FILE, RecordingSchedd, leaf_plan, record_bindings


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


def test_unused_storage_options_are_not_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = record_bindings(monkeypatch, RecordingSchedd())
    submit_driverless(
        leaf_plan(tmp_path),
        request_memory_mb=1024,
        log_dir=tmp_path / "submit",
        user_modules=[HARNESS_FILE],
        storage_options={"client": object()},
    )
    assert [entry[0] for entry in fake.log].count("submit") == 1, fake.log


@pytest.mark.parametrize(
    ("store", "expected"),
    [
        ("rel", "{cwd}/rel"),
        (Path("rel"), "{cwd}/rel"),
        (Path("a/../abs"), "{cwd}/abs"),
        ("s3://b/p", "s3://b/p"),
        ("file:///x", "file:///x"),
    ],
)
def test_run_json_holds_an_absolute_local_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store: Any, expected: str
) -> None:
    cwd = tmp_path / "sub"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    record_bindings(monkeypatch, RecordingSchedd())
    log_dir = tmp_path / "submit"
    submit_driverless(
        leaf_plan(tmp_path), request_memory_mb=1024, log_dir=log_dir, user_modules=[HARNESS_FILE], store=store
    )
    assert json.loads((log_dir / "run.json").read_text())["store"] == expected.format(cwd=cwd)


@pytest.mark.parametrize(("store", "error"), [(123, TypeError), ("", ValueError)])
def test_a_bad_store_is_refused_before_the_bindings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store: Any, error: type[Exception]
) -> None:
    fake = record_bindings(monkeypatch, RecordingSchedd())
    log_dir = tmp_path / "submit"
    with pytest.raises(error):
        submit_driverless(
            leaf_plan(tmp_path),
            request_memory_mb=1024,
            log_dir=log_dir,
            user_modules=[HARNESS_FILE],
            store=store,
        )
    assert fake.log == []
    assert not log_dir.exists()


PICKLED: list[str] = []


class Process:
    """A process that records each pickling of it."""

    def __call__(self, partition: Partition, resources: object) -> float:
        return 0.0

    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        PICKLED.append("process")
        return (float, ())


def test_check_resumable_refuses_before_the_process_is_pickled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = record_bindings(monkeypatch, RecordingSchedd())
    PICKLED.clear()
    plan = Plan(process=Process(), combine=operator.add, empty=float, next_tasks=lambda ctx: None)
    with pytest.raises(TypeError, match="fixed task set"):
        submit_driverless(
            plan, request_memory_mb=1024, log_dir=tmp_path / "submit", store=str(tmp_path / "s")
        )
    assert PICKLED == []
    assert fake.log == []
