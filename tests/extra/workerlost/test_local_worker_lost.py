"""A dead local worker is one error on every route, retried when asked for."""

from __future__ import annotations

import os
import pickle
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import graphed
import pytest
from graphed.core import (
    ExecContext,
    ExecResult,
    LocalResources,
    Partition,
    Plan,
    Task,
    chunk_label,
)
from graphed.debug import StageError

from graphed_executors import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor
from graphed_executors.local import _driver
from graphed_executors.local._driver import _InFlight

N = 8
KEY = 5
TASKS = tuple(Task(i, Partition("data.root", "Events", i, i + 1)) for i in range(N))
ROUTES: dict[str, tuple[type[ProcessPoolExecutor | PinnedPoolExecutor], dict[str, Any]]] = {
    "ipc": (ProcessPoolExecutor, {}),
    "http": (ProcessPoolExecutor, {"comms": "http"}),
    "pinned": (PinnedPoolExecutor, {}),
    "hub": (ProcessPoolExecutor, {"comms": None}),
    "pooled": (ProcessPoolExecutor, {"comms": None, "pooled_combines": True}),
}


@dataclass(frozen=True)
class Die:
    """Count one, except on chunk ``KEY``, which kills the first ``deaths`` workers to reach it."""

    log: str
    deaths: int = 1

    def __call__(self, part: Partition, res: object) -> int:
        if part.entry_start == KEY and self.fired() < self.deaths:
            with Path(self.log).open("a") as log:
                log.write(f"{os.getpid()}\n")
            os._exit(17)
        return 1

    def fired(self) -> int:
        return len(Path(self.log).read_text().split()) if Path(self.log).exists() else 0


def add(a: int, b: int) -> int:
    return a + b


def zero() -> int:
    return 0


def plan(process: Die) -> Plan[int]:
    return Plan(process=process, combine=add, empty=zero, tasks=TASKS)


@pytest.fixture(params=list(ROUTES))
def route(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def make(route: str, **kw: Any) -> Any:
    kind, base = ROUTES[route]
    return kind(2, **base, **kw)


def test_a_dead_worker_is_a_worker_lost_error_naming_its_chunk(route: str, tmp_path: Path) -> None:
    die = Die(str(tmp_path / "log"))
    start = time.monotonic()
    with make(route) as ex, pytest.raises(graphed.WorkerLostError) as info:
        ex.run(plan(die))
    assert time.monotonic() - start < 30, "noticed by liveness, not by the root deadline"
    err = info.value
    assert isinstance(err, StageError)
    assert err.partition == chunk_label(TASKS[KEY].partition)
    assert err.worker == f"pid {int(Path(die.log).read_text())}"
    assert err.frames == (), "the source-line slot never holds a data file"
    again = pickle.loads(pickle.dumps(err))
    assert type(again) is graphed.WorkerLostError
    assert (again.partition, again.worker) == (err.partition, err.worker)


def test_a_retry_returns_the_whole_answer(route: str, tmp_path: Path) -> None:
    die = Die(str(tmp_path / "log"))
    with make(route, retries=1) as ex:
        result = ex.run(plan(die))
    assert die.fired() == 1, "the kill must have fired"
    assert result.value == N


def test_the_last_retry_raises(tmp_path: Path) -> None:
    die = Die(str(tmp_path / "log"), deaths=2)
    with make("ipc", retries=1) as ex, pytest.raises(graphed.WorkerLostError):
        ex.run(plan(die))
    assert die.fired() == 2


@dataclass
class Pull:
    """A one-shot task stream: it has moved on when a rerun asks again."""

    batches: list[tuple[Task, ...]]

    def __call__(self, ctx: ExecContext) -> tuple[Task, ...] | None:
        return self.batches.pop() if self.batches else None


def test_a_pull_stream_is_not_replayed(tmp_path: Path) -> None:
    die = Die(str(tmp_path / "log"))
    pulled = Plan(process=die, combine=add, empty=zero, next_tasks=Pull([TASKS]))
    with make("hub", retries=1) as ex, pytest.raises(graphed.WorkerLostError):
        ex.run(pulled)


def test_a_persistent_pool_survives_a_lost_worker(tmp_path: Path) -> None:
    die = Die(str(tmp_path / "log"))
    with make("hub", persistent=True) as ex:
        with pytest.raises(graphed.WorkerLostError):
            ex.run(plan(die))
        assert ex.run(plan(die)).value == N


def test_retries_leave_a_user_error_alone(tmp_path: Path) -> None:
    calls = tmp_path / "calls"

    def fail(part: Partition, res: LocalResources) -> int:
        with calls.open("a") as log:
            log.write("x")
        raise ValueError("bad chunk")

    single = Plan(process=fail, combine=add, empty=zero, tasks=TASKS[:1])
    with ThreadExecutor(1, retries=3) as ex, pytest.raises(ValueError, match="bad chunk"):
        ex.run(single)
    assert calls.read_text() == "x"


def test_a_marker_lives_exactly_as_long_as_its_chunk(tmp_path: Path) -> None:
    seen: list[list[str]] = []

    def look(part: Partition, res: object) -> int:
        seen.append([p.read_text() for p in tmp_path.iterdir()])
        if part.entry_start:
            raise ValueError("bad chunk")
        return 1

    watched = _InFlight(look, str(tmp_path))
    assert watched(TASKS[0].partition, None) == 1
    with pytest.raises(ValueError, match="bad chunk"):
        watched(TASKS[1].partition, None)
    assert seen == [[chunk_label(TASKS[0].partition)], [chunk_label(TASKS[1].partition)]]
    assert list(tmp_path.iterdir()) == []


def test_the_chunk_named_is_the_dead_workers_not_a_survivors() -> None:
    with ThreadExecutor(1) as ex:
        for name, chunk in (("11.1", "a survivor's chunk"), ("7.1", "the lost chunk")):
            Path(ex._in_flight, name).write_text(chunk)
        workers = [SimpleNamespace(pid=11, exitcode=-15), SimpleNamespace(pid=7, exitcode=17)]
        lost = ProcessPoolExecutor._lost_in(ex, workers)  # type: ignore[arg-type]
        assert lost is not None
        assert (lost.partition, lost.worker) == ("the lost chunk", "pid 7")
        assert os.listdir(ex._in_flight) == [], "a retry starts with no markers"


class _Silent:
    def recv(self, timeout: float) -> None:
        time.sleep(timeout)


def test_the_root_deadline_waits_for_a_running_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_driver._BaseExecutor, "_peer_root_timeout_s", 0.1)
    with ThreadExecutor(1) as ex:
        marker = Path(ex._in_flight, "1.1")
        marker.write_text("chunk")
        threading.Timer(0.5, marker.unlink).start()
        start = time.monotonic()
        with pytest.raises(TimeoutError, match=r"within 0\.1s"):
            ex._await_peer_root(_Silent(), N, None, lambda: False)
    assert time.monotonic() - start >= 0.5


def test_a_healthy_run_leaves_no_marker(tmp_path: Path) -> None:
    with ThreadExecutor(2) as ex:
        result: ExecResult[int] = ex.run(plan(Die(str(tmp_path / "log"), deaths=0)))
        assert os.listdir(ex._in_flight) == []
    assert result.value == N
