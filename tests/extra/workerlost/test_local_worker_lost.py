"""A dead local worker is one error on every route, retried when asked for."""

from __future__ import annotations

import os
import pickle
import threading
import time
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import graphed
import pytest
from graphed.core import (
    ExecContext,
    ExecResult,
    Partition,
    Plan,
    Task,
    chunk_label,
)
from graphed.debug import StageError

from graphed_executors import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor
from graphed_executors.local import _driver
from graphed_executors.local._process_peer import _InFlight

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

    def fail(part: Partition, res: Any) -> int:
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
    with make("ipc") as ex:
        os.makedirs(ex._in_flight)
        for name, chunk in (("11.1", "a survivor's chunk"), ("7.1", "the lost chunk")):
            Path(ex._in_flight, name).write_text(chunk)
        workers = [SimpleNamespace(pid=11, exitcode=-15), SimpleNamespace(pid=7, exitcode=17)]
        lost = ex._lost_in(workers)
        assert lost is not None
        assert (lost.partition, lost.worker) == ("the lost chunk", "pid 7")


class _Silent:
    def recv(self, timeout: float) -> None:
        time.sleep(timeout)


def test_the_root_deadline_waits_for_a_running_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_driver._BaseExecutor, "_peer_root_timeout_s", 0.1)
    with make("ipc") as ex:
        os.makedirs(ex._in_flight)
        marker = Path(ex._in_flight, "1.1")
        marker.write_text("chunk")
        threading.Timer(0.5, marker.unlink).start()
        start = time.monotonic()
        with pytest.raises(TimeoutError, match=r"within 0\.1s"):
            ex._await_peer_root(_Silent(), N, None, lambda: False)
    assert time.monotonic() - start >= 0.5


class _RootAt:
    """Deliver the root once ``when`` has passed."""

    def __init__(self, when: float) -> None:
        self.when = when

    def recv(self, timeout: float) -> tuple[str, tuple[str, int]] | None:
        time.sleep(timeout)
        return ("w0", ("root", 7)) if time.monotonic() >= self.when else None


def test_a_gap_between_chunks_at_the_deadline_is_not_a_stall(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_driver._BaseExecutor, "_peer_root_timeout_s", 1.0)
    with make("ipc") as ex:
        os.makedirs(ex._in_flight)
        marker = Path(ex._in_flight, "1.1")
        marker.write_text("chunk")
        threading.Timer(0.6, marker.unlink).start()  # nothing runs as the first deadline lapses
        threading.Timer(1.2, marker.write_text, ("next chunk",)).start()
        threading.Timer(1.6, marker.unlink).start()
        root = _RootAt(time.monotonic() + 1.8)
        assert ex._await_peer_root(root, N, None, lambda: False) == (7, N)


def _nested_pool_fails(part: Partition, res: object) -> int:
    raise BrokenProcessPool("the chunk's own pool")


@pytest.mark.parametrize("route", ["ipc", "hub"])
def test_a_chunks_own_broken_pool_is_its_error_not_a_lost_worker(route: str) -> None:
    failed: list[BaseException] = []

    def run() -> None:
        with make(route) as ex:
            try:
                ex.run(Plan(process=_nested_pool_fails, combine=add, empty=zero, tasks=TASKS))
            except BaseException as exc:
                failed.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(60)
    assert not thread.is_alive(), "shutting the pool down before releasing its actors hangs"
    assert type(failed[0]) is BrokenProcessPool


@pytest.mark.parametrize("route", ["ipc", "http"])
def test_a_worker_lost_between_runs_is_a_worker_lost_error(route: str, tmp_path: Path) -> None:
    calm = plan(Die(str(tmp_path / "log"), deaths=0))
    with make(route, persistent=True) as ex:
        assert ex.run(calm).value == N
        pool = ex._peer_pool
        victim = next(iter(pool._processes.values()))
        victim.kill()
        victim.join()
        while not pool._broken:
            time.sleep(0.01)
        with pytest.raises(graphed.WorkerLostError) as info:
            ex.run(calm)
        assert info.value.worker == f"pid {victim.pid}"
        assert ex.run(calm).value == N, "the next run respawns"


def test_a_marker_a_shutdown_cut_off_does_not_outlive_its_run(tmp_path: Path) -> None:
    with make("ipc") as ex:
        os.makedirs(ex._in_flight)
        Path(ex._in_flight, "1.1").write_text("a chunk cut off")
        assert ex.run(plan(Die(str(tmp_path / "log"), deaths=0))).value == N
        assert os.listdir(ex._in_flight) == []


def test_a_healthy_run_leaves_no_marker(tmp_path: Path) -> None:
    with make("ipc") as ex:
        result: ExecResult[int] = ex.run(plan(Die(str(tmp_path / "log"), deaths=0)))
        assert os.listdir(ex._in_flight) == []
    assert result.value == N


def slow(part: Partition, res: object) -> int:
    time.sleep(0.5)
    return 1


def slow_add(a: int, b: int) -> int:
    time.sleep(0.5)
    return a + b


def test_a_thread_run_outlasts_the_deadline_while_a_chunk_or_combine_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_driver._BaseExecutor, "_peer_root_timeout_s", 0.2)
    for process, combine in ((slow, add), (lambda p, r: 1, slow_add)):
        tasks = TASKS[:2]
        with ThreadExecutor(1) as ex:
            assert (
                ex.run(Plan(process=process, combine=combine, empty=zero, tasks=tasks)).value == 2
            )


def test_a_combine_names_itself_on_a_process_worker(tmp_path: Path) -> None:
    seen: list[str] = []

    def look(a: int, b: int) -> int:
        seen.extend(p.read_text() for p in tmp_path.iterdir())
        return a + b

    assert _InFlight(look, str(tmp_path), "a combine")(1, 2) == 3
    assert seen == ["a combine"]
