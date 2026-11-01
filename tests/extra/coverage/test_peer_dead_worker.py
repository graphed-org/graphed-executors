"""The process peer driver's backstop for a pinned worker that died without reporting."""

from __future__ import annotations

import threading
import time
from concurrent.futures import Future
from types import SimpleNamespace
from typing import Any

import pytest
from graphed import WorkerLostError
from graphed.core import Plan

from graphed_executors.local import ProcessPoolExecutor


class _SilentDriver:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []
        self.lock = threading.Lock()

    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        return None

    def poll(self) -> list[tuple[str, object]]:
        return []

    def peers(self) -> tuple[str, ...]:
        return ("w0",)

    def send(self, dest: str, message: object) -> bool:
        with self.lock:
            self.sent.append((dest, message))
        return True

    def released(self) -> bool:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with self.lock:
                if ("w0", ("done",)) in self.sent:
                    return True
            time.sleep(0.01)
        return False


class _Pool:
    def __init__(self, alive: bool) -> None:
        self._processes = {1: SimpleNamespace(pid=1, exitcode=None if alive else 17)}


def _collect(drv: _SilentDriver, alive: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ProcessPoolExecutor, "_peer_root_timeout_s", 0.5)
    plan: Plan[float] = Plan(process=lambda p, r: 1.0, combine=lambda a, b: a + b, empty=float)
    ex = ProcessPoolExecutor(1)
    try:
        unresolved: Future[Any] = Future()
        ex._collect_peer(drv, plan, 2, [unresolved], _Pool(alive))
    finally:
        ex.close()


def test_a_dead_worker_raises_and_releases_the_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    drv = _SilentDriver()
    with pytest.raises(WorkerLostError, match="worker pid 1 died"):
        _collect(drv, False, monkeypatch)
    assert drv.released()


class _RootDriver(_SilentDriver):
    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        return ("w0", ("root", 1.0))


@pytest.mark.parametrize("monitored", [False, True])
def test_a_worker_dead_after_the_root_raises_instead_of_hanging(monitored: bool) -> None:
    plan: Plan[float] = Plan(process=lambda p, r: 1.0, combine=lambda a, b: a + b, empty=float)
    outcome: list[BaseException] = []
    ex = ProcessPoolExecutor(1)
    ex._run_monitor = object() if monitored else None  # type: ignore[assignment]

    def run() -> None:
        try:
            ex._collect_peer(_RootDriver(), plan, 2, [Future()], _Pool(False))
        except BaseException as exc:
            outcome.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(10)
    ex._run_monitor = None
    ex.close()
    assert not thread.is_alive(), "an unresolved future hangs the finish"
    assert isinstance(outcome[0], WorkerLostError)


def test_live_workers_wait_for_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    drv = _SilentDriver()
    with pytest.raises(TimeoutError):
        _collect(drv, True, monkeypatch)
    assert drv.released()
