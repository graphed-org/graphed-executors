"""m66 task-server and pilot paths the frozen suite does not drive: a cancelled queued task, an idle long poll,
a stale or unusable result, close with held tasks, an exhausted port range, and a pilot orphaned by its driver."""

from __future__ import annotations

import pickle
import subprocess
import sys
import threading
import time
from concurrent.futures import CancelledError, Future
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from graphed_executors.htcondor_backend import HTCondorBackend, LocalPilots, server
from graphed_executors.htcondor_backend.launch import write_secret
from graphed_executors.parsl_backend.backend import _ParslFuture

PORTS = (10000, 10100)


def double(x: int) -> int:
    return 2 * x


def raise_unpicklable() -> None:
    raise ValueError(threading.Lock())


def task_server() -> server.TaskServer:
    return server.TaskServer("127.0.0.1", PORTS, SimpleNamespace(alive=lambda: 1))


def test_a_cancelled_queued_task_is_skipped_and_fails_its_dependent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "POLL_S", 0.2)
    ts = task_server()
    try:
        first: Future[Any] = Future()
        ts.add("k1", double, (1,), first)
        second: Future[Any] = Future()
        ts.add("k2", double, (_ParslFuture(first, {}),), second)
        assert first.cancel()
        assert ts.lease("pilot-a") is None  # the cancelled task is dropped, then the poll times out
        assert isinstance(second.exception(0), CancelledError)
    finally:
        ts.close()
        ts.shutdown()


def test_stale_and_unusable_results_and_close(monkeypatch: pytest.MonkeyPatch) -> None:
    ts = task_server()
    try:
        done: Future[Any] = Future()
        held: Future[Any] = Future()
        ts.add("k1", double, (1,), done)
        ts.add("k2", double, (2,), held)
        leased = ts.lease("pilot-a")
        assert leased is not None
        ts.result("pilot-b", leased[0], True, pickle.dumps(2))
        assert not done.done(), "a result from a pilot without the lease was accepted"
        ts.result("pilot-a", leased[0], True, b"not a pickle")
        assert "does not unpickle" in str(done.exception(0))
        ts.close()
        assert "unfinished" in str(held.exception(0))
        assert ts.lease("pilot-a") is None
    finally:
        ts.close()
        ts.shutdown()


def test_an_exhausted_port_range_is_an_error() -> None:
    ts = task_server()
    try:
        port = int(ts.url.rsplit(":", 1)[1])
        with pytest.raises(OSError, match="no free port"):
            server.TaskServer("127.0.0.1", (port, port), SimpleNamespace(alive=lambda: 1))
    finally:
        ts.close()
        ts.shutdown()


def test_a_pilot_exits_1_once_its_driver_is_gone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "LEASE_S", 1.0)
    monkeypatch.setattr(server, "POLL_S", 0.2)
    ts = task_server()
    secret = tmp_path / "graphed-secret"
    write_secret(secret, ts.secret)
    pilot = subprocess.Popen(
        [sys.executable, "-m", "graphed_executors.htcondor_backend.pilot", ts.url, str(secret)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.monotonic() + 60
    while ts.live_pilots() < 1 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert ts.live_pilots() == 1
    ts.shutdown()  # the socket goes away without a 410: the pilot cannot tell a crash from a partition
    out, _ = pilot.communicate(timeout=60)
    ts.close()
    assert pilot.returncode == 1, out
    assert "unreachable" in out, out


def test_an_unpicklable_task_error_still_settles_the_future(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "POLL_S", 0.2)  # the idle pilot also sees 204s
    backend = HTCondorBackend(LocalPilots(pythonpath=[Path(__file__).parent]), 1, host="127.0.0.1")
    try:
        backend.wait_for_pilots(1, timeout=60)
        time.sleep(0.5)
        err = backend.submit(raise_unpicklable, key="graphed-m66-unpicklable").exception(timeout=60)
    finally:
        backend.close()
    assert isinstance(err, RuntimeError), err
    assert "ValueError" in str(err) and "Traceback" in str(err), err
