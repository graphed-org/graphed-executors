"""A port another process takes between the driver's scan and the child's bind is a scan step."""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec

from graphed_executors.submit import ThreadBackend
from graphed_executors.submit import services as svc

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68a"))
from services_harness import free_range

# On its first port the child has another process take the port, then tries to serve on it anyway.
CHILD = r"""
import subprocess, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port, first, pidfile = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
if port == first:
    hold = (
        "import socket, sys, time; s = socket.socket()\n"
        # on Windows a SO_REUSEADDR bind takes over a port unless it is held exclusively
        "if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):\n"
        "    s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)\n"
        "s.bind(('', int(sys.argv[1]))); s.listen(); print('held', flush=True); time.sleep(60)"
    )
    holder = subprocess.Popen([sys.executable, "-c", hold, str(port)], stdout=subprocess.PIPE)
    holder.stdout.readline()
    open(pidfile, "w").write(str(holder.pid))


class Ok(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


ThreadingHTTPServer(("", port), Ok).serve_forever()
"""


def _spec(ports: tuple[int, int], pidfile: Path) -> ServiceSpec:
    argv = ("{python}", "-c", CHILD, "{port}", str(ports[0]), str(pidfile))
    return ServiceSpec("web", "http", check="http:/", ports=ports, launch=Launch(argv=argv))


def test_a_port_taken_after_the_scan_starts_the_service_on_the_next_port(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="graphed_executors.services")
    ports, pidfile = free_range(2), tmp_path / "holder.pid"
    backend = ThreadBackend(1)
    try:
        with svc.ServiceSet([_spec(ports, pidfile)], backend) as eps:
            assert eps["web"] == f"http://127.0.0.1:{ports[1]}"
    finally:
        backend.close()
        if pidfile.exists():
            os.kill(int(pidfile.read_text()), signal.SIGTERM)
    assert f"port {ports[0]} taken after the scan" in caplog.text


def test_a_check_passed_on_another_listener_then_the_child_exiting_moves_to_the_next_port(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(logging.INFO, logger="graphed_executors.services")
    ports = free_range(2)
    check_ready, bindable = svc.check_ready, svc._bindable

    def foreign_first(endpoint: str, check: str, timeout: float) -> str | None:
        if endpoint.endswith(f":{ports[0]}"):
            time.sleep(1.0)  # the child exits meanwhile
            return None
        return check_ready(endpoint, check, timeout)

    monkeypatch.setattr(svc, "check_ready", foreign_first)
    monkeypatch.setattr(svc, "_bindable", lambda h, p: p != ports[0] and bindable(h, p))
    serve = (
        "import sys, http.server as h; p = int(sys.argv[1]); "
        f"p == {ports[0]} or h.ThreadingHTTPServer(('', p), h.BaseHTTPRequestHandler)"
        ".serve_forever()"
    )
    spec = ServiceSpec(
        "web",
        "tcp",
        check="tcp",
        ports=ports,
        launch=Launch(argv=("{python}", "-c", serve, "{port}")),
    )
    backend = ThreadBackend(1)
    try:
        with svc.ServiceSet([spec], backend) as eps:
            assert eps["web"] == f"tcp://127.0.0.1:{ports[1]}"
    finally:
        backend.close()
    assert f"port {ports[0]} taken after the scan (the child exited 0), next" in caplog.text


def test_a_foreign_listener_the_child_then_exits_on_moves_to_the_next_port(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(logging.INFO, logger="graphed_executors.services")
    ports = free_range(2)
    bindable = svc._bindable
    monkeypatch.setattr(svc, "_FOREIGN_GRACE_S", 10.0)  # the child's exit, not the grace, decides
    monkeypatch.setattr(svc, "check_ready", lambda endpoint, check, timeout: None)
    monkeypatch.setattr(
        svc, "listeners", lambda p, proc="/proc": {"4242"} if p == ports[0] else set()
    )
    monkeypatch.setattr(svc, "held_by", lambda pid, inodes, proc="/proc": False)
    monkeypatch.setattr(svc, "_bindable", lambda h, p: p != ports[0] and bindable(h, p))
    # on the stolen port the child outlives the first dials, then fails its bind
    serve = f"import sys, time; time.sleep(0.3 if int(sys.argv[1]) == {ports[0]} else 60)"
    argv = ("{python}", "-c", serve, "{port}")
    spec = ServiceSpec(
        "web", "tcp", check="tcp", ports=ports, launch=Launch(argv=argv), timeout_s=20.0
    )
    backend = ThreadBackend(1)
    try:
        with svc.ServiceSet([spec], backend) as eps:
            assert eps["web"] == f"tcp://127.0.0.1:{ports[1]}"
    finally:
        backend.close()
    assert f"port {ports[0]} taken after the scan (the child exited 0), next" in caplog.text


def test_a_child_that_exits_during_the_last_dial_is_not_refused_as_foreign(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ports = free_range(2)
    bindable = svc._bindable

    def hangs(endpoint: str, check: str, timeout: float) -> str:
        time.sleep(timeout)
        return "timed out"

    monkeypatch.setattr(svc, "check_ready", hangs)
    monkeypatch.setattr(svc, "listeners", lambda p, proc="/proc": {"4242"})
    monkeypatch.setattr(svc, "held_by", lambda pid, inodes, proc="/proc": False)
    monkeypatch.setattr(svc, "_bindable", lambda h, p: p != ports[0] and bindable(h, p))
    argv = ("{python}", "-c", "import time; time.sleep(0.2)", "{port}")
    spec = ServiceSpec(
        "web", "tcp", check="tcp", ports=ports, launch=Launch(argv=argv), timeout_s=1.5
    )
    backend = ThreadBackend(1)
    services = svc.ServiceSet([spec], backend)
    try:
        with pytest.raises(svc.ServiceUnavailableError) as info:
            services.start()
    finally:
        services.close()
        backend.close()
    assert info.value.legs["managed"] == f"no port of {ports[0]}-{ports[1]} came up within 1.5s"


def test_a_live_child_behind_a_foreign_listener_is_refused_within_the_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    low, high = free_range(2)
    timeouts: list[float] = []

    def dial(endpoint: str, check: str, timeout: float) -> None:
        timeouts.append(timeout)

    monkeypatch.setattr(svc, "_FOREIGN_GRACE_S", 0.5)
    monkeypatch.setattr(svc, "check_ready", dial)
    monkeypatch.setattr(svc, "listeners", lambda p, proc="/proc": {"4242"})
    monkeypatch.setattr(svc, "held_by", lambda pid, inodes, proc="/proc": False)
    argv = ("{python}", "-c", "import time; time.sleep(60)", "{port}")
    spec = ServiceSpec(
        "web", "tcp", check="tcp", ports=(low, high), launch=Launch(argv=argv), timeout_s=30.0
    )
    backend = ThreadBackend(1)
    services = svc.ServiceSet([spec], backend)
    started = time.monotonic()
    try:
        with pytest.raises(svc.ServiceUnavailableError) as info:
            services.start()
    finally:
        services.close()
        backend.close()
    assert info.value.legs["managed"] == f"port {low} on 127.0.0.1 is held by another process"
    assert time.monotonic() - started < 15.0
    # once the foreign listener is seen, no dial outlasts the grace
    assert len(timeouts) > 1 and max(timeouts[1:]) <= 0.5, timeouts


def test_a_live_child_whose_foreign_listener_goes_away_waits_out_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    low, high = free_range(2)
    reads: list[int] = []

    def foreign_once(p: int, proc: str = "/proc") -> set[str]:
        reads.append(p)
        return {"4242"} if len(reads) == 1 else set()

    monkeypatch.setattr(svc, "_FOREIGN_GRACE_S", 0.2)
    monkeypatch.setattr(svc, "check_ready", lambda endpoint, check, timeout: "refused")
    monkeypatch.setattr(svc, "listeners", foreign_once)
    monkeypatch.setattr(svc, "held_by", lambda pid, inodes, proc="/proc": False)
    argv = ("{python}", "-c", "import time; time.sleep(60)", "{port}")
    spec = ServiceSpec(
        "web", "tcp", check="tcp", ports=(low, high), launch=Launch(argv=argv), timeout_s=1.5
    )
    backend = ThreadBackend(1)
    services = svc.ServiceSet([spec], backend)
    started = time.monotonic()
    try:
        with pytest.raises(svc.ServiceUnavailableError) as info:
            services.start()
    finally:
        services.close()
        backend.close()
    assert time.monotonic() - started >= 1.5
    assert info.value.legs["managed"] == (
        f"'tcp' never passed on tcp://127.0.0.1:{low} within 1.5s: refused"
    )


def test_a_step_past_the_deadline_starts_no_further_child(monkeypatch: pytest.MonkeyPatch) -> None:
    ports = free_range(2)
    started: list[int] = []
    popen = subprocess.Popen

    def counting(argv: list[str], **kwargs: Any) -> object:
        started.append(int(argv[-1]))
        return popen(argv, **kwargs)

    def slow(endpoint: str, check: str, timeout: float) -> None:
        time.sleep(0.5)

    monkeypatch.setattr(subprocess, "Popen", counting)
    monkeypatch.setattr(svc, "check_ready", slow)
    monkeypatch.setattr(svc, "_bindable", lambda host, port: False)
    argv = ("{python}", "-c", "import time; time.sleep(0.3)", "{port}")  # exits mid-check
    spec = ServiceSpec(
        "web", "tcp", check="tcp", ports=ports, launch=Launch(argv=argv), timeout_s=0.2
    )
    backend = ThreadBackend(1)
    try:
        with pytest.raises(svc.ServiceUnavailableError, match=r"came up within 0\.2s"):
            svc.ServiceSet([spec], backend).start()
    finally:
        backend.close()
    assert started == [ports[0]]


def test_a_range_with_no_port_left_after_a_step_raises_naming_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    low, _ = free_range(2)
    monkeypatch.setattr(svc, "_bindable", lambda host, port: False)
    argv = ("{python}", "-c", "pass", "{port}")
    spec = ServiceSpec("web", "tcp", check="tcp", ports=(low, low), launch=Launch(argv=argv))
    backend = ThreadBackend(1)
    try:
        with pytest.raises(
            OSError, match=rf"^\[Errno \d+\] no port in {low}-{low} on 127\.0\.0\.1"
        ):
            svc.ServiceSet([spec], backend).start()
    finally:
        backend.close()
