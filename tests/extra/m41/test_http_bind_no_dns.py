"""Binding an HTTP transport must not resolve the loopback host (tests/extra — NOT frozen).

``http.server.HTTPServer.server_bind`` looks the bound host up with ``socket.getfqdn``: a reverse DNS
query that a resolver with no answer for loopback leaves hanging for tens of seconds per endpoint —
long enough for the discovery handshake to time out on a CI runner.
"""

from __future__ import annotations

import socket

import pytest

from graphed_executors.local._transport import HttpTransport


def test_binding_does_not_look_the_host_up(monkeypatch: pytest.MonkeyPatch) -> None:
    def resolver_hangs(name: str = "") -> str:
        raise AssertionError(f"getfqdn({name!r}) called while binding")

    monkeypatch.setattr(socket, "getfqdn", resolver_hangs)
    transport = HttpTransport("w0")
    try:
        assert transport.host == "127.0.0.1"
        assert transport.port > 0
    finally:
        transport.close()
