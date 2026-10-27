"""Unit coverage for the parsl transport's error branches no HTEX run reaches deterministically."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("parsl", reason="graphed-executors[parsl] extra not installed")

from graphed.debug import StageError

import graphed_executors.parsl_backend.transport_shuffle as ts
from graphed_executors.common.http_plane import EscalatingHttpTransport, PullTimeoutError
from graphed_executors.parsl_backend.api import run_join
from graphed_executors.parsl_backend.transport_peer import ProbeUnreachableError, _peer_rendezvous
from graphed_executors.parsl_backend.transport_shuffle import _HttpBlockPlane


def test_pull_from_a_holder_without_the_digest_times_out() -> None:
    holder = EscalatingHttpTransport("holder", epoch="e")
    puller = EscalatingHttpTransport("puller", epoch="e")
    try:
        puller.set_registry({"holder": (holder.host, holder.port)})
        with pytest.raises(PullTimeoutError, match="served no bytes for 1 digest"):
            puller.pull("holder", ["absent"], timeout_s=5.0)
    finally:
        holder.close()
        puller.close()


def test_missing_self_fragment_raises_pull_timeout() -> None:
    ep = SimpleNamespace(address="me", take_local=lambda digest: None)
    with pytest.raises(PullTimeoutError, match="self-fragment deadbeef missing"):
        _HttpBlockPlane(ep).pull_blocks("me", ["deadbeefcafe"], timeout_s=1.0)


def test_lost_registry_resolves_as_released() -> None:
    sent: list[Any] = []
    ep = SimpleNamespace(
        address="p0",
        host="127.0.0.1",
        port=1,
        set_registry=lambda reg: None,
        send=lambda to, msg: sent.append((to, msg)),
        recv=lambda timeout: None,
    )
    spec = {"driver_host": "127.0.0.1", "driver_port": 2, "epoch": "e", "registry_wait_s": 0.0}
    assert _peer_rendezvous(ep, spec) == (None, [], True)  # type: ignore[arg-type]
    assert sent[0][1][0] == "hello"


def test_join_probe_failure_routes_to_an_attributed_stage_error(monkeypatch: Any) -> None:
    def unreachable(*args: Any, **kwargs: Any) -> Any:
        raise ProbeUnreachableError([("p0", "p1")])

    monkeypatch.setattr(ts, "transport_run_join", unreachable)
    pbackend = SimpleNamespace(capabilities=None, peer_transport=True)
    with pytest.raises(StageError) as info:
        run_join(None, [], [], 2, pbackend=pbackend, shuffle_method="transport")
    assert info.value.cause_type == "ProbeUnreachableError"
    assert "('p0', 'p1')" in info.value.cause_message
