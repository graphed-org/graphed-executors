"""m68b part B2 extras: the in-job backend's SERVICE-node paths and a driver-node state the frozen suite
does not reach."""

from __future__ import annotations

from http import HTTPStatus
from pathlib import Path
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec

from graphed_executors.htcondor_backend import SITES, HTCondorBackend, LocalPilots, RunHandle
from graphed_executors.htcondor_backend.server import sign


def test_a_node_that_never_announces_times_out_and_a_release_drops_its_pending_announce() -> None:
    backend = HTCondorBackend(
        LocalPilots(),
        0,
        host="127.0.0.1",
        port_range=(0, 0),
        in_job=SITES["generic"],
        announced={"web": "svc0"},
    )
    try:
        launch = Launch(argv=("{python}",), resources={"gpus": 1})
        spec = ServiceSpec("web", "http", check="http:/", launch=launch, timeout_s=0.2)
        with pytest.raises(TimeoutError, match=r"SERVICE node svc0 .*timeout_s=0\.2"):
            backend.host_service(spec, "scope")
        secret = backend._server.announce_secret(["svc0"])
        body = b"svc0 127.0.0.1:4242 node.m68b.example"
        assert backend._server.announce(body, sign(secret, body).encode()) == HTTPStatus.OK
        got = backend.host_service(spec, "scope")
        assert got == ("http://127.0.0.1:4242", "node.m68b.example", "svc0"), "control: an announce resolves"
        assert backend._server.announce(body, sign(secret, body).encode()) == HTTPStatus.OK
        backend.release_service("svc0")
        with pytest.raises(TimeoutError, match="svc0"):
            backend.host_service(spec, "scope")
    finally:
        backend.close()


class _Schedd:
    """A running DAGMan job whose driver node's queue ad is ``node``."""

    def __init__(self, node: dict[str, Any]) -> None:
        self.node = node

    def query(self, constraint: str, projection: list[str]) -> list[dict[str, Any]]:
        return [dict(self.node)] if "DAGNodeName" in constraint else [{"JobStatus": 2}]


def test_a_driver_node_held_only_while_its_input_spools_is_queued(tmp_path: Path) -> None:
    handle = RunHandle(site="generic", schedd="s", cluster=7, log_dir=tmp_path, submitted_at=0.0, dag=True)
    assert handle._poll(_Schedd({"JobStatus": 5, "HoldReasonCode": 16}))[0] == "queued"
    assert handle._poll(_Schedd({"JobStatus": 5, "HoldReasonCode": 12}))[0] == "held", "control: a real hold"
