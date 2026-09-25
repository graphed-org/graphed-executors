"""m66 launcher paths a single personal pool cannot reach: collector failover and the default log_dir."""

from __future__ import annotations

import getpass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from graphed_executors.htcondor_backend import CondorPilots, SiteProfile, launch

AD = {
    "Name": "s1",
    "RecentDaemonCoreDutyCycle": 0.1,
    "ShadowsRunning": 1,
    "MaxJobsRunning": 10,
    "TotalIdleJobs": 0,
}


class FakeCollector:
    asked: list[str] = []  # noqa: RUF012  (reset per test)

    def __init__(self, node: str) -> None:
        self.node = node
        FakeCollector.asked.append(node)

    def query(self, ad_type: str, constraint: str, projection: list[str]) -> list[dict[str, Any]]:
        if self.node == "down":
            raise OSError("collector down")
        return [AD] if self.node == "up" else []

    def locate(self, daemon: str, name: str) -> dict[str, str]:
        return {"Name": name, "via": self.node}


def fake_htcondor(pool: str) -> Any:
    kinds = SimpleNamespace(Schedd="schedd")
    return SimpleNamespace(
        param={"POOL": pool}, Collector=FakeCollector, AdType=kinds, DaemonType=kinds, Schedd=lambda ad: ad
    )


def queried_site() -> SiteProfile:
    return SiteProfile(
        name="q", submit={}, spool=False, ship_env=False, sandbox_root=None, schedd_query=("POOL", "true")
    )


def test_a_failing_collector_hands_over_to_the_next_node() -> None:
    FakeCollector.asked = []
    name, schedd = CondorPilots(queried_site())._choose(fake_htcondor("down, up"))
    assert (name, schedd["via"]) == ("s1", "up")
    assert FakeCollector.asked == ["down", "up"]


def test_no_answering_collector_is_an_error_naming_each_node() -> None:
    with pytest.raises(RuntimeError, match="down: collector down; empty: no schedd matches true"):
        CondorPilots(queried_site())._choose(fake_htcondor("down empty"))


class BindingsReached(Exception):
    pass


def stop_at_bindings() -> Any:
    raise BindingsReached


def test_the_default_log_dir_is_fresh_under_the_sandbox_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launch, "_htcondor", stop_at_bindings)
    user_root = tmp_path / getpass.getuser()
    user_root.mkdir()
    site = SiteProfile(
        name="sb",
        submit={},
        spool=False,
        ship_env=False,
        sandbox_root=str(tmp_path / "{user}"),
        schedd_query=None,
    )
    pilots = [CondorPilots(site) for _ in range(2)]
    for p in pilots:
        with pytest.raises(BindingsReached):
            p.start("http://127.0.0.1:1", b"secret", 1)
    dirs = [p.log_dir for p in pilots]
    assert dirs[0] != dirs[1]
    for d in dirs:
        assert d is not None and d.parent == user_root
        assert (d / "graphed-secret").read_text() == b"secret".hex()
