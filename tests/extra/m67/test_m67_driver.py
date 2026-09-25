"""m67 driver-entry and launcher paths the frozen suite does not reach: the host fallback, an outcome
that does not pickle, the default schedd's name, and ``service_hosts`` with worker ports alone."""

from __future__ import annotations

import pickle
import socket
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from graphed_executors.htcondor_backend import CondorPilots, SiteProfile, driver
from graphed_executors.htcondor_backend.sites import SITES


def test_the_driver_host_falls_back_to_this_host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(socket, "getfqdn", lambda: "fallback.example")
    monkeypatch.delenv("_CONDOR_MACHINE_AD", raising=False)
    assert driver.machine_host() == "fallback.example"
    ad = tmp_path / ".machine.ad"
    ad.write_text('Name = "slot1@x"\n')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    assert driver.machine_host() == "fallback.example"
    ad.write_text('Name = "slot1@x"\nMachine = "node7.example"\n')
    assert driver.machine_host() == "node7.example"


class Unpicklable(Exception):
    def __reduce__(self) -> Any:
        raise TypeError("no pickling")


def test_an_unpicklable_outcome_comes_back_as_text() -> None:
    ok, err = pickle.loads(driver._result_blob(False, Unpicklable("lost handle")))
    assert ok is False and type(err) is RuntimeError
    assert "Unpicklable did not pickle (no pickling): lost handle" in str(err)


def test_the_default_schedd_is_named_by_its_own_ad() -> None:
    located: list[Any] = []

    class Collector:
        def __init__(self, pool: str | None = None) -> None:
            self.pool = pool

        def locate(self, daemon: str, name: str | None = None) -> dict[str, str]:
            located.append((self.pool, daemon, name))
            return {"Name": "local-schedd"}

    kinds = SimpleNamespace(Schedd="schedd")
    fake = SimpleNamespace(param={}, Collector=Collector, DaemonType=kinds, Schedd=lambda: "default")
    assert CondorPilots(SITES["generic"])._choose(fake) == ("local-schedd", "default")
    assert located == [(None, "schedd", None)]


def test_service_hosts_follow_the_port_rows() -> None:
    only_workers = SiteProfile(
        name="w",
        submit={},
        spool=False,
        ship_env=False,
        sandbox_root=None,
        schedd_query=None,
        worker_ports=(1, 2),
    )
    assert only_workers.service_hosts == ("cluster",)
