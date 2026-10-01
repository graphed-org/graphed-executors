"""m69b schedulability paths the frozen suite does not reach: the physical-memory read on the other OS's branch,
the driver check's fall-through to a cluster host and its refusal beside the earlier legs, the per-start sum,
a size-less spec; a driver job's slot ``Memory``; the condor announce wait's idle log cadence and a deadline
that outlives an eviction; the collector query, a partitionable slot's totals, and a job that left the queue
before its match. No bindings and no pool: the condor pieces run over stand-ins."""

from __future__ import annotations

import ctypes
import logging
import os
import socket
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec

from graphed_executors.htcondor_backend import SITES, CondorPilots, HTCondorBackend, launch
from graphed_executors.htcondor_backend import backend as backend_mod
from graphed_executors.htcondor_backend import server as server_mod
from graphed_executors.htcondor_backend.services import ServiceJob, _as_whole, machine_ads
from graphed_executors.submit import ThreadBackend
from graphed_executors.submit.services import (
    ServiceSet,
    ServiceUnavailable,
    host_identity,
    machine_ad,
    physical_memory_mb,
)

MIB = 1 << 20
LISTEN = (
    "import socket, sys, time; s = socket.create_server((sys.argv[1], int(sys.argv[2]))); time.sleep(120)"
)


# ---- the host's physical memory ------------------------------------------------------------------------


def test_the_windows_branch_reads_total_physical_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    lengths: list[int] = []

    def status(ref: Any) -> int:
        lengths.append(ref._obj.dwLength)
        ref._obj.ullTotalPhys = 3 * 1024 * MIB + 5
        return 1

    monkeypatch.setattr(sys, "platform", "win32")
    kernel32 = SimpleNamespace(GlobalMemoryStatusEx=status)
    monkeypatch.setattr(ctypes, "windll", SimpleNamespace(kernel32=kernel32), raising=False)
    assert physical_memory_mb() == 3072
    assert lengths == [64]  # MEMORYSTATUSEX's size, which the call requires in dwLength


def test_a_failed_windows_call_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    kernel32 = SimpleNamespace(GlobalMemoryStatusEx=lambda ref: 0)
    monkeypatch.setattr(ctypes, "windll", SimpleNamespace(kernel32=kernel32), raising=False)
    monkeypatch.setattr(ctypes, "WinError", lambda: OSError("GlobalMemoryStatusEx failed"), raising=False)
    with pytest.raises(OSError, match="GlobalMemoryStatusEx failed"):
        physical_memory_mb()


def test_the_posix_branch_multiplies_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = {"SC_PAGE_SIZE": 16384, "SC_PHYS_PAGES": 3 * 65536 + 7}
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "sysconf", pages.__getitem__, raising=False)
    assert physical_memory_mb() == 3072


# ---- the engine's driver check ---------------------------------------------------------------------------


class HostingBackend(ThreadBackend):
    """``ThreadBackend(1)`` holding ``driver_memory_mb`` beside the driver and hosting the rest "on the
    cluster" as listeners here, or refusing them with ``refuse``."""

    service_hosts = ("driver", "cluster")

    def __init__(self, driver_memory_mb: int, refuse: str | None = None) -> None:
        super().__init__(1)
        self.driver_memory_mb = driver_memory_mb
        self.refuse = refuse
        self.hosted: dict[str, socket.socket] = {}

    def host_service(self, spec: ServiceSpec, scope: str) -> tuple[str, str, str]:
        if self.refuse is not None:
            raise ServiceUnavailable(spec.name, {"managed": self.refuse})
        sock = socket.create_server(("127.0.0.1", 0))
        key = f"{scope}-{len(self.hosted)}"
        self.hosted[key] = sock
        return f"tcp://127.0.0.1:{sock.getsockname()[1]}", host_identity(), key

    def release_service(self, key: str) -> None:
        self.hosted.pop(key).close()


def spec(name: str, memory_mb: int | None) -> ServiceSpec:
    resources = {} if memory_mb is None else {"memory_mb": memory_mb}
    argv = ("{python}", "-c", LISTEN, "{host}", "{port}")
    return ServiceSpec(
        name, kind="m69b", check="tcp", launch=Launch(argv, resources=resources), timeout_s=30.0
    )


@pytest.fixture
def backends() -> Iterator[list[HostingBackend]]:
    made: list[HostingBackend] = []
    yield made
    for backend in made:
        backend.close()


def hosting(made: list[HostingBackend], driver_memory_mb: int, refuse: str | None = None) -> HostingBackend:
    made.append(HostingBackend(driver_memory_mb, refuse))
    return made[-1]


def test_a_server_the_driver_cannot_hold_runs_on_the_cluster_and_says_why(
    backends: list[HostingBackend],
) -> None:
    backend = hosting(backends, 137)
    services = ServiceSet([spec("big", 311)], backend)
    with services:
        ((key, _sock),) = backend.hosted.items()
        (status,) = services.statuses()
    assert (status.leg, status.host) == ("managed", "cluster")
    assert "311 MiB" in status.detail and "137 MiB" in status.detail, status.detail
    assert backend.hosted == {}, "the cluster host outlived its set"
    assert key.startswith(services.scope)


def test_a_cluster_refusal_keeps_why_the_earlier_legs_failed(backends: list[HostingBackend]) -> None:
    backend = hosting(backends, 137, refuse="no slot holds it")
    with pytest.raises(ServiceUnavailable) as err:
        ServiceSet([spec("big", 311)], backend).start()
    assert list(err.value.legs) == ["user", "site", "driver", "managed"]
    assert err.value.legs["managed"] == "no slot holds it"
    assert "311 MiB" in err.value.legs["driver"] and "137 MiB" in err.value.legs["driver"]


def test_the_driver_sum_starts_afresh_with_each_start(backends: list[HostingBackend]) -> None:
    backend = hosting(backends, 300)
    services = ServiceSet([spec("again", 300)], backend)
    for _ in range(2):
        with services:
            pass
        assert [(s.leg, s.host) for s in services.statuses()] == [("managed", "driver")]
    assert backend.hosted == {}


def test_a_service_without_a_size_fits_any_driver(backends: list[HostingBackend]) -> None:
    backend = hosting(backends, 0)
    services = ServiceSet([spec("sizeless", None)], backend)
    with services:
        pass
    assert [(s.leg, s.host) for s in services.statuses()] == [("managed", "driver")]


# ---- machine ads ----------------------------------------------------------------------------------------


def test_machine_ad_reads_one_attribute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("_CONDOR_MACHINE_AD", raising=False)
    assert machine_ad("Memory") is None
    ad = tmp_path / ".machine.ad"
    ad.write_text('Machine = "node7.example"\nMemory = 3072\n')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    assert (machine_ad("Machine"), machine_ad("Memory"), machine_ad("Cpus")) == (
        "node7.example",
        "3072",
        None,
    )


class NoPilots:
    def start(self, url: str, secret: bytes, n: int) -> None:
        pass

    def alive(self) -> int:
        return 0

    def stop(self) -> None:
        pass


@pytest.mark.parametrize(("lines", "expected"), [("Memory = 3072\n", 3072), ("Cpus = 4\n", None)])
def test_a_driver_job_holds_its_services_to_its_slot_memory(
    lines: str, expected: int | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ad = tmp_path / ".machine.ad"
    ad.write_text(f'Machine = "127.0.0.1"\n{lines}')
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(ad))
    attached = HTCondorBackend(NoPilots(), 1, host="127.0.0.1")
    in_job = HTCondorBackend(NoPilots(), 1, host="127.0.0.1", in_job=SITES["generic"])
    try:
        assert (attached.driver_memory_mb, in_job.driver_memory_mb) == (None, expected)
    finally:
        attached.close()
        in_job.close()


# ---- the announce wait ------------------------------------------------------------------------------------


class Answers:
    """A ``ServiceJob`` stand-in whose ``ad()`` gives ``states`` in turn, the last repeating."""

    def __init__(self, states: list[dict[str, Any]]) -> None:
        self.key = "scope-0123456789abcdef"
        self.dir = Path("service-scope")
        self.states = states

    def ad(self) -> dict[str, Any]:
        return self.states.pop(0) if len(self.states) > 1 else self.states[0]


@pytest.fixture
def condor(monkeypatch: pytest.MonkeyPatch) -> Iterator[HTCondorBackend]:
    monkeypatch.setattr(server_mod, "POLL_S", 0.02)
    backend = HTCondorBackend(NoPilots(), 1, host="127.0.0.1")
    try:
        yield backend
    finally:
        backend.close()


def announcing_after(backend: HTCondorBackend, monkeypatch: pytest.MonkeyPatch, polls: int) -> None:
    """``wait_announce`` answers on its ``polls``-th call."""
    calls = iter(range(polls))

    def wait(key: str, timeout: float) -> tuple[str, str] | None:
        return None if next(calls, None) is not None else ("127.0.0.1:10007", "node7")

    monkeypatch.setattr(backend._server, "wait_announce", wait)


@pytest.mark.parametrize(("every_s", "lines"), [(1000.0, 1), (0.0, 3)])
def test_a_waiting_job_is_logged_once_per_interval_and_never_timed_out(
    every_s: float,
    lines: int,
    condor: HTCondorBackend,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(backend_mod, "IDLE_LOG_S", every_s)
    announcing_after(condor, monkeypatch, 3)
    job: Any = Answers([{"JobStatus": 1}, {"JobStatus": 1}, {"JobStatus": 5, "HoldReasonCode": 16}])
    waits = ServiceSpec("waits", kind="m69b", check="tcp", timeout_s=0.01)
    with caplog.at_level(logging.INFO, logger="graphed_executors"):
        got = condor._await_announce(job, waits)
    assert got == ("127.0.0.1:10007", "node7")
    logged = [r.getMessage() for r in caplog.records if job.key in r.getMessage()]
    assert len(logged) == lines and "JobStatus=1" in logged[0], logged


def test_the_deadline_outlives_an_eviction(condor: HTCondorBackend, monkeypatch: pytest.MonkeyPatch) -> None:
    announcing_after(condor, monkeypatch, 10**6)
    job: Any = Answers([{"JobStatus": 2}, {"JobStatus": 1}])
    quick = ServiceSpec("evicted", kind="m69b", check="tcp", timeout_s=0.2)
    with pytest.raises(TimeoutError, match=r"timeout_s=0\.2 of its start: JobStatus=1"):
        condor._await_announce(job, quick)


# ---- the pool's slots -------------------------------------------------------------------------------------


@pytest.mark.parametrize("locate", [None, ("cm.example:9618", "schedd.example")])
def test_machine_ads_ask_the_schedd_s_pool_and_drop_dynamic_slots(
    locate: tuple[str, str] | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[tuple[Any, ...]] = []
    ads = [{"Name": "slot1", "SlotType": "Partitionable"}, {"Name": "slot1_1", "SlotType": "Dynamic"}]

    def collector(*pool: str) -> Any:
        def query(constraint: str) -> list[dict[str, str]]:
            asked.append((*pool, constraint))
            return ads

        return SimpleNamespace(query=query)

    monkeypatch.setattr(launch, "_htcondor", lambda: SimpleNamespace(Collector=collector))
    got = machine_ads(CondorPilots("generic", schedd_locate=locate))
    assert [ad["Name"] for ad in got] == ["slot1"]
    pool = () if locate is None else (locate[0],)
    assert asked == [(*pool, 'MyType == "Machine"')]


def test_a_partitionable_slot_is_matched_at_its_totals() -> None:
    busy = {
        "PartitionableSlot": True,
        "Memory": 997,
        "TotalSlotMemory": 15973,
        "Cpus": 1,
        "TotalSlotCpus": 10,
    }
    assert _as_whole(dict(busy)) == {**busy, "Memory": 15973, "Cpus": 10}
    static = {"Memory": 2048, "TotalSlotMemory": 4096}
    assert _as_whole(dict(static)) == static


def test_a_job_that_left_the_queue_is_not_refused_by_the_match(tmp_path: Path) -> None:
    schedd = SimpleNamespace(query=lambda constraint: [])
    pilots = CondorPilots("generic", log_dir=tmp_path)
    pilots._schedd = schedd
    job = ServiceJob(spec("gone", None), pilots, key="scope-gone", url="http://127.0.0.1:1", secret=b"s")
    job.cluster = 4242
    assert job.match_refusal([{"SlotType": "Partitionable"}]) is None
