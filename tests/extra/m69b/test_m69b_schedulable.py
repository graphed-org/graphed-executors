"""m69b schedulability paths the frozen suite does not reach: the physical-memory read on the other OS's branch,
the driver check's fall-through to a cluster host and its refusal beside the earlier legs, the per-start sum,
a size-less spec; a driver job's slot ``Memory``; the condor announce wait's idle log cadence and a deadline
that outlives an eviction; the collector query, a partitionable slot's totals, and a job that left the queue
before its match; a pool whose collector lists no slot (submit and wait); a node's disk counted whole; the
backend's close removing a waiting job in its own thread, a serialized stop(), no submit after a stop or
once the waits are stopped; a held pilot counted alive, a running pilot's claim taken out of its slot, and
graphed's release leaving a user's hold (that one on a real schedd). The rest run over stand-ins."""

from __future__ import annotations

import ctypes
import logging
import os
import socket
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec

from graphed_executors.htcondor_backend import SITES, CondorPilots, HTCondorBackend, launch
from graphed_executors.htcondor_backend import backend as backend_mod
from graphed_executors.htcondor_backend import server as server_mod
from graphed_executors.htcondor_backend.services import ServiceJob, _as_whole, _less_claim, machine_ads
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
        "Disk": 524288,
        "TotalSlotDisk": 2097152,
    }
    assert _as_whole(dict(busy)) == {**busy, "Memory": 15973, "Cpus": 10, "Disk": 2097152}
    static = {"Memory": 2048, "TotalSlotMemory": 4096}
    assert _as_whole(dict(static)) == static


def test_a_job_that_left_the_queue_is_not_refused_by_the_match(tmp_path: Path) -> None:
    schedd = SimpleNamespace(query=lambda constraint: [])
    pilots = CondorPilots("generic", log_dir=tmp_path)
    pilots._schedd = schedd
    job = ServiceJob(spec("gone", None), pilots, key="scope-gone", url="http://127.0.0.1:1", secret=b"s")
    job.cluster = 4242
    assert job.match_refusal([{"SlotType": "Partitionable"}]) is None


def test_a_job_waiting_only_on_a_busy_node_s_disk_matches() -> None:
    classad2 = pytest.importorskip("classad2")  # ships with the htcondor bindings (Linux)
    job = classad2.ClassAd("[ RequestDisk = 1048576; Requirements = TARGET.Disk >= RequestDisk ]")
    busy = "[ PartitionableSlot = true; Disk = 524288; TotalSlotDisk = 2097152; Requirements = true ]"
    assert not job.symmetricMatch(classad2.ClassAd(busy))  # the collector's busy node, as it reports it
    assert job.symmetricMatch(_as_whole(classad2.ClassAd(busy)))


# ---- a whole backend over a stand-in pool -------------------------------------------------------------------


class Pool:
    """``htcondor2`` over a schedd whose every job answers ``status`` (``None``: gone) and a collector that
    lists no slot; ``log`` records submits (by batch name) and actions."""

    def __init__(self, status: int | None) -> None:
        self.status = status
        self.log: list[tuple[str, str]] = []
        self.clusters = iter(range(70, 100))
        self.JobAction = SimpleNamespace(Remove="Remove")
        self.param = {"SCHEDD_HOST": "s1"}
        self.Submit = dict

    def Schedd(self, ad: Any = None) -> Any:
        return self

    def Collector(self, *pool: str) -> Any:
        return SimpleNamespace(query=lambda constraint: [])

    def submit(self, desc: dict[str, str], count: int = 0, spool: bool = False) -> Any:
        cluster = next(self.clusters)
        self.log.append(("submit", f"{desc['JobBatchName']} {cluster}"))
        return SimpleNamespace(cluster=lambda: cluster)

    def query(self, constraint: str = "", projection: Any = None) -> list[dict[str, Any]]:
        return [] if self.status is None else [{"JobStatus": self.status}]

    def history(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return []

    def act(self, action: str, constraint: str, reason: str = "") -> None:
        self.log.append(("act", f"{action} {constraint}"))

    def service_cluster(self) -> str:
        (submit,) = [entry for kind, entry in self.log if kind == "submit" and "graphed-service-" in entry]
        return submit.rsplit(" ", 1)[1]


def pooled(
    status: int | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[HTCondorBackend, Pool]:
    pool = Pool(status)
    monkeypatch.setattr(launch, "_htcondor", lambda: pool)
    monkeypatch.setattr(launch, "CLOSE_WAIT_S", 0.0)  # the stand-in's pilots never exit by themselves
    pilots = CondorPilots("generic", log_dir=tmp_path)
    return HTCondorBackend(pilots, 1, host="127.0.0.1", service_hosts=("cluster",)), pool


def test_a_pool_whose_collector_lists_no_slot_submits_and_waits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # were the empty ad list matched, this classad2 would reach max() over no slot
    monkeypatch.setitem(sys.modules, "classad2", SimpleNamespace(ClassAd=dict))
    monkeypatch.setattr(server_mod, "POLL_S", 0.02)
    backend, pool = pooled(2, tmp_path, monkeypatch)
    try:
        announcing_after(backend, monkeypatch, 2)
        endpoint, identity, key = backend.host_service(spec("unlisted", 64), "scope")
        assert (endpoint, identity) == ("tcp://127.0.0.1:10007", "node7")
        backend.release_service(key)
        assert ("act", f"Remove ClusterId == {pool.service_cluster()}") in pool.log
    finally:
        pool.status = None
        backend.close()


def key_of(pool: Pool) -> str:
    (submit,) = [entry for kind, entry in pool.log if kind == "submit" and "graphed-service-" in entry]
    return submit.split(" ", 1)[0].removeprefix("graphed-service-")


def test_backend_close_removes_a_waiting_service_job_before_it_returns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_mod, "POLL_S", 3.0)  # the waiter's own poll comes long after close() returns
    backend, pool = pooled(1, tmp_path, monkeypatch)  # the job stays idle
    raised: list[BaseException] = []

    def wait() -> None:
        try:
            backend.host_service(spec("idle", 64), "scope")
        except Exception as exc:
            raised.append(exc)

    waiter = threading.Thread(target=wait, daemon=True)
    waiter.start()
    deadline = time.monotonic() + 10.0
    while not any(kind == "submit" for kind, _ in pool.log) and time.monotonic() < deadline:
        time.sleep(0.01)
    backend.close()
    removed = ("act", f"Remove ClusterId == {pool.service_cluster()}") in pool.log
    waiter.join(30.0)
    assert removed, pool.log
    (error,) = raised
    assert key_of(pool) in str(error), error


class BlockingPool(Pool):
    """A ``Pool`` whose every ``act`` waits for ``gate``, having set ``acting``."""

    def __init__(self) -> None:
        super().__init__(1)
        self.acting = threading.Event()
        self.gate = threading.Event()

    def act(self, action: str, constraint: str, reason: str = "") -> None:
        self.acting.set()
        self.gate.wait(30.0)
        super().act(action, constraint, reason)


def submitted_job(
    pool: Pool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, submit: bool = True
) -> ServiceJob:
    monkeypatch.setattr(launch, "_htcondor", lambda: pool)
    pilots = CondorPilots("generic", log_dir=tmp_path)
    pilots.prepare("http://127.0.0.1:1", b"s")
    job = ServiceJob(spec("web", 64), pilots, key="scope-web", url="http://127.0.0.1:1", secret=b"s")
    if submit:
        job.submit()
    return job


def test_a_second_stop_returns_only_after_the_first_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = BlockingPool()
    job = submitted_job(pool, tmp_path, monkeypatch)
    returned: list[str] = []
    raised: list[BaseException] = []

    def stop(name: str) -> None:
        try:
            job.stop()
        except BaseException as exc:
            raised.append(exc)
        returned.append(name)

    first = threading.Thread(target=stop, args=("first",), daemon=True)
    second = threading.Thread(target=stop, args=("second",), daemon=True)
    try:
        first.start()
        assert pool.acting.wait(10.0), "the first stop() never reached its removal"
        second.start()
        second.join(1.0)
        assert second.is_alive() and returned == [], "a second stop() returned before the removal ran"
    finally:
        pool.gate.set()
        first.join(10.0)
        second.join(10.0)
    assert sorted(returned) == ["first", "second"] and raised == []
    assert job.dir is not None
    secret = job.dir / launch.SECRET_FILE
    assert not secret.exists()
    secret.write_text("")
    job.stop()  # a later stop() repeats none of the work
    assert secret.exists()
    assert [entry for entry in pool.log if entry[0] == "act"] == [
        ("act", f"Remove ClusterId == {job.cluster}")
    ]


def test_a_job_stopped_before_its_submit_is_never_submitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = Pool(1)
    job = submitted_job(pool, tmp_path, monkeypatch, submit=False)
    job.stop()
    with pytest.raises(RuntimeError, match="stopped before it was submitted"):
        job.submit()
    assert [kind for kind, _ in pool.log] == []


def test_no_service_job_is_submitted_once_the_waits_are_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, pool = pooled(1, tmp_path, monkeypatch)
    try:
        backend.stop_waiting()
        with pytest.raises(RuntimeError, match="not submitted: the runner is closing"):
            backend.host_service(spec("late", 64), "scope")
        assert pool.log == []
        assert backend._server._announce_secrets == {}
    finally:
        backend.close()


# ---- the runner's own pilots --------------------------------------------------------------------------


def test_alive_counts_a_pilot_graphed_held_and_not_one_the_user_held(tmp_path: Path) -> None:
    held = f"{launch.HOLD_REASON} (by user someone)"
    ads = [
        {"JobStatus": 2},
        {"JobStatus": 5, "HoldReasonCode": 1, "HoldReason": held},
        {"JobStatus": 5, "HoldReasonCode": 1, "HoldReason": "via condor_hold (by user someone)"},
    ]
    pilots = CondorPilots("generic", log_dir=tmp_path)
    pilots._schedd = SimpleNamespace(query=lambda constraint, projection: ads)
    assert pilots.alive() == 2


class Ad(dict[str, Any]):
    """A job ad as ``schedd.query`` returns it: ``eval`` reads an attribute's value."""

    def eval(self, attr: str) -> Any:
        return self[attr]


def test_a_running_job_s_claim_leaves_the_slot_it_runs_in() -> None:
    whole = {"Name": "slot1@wn", "Memory": 16000, "Cpus": 8, "Disk": 1000}
    static = {"Name": "slot2@wn", "Memory": 4000, "Cpus": 1, "Disk": 1000}
    other = {"Name": "slot1@wn2", "Memory": 16000, "Cpus": 8, "Disk": 1000}
    slots = [dict(whole), dict(static), dict(other)]
    dynamic = Ad(RemoteHost="slot1_3@wn", MemoryProvisioned=6144, CpusProvisioned=2, RequestMemory=6000)
    asked_only = Ad(RemoteHost="slot2@wn", RequestMemory=4000, RequestCpus=1, RequestGPUs=1)
    _less_claim(slots, dynamic)
    _less_claim(slots, asked_only)
    assert slots == [
        {**whole, "Memory": 16000 - 6144, "Cpus": 6},
        {**static, "Memory": 0, "Cpus": 0},
        other,
    ]


def test_a_real_schedd_releases_graphed_s_hold_and_not_the_user_s(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    htc = pytest.importorskip("htcondor2", reason="the htcondor bindings (Linux; the test-htcondor job)")
    monkeypatch.setattr(launch, "CLOSE_WAIT_S", 0.0)
    pilots = CondorPilots("generic", log_dir=tmp_path, request_memory_mb=10**7)  # never matched: idle
    pilots.start("http://127.0.0.1:1", b"s", 2)
    try:
        assert pilots.cluster is not None
        cluster = pilots.cluster[1]
        pilots._schedd.act(htc.JobAction.Hold, f"ClusterId == {cluster} && ProcId == 1")  # the user's

        def statuses() -> list[int]:
            ads = pilots._schedd.query(
                constraint=f"ClusterId == {cluster}", projection=["ProcId", "JobStatus"]
            )
            return [int(ad["JobStatus"]) for ad in sorted(ads, key=lambda ad: int(ad["ProcId"]))]

        pilots.hold_queued()
        assert wait_until(lambda: statuses() == [5, 5]), statuses()
        assert pilots.alive() == 1
        pilots.release_held()
        assert wait_until(lambda: statuses() == [1, 5]), statuses()
    finally:
        pilots.stop()


def wait_until(predicate: Any, timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.2)
    return True
