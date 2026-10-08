"""Shared harness for the m74 frozen suite: a run whose driver is SIGKILLed resumes from the
content-addressed store on every runner (lanes/ckpt-resume/plan.md §4.2).

Everything a worker, pilot or driver subprocess unpickles is module-level here: the T-leaf plan
(``Leaf``) and the counting codec (``CountingCodec``). ``drive`` is the driver entry of every runner leg,
run as ``python -c "import m74_harness; m74_harness.drive()" <runner> <store> <work> <out>`` in its own
session; ``crash_and_resume`` kills that session after ``MIN_DONE`` tasks are in the store and reruns
it. The m67 driverless pieces E6/E8 use are copied here (frozen suites never import across
directories).
"""

from __future__ import annotations

import contextlib
import importlib
import logging
import os
import pickle
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import awkward as ak
import numpy as np
from graphed.checkpoint import Store, resumable
from graphed.core.execution import Partition, Plan, Task

if TYPE_CHECKING:
    import pytest

HARNESS_DIR = str(Path(__file__).resolve().parent)
HARNESS_FILE = str(Path(__file__).resolve())

T = 16
LEAF_SLEEP_S = 0.2
MIN_DONE = 4
WORKERS = 2
RUN_TIMEOUT_S = 240.0
START_TIMEOUT_S = 120.0
KILL_TIMEOUT_S = 60.0

REUSED = re.compile(r"(\d+) of (\d+) tasks reused from ")
DRIVER_HEADER = "driver pid="
DRIVER_ENTRY = "import m74_harness; m74_harness.drive()"
PROCESS_WORKERS = frozenset({"process", "dask", "parsl"})

FAKE_POOL = "cm.m74.example:9618"
FAKE_SCHEDD = "schedd.m74.example"
FAKE_CLUSTER = 7474
DRIVER_MODULE = "graphed_executors.htcondor_backend.driver"

# ---- the plan --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Leaf:
    """Leaf ``i``: a marker file per execution in ``markers``, then ``[1e16 if i == 0 else 1.0, i]``,
    so the fold's grouping shows in the bits of element 0."""

    markers: str

    def __call__(self, partition: Partition, resources: object) -> Any:
        i = partition.entry_start
        Path(self.markers, f"{i}-{os.getpid()}-{uuid.uuid4().hex}").touch()
        time.sleep(LEAF_SLEEP_S)
        return np.array([1e16 if i == 0 else 1.0, float(i)], dtype=np.float64)


def add(a: Any, b: Any) -> Any:
    return a + b


def zeros() -> Any:
    return np.zeros(2, dtype=np.float64)


def leaf_plan(markers: str | Path) -> Plan[Any]:
    tasks = tuple(Task(i, Partition(f"mem://m74/{i}", "", i, i + 1)) for i in range(T))
    return Plan(process=Leaf(str(markers)), combine=add, empty=zeros, tasks=tasks)


@dataclass(frozen=True)
class CountingCodec:
    """Pickle, with a ``<pid>-<uuid>`` file in ``decodes`` per decode."""

    decodes: str

    def encode(self, value: Any) -> bytes:
        return pickle.dumps(value, protocol=5)

    def decode(self, data: bytes) -> Any:
        Path(self.decodes, f"{os.getpid()}-{uuid.uuid4().hex}").touch()
        return pickle.loads(data)


def work_dirs(work: Path) -> tuple[Path, Path]:
    """``(markers, decodes)`` of a work dir, created."""
    markers, decodes = work / "markers", work / "decodes"
    markers.mkdir(parents=True, exist_ok=True)
    decodes.mkdir(parents=True, exist_ok=True)
    return markers, decodes


# ---- the driver entry --------------------------------------------------------------------------------


def _run(runner: str, plan: Plan[Any], work: Path) -> Any:
    if runner in ("thread", "process"):
        from graphed_executors.local import ProcessPoolExecutor, ThreadExecutor  # noqa: PLC0415

        executor_type = ThreadExecutor if runner == "thread" else ProcessPoolExecutor
        with executor_type(WORKERS) as executor:
            return executor.run(plan).value
    if runner == "submit-thread":
        from graphed_executors.submit import SubmitRunner, ThreadBackend  # noqa: PLC0415

        with SubmitRunner(ThreadBackend(WORKERS)) as submit_runner:
            return submit_runner.run(plan).value
    if runner in ("dask", "dask-peer"):
        import distributed  # noqa: PLC0415

        from graphed_executors.dask_backend import DaskBackend, dask_runner  # noqa: PLC0415
        from graphed_executors.dask_backend.transport_peer import transport_run_plan  # noqa: PLC0415

        with (
            distributed.LocalCluster(
                n_workers=WORKERS, threads_per_worker=1, processes=True, dashboard_address=":0"
            ) as cluster,
            distributed.Client(cluster) as client,
        ):
            if runner == "dask-peer":
                transport = importlib.import_module("graphed_executors.dask_backend.transport")
                client.register_plugin(transport.GraphedTransportPlugin())
                return transport_run_plan(plan, DaskBackend(client)).value
            with dask_runner(client) as dask_submit:
                return dask_submit.run(plan).value
    if runner in ("parsl", "parsl-peer"):
        from graphed_executors.parsl_backend import (  # noqa: PLC0415
            ParslBackend,
            parsl_runner,
            start_htex,
            stop_htex,
        )
        from graphed_executors.parsl_backend.transport_peer import parsl_run_plan  # noqa: PLC0415

        htex = start_htex(workers=WORKERS, run_dir=str(work / f"htex-{os.getpid()}"))
        try:
            if runner == "parsl-peer":
                return parsl_run_plan(plan, ParslBackend(htex)).value
            with parsl_runner(htex) as parsl_submit:
                return parsl_submit.run(plan).value
        finally:
            stop_htex(htex)
    raise ValueError(f"unknown runner {runner!r}")


def drive(argv: list[str] | None = None) -> None:
    """``<runner> <store> <work> <out>``: the leaf plan over ``work``, made resumable on ``store`` with
    the counting codec, run on ``runner``; ``pickle.dumps(value)`` to ``out``. ``graphed.checkpoint``
    logs at INFO to stderr."""
    runner, store, work, out = argv if argv is not None else sys.argv[1:5]
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
    log = logging.getLogger("graphed.checkpoint")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    markers, decodes = work_dirs(Path(work))
    plan = resumable(leaf_plan(markers), store, codec=CountingCodec(str(decodes)))
    value = _run(runner, plan, Path(work))
    Path(out).write_bytes(pickle.dumps(value))


# ---- driver sessions -----------------------------------------------------------------------------------


def driver_env(extra_path: tuple[str, ...] = ()) -> dict[str, str]:
    """This environment with the venv's bin on ``PATH`` (HTEX's worker pool script) and this
    directory (after ``extra_path``) on ``PYTHONPATH``."""
    env = dict(os.environ)
    env["PATH"] = os.path.dirname(sys.executable) + os.pathsep + env.get("PATH", "")
    env["PYTHONPATH"] = os.pathsep.join([*extra_path, HARNESS_DIR, env.get("PYTHONPATH", "")]).rstrip(
        os.pathsep
    )
    return env


def start_driver(argv: list[str], cwd: Path, log: Path, env: dict[str, str] | None = None) -> Any:
    """``argv`` in a new session (its pid is the session id), stdout+stderr appended to ``log``."""
    with open(log, "ab") as sink:
        return subprocess.Popen(
            argv,
            cwd=cwd,
            env=env if env is not None else driver_env(),
            stdout=sink,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )


def session_members(sid: int) -> list[int]:
    """Live pids whose session is ``sid``: ``ps -A -o pid=`` filtered by ``os.getsid``."""
    listing = subprocess.run(["ps", "-A", "-o", "pid="], capture_output=True, text=True, check=True)
    members = []
    for token in listing.stdout.split():
        pid = int(token)
        with contextlib.suppress(OSError):
            if os.getsid(pid) == sid:
                members.append(pid)
    return members


def _kill(pids: list[int]) -> None:
    for pid in pids:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


def kill_session(proc: Any) -> None:
    """SIGKILL every process in ``proc``'s session, ``wait()`` the driver, then kill again until no
    member is left, within ``KILL_TIMEOUT_S``."""
    sid = proc.pid
    _kill(session_members(sid))
    proc.wait(KILL_TIMEOUT_S)
    deadline = time.monotonic() + KILL_TIMEOUT_S
    while members := session_members(sid):
        assert time.monotonic() < deadline, f"session {sid} still has {members} after the kill"
        _kill(members)
        time.sleep(0.05)


def finish(proc: Any, timeout_s: float = RUN_TIMEOUT_S) -> int:
    """The driver's exit code, killing its session if it outlives ``timeout_s``."""
    try:
        return int(proc.wait(timeout_s))
    except subprocess.TimeoutExpired:
        kill_session(proc)
        raise AssertionError(f"driver {proc.pid} did not finish within {timeout_s}s") from None


def wait_for(predicate: Callable[[], bool], timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            return predicate()
        time.sleep(0.05)
    return True


def done(store: Path) -> int:
    return len(Store(store).completed())


def files(directory: Path) -> set[str]:
    return {p.name for p in directory.iterdir()}


def reused_line(text: str) -> int:
    """``reused`` of the one ``"%d of %d tasks reused from %s"`` line in ``text``, whose total is ``T``."""
    found = REUSED.findall(text)
    assert len(found) == 1, f"expected one reused line, found {found} in:\n{text[-4000:]}"
    reused, total = (int(x) for x in found[0])
    assert total == T, found
    return reused


def last_driver_section(log: Path) -> str:
    """``driver.log`` from its last ``driver pid=`` header on (the log is appended per try)."""
    text = log.read_text()
    start = text.rfind(DRIVER_HEADER)
    assert start >= 0, f"no {DRIVER_HEADER!r} header in {log}:\n{text[-4000:]}"
    return text[start:]


# ---- a killed run and its resume ---------------------------------------------------------------------


@dataclass(frozen=True)
class Resumed:
    """What one crash-and-resume showed: tasks in the store after the kill, executions of the rerun,
    the rerun's log, both value bytes, and the pids of decodes and drivers."""

    done: int
    executions: int
    log: str
    value: bytes
    reference: bytes
    decode_pids: frozenset[int]
    driver_pids: frozenset[int]


def _drive_argv(runner: str, store: Path, work: Path, out: Path) -> list[str]:
    return [sys.executable, "-c", DRIVER_ENTRY, runner, str(store), str(work), str(out)]


def run_to_end(runner: str, store: Path, work: Path, name: str) -> tuple[int, bytes, str]:
    """(driver pid, value bytes, its log) of an uninterrupted driver run."""
    work_dirs(work)
    out, log = work / f"{name}.pkl", work / f"{name}.log"
    proc = start_driver(_drive_argv(runner, store, work, out), work, log)
    code = finish(proc)
    kill_session(proc)
    text = log.read_text()
    assert code == 0, f"driver {runner} exited {code}:\n{text[-4000:]}"
    return proc.pid, out.read_bytes(), text


def kill_after_min_done(runner: str, store: Path, work: Path) -> int:
    """Start a driver, kill its session once ``MIN_DONE`` tasks are in ``store``; the tasks left done."""
    work_dirs(work)
    log = work / "killed.log"
    proc = start_driver(_drive_argv(runner, store, work, work / "killed.pkl"), work, log)
    try:
        wait_for(lambda: proc.poll() is not None or done(store) >= MIN_DONE, START_TIMEOUT_S)
    finally:
        kill_session(proc)
    left = done(store)
    assert 0 < left < T, f"{left} of {T} tasks done after the kill:\n{log.read_text()[-4000:]}"
    return left


def crash_and_resume(tmp_path: Path, runner: str, resume_on: str | None = None) -> Resumed:
    """Kill a ``runner`` driver after ``MIN_DONE`` stored tasks, rerun on ``resume_on`` (default the
    same runner) to the end, and run ``resume_on`` uninterrupted on a fresh store for reference."""
    resume_on = resume_on or runner
    store, work = tmp_path / "store", tmp_path / "work"
    left = kill_after_min_done(runner, store, work)
    markers, decodes = work_dirs(work)
    before = files(markers)
    seen = files(decodes)
    pid, value, text = run_to_end(resume_on, store, work, "resumed")
    _, reference, _ = run_to_end(resume_on, tmp_path / "ref-store", tmp_path / "ref-work", "reference")
    return Resumed(
        done=left,
        executions=len(files(markers) - before),
        log=text,
        value=value,
        reference=reference,
        decode_pids=frozenset(int(name.split("-")[0]) for name in files(decodes) - seen),
        driver_pids=frozenset({pid}),
    )


def assert_resumed(result: Resumed, process_workers: bool) -> None:
    """Plan §4.2 (i)-(iv)."""
    assert result.executions == T - result.done, (
        f"the rerun executed {result.executions} tasks; {T} - {result.done} were not done"
    )
    assert result.value == result.reference, (pickle.loads(result.value), pickle.loads(result.reference))
    assert reused_line(result.log) == result.done, result.done
    if process_workers:
        assert result.decode_pids, "no decode ran in the rerun"
        assert not result.decode_pids & result.driver_pids, (result.decode_pids, result.driver_pids)


# ---- a fake distribution ---------------------------------------------------------------------------------


def fake_dist(root: Path, version: str, name: str = "zzfake") -> Path:
    """A ``PYTHONPATH`` entry holding ``<name>-<version>.dist-info/METADATA``."""
    info = root / f"{name}-{version}.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
    return root


def record_environment_with(path_entry: Path, store: Path, salt: str, cwd: Path) -> None:
    """``resumable(leaf_plan, store, salt=salt)`` in a ``python`` subprocess with ``path_entry`` ahead
    on ``PYTHONPATH``: the store's environment record for ``salt`` is then that environment's."""
    code = (
        "import sys; from m74_harness import leaf_plan, resumable; "
        "resumable(leaf_plan(sys.argv[3]), sys.argv[1], salt=sys.argv[2])"
    )
    cwd.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [sys.executable, "-c", code, str(store), salt, str(cwd)],
        cwd=cwd,
        env=driver_env((str(path_entry),)),
        capture_output=True,
        text=True,
        timeout=RUN_TIMEOUT_S,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


# ---- the HTCondor driverless job (copied from m67's driverless_harness and test_driver_entry) -------


def htcondor_api() -> Any:
    return importlib.import_module("graphed_executors.htcondor_backend")


def launch_api() -> Any:
    return importlib.import_module("graphed_executors.htcondor_backend.launch")


def driver_api() -> Any:
    return importlib.import_module("graphed_executors.htcondor_backend.driver")


class SubmitResult:
    def __init__(self, cluster: int) -> None:
        self._cluster = cluster

    def cluster(self) -> int:
        return self._cluster


class RecordingSchedd:
    """A stand-in ``htcondor2.Schedd`` that appends every call to ``log``."""

    def __init__(self) -> None:
        self.log: list[tuple[Any, ...]] = []

    def query(self, constraint: str = "true", projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("query", str(constraint), tuple(projection or ())))
        return []

    def history(self, constraint: Any = None, projection: Any = None, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("history", str(constraint), tuple(projection or ())))
        return []

    def submit(self, description: Any, count: int = 0, spool: bool = False, **kwargs: Any) -> SubmitResult:
        self.log.append(("submit", dict(description), count, spool))
        return SubmitResult(FAKE_CLUSTER)

    def spool(self, result: Any, *args: Any, **kwargs: Any) -> None:
        self.log.append(("spool",))

    def act(self, action: Any, constraint: Any = None, *args: Any, **kwargs: Any) -> None:
        self.log.append(("act", str(action), str(constraint)))


class _Collector:
    def __init__(self, fake: FakeHTCondor, pool: str | None) -> None:
        self.fake = fake
        self.pool = pool

    def locate(self, daemon_type: Any, name: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.fake.log.append(("locate", self.pool, str(daemon_type), name))
        return {"Name": name, "MyAddress": "<127.0.0.1:9618>", "located": True}

    def query(
        self, ad_type: Any = None, constraint: Any = None, projection: Any = None, **kwargs: Any
    ) -> Any:
        self.fake.log.append(("collector-query", self.pool))
        return list(self.fake.schedd_ads)


class _Credd:
    def query_user_cred(self, *args: Any, **kwargs: Any) -> int:
        return 1790802437


class FakeHTCondor:
    """What ``launch._htcondor()`` returns under the recorder."""

    class DaemonType:
        Schedd = "Schedd"

    class AdType:
        Schedd = "Schedd"

    class JobAction:
        Remove = "Remove"

    class CredType:
        Kerberos = "Kerberos"

    def __init__(self, schedd: RecordingSchedd) -> None:
        self.schedd = schedd
        self.log = schedd.log
        self.param = {"COLLECTOR_HOST": FAKE_POOL, "SCHEDD_HOST": FAKE_SCHEDD, "FERMIHTC_REMOTE_POOL": FAKE_POOL}
        self.schedd_ads: list[dict[str, Any]] = [
            {
                "Name": FAKE_SCHEDD,
                "RecentDaemonCoreDutyCycle": 0.1,
                "ShadowsRunning": 1,
                "MaxJobsRunning": 10,
                "TotalIdleJobs": 1,
            }
        ]

    def Credd(self, *args: Any, **kwargs: Any) -> _Credd:
        return _Credd()

    def Collector(self, pool: str | None = None, *args: Any, **kwargs: Any) -> _Collector:
        self.log.append(("Collector", pool))
        return _Collector(self, pool)

    def Schedd(self, location: Any = None, *args: Any, **kwargs: Any) -> RecordingSchedd:
        self.log.append(("Schedd", None if location is None else location["Name"]))
        return self.schedd

    def Submit(self, description: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return dict(description or {})


def record_bindings(monkeypatch: pytest.MonkeyPatch, schedd: RecordingSchedd) -> FakeHTCondor:
    """Patch ``launch._htcondor`` to log ``("_htcondor",)`` and return a ``FakeHTCondor`` over ``schedd``."""
    fake = FakeHTCondor(schedd)

    def _htcondor() -> FakeHTCondor:
        fake.log.append(("_htcondor",))
        return fake

    monkeypatch.setattr(launch_api(), "_htcondor", _htcondor)
    return fake


def submits(log: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return [entry for entry in log if entry[0] == "submit"]


def job_dir(desc: dict[str, Any], log_dir: Path, dest: Path) -> Path:
    """A job scratch dir holding each ``transfer_input_files`` entry under its base name."""
    iwd = Path(desc.get("initialdir") or log_dir)
    dest.mkdir(parents=True)
    for entry in (e.strip() for e in str(desc["transfer_input_files"]).split(",") if e.strip()):
        src = iwd / entry
        if src.is_dir():
            shutil.copytree(src, dest / src.name)
        else:
            shutil.copy2(src, dest / src.name)
    return dest


def prepared_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plan: Any, name: str, **kwargs: Any) -> Path:
    """A job scratch dir ``tmp_path/<name>`` from a recorded ``submit_driverless``, with ``.machine.ad``."""
    log_dir = tmp_path / f"{name}-submit"
    log_dir.mkdir()
    fake = record_bindings(monkeypatch, RecordingSchedd())
    htcondor_api().submit_driverless(
        plan, log_dir=log_dir, request_memory_mb=2048, user_modules=[HARNESS_FILE], **kwargs
    )
    (entry,) = submits(fake.log)
    job = job_dir(entry[1], log_dir, tmp_path / name)
    (job / ".machine.ad").write_text('Machine = "127.0.0.1"\nName = "slot1@127.0.0.1"\nCpus = 2\n')
    return job


def start_job_driver(job: Path) -> Any:
    """``python -m ...driver <job>`` in ``job``, in a new session, as the job's ``driver.sh`` runs it."""
    env = dict(driver_env(), _CONDOR_MACHINE_AD=str(job / ".machine.ad"))
    return start_driver([sys.executable, "-m", DRIVER_MODULE, str(job)], job, job / "driver.out", env)


def read_result(job: Path) -> tuple[bool, Any]:
    ok, payload = pickle.loads((job / "result.pkl").read_bytes())
    return ok, payload


# ---- a gh plan over in-memory events (copied from m69b) ------------------------------------------------


@dataclass(frozen=True)
class Steps:
    """A ``PartitionedSource`` over in-memory events: ``n`` blind steps of ``uri``."""

    data: ak.Array
    uri: str

    def __call__(self) -> ak.Array:
        raise AssertionError("the whole-dataset loader must never run during a plan")

    def partitions(self, steps_per_file: int = 1) -> tuple[Partition, ...]:
        return tuple(Partition.blind(self.uri, "", s, steps_per_file) for s in range(steps_per_file))

    def read_partition(self, partition: Partition, columns: Any, resources: Any) -> ak.Array:
        part = partition.resolve(len(self.data))
        return self.data[part.entry_start : part.entry_stop]


def events_data(n: int = 200) -> ak.Array:
    rng = np.random.default_rng(7474)
    return ak.Array({"x": rng.random(n), "w": rng.choice(np.array([0.5, 1.0, 2.0]), n)})


def histogram_plan(context: Any = None) -> Any:
    """``gh.plan`` of one Weight fill of ``x``, backed on ``context`` (a histserv ``Context``) when given."""
    import boost_histogram as bh  # noqa: PLC0415
    from graphed import Session  # noqa: PLC0415
    from graphed.awkward import AwkwardBackend, AwkwardForm  # noqa: PLC0415

    gh = importlib.import_module("graphed_histogram")
    data = events_data()
    session = Session(AwkwardBackend())
    form = AwkwardForm(ak.Array(data.layout.to_typetracer(forget_length=True)))
    events = session.source("events", form=form, data=Steps(data, "mem://m74/events"))
    axis = bh.axis.Regular(8, 0.0, 1.0)
    if context is None:
        h = gh.boost.Histogram(axis, storage=bh.storage.Weight())
    else:
        hs = importlib.import_module("graphed_histogram.histserv")
        h = hs.Histogram(axis, storage=bh.storage.Weight(), context=context)
    h.fill(events.x, weight=events.w)
    return gh.plan({"h": h}, steps_per_file=4)


def run_bounded(fn: Callable[[], Any], timeout_s: float = RUN_TIMEOUT_S) -> Any:
    """``fn()`` on a daemon thread; a hang fails the test instead of wedging the job."""
    out: dict[str, Any] = {}

    def _drive() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    thread = threading.Thread(target=_drive, daemon=True)
    thread.start()
    thread.join(timeout_s)
    assert not thread.is_alive(), f"HARD TIMEOUT: call did not finish within {timeout_s}s"
    if "error" in out:
        raise out["error"]
    return out["result"]
