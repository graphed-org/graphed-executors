"""m68c §6 on HTCondor: ``HTCondorRunner.run`` and ``submit_driverless`` pre-check a join plan's stage
processes, the driverless DAG derives a join plan's SERVICE nodes, and a live join runs over pool pilots.

The refusal and derivation legs run anywhere (no bindings: a recorder stands in for ``htcondor2``); the
live leg needs the bindings and a personal HTCondor (the test-htcondor job), skipped without bindings."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from graphed.services import Launch, ServiceSpec
from m68c_harness import (
    HARNESS_FILE,
    StandIn,
    given_spec,
    join_v2,
    run_bounded,
    sequential,
    stage_index,
)

from graphed_executors.htcondor_backend import (
    HTCondorBackend,
    HTCondorRunner,
    htcondor_runner,
    submit_driverless,
)
from graphed_executors.htcondor_backend import launch as launch_mod

LIVE_S = 300.0
POOL_PROBE_S = 30.0


def _lambda_combine_plan(tag: str) -> Any:
    """A join plan whose fold stage holds a lambda ``combine``: pilots cannot import it."""
    return join_v2(tag, combine=lambda a, b: sorted(a + b))


# ---- the bindings recorder (m68b's, trimmed) ------------------------------------------------------


class _Result:
    def cluster(self) -> int:
        return 4242


class _Schedd:
    def __init__(self, log: list[tuple[Any, ...]]) -> None:
        self.log = log

    def submit(self, description: Any, count: int = 0, spool: bool = False, **kwargs: Any) -> _Result:
        self.log.append(("submit", dict(description), count, spool))
        return _Result()

    def spool(self, result: Any, *args: Any, **kwargs: Any) -> None:
        self.log.append(("spool",))

    def query(self, *args: Any, **kwargs: Any) -> list[Any]:
        self.log.append(("query",))
        return []

    def act(self, *args: Any, **kwargs: Any) -> None:
        self.log.append(("act",))


class _Collector:
    def __init__(self, log: list[tuple[Any, ...]]) -> None:
        self.log = log

    def locate(self, daemon_type: Any, name: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.log.append(("locate", name))
        return {"Name": name or "schedd.m68c.example", "MyAddress": "<127.0.0.1:9618>"}

    def query(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.log.append(("collector-query",))
        return [
            {
                "Name": "schedd.m68c.example",
                "RecentDaemonCoreDutyCycle": 0.1,
                "ShadowsRunning": 1,
                "MaxJobsRunning": 10,
                "TotalIdleJobs": 1,
            }
        ]


class _Submit(dict[str, Any]):
    def __str__(self) -> str:
        return "".join(f"{k} = {v}\n" for k, v in self.items()) + "queue\n"


class _SubmitType:
    def __init__(self, log: list[tuple[Any, ...]]) -> None:
        self.log = log

    def __call__(self, description: Any = None, *args: Any, **kwargs: Any) -> _Submit:
        return _Submit(dict(description or {}))

    def from_dag(self, filename: Any, options: Any = None, **kwargs: Any) -> _Submit:
        self.log.append(("from_dag", str(filename)))
        return _Submit({"dag_file": str(filename)})


class _FakeHTCondor:
    class DaemonType:
        Schedd = "Schedd"

    class AdType:
        Schedd = "Schedd"

    class JobAction:
        Remove = "Remove"

    def __init__(self) -> None:
        self.log: list[tuple[Any, ...]] = []
        self.Submit = _SubmitType(self.log)
        self.param = {
            "COLLECTOR_HOST": "cm.m68c.example:9618",
            "SCHEDD_HOST": "schedd.m68c.example",
            "FERMIHTC_REMOTE_POOL": "cm.m68c.example:9618",
            "FULL_HOSTNAME": "login.m68c.example",
        }

    def Collector(self, pool: str | None = None, *args: Any, **kwargs: Any) -> _Collector:
        self.log.append(("Collector", pool))
        return _Collector(self.log)

    def Schedd(self, location: Any = None, *args: Any, **kwargs: Any) -> _Schedd:
        self.log.append(("Schedd",))
        return _Schedd(self.log)


def _record_bindings(monkeypatch: pytest.MonkeyPatch) -> _FakeHTCondor:
    fake = _FakeHTCondor()

    def _htcondor() -> _FakeHTCondor:
        fake.log.append(("_htcondor",))
        return fake

    monkeypatch.setattr(launch_mod, "_htcondor", _htcondor)
    monkeypatch.setitem(sys.modules, "htcondor2", fake)
    return fake


class _NoStart:
    """A pilot launcher that starts nothing."""

    log_dir = None

    def start(self, url: str, secret: bytes, n: int) -> None:
        return None

    def alive(self) -> int:
        return 0

    def stop(self) -> None:
        return None


# ---- the stage-process pre-check ------------------------------------------------------------------


def test_the_htcondor_runner_refuses_a_stage_process_pilots_cannot_import() -> None:
    plan = _lambda_combine_plan("htc-refuse")
    index = stage_index(plan, "reduce")
    backend = run_bounded(lambda: HTCondorBackend(_NoStart(), 1, host="127.0.0.1", port_range=(0, 0)))
    keys: list[str] = []
    inner = backend.submit

    def counting(fn: Any, /, *args: Any, key: str, **kwargs: Any) -> Any:
        keys.append(key)
        return inner(fn, *args, key=key, **kwargs)

    backend.submit = counting
    runner = HTCondorRunner(backend, min_pilots=0)
    try:
        with pytest.raises(ValueError) as excinfo:
            run_bounded(lambda: runner.run(plan))
    finally:
        run_bounded(runner.close)
    assert f"stages[{index}].process" in str(excinfo.value), excinfo.value
    assert keys == [], keys


def test_submit_driverless_refuses_a_stage_process_pilots_cannot_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _lambda_combine_plan("dl-refuse")
    index = stage_index(plan, "reduce")
    fake = _record_bindings(monkeypatch)
    with pytest.raises(ValueError) as excinfo:
        run_bounded(lambda: submit_driverless(plan, request_memory_mb=1024))
    assert f"stages[{index}].process" in str(excinfo.value), excinfo.value
    assert fake.log == [], fake.log


def test_submit_driverless_derives_a_join_plan_s_service_nodes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launch = Launch(argv=("{python}", "-m", "http.server", "{port}"), resources={"gpus": 1})
    spec = ServiceSpec("sf", "http", check="http:/", ports=(40000, 40010), launch=launch, timeout_s=60.0)
    plan = join_v2("dl-dag", spec=spec)
    assert plan.services == (spec,)
    fake = _record_bindings(monkeypatch)
    handle = run_bounded(
        lambda: submit_driverless(
            plan, site="generic", n_pilots=2, request_memory_mb=1024, log_dir=tmp_path / "logs"
        )
    )
    assert handle.dag is True
    run = json.loads((Path(handle.log_dir) / "run.json").read_text())
    assert run["announce_only"] == {"sf": "svc0"}, run
    assert [e for e in fake.log if e[0] == "from_dag"], fake.log


# ---- live -----------------------------------------------------------------------------------------


def _require_pool() -> None:
    htcondor2: Any = pytest.importorskip(
        "htcondor2",
        reason="the htcondor bindings are not installed (Linux wheels only; the test-htcondor job)",
    )
    try:
        ads = run_bounded(
            lambda: htcondor2.Collector().query(htcondor2.AdType.Schedd, projection=["Name"]), POOL_PROBE_S
        )
    except Exception as exc:
        pytest.fail(f"htcondor2 is installed but no pool answers ({exc}): start a personal HTCondor")
    assert ads, "htcondor2 is installed but the collector lists no schedd: start a personal HTCondor"


def test_a_live_join_over_pool_pilots_calls_the_given_server(tmp_path: Path) -> None:
    _require_pool()
    plan = join_v2("htc-live", spec=given_spec())
    with StandIn() as server:
        runner = run_bounded(
            lambda: htcondor_runner(
                n_pilots=2,
                site="generic",
                log_dir=tmp_path / "pilots",
                user_modules=[HARNESS_FILE],
                min_pilots=2,
                services={"sf": server.endpoint()},
            ),
            LIVE_S,
        )
        try:
            got = run_bounded(lambda: runner.run(plan), LIVE_S)
        finally:
            run_bounded(runner.close, LIVE_S)
        calls = server.calls()
        ref = sequential(plan, {"sf": server.endpoint()})
    assert calls > 0, "no pilot task called the server"
    assert got.value == ref.value and got.value, (got.value, ref.value)
