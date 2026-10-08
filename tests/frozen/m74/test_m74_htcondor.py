"""m74 §4.2 E6 and E8: HTCondor driverless runs resume from a store on every try.

E6 runs the driver entry from a job dir laid out by a recorded ``submit_driverless(store=...)`` with two
local pilots, kills its session after ``MIN_DONE`` stored tasks and reruns it as the job's retry would;
``driver.log`` reports the reused count. E8 pins the submit side (``run.json`` fields, the submit-time
``check_resumable`` refusal of a histserv-backed plan), a driver whose store records another
environment (exit 3, ``EnvironmentChanged`` in ``result.pkl``, no pilot), and ``_exit_code`` of a
``StoreUnavailable``."""

from __future__ import annotations

import importlib
import json
import pickle
import sys
import uuid
from pathlib import Path

import pytest
from graphed.checkpoint import EnvironmentChanged, StoreUnavailable
from m74_harness import (
    HARNESS_FILE,
    MIN_DONE,
    START_TIMEOUT_S,
    RecordingSchedd,
    T,
    done,
    driver_api,
    fake_dist,
    files,
    finish,
    histogram_plan,
    htcondor_api,
    kill_session,
    last_driver_section,
    leaf_plan,
    prepared_job,
    read_result,
    record_bindings,
    record_environment_with,
    reused_line,
    start_job_driver,
    submits,
    wait_for,
    work_dirs,
)

LOCAL = {"site": "generic", "n_pilots": 2, "pilots": "local", "min_pilots": 2}
posix = pytest.mark.skipif(sys.platform == "win32", reason="kills a POSIX session")


def run_job(job: Path) -> int:
    proc = start_job_driver(job)
    code = finish(proc)
    kill_session(proc)
    return code


@posix
def test_e6_a_killed_driverless_driver_resumes_on_its_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = tmp_path / "store"
    markers, _ = work_dirs(tmp_path / "work")
    job = prepared_job(tmp_path, monkeypatch, leaf_plan(markers), "job", store=str(store), **LOCAL)
    proc = start_job_driver(job)
    try:
        wait_for(lambda: proc.poll() is not None or done(store) >= MIN_DONE, START_TIMEOUT_S)
    finally:
        kill_session(proc)
    left = done(store)
    assert 0 < left < T, (left, (job / "driver.log").read_text()[-4000:])
    before = files(markers)
    code = run_job(job)
    assert code == 0, (job / "driver.log").read_text()[-4000:]
    executions = len(files(markers) - before)
    ok, result = read_result(job)
    assert ok is True, result

    ref_markers, _ = work_dirs(tmp_path / "ref-work")
    ref_job = prepared_job(
        tmp_path, monkeypatch, leaf_plan(ref_markers), "ref-job", store=str(tmp_path / "ref-store"), **LOCAL
    )
    assert run_job(ref_job) == 0, (ref_job / "driver.log").read_text()[-4000:]
    ref_ok, reference = read_result(ref_job)
    assert ref_ok is True, reference

    assert executions == T - left, f"the retry executed {executions} tasks; {T} - {left} were not done"
    assert pickle.dumps(result.value) == pickle.dumps(reference.value), (result.value, reference.value)
    assert reused_line(last_driver_section(job / "driver.log")) == left


def test_e8_submit_driverless_writes_the_store_fields_into_run_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    markers, _ = work_dirs(tmp_path / "work")
    store = str(tmp_path / "store")
    options = {"auto_mkdir": True}
    job = prepared_job(
        tmp_path,
        monkeypatch,
        leaf_plan(markers),
        "job",
        store=store,
        storage_options=options,
        salt="m74-salt",
        accept_environment=True,
        **LOCAL,
    )
    run = json.loads((job / "run.json").read_text())
    assert run["store"] == store, run
    assert run["storage_options"] == options, run
    assert run["salt"] == "m74-salt", run
    assert run["accept_environment"] is True, run


def test_e8_a_driver_whose_store_records_another_environment_exits_3_without_a_pilot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = tmp_path / "store"
    salt = "m74-env"
    markers, _ = work_dirs(tmp_path / "work")
    job = prepared_job(tmp_path, monkeypatch, leaf_plan(markers), "job", store=str(store), salt=salt, **LOCAL)
    record_environment_with(fake_dist(tmp_path / "zz", "1.0"), store, salt, tmp_path / "other")
    code = finish(start_job_driver(job))
    section = last_driver_section(job / "driver.log")
    assert code == 3, section
    ok, err = read_result(job)
    assert ok is False and isinstance(err, EnvironmentChanged), (ok, err)
    assert "zzfake" in str(err), err
    assert "pilots on" not in section, section
    assert not files(markers), sorted(files(markers))


def test_e8_a_histserv_backed_plan_with_a_store_is_refused_before_any_bindings_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    histserv = importlib.import_module("graphed_histogram.histserv")
    context = histserv.Context(memory_mb=1 << 20, workers=2, name=f"m74-{uuid.uuid4().hex[:8]}")
    plan = histogram_plan(context)
    assert plan.services, "fixture: the backed plan carries its servers"
    fake = record_bindings(monkeypatch, RecordingSchedd())
    submit = htcondor_api().submit_driverless
    common = {"request_memory_mb": 2048, "user_modules": [HARNESS_FILE]}
    with pytest.raises(TypeError) as excinfo:
        submit(plan, log_dir=tmp_path / "refused", store=str(tmp_path / "store"), **common)
    assert "checkpointable" in str(excinfo.value), excinfo.value
    assert fake.log == [], fake.log
    submit(plan, log_dir=tmp_path / "accepted", **common)
    assert len(submits(fake.log)) == 1, fake.log


def test_e8_an_unbacked_gh_plan_with_a_store_is_accepted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = record_bindings(monkeypatch, RecordingSchedd())
    store = str(tmp_path / "store")
    log_dir = tmp_path / "submit"
    htcondor_api().submit_driverless(
        histogram_plan(), log_dir=log_dir, request_memory_mb=2048, user_modules=[HARNESS_FILE], store=store
    )
    assert len(submits(fake.log)) == 1, fake.log
    assert json.loads((log_dir / "run.json").read_text())["store"] == store


def test_e8_a_store_unavailable_that_crossed_pickle_exits_1() -> None:
    err = pickle.loads(pickle.dumps(StoreUnavailable("store /nowhere: OSError: unreachable")))
    assert type(err) is StoreUnavailable
    assert driver_api()._exit_code(err) == 1
