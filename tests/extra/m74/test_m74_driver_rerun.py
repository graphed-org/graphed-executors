"""A driver's in-job rerun after a SERVICE node re-announces runs the plan ``resumable`` wrapped, so
with a store the tasks the first run finished are served, not run again."""

from __future__ import annotations

import pickle
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m68d"))
from m68d_harness import (
    Background,
    GatedGet,
    Web,
    announce_node,
    driver_api,
    free_range,
    gated_plan,
    log_lines,
    node_spec,
    run_json,
    site_copy,
    wait_for,
    write_driver_job,
    write_machine_ad,
)


@pytest.mark.parametrize("store", [True, False])
def test_a_rerun_reuses_what_the_first_run_stored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store: bool
) -> None:
    # its result is a status code, self-contained; the driver unpickles the plan into this module
    monkeypatch.setattr(GatedGet, "checkpointable", True, raising=False)
    monkeypatch.setenv("_CONDOR_MACHINE_AD", str(write_machine_ad(tmp_path / "driver.ad", "127.0.0.1")))
    site_copy(monkeypatch, "m-dee-node", "generic", worker_ports=free_range(3))
    mark, job, dag = tmp_path / "mark", tmp_path / "job", tmp_path / "dag"
    dag.mkdir()
    extra = {"store": str(tmp_path / "store"), "storage_options": {}} if store else {}
    write_driver_job(
        job, gated_plan(mark, node_spec(30.0), n=2), run_json("m-dee-node", dag_dir=dag, **extra)
    )
    with Web() as second:
        first = Web()
        driver = Background(lambda: driver_api().main([str(job)]))
        assert announce_node(dag, first.port) == 200
        assert wait_for((mark / "t0.done").exists, 120.0), (job / "driver.log").read_text()[-4000:]
        first.close()
        assert announce_node(dag, second.port) == 200
        (mark / "t0.start").unlink()
        (mark / "killed").touch()
        code = driver.result()
    ok, value = pickle.loads((job / "result.pkl").read_bytes())
    assert (code, ok) == (0, True), (code, value, (job / "driver.log").read_text()[-4000:])
    assert len(log_lines(job, "rerun:")) == 1, log_lines(job, "rerun:")
    assert (mark / "t0.start").exists() is not store
    reused = [line.split(" INFO ")[-1].split(" from ")[0] for line in log_lines(job, "tasks reused from")]
    assert reused == (["0 of 2 tasks reused", "1 of 2 tasks reused"] if store else []), reused
