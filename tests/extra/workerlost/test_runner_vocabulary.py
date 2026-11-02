"""Swapping runners changes the class name, not the keywords."""

from __future__ import annotations

import inspect
import operator
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from graphed.core import Partition, Plan, RunControl, StopReason, Task

from graphed_executors import PinnedPoolExecutor, ProcessPoolExecutor
from graphed_executors import ThreadExecutor as Threads
from graphed_executors.common import renamed
from graphed_executors.dask_backend.backend import dask_runner
from graphed_executors.htcondor_backend import backend as condor
from graphed_executors.htcondor_backend.driverless import submit_driverless
from graphed_executors.parsl_backend.backend import parsl_runner
from graphed_executors.submit import SubmitRunner, ThreadBackend

LOCAL = [Threads, ProcessPoolExecutor, PinnedPoolExecutor]
TASKS = tuple(Task(i, Partition("f", "t", i, i + 1)) for i in range(4))


def one(part: Partition, res: object) -> int:
    return 1


PLAN: Plan[int] = Plan(process=one, combine=operator.add, empty=int, tasks=TASKS)


@pytest.mark.parametrize("runner", LOCAL)
def test_one_set_of_keywords_runs_every_local_runner(runner: Any) -> None:
    with runner(workers=2, retries=1, control=RunControl()) as ex:
        assert ex.run(PLAN).value == 4
        assert ex.max_workers == 2


@pytest.mark.parametrize("runner", LOCAL)
def test_the_old_spelling_warns_where_it_is_written(runner: Any) -> None:
    with pytest.warns(DeprecationWarning, match="max_workers= is deprecated") as caught:
        ex = runner(max_workers=3)
    assert ex.max_workers == 3
    assert caught[0].filename == __file__
    ex.close()


@pytest.mark.parametrize("runner", LOCAL)
def test_both_spellings_are_refused(runner: Any) -> None:
    with pytest.raises(TypeError, match="not both"):
        runner(workers=2, max_workers=3)


def test_renamed_keeps_a_zero_and_fills_only_an_absent_count() -> None:
    assert renamed(0, None, "n_pilots", 2) == 0
    assert renamed(None, None, "n_pilots", 2) == 2


@pytest.fixture(params=["dask", "parsl"])
def cluster_runner(request: pytest.FixtureRequest) -> Iterator[Callable[[RunControl], Any]]:
    if request.param == "dask":
        distributed = pytest.importorskip("distributed")
        with (
            distributed.LocalCluster(n_workers=1, processes=False, dashboard_address=":0") as c,
            distributed.Client(c) as client,
        ):
            yield lambda control: dask_runner(client, control=control)
    else:
        threads = pytest.importorskip("parsl.executors").ThreadPoolExecutor
        tpe = threads(label=f"vocab-{uuid.uuid4().hex[:8]}", max_threads=2)
        tpe.start()
        yield lambda control: parsl_runner(tpe, control=control)
        tpe.shutdown()


def test_control_at_construction_pauses_a_cluster_runner(
    cluster_runner: Callable[[RunControl], Any],
) -> None:
    control = RunControl()
    control.pause()
    with cluster_runner(control) as runner, ThreadPoolExecutor(1) as pool:
        run = pool.submit(runner.run, PLAN)
        with pytest.raises(TimeoutError):
            run.result(1)
        control.resume()
        assert run.result(60).value == 4


def test_control_at_construction_cancels_a_cluster_runner(
    cluster_runner: Callable[[RunControl], Any],
) -> None:
    control = RunControl()
    control.cancel()
    with cluster_runner(control) as runner:
        assert runner.run(PLAN).stopped is StopReason.CANCELLED


def test_dashboard_attach_replaces_a_control_given_at_construction() -> None:
    board = pytest.importorskip("graphed.debug.dashboard").Dashboard
    with (
        board(control=True) as dashboard,
        SubmitRunner(ThreadBackend(1), control=RunControl()) as r,
    ):
        assert dashboard.attach(r).control is dashboard.control


def _defaults(fn: Any) -> dict[str, Any]:
    return {k: p.default for k, p in inspect.signature(fn).parameters.items()}


def test_the_condor_entry_points_agree_on_their_defaults() -> None:
    runner, driverless = _defaults(condor.htcondor_runner), _defaults(submit_driverless)
    for name in ("workers", "n_pilots", "request_memory_mb", "retries"):
        assert runner[name] == driverless[name] is not inspect.Parameter.empty
    assert "control" in runner


def test_htcondor_runner_wires_workers_and_control(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(condor, "CondorPilots", lambda *_, **__: None)
    monkeypatch.setattr(condor, "HTCondorBackend", lambda _, n, **__: n)
    monkeypatch.setattr(
        condor, "HTCondorRunner", lambda n, **kw: (n, kw["control"], kw["retry_user_errors"])
    )
    control = RunControl()
    build: Any = condor.htcondor_runner
    assert build(workers=5, control=control, retry_user_errors=1) == (5, control, 1)
    assert build() == (condor.DEFAULT_PILOTS, None, 0)
    with pytest.warns(DeprecationWarning, match="n_pilots= is deprecated"):
        assert build(n_pilots=7)[0] == 7


def test_submit_driverless_refuses_both_spellings() -> None:
    with pytest.raises(TypeError, match="not both"):
        submit_driverless(PLAN, workers=2, n_pilots=3)
