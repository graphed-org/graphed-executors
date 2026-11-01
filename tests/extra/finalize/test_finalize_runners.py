"""Every runner applies ``Plan.finalize`` once, on the driver, to the value it returns."""

from __future__ import annotations

import os
import pickle
import sys
import threading
from collections.abc import Callable, Iterator, Sequence
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from graphed.core import (
    DurablePlanV2,
    ExecContext,
    ExecResult,
    OpSpec,
    Partition,
    Plan,
    RunControl,
    StageSpec,
    StopReason,
    Task,
)

from graphed_executors.local import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor
from graphed_executors.submit import SubmitRunner, ThreadBackend

CALLS: list[tuple[int, int, Any]] = []
N = 6
TOTAL = N * (N + 1) // 2


def count_entries(part: Partition, _resources: object) -> int:
    return part.n_entries


def add(a: int, b: int) -> int:
    return a + b


def zero() -> int:
    return 0


def finalize(value: Any) -> Any:
    CALLS.append((os.getpid(), threading.get_ident(), value))
    return ("final", value)


def _tasks(n: int = N) -> tuple[Task, ...]:
    return tuple(Task(i, Partition("mem://finalize", "t", 0, i + 1)) for i in range(n))


def _plan(n: int = N, **kw: Any) -> Plan[Any]:
    return Plan(
        process=count_entries,
        combine=add,
        empty=zero,
        tasks=_tasks(n),
        **{"finalize": finalize, **kw},
    )


def _adaptive() -> Plan[Any]:
    batches = iter([_tasks()])

    def next_tasks(_ctx: ExecContext) -> Sequence[Task] | None:
        return next(batches, None)

    return _plan(0, next_tasks=next_tasks)


@pytest.fixture(autouse=True)
def _clear() -> None:
    CALLS.clear()


def _on_driver(run: Callable[[], Any], expected: Any) -> Any:
    """Run ``run`` here and check finalize ran once, in this thread, on ``expected``."""
    res = run()
    assert [(os.getpid(), threading.get_ident(), expected)] == CALLS
    assert res.value == ("final", expected)
    return res


LOCAL: dict[str, Callable[[], Any]] = {
    "thread-peer": lambda: ThreadExecutor(max_workers=2),
    "thread-hub": lambda: ThreadExecutor(max_workers=2, comms=None),
    "thread-pooled": lambda: ThreadExecutor(max_workers=2, comms=None, pooled_combines=True),
    "thread-windowed": lambda: ThreadExecutor(max_workers=2, comms=None, control=RunControl()),
    "process-peer": lambda: ProcessPoolExecutor(max_workers=2),
    "process-hub": lambda: ProcessPoolExecutor(max_workers=2, comms=None),
    "pinned-peer": lambda: PinnedPoolExecutor(max_workers=2),
    "submit-thread": lambda: SubmitRunner(ThreadBackend(2)),
    "submit-thread-windowed": lambda: SubmitRunner(ThreadBackend(2), control=RunControl()),
}


@pytest.mark.parametrize("make", LOCAL.values(), ids=LOCAL.keys())
def test_local_runners_finalize_once_on_the_driver(make: Callable[[], Any]) -> None:
    with make() as runner:
        _on_driver(lambda: runner.run(_plan()), TOTAL)
        CALLS.clear()
        _on_driver(lambda: runner.run(_plan(0)), 0)


@pytest.mark.parametrize(
    "make",
    [
        lambda: ThreadExecutor(max_workers=2),
        lambda: SubmitRunner(ThreadBackend(2)),
        lambda: SubmitRunner(ThreadBackend(2), control=RunControl()),
    ],
    ids=["thread", "submit", "submit-windowed"],
)
def test_adaptive_runs_finalize_once_on_the_driver(make: Callable[[], Any]) -> None:
    with make() as runner:
        _on_driver(lambda: runner.run(_adaptive()), TOTAL)


def _submit_thread(**kw: Any) -> SubmitRunner:
    return SubmitRunner(ThreadBackend(2), **kw)


@pytest.mark.parametrize("make", [ThreadExecutor, _submit_thread])
def test_a_cancelled_run_finalizes_its_empty_value(make: Callable[..., Any]) -> None:
    control = RunControl()
    control.cancel()
    with make(control=control) as runner:
        assert _on_driver(lambda: runner.run(_plan()), 0).stopped is StopReason.CANCELLED


def test_a_plan_without_finalize_returns_the_combined_value() -> None:
    with ThreadExecutor(max_workers=2) as runner:
        assert runner.run(_plan(finalize=None)).value == TOTAL
    assert CALLS == []


# ---- DurablePlanV2 on SubmitRunner ----------------------------------------------------------


def _map_stage(task: Task, _inputs: Sequence[bytes], _r: Any) -> bytes:
    return pickle.dumps(task.partition.n_entries)


def _reduce_stage(_task: Task, inputs: Sequence[bytes], _r: Any) -> bytes:
    return pickle.dumps(sum(pickle.loads(b) for b in inputs))


def _staged() -> DurablePlanV2:
    return DurablePlanV2(
        ir=b"finalize-v2",
        stages=(
            StageSpec("map", process=OpSpec("ref", "t:map", live=_map_stage), tasks=_tasks()),
            StageSpec(
                "reduce",
                inputs=(0,),
                process=OpSpec("ref", "t:reduce", live=_reduce_stage),
                tasks=(Task(0, Partition("mem://finalize", "t", 0, 0)),),
            ),
        ),
        finalize=OpSpec("ref", "t:finalize", live=finalize),
    )


def test_submit_runner_finalizes_a_staged_value_and_not_a_cancelled_one() -> None:
    with SubmitRunner(ThreadBackend(2)) as runner:
        _on_driver(lambda: runner.run(_staged()), TOTAL)
    CALLS.clear()
    control = RunControl()
    control.cancel()
    with SubmitRunner(ThreadBackend(2), control=control) as runner:
        assert runner.run(_staged()) == ExecResult(None, 0, 0, StopReason.CANCELLED)
    assert CALLS == []


# ---- dask -----------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dask_client() -> Iterator[Any]:
    distributed = pytest.importorskip("distributed")
    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=True, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        yield client


def test_dask_runners_finalize_once_on_the_driver(dask_client: Any) -> None:
    from graphed_executors.dask_backend import DaskBackend, dask_runner  # noqa: PLC0415
    from graphed_executors.dask_backend.transport import GraphedTransportPlugin  # noqa: PLC0415
    from graphed_executors.dask_backend.transport_peer import transport_run_plan  # noqa: PLC0415

    with dask_runner(dask_client) as runner:
        _on_driver(lambda: runner.run(_plan()), TOTAL)
    dask_client.register_plugin(GraphedTransportPlugin())
    for n, expected in ((N, TOTAL), (0, 0)):
        CALLS.clear()
        backend = DaskBackend(dask_client)
        _on_driver(partial(transport_run_plan, _plan(n), backend), expected)


# ---- parsl ----------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def parsl_tpe() -> Iterator[Any]:
    pytest.importorskip("parsl")
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m47"))
    from parsl_transport_harness import p2p_tpe_pool  # noqa: PLC0415

    with p2p_tpe_pool(max_threads=2) as tpe:
        yield tpe


def test_parsl_runners_finalize_once_on_the_driver(parsl_tpe: Any) -> None:
    from graphed_executors.parsl_backend import ParslBackend, parsl_runner  # noqa: PLC0415
    from graphed_executors.parsl_backend.transport_peer import parsl_run_plan  # noqa: PLC0415

    with parsl_runner(parsl_tpe) as runner:
        _on_driver(lambda: runner.run(_plan()), TOTAL)
    for n, expected in ((N, TOTAL), (0, 0)):
        CALLS.clear()
        backend = ParslBackend(parsl_tpe)
        _on_driver(partial(parsl_run_plan, _plan(n), backend, workers=2), expected)
