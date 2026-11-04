"""VineGraphBackend's contract with vine_graph, over a stand-in ``Executor``."""

from __future__ import annotations

import gc
import pickle
import sys
from collections import Counter
from dataclasses import dataclass
from types import ModuleType
from typing import Any

import fake_vine_graph as fake
import pytest
from graphed.core import (
    DurablePlanV2,
    OpSpec,
    Partition,
    Plan,
    RunControl,
    SequentialRunner,
    StageSpec,
    Task,
)
from graphed.core.execution import TaskPhase
from graphed.debug import StageError
from vine_plan_fns import Recorder

from graphed_executors import vine_graph_runner
from graphed_executors.submit import RunContext, SubmitFuture, SubmitRunner, current_env
from graphed_executors.submit.engine import _RunTasks
from graphed_executors.submit.worker import (
    BufferedEnv,
    Emitted,
    _combine_task,
    _leaf_task,
    buffered_task,
    dispatch_events,
    run_buffered,
    subscribe,
)
from graphed_executors.vine_graph_backend import VineGraphBackend
from graphed_executors.vine_graph_backend._shim import _take_events, _vine_task_shim
from graphed_executors.vine_graph_backend.backend import _dispatch


@pytest.fixture(autouse=True)
def vine_graph_module(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("ndcctools", "ndcctools.taskvine"):
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    monkeypatch.setitem(sys.modules, "ndcctools.taskvine.vine_graph", fake)


def _send_nothing() -> Any:
    return Recorder()


class Pushing(Recorder):
    def worker_monitor_factory(self) -> Any:
        return _send_nothing


def entries(partition: Partition, resources: object) -> int:
    return partition.entry_stop - partition.entry_start


def add(a: int, b: int) -> int:
    return a + b


def zero() -> int:
    return 0


def raising(partition: Partition, resources: object) -> int:
    raise ValueError(f"bad entry {partition.entry_start}")


def lost_to_signal(partition: Partition, resources: object) -> int:
    raise fake.Lost("signal")


def lost_to_workers(partition: Partition, resources: object) -> int:
    raise fake.Lost("workers")


def lost_to_retries(partition: Partition, resources: object) -> int:
    raise fake.Lost("retries")


def lost_to_input(partition: Partition, resources: object) -> int:
    raise fake.Lost("input")


def tasks(n: int) -> tuple[Task, ...]:
    return tuple(Task(i, Partition("data", "Events", i, i + 2)) for i in range(n))


def plan(process: Any = entries, n: int = 4) -> Plan[int]:
    return Plan(process, add, zero, tasks(n))


def test_a_fixed_plan_matches_sequential_with_results_left_on_the_workers() -> None:
    executor = fake.Executor()
    with vine_graph_runner(executor) as runner:
        result = runner.run(plan())
    assert result.value == SequentialRunner().run(plan()).value == 8
    assert executor.loaded == [_combine_task]


def test_an_adaptive_plan_folds_every_leaf() -> None:
    batches = [tasks(3)]

    def next_tasks(_: object) -> Any:
        return batches.pop(0) if batches else None

    with vine_graph_runner(fake.Executor()) as runner:
        result = runner.run(Plan(entries, add, zero, next_tasks=next_tasks))
    assert result.value == 6


def stage(task: Task, inputs: Any, resources: object) -> bytes:
    return pickle.dumps(task.key + sum(pickle.loads(i) for i in inputs))


def test_a_staged_plan_reads_its_inputs_on_the_workers() -> None:
    def spec() -> OpSpec:
        return OpSpec(kind="ref", ref=f"{__name__}:stage", live=stage)

    staged = DurablePlanV2(
        ir=b"vine",
        stages=(
            StageSpec(kind="gather", process=spec(), tasks=tasks(2)),
            StageSpec(kind="gather", inputs=(0,), process=spec(), tasks=tasks(1)),
        ),
    )
    with vine_graph_runner(fake.Executor()) as runner:
        result = runner.run(staged)
    assert result.value == (1,)


def test_only_a_leaf_of_a_monitored_run_takes_an_events_task() -> None:
    executor = fake.Executor()
    monitor = Recorder()
    with vine_graph_runner(executor, monitor=monitor) as runner:
        result = runner.run(plan())
    assert result.value == 8
    assert monitor.phases() == {
        TaskPhase.SUBMITTED: 4,
        TaskPhase.STARTED: 4,
        TaskPhase.FINISHED: 4,
    }
    assert executor.submitted.count(_leaf_task) == 4
    assert executor.submitted.count(_combine_task) == 3


def test_a_monitored_run_of_one_leaf_returns_the_plain_value() -> None:
    with vine_graph_runner(fake.Executor(), monitor=Recorder()) as runner:
        assert runner.run(plan(n=1)).value == 2


def test_a_monitored_run_loads_no_leaf_result_on_the_driver() -> None:
    executor = fake.Executor()
    with vine_graph_runner(executor, monitor=Recorder()) as runner:
        runner.run(plan())
    assert Counter(executor.loaded) == {_take_events: 4, _combine_task: 1}


def test_a_per_worker_monitor_gets_its_events_on_the_driver() -> None:
    executor = fake.Executor()
    monitor = Pushing()
    with vine_graph_runner(executor, monitor=monitor) as runner:
        runner.run(plan())
    assert monitor.phases()[TaskPhase.FINISHED] == 4
    assert executor.loaded.count(_take_events) == 4


def test_events_that_land_after_the_run_unsubscribed_reach_its_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = VineGraphBackend(fake.Executor())
    seen: list[Any] = []
    unsubscribe = backend.subscribe_events("t", seen.extend)
    late: list[Any] = []
    monkeypatch.setattr(fake.Future, "add_done_callback", lambda self, fn: late.append((fn, self)))
    ctx = RunContext("run", "t", None)
    leaf = backend.submit(_leaf_task, ctx, pickle.dumps(raising), "late", tasks(1)[0], key="leaf")
    backend.take_events(leaf)
    unsubscribe()
    for fn, future in late:
        fn(future)
    assert [event["phase"] for event in seen] == ["started", "errored"]


def test_a_service_checked_leaf_reports_its_events_and_a_plain_task_takes_no_events_task() -> None:
    executor = fake.Executor()
    backend = VineGraphBackend(executor)
    seen: list[Any] = []
    backend.subscribe_events("t", seen.extend)
    submits = _RunTasks(backend, (("svc", "tcp://localhost:1", "tcp"),))
    submits.submit(entries, tasks(1)[0].partition, None, key="plain", retries=0)
    assert _take_events not in executor.submitted
    ctx = RunContext("run", "t", None)
    payload = pickle.dumps(entries)
    submits.submit(_leaf_task, ctx, payload, "tok", tasks(1)[0], key="leaf", retries=0)
    assert [event["phase"] for event in seen] == ["started", "finished"]


def test_a_run_without_a_monitor_takes_no_events_task() -> None:
    executor = fake.Executor()
    backend = VineGraphBackend(executor)
    backend.take_events(backend.submit(int, key="k"))
    assert _take_events not in executor.submitted


def test_a_function_exception_reaches_the_caller_with_one_errored_event() -> None:
    monitor = Recorder()
    with (
        vine_graph_runner(fake.Executor(), monitor=monitor) as runner,
        pytest.raises(ValueError, match="bad entry 0"),
    ):
        runner.run(plan(raising))
    assert monitor.phases()[TaskPhase.ERRORED] == 4


def test_result_and_exception_unwrap_alike() -> None:
    executor = fake.Executor()
    backend = VineGraphBackend(executor)

    def fails() -> None:
        raise KeyError("x")

    failed = backend.submit(fails, key="k")
    assert isinstance(failed.exception(), KeyError)
    with pytest.raises(KeyError):
        failed.result()
    good = backend.submit(int, key="ok")
    assert good.exception() is None
    assert good.result() == 0


def test_an_exception_that_refuses_the_attribute_is_raised_all_the_same() -> None:
    @dataclass(frozen=True)
    class FrozenError(Exception):
        code: int

    def emits_and_raises() -> None:
        current_env().emit("t", [{"n": 1}])
        raise FrozenError(3)

    with pytest.raises(FrozenError):
        _vine_task_shim(emits_and_raises)


def test_a_raising_call_carries_its_events_on_the_exception() -> None:
    def emits_and_raises() -> None:
        current_env().emit("t", [{"n": 1}])
        raise ValueError("x")

    with pytest.raises(ValueError, match="x") as raised:
        _vine_task_shim(emits_and_raises)
    assert raised.value.graphed_events == [("t", [{"n": 1}])]  # type: ignore[attr-defined]


@pytest.mark.parametrize("process", [lost_to_signal, lost_to_workers, lost_to_retries])
def test_a_lost_task_names_its_partition(process: Any) -> None:
    with (
        vine_graph_runner(fake.Executor()) as runner,
        pytest.raises(StageError) as raised,
    ):
        runner.run(plan(process))
    assert raised.value.partition == "data:Events:0-2"
    assert process.__name__.removeprefix("lost_to_") in raised.value.cause_message
    assert raised.value.cause_type == "KilledWorker"


def test_a_failure_the_function_or_its_input_explains_is_raised_as_it_is() -> None:
    with (
        vine_graph_runner(fake.Executor()) as runner,
        pytest.raises(fake.TaskError, match="input"),
    ):
        runner.run(plan(lost_to_input))


def test_a_task_nothing_holds_is_left_unattributed() -> None:
    backend = VineGraphBackend(fake.Executor())
    error = fake.TaskError("m", None, None, "signal", None)
    assert backend.describe_failure(error) is None


def test_the_backend_forgets_a_task_with_its_future() -> None:
    backend = VineGraphBackend(fake.Executor())
    future = backend.submit(int, key="k")
    node = future._future.target
    error = fake.TaskError("m", node, None, "signal", None)
    assert backend.describe_failure(error) == ("k", "m")
    del future
    gc.collect()
    assert backend.describe_failure(error) is None


def test_a_failure_whose_source_is_forgotten_is_attributed_to_its_task() -> None:
    backend = VineGraphBackend(fake.Executor())
    source = backend.submit(int, key="source")
    gone = source._future.target
    dependent = backend.submit(int, key="dependent")
    del source
    gc.collect()
    error = fake.TaskError("m", dependent._future.target, gone, "signal", None)
    assert backend.describe_failure(error) == ("dependent", "m")


def test_a_dependent_read_first_is_attributed_to_its_source() -> None:
    backend = VineGraphBackend(fake.Executor())
    source = backend.submit(lost_to_signal, tasks(1)[0].partition, None, key="leaf")
    dependent = backend.submit(add, source, source, key="combine")
    error = dependent.exception()
    assert isinstance(error, fake.TaskError)
    assert error.task != error.source
    assert backend.describe_failure(error) == ("leaf", str(error))


def test_an_exception_of_another_kind_is_left_alone() -> None:
    backend = VineGraphBackend(fake.Executor())
    assert backend.describe_failure(ValueError("x")) is None


def test_vine_graph_runner_takes_a_control() -> None:
    control = RunControl()
    with vine_graph_runner(fake.Executor(), control=control) as runner:
        assert runner.control is control


def test_vine_graph_runner_has_no_retries_to_ignore() -> None:
    with pytest.raises(TypeError, match="retries"):
        vine_graph_runner(fake.Executor(), retries=3)  # type: ignore[call-arg]
    with vine_graph_runner(fake.Executor()) as runner:
        assert isinstance(runner, SubmitRunner)
        assert runner._retries == 0


def test_a_worker_is_named_for_its_library_process(monkeypatch: pytest.MonkeyPatch) -> None:
    def worker() -> str:
        return current_env().worker

    # Forks of one library differ in pid and share a parent.
    monkeypatch.setattr("os.getppid", lambda: 4242)
    monkeypatch.setattr("os.getpid", lambda: 1)
    first = _vine_task_shim(worker).value
    monkeypatch.setattr("os.getpid", lambda: 2)
    assert _vine_task_shim(worker).value == first
    assert str(first).endswith(":4242")
    monkeypatch.setattr("os.getppid", lambda: 4343)
    assert _vine_task_shim(worker).value != first


def test_capacity_reads_the_executors() -> None:
    backend = VineGraphBackend(fake.Executor(workers=3, cores=24))
    assert backend.n_workers() == 3
    assert backend.task_slots() == 24


def test_the_backend_adapts_the_submit_seam() -> None:
    executor = fake.Executor()
    backend = VineGraphBackend(executor)
    assert backend.broadcast(b"payload", token="t") == b"payload"
    future = backend.submit(int, key="k")
    assert isinstance(future, SubmitFuture)
    assert future.done()
    called: list[object] = []
    future.add_done_callback(called.append)
    assert called == [future]
    assert backend.submit(add, future, future, key="dep").result() == 0
    backend.cancel([future])
    assert future._future.cancelled
    backend.close()
    assert not executor.closed


def test_a_closed_executor_ends_dispatch_quietly() -> None:
    executor = fake.Executor()
    backend = VineGraphBackend(executor)
    seen: list[Any] = []
    backend.subscribe_events("t", seen.extend)
    future = backend.submit(int, key="k")
    executor.closed = True
    _dispatch(backend._handlers, future._future)
    assert seen == []


def test_take_events_reads_an_envelope() -> None:
    assert _take_events(Emitted(1, [("t", [])])) == [("t", [])]


def test_emitted_pickles_on_every_protocol() -> None:
    envelope = Emitted({"value": 1}, [("t", [{"n": 1}])])
    for protocol in range(2, pickle.HIGHEST_PROTOCOL + 1):
        assert pickle.loads(pickle.dumps(envelope, protocol)) == envelope


def test_run_buffered_returns_the_value_with_its_events() -> None:
    def noisy(a: int) -> int:
        current_env().emit("t", [{"n": a}])
        return a + 1

    assert run_buffered(BufferedEnv("w"), noisy, (1,)) == Emitted(2, [("t", [{"n": 1}])])


def test_the_vine_shim_unwraps_a_dependency() -> None:
    assert _vine_task_shim(int, Emitted("3", [])) == Emitted(3, [])


def test_buffered_env_keeps_one_resources_per_process() -> None:
    assert BufferedEnv("a").resources is BufferedEnv("b").resources


def test_buffered_task_names_host_and_process() -> None:
    def worker() -> str:
        return current_env().worker

    host, _, pid = str(buffered_task(worker).value).rpartition(":")
    assert host
    assert pid.isdigit()


def test_dispatch_reaches_only_subscribed_topics() -> None:
    seen: list[Any] = []
    handlers: dict[str, Any] = {}
    unsub = subscribe(handlers, "t", seen.extend)
    dispatch_events([("t", [{"n": 1}]), ("other", [{"n": 2}])], handlers)
    assert seen == [{"n": 1}]
    unsub()
    unsub()
    assert handlers == {}
