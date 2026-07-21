"""Shared harness for the m42 frozen suite: `SubmitBackend` protocol + dask Plan-path executor
(dask-parallel-backends-plan §1.1-§1.5, §2 m42, §3).

Two disciplines shape everything here (plan §3 fixture discipline):

1. **Spawn safety.** Every function or class a worker process may unpickle lives at MODULE LEVEL
   in this file. dask nanny workers inherit the driver's ``sys.path`` (multiprocessing spawn
   preparation data), so this module is importable on them by reference — verified against
   distributed 2026.7 before authoring.
2. **Deferred implementation imports.** The implementation under test
   (``graphed_executors.submit`` / ``graphed_executors.dask_backend``) is imported ONLY through
   the ``*_api``/``make_*`` accessors below, inside test bodies. Pre-implementation the whole
   suite therefore COLLECTS cleanly and every test FAILS in its body with the right-reason
   ``ModuleNotFoundError`` — the TEST_SANITY non-vacuity gate.

Provenance-carrying partials (``Prov``/``FoldPart``/``AggPart``) witness the MECHANISM (where
leaves/combines ran, where a combine's inputs were materialized, per-invocation re-execution
marks), never a clock (R0.10a).
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from graphed.core import PayloadDescriptor
from graphed.core.execution import ExecContext, Partition, Plan, Task
from graphed.debug import SourceFrame, StageError

# ---- deferred accessors for the implementation under test ---------------------------------------


def submit_api() -> Any:
    """``graphed_executors.submit`` — absent pre-implementation (right-reason failure lands here)."""
    import graphed_executors.submit as mod  # noqa: PLC0415  (deferred: the module under test)

    return mod


def dask_api() -> Any:
    """``graphed_executors.dask_backend`` — absent pre-implementation."""
    import graphed_executors.dask_backend as mod  # noqa: PLC0415  (deferred: the module under test)

    return mod


def make_thread_backend(max_workers: int = 2) -> Any:
    """The conformance second backend (plan §1.2.5). Ctor pin: ``ThreadBackend(max_workers=N)``."""
    return submit_api().ThreadBackend(max_workers=max_workers)


def make_runner(backend: Any, *, monitor: Any = None, retries: int = 3) -> Any:
    """``SubmitRunner(backend, *, monitor=None, retries=3)`` (plan §1.2 pinned signature)."""
    return submit_api().SubmitRunner(backend, monitor=monitor, retries=retries)


def make_dask_backend(client: Any, **kwargs: Any) -> Any:
    """``DaskBackend(client, *, replicate_broadcast=False, default_retries=3)`` (plan §1.3.2)."""
    return dask_api().DaskBackend(client, **kwargs)


def make_dask_runner(client: Any, *, monitor: Any = None, retries: int = 3, **kwargs: Any) -> Any:
    """``dask_runner(client, *, monitor=None, retries=3, replicate_broadcast=False)`` (plan §1.3.2)."""
    return dask_api().dask_runner(client, monitor=monitor, retries=retries, **kwargs)


def register_worker_plugin(client: Any) -> None:
    """Register the §1.3.3 ``GraphedWorkerPlugin`` (name ``graphed-worker``, ``idempotent=True``).

    ``dask_runner`` registers it itself; tests that drive ``SubmitRunner`` over a (Spy-wrapped)
    ``DaskBackend`` directly call this first — re-registration must be a no-op (idempotent pin)."""
    from graphed_executors.dask_backend.plugin import GraphedWorkerPlugin  # noqa: PLC0415  (deferred)

    client.register_plugin(GraphedWorkerPlugin())


# ---- LocalCluster fixture discipline (plan §3: tier A processes=False, tier B processes=True) ----


@contextmanager
def cluster_client(
    n_workers: int = 2, *, processes: bool = True, allowed_failures: int | None = None
) -> Iterator[Any]:
    """A context-managed LocalCluster + Client: 1 thread per worker (tier B pin), random dashboard
    port (anti-EADDRINUSE). ``allowed_failures`` sets ``distributed.scheduler.allowed-failures``
    via ``dask.config`` BEFORE the scheduler is built (worker-death tests configure it explicitly)."""
    import dask  # noqa: PLC0415  (dask-touching modules call this after their importorskip)
    import distributed  # noqa: PLC0415

    overrides: dict[str, object] = {}
    if allowed_failures is not None:
        overrides["distributed.scheduler.allowed-failures"] = allowed_failures
    with (
        dask.config.set(overrides), distributed.LocalCluster(
            n_workers=n_workers,
            threads_per_worker=1,
            processes=processes,
            dashboard_address=":0",
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        yield client


def worker_pids(client: Any) -> dict[str, int]:
    """address -> pid for every live worker (the driver-vs-worker discrimination map)."""
    return dict(client.run(os.getpid))


def worker_addresses(client: Any) -> set[str]:
    return set(client.scheduler_info()["workers"])


def where() -> tuple[int, str]:
    """(pid, worker identity) at the point of call: the dask worker address when inside a dask
    worker (tier A or B), else the current thread name (ThreadBackend / driver)."""
    try:
        from distributed import get_worker  # noqa: PLC0415  (absent or outside a worker -> fallback)

        return os.getpid(), str(get_worker().address)
    except Exception:
        return os.getpid(), threading.current_thread().name


# ---- scenario partitions + pure payloads --------------------------------------------------------


def mem_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    """n one-entry partitions ``mem://{tag}/{i}`` — ``n_entries == 1`` each, so adaptive
    ``target_events`` counts consumed tasks exactly."""
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def part_index(uri: str) -> int:
    """The partition index encoded in a ``mem://{tag}/{i}`` uri (the driver-side oracle key)."""
    return int(uri.rsplit("/", 1)[1])


def leaf_text(partition: Partition) -> str:
    return f"[{partition.uri}:{partition.entry_start}-{partition.entry_stop}]"


def leaf_int(partition: Partition) -> int:
    return 7 * partition.entry_start + 3


def concat_process(partition: Partition, resources: object) -> str:
    return leaf_text(partition)


def staggered_concat_process(partition: Partition, resources: object) -> str:
    """Later keys complete FIRST (delay falls with the index): an arrival-ordered fold concats out
    of key order and diverges from ``SequentialRunner``. Sleep is scenario construction, never an
    assertion (R0.10a)."""
    time.sleep(0.01 * max(0, 17 - partition.entry_start))
    return leaf_text(partition)


def concat(a: str, b: str) -> str:
    return a + b


def empty_text() -> str:
    return ""


def sum_process(partition: Partition, resources: object) -> int:
    return leaf_int(partition)


def add(a: int, b: int) -> int:
    return a + b


def zero() -> int:
    return 0


def sum_plan_over(n: int, tag: str) -> Plan[int]:
    """A pure commutative int-sum plan (exact arithmetic — grouping-safe)."""
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=sum_process, combine=add, empty=zero, tasks=tasks)


def concat_plan(n: int, tag: str, *, staggered: bool = False, reverse_tasks: bool = False) -> Plan[str]:
    """The order-witnessing pure plan: associative, NON-commutative concat with an identity empty —
    any grouping over key-ordered leaves matches ``SequentialRunner`` bit-for-bit; any
    arrival-ordered fold does not."""
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    if reverse_tasks:
        tasks = tuple(reversed(tasks))
    process = staggered_concat_process if staggered else concat_process
    return Plan(process=process, combine=concat, empty=empty_text, tasks=tasks)


def expected_concat(n: int, tag: str) -> str:
    return "".join(leaf_text(p) for p in mem_partitions(n, tag))


# ---- provenance-carrying partials (the mechanism witnesses) -------------------------------------


@dataclass(frozen=True)
class Prov:
    """A partial that records WHERE work happened. ``payload`` is the concat value (order witness);
    ``leaves`` = (uri, pid, worker) per leaf; ``combine_sites`` = (pid, worker) that executed a
    combine; ``moves`` = (input.made_on worker, combiner worker) pairs — a pair with distinct
    members proves a combine consumed an input materialized on a DIFFERENT worker;
    ``marks`` = per-invocation nonces (|marks| == leaves + combines run; disjoint across runs
    proves re-execution); ``made_on`` = who materialized THIS partial."""

    payload: str
    leaves: frozenset[tuple[str, int, str]]
    combine_sites: frozenset[tuple[int, str]]
    moves: frozenset[tuple[str, str]]
    marks: frozenset[str]
    made_on: tuple[int, str]


def prov_process(partition: Partition, resources: object) -> Prov:
    pid, worker = where()
    return Prov(
        payload=leaf_text(partition),
        leaves=frozenset({(partition.uri, pid, worker)}),
        combine_sites=frozenset(),
        moves=frozenset(),
        marks=frozenset({uuid.uuid4().hex}),
        made_on=(pid, worker),
    )


def prov_combine(a: Prov, b: Prov) -> Prov:
    pid, worker = where()
    return Prov(
        payload=a.payload + b.payload,
        leaves=a.leaves | b.leaves,
        combine_sites=a.combine_sites | b.combine_sites | {(pid, worker)},
        moves=a.moves | b.moves | {(a.made_on[1], worker), (b.made_on[1], worker)},
        marks=a.marks | b.marks | {uuid.uuid4().hex},
        made_on=(pid, worker),
    )


def prov_empty() -> Prov:
    return Prov("", frozenset(), frozenset(), frozenset(), frozenset(), where())


def prov_plan(n: int, tag: str) -> Plan[Prov]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=prov_process, combine=prov_combine, empty=prov_empty, tasks=tasks)


# ---- broadcast-once witness: an unpickle-counted process ----------------------------------------

_COUNTS: dict[str, int] = {}


def counts_snapshot() -> dict[str, int]:
    """``client.run`` target: THIS process's counter dict (also callable driver-side)."""
    return dict(_COUNTS)


class CountingProcess:
    """A ``Plan.process`` whose UNPICKLING is counted per process. Broadcast-once + the worker-side
    token cache deserialize it exactly once per worker pid however many tasks that worker runs;
    closure-per-task shipping deserializes once per task (the dask/dask#5503 pathology). A fresh
    instance (uuid ``stamp``) gives every test run distinct payload bytes, hence a fresh token."""

    def __init__(self) -> None:
        self.stamp = uuid.uuid4().hex

    def __call__(self, partition: Partition, resources: object) -> Prov:
        return prov_process(partition, resources)

    def __getstate__(self) -> dict[str, object]:
        return {"stamp": self.stamp}

    def __setstate__(self, state: dict[str, object]) -> None:
        self.__dict__.update(state)
        _COUNTS["process_unpickle"] = _COUNTS.get("process_unpickle", 0) + 1


def counting_plan(n: int, tag: str) -> Plan[Prov]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=CountingProcess(), combine=prov_combine, empty=prov_empty, tasks=tasks)


# ---- open_once witness --------------------------------------------------------------------------


def token_opener(uri: str) -> str:
    """Every REAL open mints a fresh pid-prefixed token; ``open_once`` reuse returns the cached one,
    so distinct tokens per pid == real opens per worker."""
    return f"{os.getpid()}:{uuid.uuid4().hex}"


@dataclass(frozen=True)
class OpenOnceProcess:
    """Opens ONE shared uri via ``resources.open_once`` and reports (partition uri, pid, worker,
    handle token) — the opens-per-worker==1-while-tasks-per-worker>1 witness (plan §1.5 row 11)."""

    shared_uri: str

    def __call__(self, partition: Partition, resources: Any) -> frozenset[tuple[str, int, str, str]]:
        handle = resources.open_once(self.shared_uri, token_opener)
        pid, worker = where()
        return frozenset({(partition.uri, pid, worker, str(handle))})


def union(a: frozenset[Any], b: frozenset[Any]) -> frozenset[Any]:
    return a | b


def empty_set() -> frozenset[Any]:
    return frozenset()


def open_once_plan(n: int, tag: str, shared_uri: str) -> Plan[frozenset[tuple[str, int, str, str]]]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(
        process=OpenOnceProcess(shared_uri), combine=union, empty=empty_set, tasks=tasks, open_once=True
    )


# ---- failure scenarios --------------------------------------------------------------------------

USER_FRAME = SourceFrame(filename="user_analysis.py", lineno=42, function="my_cut", source="pt > 30")


def make_stage_error() -> StageError:
    """A fully-populated deterministic StageError — the byte-equal round-trip reference."""
    return StageError(
        op="mul",
        frames=(USER_FRAME,),
        input_forms=("float64[]",),
        partition="mem://boom/0:0-1",
        cause_type="ValueError",
        cause_message="negative pt",
        opt_level=2,
    )


def raise_stage_error(partition: Partition, resources: object) -> str:
    raise make_stage_error()


def stage_error_plan(n: int, tag: str) -> Plan[str]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=raise_stage_error, combine=concat, empty=empty_text, tasks=tasks)


def raise_value_error(partition: Partition, resources: object) -> str:
    raise ValueError(f"broken partition {partition.uri}")


def exit_process(partition: Partition, resources: object) -> str:
    """Hard worker death (segfault/OOM stand-in): kills the WORKER PROCESS, not just the task."""
    os._exit(1)


@dataclass(frozen=True)
class DieOnceProcess:
    """First attempt at the poisoned uri hard-kills its worker after atomically dropping a pid
    marker; the rescheduled attempt (marker present) succeeds — the reroute witness. All other
    partitions compute normally."""

    marker_path: str
    poison_uri: str

    def __call__(self, partition: Partition, resources: object) -> Prov:
        if partition.uri == self.poison_uri and not os.path.exists(self.marker_path):
            tmp = f"{self.marker_path}.{os.getpid()}"
            with open(tmp, "w") as f:
                f.write(str(os.getpid()))
            os.replace(tmp, self.marker_path)
            os._exit(1)
        return prov_process(partition, resources)


# ---- adaptive scenarios -------------------------------------------------------------------------


@dataclass(frozen=True)
class FoldPart:
    """The adaptive partial: a commutative sum + the uri set that produced it. Internal consistency
    (``value == sum(oracle(uri) for uri in uris)``) IS the running_fold correctness oracle under a
    nondeterministic completion set."""

    value: int
    uris: frozenset[str]


def fold_oracle(uris: frozenset[str]) -> int:
    return sum(7 * part_index(u) + 3 for u in uris)


def fold_process(partition: Partition, resources: object) -> FoldPart:
    return FoldPart(leaf_int(partition), frozenset({partition.uri}))


def fold_combine(a: FoldPart, b: FoldPart) -> FoldPart:
    return FoldPart(a.value + b.value, a.uris | b.uris)


def fold_empty() -> FoldPart:
    return FoldPart(0, frozenset())


@dataclass(frozen=True)
class SlowFoldProcess:
    """``fold_process`` with ONE slow leaf — guarantees an outstanding future exists when the stop
    condition fires (scenario construction; the assertions are counters, R0.10a)."""

    slow_uri: str
    delay_s: float

    def __call__(self, partition: Partition, resources: object) -> FoldPart:
        if partition.uri == self.slow_uri:
            time.sleep(self.delay_s)
        return fold_process(partition, resources)


class Feed:
    """Driver-side ``next_tasks``: hands out the given batches then ``None`` (DONE), recording the
    ``ExecContext.events_done`` seen at every call — a call observed at or past the stop target is
    the refill-after-stop violation witness."""

    def __init__(self, batches: Sequence[Sequence[Task]]) -> None:
        self.batches = [list(b) for b in batches]
        self.calls: list[int] = []

    def __call__(self, ctx: ExecContext) -> list[Task] | None:
        self.calls.append(ctx.events_done)
        return self.batches.pop(0) if self.batches else None


# ---- direct-seam probe fns ----------------------------------------------------------------------


def double(x: int) -> int:
    return 2 * x


def payload_len(x: object) -> int:
    """The broadcast contract probe: the task fn must receive the RAW BYTES on the worker (§1.1)."""
    assert isinstance(x, (bytes, bytearray)), f"broadcast handle must resolve to raw bytes, got {type(x)}"
    return len(x)


# ---- SpyBackend: the protocol-shaped mechanism tap ----------------------------------------------


class SpyBackend:
    """A delegating ``SubmitBackend``: records submitted keys, broadcast tokens, cancel batches,
    and (un)subscribed topics. Drives key-namespace/nonce, cancel-on-stop, and monitor-topic
    witnesses without touching engine internals."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.submitted_keys: list[str] = []
        self.broadcast_tokens: list[str] = []
        self.cancel_batches: list[int] = []
        self.topics: list[str] = []
        self.unsubscribed: list[str] = []

    @property
    def capabilities(self) -> Any:
        return self.inner.capabilities

    def n_workers(self) -> int:
        return int(self.inner.n_workers())

    def submit(
        self,
        fn: Any,
        /,
        *args: Any,
        key: str,
        retries: int = 0,
        priority: int = 0,
        resources: Mapping[str, float] | None = None,
    ) -> Any:
        self.submitted_keys.append(key)
        return self.inner.submit(fn, *args, key=key, retries=retries, priority=priority, resources=resources)

    def broadcast(self, payload: bytes, *, token: str) -> Any:
        self.broadcast_tokens.append(token)
        return self.inner.broadcast(payload, token=token)

    def subscribe_events(self, topic: str, handler: Any) -> Any:
        self.topics.append(topic)
        unsub = self.inner.subscribe_events(topic, handler)

        def _unsub() -> None:
            self.unsubscribed.append(topic)
            unsub()

        return _unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        futures = list(futures)
        self.cancel_batches.append(len(futures))
        self.inner.cancel(futures)

    def close(self) -> None:
        self.inner.close()


# ---- RecordingMonitor (M37 driver-side observer) ------------------------------------------------


class RecordingMonitor:
    """A passive driver-side Monitor: appends every TaskEvent (thread-safe by the GIL's list
    append). ``fail=True`` makes ``on_task`` raise — the emit-swallow passivity probe."""

    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[Any] = []
        self.combines: list[int] = []
        self.profiles: list[tuple[str, bytes]] = []
        self.fail = fail

    def on_task(self, event: Any) -> None:
        if self.fail:
            raise RuntimeError("misbehaving monitor")
        self.events.append(event)

    def on_profile(self, worker: str, payload: bytes) -> None:
        self.profiles.append((worker, payload))

    def on_combine(self, leaves_done: int) -> None:
        self.combines.append(leaves_done)

    def worker_profiler_factory(self) -> None:
        return None


def events_by_key(events: Sequence[Any]) -> dict[int, list[Any]]:
    out: dict[int, list[Any]] = {}
    for ev in events:
        out.setdefault(ev.key, []).append(ev)
    return out


def wait_for(predicate: Any, timeout_s: float = 30.0) -> None:
    """Poll a driver-side predicate with a generous bound; assertions on the state happen AFTER
    (bounded wait is fixture discipline, never an assertion — R0.10a)."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


# ---- toy frontend backend + partitioned source (aggregate/External parity, no array dep) --------


@dataclass(frozen=True)
class ToyForm:
    kind: str

    def describe(self) -> str:
        return self.kind


class ToyListBackend:
    """A minimal evaluation backend over plain Python lists (the m5/m10 frozen-toy shape): pairwise
    add/mul, scalar add/mul, a ``sum`` reduction, and a ``map`` External carrying a real
    ``PayloadDescriptor`` (content-hash-addressed, resolved worker-side via ``externals``)."""

    def op_form(self, op: str, inputs: Sequence[object], params: Mapping[str, object]) -> ToyForm:
        if op in {"add", "mul", "sub"}:
            return ToyForm("num")
        if op == "sum":
            return ToyForm("scalar")
        if op == "map":
            return ToyForm("object")
        raise TypeError(f"unsupported toy op {op!r}")

    def eval_stage(self, op: str, inputs: Sequence[object], params: Mapping[str, object]) -> object:
        if op in {"add", "mul", "sub"} and "scalar" in params:
            s = params["scalar"]
            assert isinstance(s, (int, float))
            seq = list(inputs[0])  # type: ignore[call-overload]
            if op == "add":
                return [x + s for x in seq]
            if op == "sub":
                return [x - s for x in seq]
            return [x * s for x in seq]
        if op in {"add", "mul", "sub"}:
            left, right = list(inputs[0]), list(inputs[1])  # type: ignore[call-overload]
            if op == "add":
                return [a + b for a, b in zip(left, right, strict=True)]
            if op == "sub":
                return [a - b for a, b in zip(left, right, strict=True)]
            return [a * b for a, b in zip(left, right, strict=True)]
        if op == "sum":
            return sum(list(inputs[0]))  # type: ignore[call-overload]
        raise TypeError(f"unsupported toy op {op!r}")

    def boundary_ops(self) -> frozenset[str]:
        return frozenset({"source", "sum", "map"})

    def project(self, op: str, used: object, params: Mapping[str, object]) -> object:
        return used

    def external_payload(self, op: str, params: Mapping[str, object]) -> PayloadDescriptor | None:
        if op != "map":
            return None
        return PayloadDescriptor(
            kind="opaque_callable",
            content_hash=f"toy-opaque:{params.get('fn', 'lambda')}",
            framework="python",
            version="3",
            io_schema="opaque->opaque",
            preprocessing_ref=None,
        )


@dataclass(frozen=True)
class ToyListSource:
    """A picklable ``graphed.write.PartitionedSource`` over an in-memory tuple (blind partitions)."""

    data: tuple[int, ...]
    n_parts: int

    def __call__(self) -> list[int]:
        raise AssertionError("the whole-dataset loader must never run during a plan")

    def partitions(self, steps_per_file: int = 1) -> tuple[Partition, ...]:
        return tuple(Partition.blind("toy://list", "", s, self.n_parts) for s in range(self.n_parts))

    def read_partition(self, partition: Partition, columns: object, resources: object) -> list[int]:
        part = partition.resolve(len(self.data))
        return list(self.data[part.entry_start : part.entry_stop])


def times10(xs: object) -> list[int]:
    """The External payload evaluator (module-level: ships to workers by reference)."""
    assert isinstance(xs, list)
    return [x * 10 for x in xs]


# ---- aggregate provenance partial (parity surface 1: value parity + worker spread in one run) ----


@dataclass(frozen=True)
class AggPart:
    """Multi-output aggregate partial: ``outs`` (per-output totals, exact for int-valued data) plus
    the (pid, worker) sites that computed — outs compare bit-for-bit vs SequentialRunner while
    sites witness the spread."""

    outs: tuple[float, ...]
    sites: frozenset[tuple[int, str]]


def agg_reduce(values: list[Any]) -> AggPart:
    return AggPart(tuple(float(v) for v in values), frozenset({where()}))


def agg_combine(a: AggPart, b: AggPart) -> AggPart:
    if not a.outs:
        return AggPart(b.outs, a.sites | b.sites)
    if not b.outs:
        return AggPart(a.outs, a.sites | b.sites)
    outs = tuple(x + y for x, y in zip(a.outs, b.outs, strict=True))
    return AggPart(outs, a.sites | b.sites | {where()})


def agg_empty() -> AggPart:
    return AggPart((), frozenset())
