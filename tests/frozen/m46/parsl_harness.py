"""Shared harness for the m46 frozen suite: `ParslBackend` over direct parsl executor submit +
the relay (as-tasks/workflow) shuffle engine + the `common/` bootstrap
(parsl-backend-plan §1.1-§1.3, §1.6, §2 m46, §3 m46 — FINAL at r3).

Pinned execution contract (test-author restatement of the plan's Implementation Targets — the
implementer builds exactly these module paths and symbols; see the m46 README):

    graphed_executors.parsl_backend:
        ParslBackend(executor)            # a STARTED parsl HighThroughputExecutor or parsl
                                          # ThreadPoolExecutor; any other type -> loud TypeError
                                          # naming both supported classes (plan §1.1)
        parsl_runner(executor) -> SubmitRunner
    graphed_executors.parsl_backend.launch:
        start_htex(*, workers, run_dir, address=None, encrypted=False) -> HighThroughputExecutor
        stop_htex(executor) -> None       # the three measured direct-use moves live here (§0.3)
    graphed_executors.common.tasks_engine:
        the ENTIRE m43 engine moved with names VERBATIM (§1.6 m46 move table):
        dask_run_repartition / dask_run_join / _dask_map_write / _dask_pick / _dask_gather /
        _dask_gather_join / _dask_broadcast_join_part / DaskShuffleWitness / _require_peer ...
    graphed_executors.common.relay_engine:
        relay_run_repartition(backend, src_blocks, parts, *, runner, salt=0) -> ShuffleResult
        relay_run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",),
                       how="inner", runner, broadcast=None, salt=0, mem_budget_bytes=None)
                       -> ShuffleResult
        RelayShuffleWitness               # the DaskShuffleWitness counters PLUS
                                          # head_node_routed: bool (True) and
                                          # driver_relay_bytes: int (sum of map wire sizes) — §1.3

Disciplines carried from the m42/m43/m45 harnesses (replicated with distinct names — frozen
suites never import each other cross-directory):

1. **Spawn safety + worker importability.** Everything an HTEX worker may unpickle is
   MODULE-LEVEL here, and :func:`ensure_harness_on_worker_path` prepends this directory to
   ``PYTHONPATH`` BEFORE any HTEX starts. MEASURED (this suite's authoring probe, parsl
   2026.7.20): HTEX workers are launched via the ``process_worker_pool`` console script and do
   NOT inherit pytest's driver-side ``sys.path`` insertion — without the export every
   harness-defined task fn dies on the worker with ``ModuleNotFoundError: parsl_harness``; with
   it the round trip works (first HTEX result ~1.0-1.3 s after a ~0.3 s start).
2. **Deferred implementation imports.** ``graphed_executors.parsl_backend`` /
   ``graphed_executors.common`` are imported ONLY through the accessors below, inside test
   bodies/fixtures, so pre-implementation the suite COLLECTS cleanly and every test fails with
   the right-reason ``ModuleNotFoundError`` (TEST_SANITY non-vacuity). parsl itself is imported
   lazily too: the module top level stays importable on the parsl-free main matrix.

Two spy seams (plan §1.2 step 2 / §3 harness row — they observe DIFFERENT layers):

- :class:`SpyParslBackend` wraps the ``SubmitBackend`` seam and records the RAW submitted
  ``fn.__name__`` + key (the shim wrap happens INSIDE ``ParslBackend.submit``, after this seam —
  design-minor-3), feeding the frozen engine-shape Counters.
- :func:`executor_submit_recorder` instance-patches the PARSL EXECUTOR's
  ``submit(func, resource_specification, ...)`` and records the ``resource_specification`` that
  actually reaches parsl (TPE must always get ``{}`` — measured ``InvalidResourceSpecification``
  otherwise; HTEX gets ``{"priority": p}`` iff p != 0).

All witnesses are counters, content hashes, pids, worker identities, and site provenance — never
clocks (R0.10a). The one ``sleep`` in :func:`pv_process` is scenario construction (it overlaps
tiny leaves so a 2-worker HTEX pool demonstrably engages both workers), never an assertion.
"""

from __future__ import annotations

import functools
import os
import shutil
import tempfile
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from graphed.core.execution import Partition, Plan, Task
from graphed.debug import SourceFrame, StageError
from graphed.numpy import NumpyBackend

from graphed_executors.submit import SubmitRunner, current_env
from graphed_executors.submit.protocol import SubmitCapabilities

HARNESS_DIR = str(Path(__file__).resolve().parent)
REPO_ROOT = Path(__file__).resolve().parents[3]

# a joined output row, backend-independent: (key, left value, right value)
JRow = tuple[int, int, int]
# a null-aware joined row (non-inner arms): key is always coalesced, an absent side's value is None
NJRow = tuple[int | None, int | None, int | None]


# ---- deferred accessors for the implementation under test ---------------------------------------


def parsl_backend_api() -> Any:
    """``graphed_executors.parsl_backend`` — absent pre-implementation: the right-reason
    ``ModuleNotFoundError`` for every ParslBackend-theme test lands here."""
    import graphed_executors.parsl_backend as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def launch_api() -> Any:
    """``graphed_executors.parsl_backend.launch`` — ``start_htex``/``stop_htex`` (plan §1.2)."""
    import graphed_executors.parsl_backend.launch as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def relay_api() -> Any:
    """``graphed_executors.common.relay_engine`` — the §1.3 relay engine (plan §2 m46 IT2)."""
    import graphed_executors.common.relay_engine as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def tasks_engine_api() -> Any:
    """``graphed_executors.common.tasks_engine`` — the §1.6 verbatim m43-engine move (m46 IT1)."""
    import graphed_executors.common.tasks_engine as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def make_parsl_backend(executor: Any) -> Any:
    """``ParslBackend(executor)`` (plan §1.2 pinned constructor: a started executor, nothing else)."""
    return parsl_backend_api().ParslBackend(executor)


def make_runner(backend: Any, *, monitor: Any = None, retries: int = 3) -> SubmitRunner:
    return SubmitRunner(backend, monitor=monitor, retries=retries)


def relay_repartition(*args: Any, **kwargs: Any) -> Any:
    """``relay_run_repartition(backend, src_blocks, parts, *, runner, salt=0)`` (plan §1.3 pin)."""
    return relay_api().relay_run_repartition(*args, **kwargs)


def relay_join(*args: Any, **kwargs: Any) -> Any:
    """``relay_run_join(backend, left, right, parts, *, on=..., how=..., runner, broadcast=None,
    salt=0, mem_budget_bytes=None)`` (plan §1.3 pin — signatures mirror the m43 pair)."""
    return relay_api().relay_run_join(*args, **kwargs)


def m43_repartition(*args: Any, **kwargs: Any) -> Any:
    """The MOVED m43 engine at its new home — ``common.tasks_engine.dask_run_repartition``."""
    return tasks_engine_api().dask_run_repartition(*args, **kwargs)


def m43_join(*args: Any, **kwargs: Any) -> Any:
    """``common.tasks_engine.dask_run_join`` (the §1.6 move; names verbatim)."""
    return tasks_engine_api().dask_run_join(*args, **kwargs)


# ---- expected capability vectors (the frozen 7-flag SubmitCapabilities; NO new fields) -----------


def expected_htex_caps() -> SubmitCapabilities:
    """HTEX = all-seven-False — literally the protocol docstring's "parsl floor" (plan §1.1)."""
    return SubmitCapabilities(
        peer_data_movement=False,
        scatter_broadcast=False,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=False,
        worker_file_cache=False,
    )


def expected_tpe_caps() -> SubmitCapabilities:
    """TPE = the ThreadBackend shape: same-process shared memory ⇒ ``peer_data_movement=True``,
    everything else False (plan §1.1 table; threadpool.py:45-53 precedent)."""
    return SubmitCapabilities(
        peer_data_movement=True,
        scatter_broadcast=False,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=False,
        worker_file_cache=False,
    )


# ---- fixture substrate: HTEX / TPE pools through the PLANNED launch recipe ----------------------


def ensure_harness_on_worker_path() -> None:
    """Prepend this directory to ``PYTHONPATH`` so spawned HTEX workers can import this module by
    reference (measured load-bearing — module docstring point 1). Idempotent; deliberately NOT
    restored (process-level, harmless: the dir only holds this suite's helpers, and restoring
    would race module-scoped pools across test files)."""
    existing = os.environ.get("PYTHONPATH", "")
    if HARNESS_DIR not in existing.split(os.pathsep):
        os.environ["PYTHONPATH"] = HARNESS_DIR + (os.pathsep + existing if existing else "")


@contextmanager
def htex_pool(workers: int = 2) -> Iterator[Any]:
    """A started 2-worker HTEX via the PLANNED ``start_htex`` (module-scoped in tests — measured
    ~1.7 s amortized once per module; ``encrypted=False`` is the pinned test default). Teardown
    goes through the paired ``stop_htex``."""
    ensure_harness_on_worker_path()
    run_dir = tempfile.mkdtemp(prefix="graphed-m46-htex-")
    api = launch_api()
    executor = api.start_htex(workers=workers, run_dir=run_dir)
    try:
        yield executor
    finally:
        api.stop_htex(executor)
        shutil.rmtree(run_dir, ignore_errors=True)


@contextmanager
def tpe_pool(max_threads: int = 2) -> Iterator[Any]:
    """A started parsl ``ThreadPoolExecutor`` (harness-owned: parsl's public executor API, not the
    implementation under test; measured — direct start + submit works, first result <1 ms)."""
    from parsl.executors import ThreadPoolExecutor  # noqa: PLC0415  (parsl-touching tests importorskip first)

    executor = ThreadPoolExecutor(label=f"graphed-m46-tpe-{uuid.uuid4().hex[:8]}", max_threads=max_threads)
    executor.start()
    try:
        yield executor
    finally:
        executor.shutdown()


# ---- executor-level spy: the resource_specification that actually reaches parsl -----------------


@contextmanager
def executor_submit_recorder(executor: Any) -> Iterator[list[dict[str, Any]]]:
    """Instance-patch ``executor.submit(func, resource_specification, ...)`` to record what parsl
    receives. The patch shadows the bound method via an instance attribute, so the executor's TYPE
    is unchanged (``ParslBackend``'s type-derived capability vector and any isinstance check see
    the real class). Construct the ``ParslBackend`` INSIDE this context so an implementation that
    binds ``executor.submit`` at ``__init__`` time is captured too."""
    records: list[dict[str, Any]] = []
    original = executor.submit

    def recording(func: Any, resource_specification: Any, *args: Any, **kwargs: Any) -> Any:
        records.append(
            {
                "fn": str(getattr(func, "__name__", type(func).__name__)),
                "resource_specification": dict(resource_specification),
            }
        )
        return original(func, resource_specification, *args, **kwargs)

    executor.submit = recording
    try:
        yield records
    finally:
        del executor.submit


# ---- SpyParslBackend: the SubmitBackend seam tap (engine-shape Counters; raw fn names) ----------


def _fn_name(fn: Any) -> str:
    while isinstance(fn, functools.partial):
        fn = fn.func
    return str(getattr(fn, "__name__", type(fn).__name__))


class SpyParslBackend:
    """A delegating ``SubmitBackend`` recording every submitted task fn's RAW name + key and every
    broadcast token — the engine-shape Counter gates read these (counts, never clocks). Delegation
    is by ``__getattr__`` (the m45 idiom) so capabilities/n_workers/describe_failure/close pass
    through untouched."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.submitted: list[tuple[str, str]] = []  # (fn name, key)
        self.broadcast_tokens: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def submit(self, fn: Any, /, *args: Any, key: str, **kwargs: Any) -> Any:
        self.submitted.append((_fn_name(fn), key))
        return self.inner.submit(fn, *args, key=key, **kwargs)

    def broadcast(self, payload: bytes, *, token: str) -> Any:
        self.broadcast_tokens.append(token)
        return self.inner.broadcast(payload, token=token)

    def fn_names(self) -> Counter[str]:
        return Counter(name for name, _key in self.submitted)


# ---- module-level spawn-safe task fns (pickled BY REFERENCE; run on HTEX workers) ---------------


def double(x: int) -> int:
    return 2 * x


def double_with_site(x: int) -> tuple[int, int, str]:
    """The future-arg composition probe: doubles a RESOLVED upstream value and reports where it
    ran — ``(2x, pid, worker)``. A backend that ships the future unresolved crashes here; one that
    computes driver-side leaks the driver pid."""
    return 2 * x, os.getpid(), current_env().worker


def payload_len(x: object) -> int:
    """The broadcast contract probe: the task fn must receive the RAW BYTES on the worker."""
    assert isinstance(x, (bytes, bytearray)), f"broadcast handle must resolve to raw bytes, got {type(x)}"
    return len(x)


def site_probe(idx: int) -> tuple[int, str]:
    """(pid, worker identity) AT EXECUTION via the submit-engine ``current_env()`` seam."""
    return os.getpid(), current_env().worker


def worker_identity_probe(idx: int) -> tuple[int, str, str]:
    """(pid, env-seam worker id, the id recomputed IN-TASK from parsl's own env vars). Equality of
    the last two pins ``worker == f"{PARSL_WORKER_POOL_ID}:{PARSL_WORKER_RANK}"`` exactly
    (plan §1.2 `_shim.py`), independent of the vars' textual format."""
    expected = (
        os.environ.get("PARSL_WORKER_POOL_ID", "<absent>")
        + ":"
        + os.environ.get("PARSL_WORKER_RANK", "<absent>")
    )
    return os.getpid(), current_env().worker, expected


def _token_opener(uri: str) -> str:
    """Every REAL open mints a fresh pid-prefixed token; ``open_once`` reuse returns the cached
    one, so distinct tokens per pid == real opens per worker process."""
    return f"{os.getpid()}:{uuid.uuid4().hex}"


def open_once_probe(uri: str, idx: int) -> tuple[int, str]:
    """(pid, open_once handle token): a per-worker-process cached ``LocalResources`` yields ONE
    token per pid across many tasks; a fresh-env-per-task implementation mints one per task."""
    handle = current_env().resources.open_once(uri, _token_opener)
    return os.getpid(), str(handle)


def emit_probe(topic: str, n: int) -> int:
    """Emit ``n`` single-event batches in sequence order on ``topic`` through the worker env seam;
    the driver-side subscriber must see them all, in order, after completion (the §1.2 buffering
    piggyback). Each event carries the emitting pid (worker-side creation witness)."""
    env = current_env()
    for i in range(n):
        env.emit(topic, [{"seq": i, "pid": os.getpid()}])
    return n


# ---- plan scenarios (m42-replicated shapes, distinct names) -------------------------------------


def mem_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def leaf_text(partition: Partition) -> str:
    return f"[{partition.uri}:{partition.entry_start}-{partition.entry_stop}]"


def concat_process(partition: Partition, resources: object) -> str:
    return leaf_text(partition)


def concat(a: str, b: str) -> str:
    return a + b


def empty_text() -> str:
    return ""


def concat_plan(n: int, tag: str) -> Plan[str]:
    """The order-witnessing pure plan: associative, NON-commutative concat with an identity empty —
    any grouping over key-ordered leaves matches ``SequentialRunner`` bit-for-bit."""
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=concat_process, combine=concat, empty=empty_text, tasks=tasks)


def expected_concat(n: int, tag: str) -> str:
    return "".join(leaf_text(p) for p in mem_partitions(n, tag))


@dataclass(frozen=True)
class PvPart:
    """The provenance partial: ``payload`` is the concat value (order witness); ``leaves`` =
    (uri, pid, worker) per leaf; ``combine_sites`` = (pid, worker) that executed a combine."""

    payload: str
    leaves: frozenset[tuple[str, int, str]]
    combine_sites: frozenset[tuple[int, str]]


def pv_process(partition: Partition, resources: object) -> PvPart:
    time.sleep(0.05)  # scenario construction: overlap tiny leaves so both pool workers engage
    return PvPart(
        payload=leaf_text(partition),
        leaves=frozenset({(partition.uri, os.getpid(), current_env().worker)}),
        combine_sites=frozenset(),
    )


def pv_combine(a: PvPart, b: PvPart) -> PvPart:
    return PvPart(
        payload=a.payload + b.payload,
        leaves=a.leaves | b.leaves,
        combine_sites=a.combine_sites | b.combine_sites | {(os.getpid(), current_env().worker)},
    )


def pv_empty() -> PvPart:
    return PvPart("", frozenset(), frozenset())


def pv_plan(n: int, tag: str) -> Plan[PvPart]:
    tasks = tuple(Task(i, p) for i, p in enumerate(mem_partitions(n, tag)))
    return Plan(process=pv_process, combine=pv_combine, empty=pv_empty, tasks=tasks)


USER_FRAME = SourceFrame(filename="user_analysis.py", lineno=42, function="my_cut", source="pt > 30")


def make_stage_error() -> StageError:
    """A fully-populated deterministic StageError — the M6 cross-process round-trip reference."""
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


# ---- RecordingMonitor + drivers -----------------------------------------------------------------


class RecordingMonitor:
    """A passive driver-side Monitor: appends every TaskEvent (list append is thread-safe)."""

    def __init__(self) -> None:
        self.events: list[Any] = []
        self.combines: list[int] = []
        self.profiles: list[tuple[str, bytes]] = []

    def on_task(self, event: Any) -> None:
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


def wait_for(predicate: Callable[[], bool], timeout_s: float = 60.0) -> None:
    """Bounded driver-side poll; assertions on the state happen AFTER (fixture discipline, never
    an assertion — R0.10a)."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.02)


def run_bounded(fn: Callable[[], Any], timeout_s: float = 240.0) -> Any:
    """Hard-timeout driver for pool-touching calls: a hang surfaces as a failure, never a hung CI
    job (the m45 discipline). Returns the value or re-raises the call's exception."""
    out: dict[str, Any] = {}

    def _drive() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    thread = threading.Thread(target=_drive, daemon=True)
    thread.start()
    thread.join(timeout_s)
    assert not thread.is_alive(), f"HARD TIMEOUT: pool call did not finish within {timeout_s}s"
    if "error" in out:
        raise out["error"]
    return out["result"]


# ---- numpy scenario builders (adapter economy: numpy suffices for every m46 theme) ---------------

BACKEND = NumpyBackend()
_BLOCK_DT = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])


def make_block(keys: Sequence[int]) -> Any:
    block = np.zeros(len(keys), dtype=_BLOCK_DT)
    block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
    block["v"] = np.arange(len(keys), dtype=np.int64)
    return block


def make_side(keys: Sequence[int], field: str, values: Sequence[int]) -> Any:
    dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
    block = np.zeros(len(keys), dtype=dt)
    block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
    block[field] = np.array(list(values), dtype=np.int64)
    return block


def repartition_blocks(copies: int = 1) -> list[Any]:
    """The m39-lineage multi-src overlapping-key scenario: 6 source blocks, several dests per
    producer task, several producers per dest. ``copies`` repeats each key list in place — MORE
    ROWS, SAME BLOCK COUNT (the row-count-independence knob for the shape gates)."""
    key_lists = [
        [0, 1, 2, 3, 4, 5, 6, 7],
        [7, 6, 5, 4, 3, 2, 1, 0],
        [10, 11, 12, 13, 10, 11, 12, 13],
        [100, 200, 300, 400],
        [1, 1, 2, 2, 3, 3],
        [42, 43, 44, 45, 46, 47, 48, 49],
    ]
    return [make_block(list(keys) * copies) for keys in key_lists]


def join_sides() -> tuple[list[Any], list[Any]]:
    """Overlapping keys plus a left-only key (13) and a right-only key (17): the non-inner arms
    have REAL unmatched rows on both sides (the m40 F1/F3 discrimination re-armed on parsl)."""
    left = [
        make_side([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
        make_side([1, 1, 2, 2, 13, 13], "lval", [22, 23, 24, 25, 32, 33]),
    ]
    right = [
        make_side([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
        make_side([3, 3, 2, 17, 17, 4], "rval", [106, 107, 108, 116, 117, 111]),
    ]
    return left, right


def join_equal_sides() -> tuple[list[Any], list[Any]]:
    """Byte-comparable sides: with ``parts=8`` the pinned rule says SHUFFLE
    (``build*8 >= build+probe``) while a live-``n_workers()`` recompute on a 1-worker pool says
    BROADCAST — the F6 plan-choice discrimination gap (m43 shape, numpy)."""
    left = [
        make_side([0, 1, 2, 3, 4, 5, 6, 7], "lval", [10, 11, 12, 13, 14, 15, 16, 17]),
        make_side([8, 9, 10, 11, 0, 1, 2, 3], "lval", [18, 19, 20, 21, 22, 23, 24, 25]),
    ]
    right = [
        make_side([0, 2, 4, 6, 8, 10, 1, 3], "rval", [100, 101, 102, 103, 104, 105, 106, 107]),
        make_side([5, 7, 9, 11, 0, 2, 4, 6], "rval", [108, 109, 110, 111, 112, 113, 114, 115]),
    ]
    return left, right


def join_small_build_sides() -> tuple[list[Any], list[Any]]:
    """A tiny build side + a larger multi-block probe side, so the pinned rule prefers BROADCAST
    at the tested ``parts`` — the forced ``broadcast=False`` honor arm runs against the model."""
    left = [make_side([1, 2, 3], "lval", [10, 20, 30])]
    right = [
        make_side([1, 1, 2, 3, 3, 3], "rval", [100, 101, 200, 300, 301, 302]),
        make_side([2, 2, 1, 3, 1, 2], "rval", [201, 202, 102, 303, 103, 203]),
        make_side([3, 1, 2, 3, 2, 1], "rval", [304, 104, 204, 305, 205, 105]),
    ]
    return left, right


def join_hot_key_sides(n_left: int, n_right: int) -> tuple[list[Any], list[Any]]:
    """A single hot key (all rows key=7 -> one dest): the inner join duplicates to
    ``n_left*n_right`` output rows in one dest — the B5 output-side blowup the budget must cover."""
    left = [make_side([7] * n_left, "lval", list(range(n_left)))]
    right = [make_side([7] * n_right, "rval", list(range(100, 100 + n_right)))]
    return left, right


def side_bytes(blocks: Sequence[Any]) -> int:
    return sum(int(BACKEND.estimated_bytes(b)) for b in blocks)


# ---- test-authored oracles (route via the golden-pinned ``partition``, never the join kernel) ----


def keys_of(block: Any) -> list[int]:
    return [int(k) for k in block["__joinkey__"]]


def side_rows(block: Any, field: str) -> list[tuple[int, int]]:
    return list(zip(keys_of(block), [int(v) for v in block[field]], strict=True))


def joined_rows(block: Any) -> list[JRow]:
    return list(
        zip(
            keys_of(block),
            [int(v) for v in block["lval"]],
            [int(v) for v in block["rval"]],
            strict=True,
        )
    )


def route_oracle_dest_keys(src_blocks: Sequence[Any], parts: int) -> dict[int, list[int]]:
    """Per dest, the ``__joinkey__`` values routed there in ascending-src, within-src order —
    built from the backend's own golden-pinned ``partition`` (routing is NOT re-implemented)."""
    per_dest: dict[int, list[int]] = {}
    for src in src_blocks:  # ascending src index
        for dest, sub in enumerate(BACKEND.partition(src, "__joinkey__", parts)):
            keys = keys_of(sub)
            if keys:
                per_dest.setdefault(dest, []).extend(keys)
    return per_dest


def result_dest_keys(value: dict[int, Any]) -> dict[int, list[int]]:
    return {dest: keys_of(block) for dest, block in value.items()}


def total_result_rows(value: Mapping[int, Any]) -> int:
    return sum(len(block) for block in value.values())


def contiguous_split(n_src: int, n_tasks: int) -> list[list[int]]:
    """The frozen contiguous-ascending producer assignment (the m43 ``_assign`` contract, pinned
    by the m43 cross-engine hash gate): even ``divmod`` split, remainder to the earliest tasks."""
    base, rem = divmod(n_src, n_tasks)
    ranges: list[list[int]] = []
    start = 0
    for t in range(n_tasks):
        size = base + (1 if t < rem else 0)
        ranges.append(list(range(start, start + size)))
        start += size
    return ranges


def expected_relay_map_bytes(src_blocks: Sequence[Any], parts: int, n_tasks: int) -> int:
    """The INDEPENDENT ``driver_relay_bytes`` oracle (plan §1.3: "Σ wire sizes of all map payloads
    resolved at the driver barrier"): per producer task (contiguous ascending split), route each
    owned src with the golden-pinned ``partition``, concat each dest's sub-blocks in ascending-src
    order, and sum the ``to_wire`` byte lengths. Salt-0 only (the oracle mirrors the frozen
    route/merge contract, not the engine code)."""
    total = 0
    for owned in contiguous_split(len(src_blocks), n_tasks):
        per_dest: dict[int, list[Any]] = {}
        for s in owned:
            for dest, sub in enumerate(BACKEND.partition(src_blocks[s], "__joinkey__", parts, salt=0)):
                if len(sub):
                    per_dest.setdefault(dest, []).append(sub)
        for subs in per_dest.values():
            total += len(bytes(BACKEND.to_wire(BACKEND.concat(subs))))
    return total


def _per_dest_side(blocks: Sequence[Any], field: str, parts: int) -> dict[int, list[tuple[int, int]]]:
    out: dict[int, list[tuple[int, int]]] = {}
    for block in blocks:  # ascending src index
        for dest, sub in enumerate(BACKEND.partition(block, "__joinkey__", parts)):
            rows = side_rows(sub, field)
            if rows:
                out.setdefault(dest, []).extend(rows)
    return out


def dup_join_all(left: Sequence[Any], right: Sequence[Any], parts: int) -> Counter[JRow]:
    """DUPLICATING relational inner join as a multiset (the m40 §3.3 pin): a probe row with k
    build matches ⇒ k rows. Routes each side with ``partition`` only — the engine under test
    cannot share a join-kernel bug with this oracle."""
    left_by_dest = _per_dest_side(left, "lval", parts)
    right_by_dest = _per_dest_side(right, "rval", parts)
    total: Counter[JRow] = Counter()
    for dest in set(left_by_dest) & set(right_by_dest):
        for k, lv in left_by_dest[dest]:
            for k2, rv in right_by_dest[dest]:
                if k == k2:
                    total[(k, lv, rv)] += 1
    return total


def observed_join_all(value: dict[int, Any]) -> Counter[JRow]:
    total: Counter[JRow] = Counter()
    for block in value.values():
        total += Counter(joined_rows(block))
    return total


def read_nullable_column(c: Any) -> list[int | None]:
    """One joined numpy column to Python ints with ``None`` for a masked entry — survives the
    masked rows the non-inner arms produce (a plain ``int()`` reader would crash)."""
    arr = np.ma.asanyarray(c)
    return [None if arr[i] is np.ma.masked else int(arr[i]) for i in range(len(arr))]


def nullable_join_multiset(value: dict[int, Any]) -> Counter[NJRow]:
    """The whole relational result as one null-aware multiset over ALL blocks (partitioning-
    independent — the join RESULT does not depend on how it is partitioned)."""
    total: Counter[NJRow] = Counter()
    for block in value.values():
        ks = read_nullable_column(block["__joinkey__"])
        lv = read_nullable_column(block["lval"])
        rv = read_nullable_column(block["rval"])
        total += Counter(zip(ks, lv, rv, strict=True))
    return total
