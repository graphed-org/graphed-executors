"""Shared harness for the m43 frozen suite: shuffle/join on dask as a NATIVE future graph
(dask-parallel-backends-plan §1.3.4, §2 m43, §3 m43) + the two carried m42-review follow-ups
(F2a adaptive monitor drain, F2b adaptive KilledWorker attribution).

Pinned execution contract (test-author decision — the implementer builds
``graphed_executors.dask_backend.shuffle`` to this; see the m43 README):

    dask_run_repartition(backend, src_blocks, parts, *, runner, salt=0) -> ShuffleResult
    dask_run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",), how="inner",
                  runner, broadcast=None, salt=0, mem_budget_bytes=None) -> ShuffleResult

``runner`` is a ``graphed_executors.submit.SubmitRunner``; ALL task futures go through
``runner.backend.submit`` (the §1.1 seam — the structure suite counts them there). The result
REUSES the local ``ShuffleResult`` shape (``value``/``dest_block_hashes`` keyed exactly as the
local engine keys them) but carries a NEW ``DaskShuffleWitness`` whose counters name only
mechanisms that exist under dask — the announcement/manifest/steal counters are RETIRED and must
be ABSENT (§1.3.4 design question f).

Two disciplines carried from the m42 harness (duplicated here with distinct names — frozen suites
never import each other cross-directory):

1. **Spawn safety.** Everything a worker may unpickle is MODULE-LEVEL in this file.
2. **Deferred implementation imports.** ``graphed_executors.dask_backend.shuffle`` is imported only
   through ``shuffle_api()``/the ``dask_repartition``/``dask_join`` wrappers inside test bodies, so
   pre-implementation the suite COLLECTS cleanly and every shuffle test FAILS in its body with the
   right-reason ``ModuleNotFoundError`` (TEST_SANITY non-vacuity). The two F2 follow-up tests
   instead exercise the EXISTING engine and fail on their specific behavioral assertions.

All witnesses are counters, content hashes, pids, and worker addresses — never clocks (R0.10a);
the one ``delay_s`` in :class:`DelayedEventBackend` and the gate in :class:`GatedShuffleBackend`
are scenario construction (they model transport latency / hold a stage open), never assertions.
"""

from __future__ import annotations

import contextlib
import functools
import os
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
import pytest

from graphed_executors.submit import SubmitRunner
from graphed_executors.submit.protocol import SubmitCapabilities

# a joined output row, backend-independent: (key, left value, right value)
JRow = tuple[int, int, int]
# a null-aware joined row (non-inner arms): key is always coalesced, an absent side's value is None
NJRow = tuple[int | None, int | None, int | None]

# ---- deferred accessors for the implementation under test ---------------------------------------


def shuffle_api() -> Any:
    """``graphed_executors.dask_backend.shuffle`` — absent pre-implementation: the right-reason
    ``ModuleNotFoundError`` for every shuffle-theme test lands here."""
    import graphed_executors.dask_backend.shuffle as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def dask_repartition(*args: Any, **kwargs: Any) -> Any:
    """``dask_run_repartition(backend, src_blocks, parts, *, runner, salt=0)`` (plan §2 m43 pin)."""
    return shuffle_api().dask_run_repartition(*args, **kwargs)


def dask_join(*args: Any, **kwargs: Any) -> Any:
    """``dask_run_join(backend, left, right, parts, *, on=("__joinkey__",), how="inner", runner,
    broadcast=None, salt=0, mem_budget_bytes=None)`` (plan §2 m43 pin)."""
    return shuffle_api().dask_run_join(*args, **kwargs)


def build_dask_backend(client: Any) -> Any:
    """A plain ``DaskBackend`` over a ready client (constructor already frozen by the m42 suite)."""
    from graphed_executors.dask_backend import DaskBackend  # noqa: PLC0415  (deferred: dask extra)

    return DaskBackend(client)


def install_worker_plugin(client: Any) -> None:
    """Register the m42 ``GraphedWorkerPlugin`` (idempotent) — required by the DaskBackend shim."""
    from graphed_executors.dask_backend.plugin import GraphedWorkerPlugin  # noqa: PLC0415  (deferred)

    client.register_plugin(GraphedWorkerPlugin())


def make_submit_runner(backend: Any, *, monitor: Any = None, retries: int = 3) -> SubmitRunner:
    return SubmitRunner(backend, monitor=monitor, retries=retries)


# ---- LocalCluster fixture discipline (plan §3: tier A processes=False, tier B processes=True) ----


@contextmanager
def shuffle_cluster(
    n_workers: int = 2, *, processes: bool = True, allowed_failures: int | None = None
) -> Iterator[Any]:
    """Context-managed LocalCluster + Client, 1 thread per worker, random dashboard port.
    ``allowed_failures`` sets ``distributed.scheduler.allowed-failures`` BEFORE the scheduler is
    built (worker-death modules configure it on a dedicated cluster)."""
    import dask  # noqa: PLC0415  (dask-touching modules call this after their importorskip)
    import distributed  # noqa: PLC0415

    overrides: dict[str, object] = {}
    if allowed_failures is not None:
        overrides["distributed.scheduler.allowed-failures"] = allowed_failures
    with (
        dask.config.set(overrides),
        distributed.LocalCluster(
            n_workers=n_workers,
            threads_per_worker=1,
            processes=processes,
            dashboard_address=":0",
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        yield client


def live_worker_pids(client: Any) -> dict[str, int]:
    """worker address -> pid for every live worker (the driver-vs-worker discrimination map)."""
    return dict(client.run(os.getpid))


def kill_worker(client: Any, address: str) -> None:
    """Hard-kill ONE worker process by address (``os._exit`` on its event loop — validated against
    distributed 2026.7: the dead-comm error is ignored and the nanny restarts a fresh process)."""
    with contextlib.suppress(Exception):  # the RPC dies with its target; on_error guards the rest
        client.run(os._exit, 1, workers=[address], on_error="ignore")


def poll_until(predicate: Callable[[], bool], timeout_s: float = 90.0) -> None:
    """Bounded driver-side poll; assertions on the state happen AFTER (fixture discipline, never an
    assertion — R0.10a)."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


def task_site() -> tuple[int, str]:
    """(pid, worker identity) at the point of call — dask worker address inside a worker, else the
    driver marker (used by the gated backend's marks, mirrors the m42 ``where`` witness)."""
    try:
        from distributed import get_worker  # noqa: PLC0415  (absent or outside a worker -> driver)

        return os.getpid(), str(get_worker().address)
    except Exception:
        return os.getpid(), "driver"


# ---- real-backend adapters (the m39/m40 a2 discipline: BOTH backends, new basenames) ------------


@dataclass
class ShuffleJoinAdapter:
    name: str
    backend: object
    # exchange half (m39 style): keys -> a block carrying __joinkey__ + a v payload column
    make_block: Callable[[Sequence[int]], object]
    keys_of: Callable[[object], list[int]]
    # join half (m40 style): (keys, value_field, values) -> a 2-column record block
    make_side: Callable[[Sequence[int], str, Sequence[int]], object]
    side_rows: Callable[[object, str], list[tuple[int, int]]]
    joined_rows: Callable[[object], list[JRow]]


def _awkward_shuffle_adapter() -> ShuffleJoinAdapter:
    ak = pytest.importorskip("awkward")
    from graphed.awkward import AwkwardBackend  # noqa: PLC0415

    def make_block(keys: Sequence[int]) -> object:
        return ak.Array(
            {
                "__joinkey__": np.array(list(keys), dtype=np.uint64),
                "v": np.arange(len(keys), dtype=np.int64),
            }
        )

    def keys_of(block: object) -> list[int]:
        return [int(k) for k in block["__joinkey__"].to_list()]

    def make_side(keys: Sequence[int], field: str, values: Sequence[int]) -> object:
        return ak.Array(
            {
                "__joinkey__": np.array(list(keys), dtype=np.uint64),
                field: np.array(list(values), dtype=np.int64),
            }
        )

    def side_rows(block: object, field: str) -> list[tuple[int, int]]:
        ks = [int(k) for k in block["__joinkey__"].to_list()]
        vs = [int(v) for v in block[field].to_list()]
        return list(zip(ks, vs, strict=True))

    def joined_rows(block: object) -> list[JRow]:
        ks = [int(k) for k in block["__joinkey__"].to_list()]
        lv = [int(v) for v in block["lval"].to_list()]
        rv = [int(v) for v in block["rval"].to_list()]
        return list(zip(ks, lv, rv, strict=True))

    return ShuffleJoinAdapter(
        "awkward", AwkwardBackend(), make_block, keys_of, make_side, side_rows, joined_rows
    )


def _numpy_shuffle_adapter() -> ShuffleJoinAdapter:
    from graphed.numpy import NumpyBackend  # noqa: PLC0415

    block_dt = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])

    def make_block(keys: Sequence[int]) -> object:
        block = np.zeros(len(keys), dtype=block_dt)
        block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
        block["v"] = np.arange(len(keys), dtype=np.int64)
        return block

    def keys_of(block: object) -> list[int]:
        return [int(k) for k in block["__joinkey__"]]

    def make_side(keys: Sequence[int], field: str, values: Sequence[int]) -> object:
        dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
        block = np.zeros(len(keys), dtype=dt)
        block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
        block[field] = np.array(list(values), dtype=np.int64)
        return block

    def side_rows(block: object, field: str) -> list[tuple[int, int]]:
        return list(
            zip([int(k) for k in block["__joinkey__"]], [int(v) for v in block[field]], strict=True)
        )

    def joined_rows(block: object) -> list[JRow]:
        return list(
            zip(
                [int(k) for k in block["__joinkey__"]],
                [int(v) for v in block["lval"]],
                [int(v) for v in block["rval"]],
                strict=True,
            )
        )

    return ShuffleJoinAdapter(
        "numpy", NumpyBackend(), make_block, keys_of, make_side, side_rows, joined_rows
    )


def shuffle_adapters() -> list[ShuffleJoinAdapter]:
    out: list[ShuffleJoinAdapter] = []
    for factory in (_awkward_shuffle_adapter, _numpy_shuffle_adapter):
        with contextlib.suppress(Exception):  # a backend not installed drops from the parametrization
            out.append(factory())
    return out


# ---- scenario builders --------------------------------------------------------------------------


def repartition_blocks(adapter: ShuffleJoinAdapter, copies: int = 1) -> list[object]:
    """The m39-style multi-src scenario: 6 source blocks with overlapping key populations, so a
    P-way hash coalesces several dests per producer task and several src blocks feed each dest.
    ``copies`` repeats each key list in place — MORE ROWS, SAME BLOCK COUNT (the row-count-
    independence knob for the structure gates)."""
    key_lists = [
        [0, 1, 2, 3, 4, 5, 6, 7],
        [7, 6, 5, 4, 3, 2, 1, 0],
        [10, 11, 12, 13, 10, 11, 12, 13],
        [100, 200, 300, 400],
        [1, 1, 2, 2, 3, 3],
        [42, 43, 44, 45, 46, 47, 48, 49],
    ]
    return [adapter.make_block(list(keys) * copies) for keys in key_lists]


def join_general_sides(adapter: ShuffleJoinAdapter) -> tuple[list[object], list[object]]:
    """Multi-block, both-large sides with overlapping keys PLUS a left-only key (13) and a
    right-only key (17) — so the non-inner arms have real unmatched rows on BOTH sides (the m40
    F1/F3 discrimination re-armed on dask)."""
    left = [
        adapter.make_side([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
        adapter.make_side([5, 4, 3, 2, 1, 0], "lval", [16, 17, 18, 19, 20, 21]),
        adapter.make_side([1, 1, 2, 2, 3, 3], "lval", [22, 23, 24, 25, 26, 27]),
        adapter.make_side([7, 8, 9, 0, 13, 13], "lval", [28, 29, 30, 31, 32, 33]),
    ]
    right = [
        adapter.make_side([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
        adapter.make_side([3, 3, 2, 1, 1, 4], "rval", [106, 107, 108, 109, 110, 111]),
        adapter.make_side([9, 8, 7, 2, 17, 17], "rval", [112, 113, 114, 115, 116, 117]),
    ]
    return left, right


def join_equal_sides(adapter: ShuffleJoinAdapter) -> tuple[list[object], list[object]]:
    """Byte-comparable sides: with ``parts=8`` the pinned rule says SHUFFLE
    (``build*8 >= build+probe``), while a live-``n_workers()`` recompute on a 1-worker cluster says
    BROADCAST (``build*1 < build+probe`` always) — the F6 plan-choice discrimination gap."""
    left = [
        adapter.make_side([0, 1, 2, 3, 4, 5, 6, 7], "lval", [10, 11, 12, 13, 14, 15, 16, 17]),
        adapter.make_side([8, 9, 10, 11, 0, 1, 2, 3], "lval", [18, 19, 20, 21, 22, 23, 24, 25]),
    ]
    right = [
        adapter.make_side([0, 2, 4, 6, 8, 10, 1, 3], "rval", [100, 101, 102, 103, 104, 105, 106, 107]),
        adapter.make_side([5, 7, 9, 11, 0, 2, 4, 6], "rval", [108, 109, 110, 111, 112, 113, 114, 115]),
    ]
    return left, right


def join_small_build_sides(adapter: ShuffleJoinAdapter) -> tuple[list[object], list[object]]:
    """A tiny build side + a larger multi-block probe side, so the pinned rule prefers BROADCAST at
    the tested ``parts`` — the forced ``broadcast=False`` honor test runs against the model."""
    left = [adapter.make_side([1, 2, 3], "lval", [10, 20, 30])]
    right = [
        adapter.make_side([1, 1, 2, 3, 3, 3], "rval", [100, 101, 200, 300, 301, 302]),
        adapter.make_side([2, 2, 1, 3, 1, 2], "rval", [201, 202, 102, 303, 103, 203]),
        adapter.make_side([3, 1, 2, 3, 2, 1], "rval", [304, 104, 204, 305, 205, 105]),
    ]
    return left, right


def join_hot_key_sides(
    adapter: ShuffleJoinAdapter, n_left: int, n_right: int
) -> tuple[list[object], list[object]]:
    """A single hot key (all rows key=7 -> one dest): the inner join emits ``n_left*n_right``
    duplicated OUTPUT rows in one dest — the B5 output-side blowup the budget must cover."""
    left = [adapter.make_side([7] * n_left, "lval", list(range(n_left)))]
    right = [adapter.make_side([7] * n_right, "rval", list(range(100, 100 + n_right)))]
    return left, right


def side_bytes(adapter: ShuffleJoinAdapter, blocks: Sequence[object]) -> int:
    be: Any = adapter.backend
    return sum(int(be.estimated_bytes(b)) for b in blocks)


# ---- test-authored oracles (route via the golden-pinned ``partition``, never the join kernel) ----


def route_oracle_dest_keys(
    adapter: ShuffleJoinAdapter, src_blocks: Sequence[object], parts: int
) -> dict[int, list[int]]:
    """Per dest, the ``__joinkey__`` values routed there in ascending-src, within-src order — built
    from the backend's own golden-pinned ``partition`` (routing is NOT re-implemented here)."""
    be: Any = adapter.backend
    per_dest: dict[int, list[int]] = {}
    for src in src_blocks:  # ascending src index
        for dest, sub in enumerate(be.partition(src, "__joinkey__", parts)):
            keys = adapter.keys_of(sub)
            if keys:
                per_dest.setdefault(dest, []).extend(keys)
    return per_dest


def result_dest_keys(adapter: ShuffleJoinAdapter, value: dict[int, object]) -> dict[int, list[int]]:
    return {dest: adapter.keys_of(block) for dest, block in value.items()}


def total_result_rows(value: Mapping[int, object]) -> int:
    return sum(len(block) for block in value.values())  # type: ignore[arg-type]


def _per_dest_side(
    adapter: ShuffleJoinAdapter, blocks: Sequence[object], field: str, parts: int
) -> dict[int, list[tuple[int, int]]]:
    be: Any = adapter.backend
    out: dict[int, list[tuple[int, int]]] = {}
    for block in blocks:  # ascending src index
        for dest, sub in enumerate(be.partition(block, "__joinkey__", parts)):
            rows = adapter.side_rows(sub, field)
            if rows:
                out.setdefault(dest, []).extend(rows)
    return out


def dup_join_per_dest(
    adapter: ShuffleJoinAdapter, left: Sequence[object], right: Sequence[object], parts: int
) -> dict[int, Counter[JRow]]:
    """DUPLICATING relational inner join per dest as a multiset (the m40 §3.3 pin): a probe row
    with k build matches ⇒ k rows. Routes each side with the backend's ``partition`` only — the
    executor cannot share a join-kernel bug with this oracle."""
    left_by_dest = _per_dest_side(adapter, left, "lval", parts)
    right_by_dest = _per_dest_side(adapter, right, "rval", parts)
    per_dest: dict[int, Counter[JRow]] = {}
    for dest in set(left_by_dest) & set(right_by_dest):
        rows = [
            (k, lv, rv) for (k, lv) in left_by_dest[dest] for (k2, rv) in right_by_dest[dest] if k == k2
        ]
        if rows:
            per_dest[dest] = Counter(rows)
    return per_dest


def dup_join_all(
    adapter: ShuffleJoinAdapter, left: Sequence[object], right: Sequence[object], parts: int
) -> Counter[JRow]:
    total: Counter[JRow] = Counter()
    for c in dup_join_per_dest(adapter, left, right, parts).values():
        total += c
    return total


def observed_join_per_dest(
    adapter: ShuffleJoinAdapter, value: dict[int, object]
) -> dict[int, Counter[JRow]]:
    out: dict[int, Counter[JRow]] = {}
    for part, block in value.items():
        rows = adapter.joined_rows(block)
        if rows:
            out[part] = Counter(rows)
    return out


def observed_join_all(adapter: ShuffleJoinAdapter, value: dict[int, object]) -> Counter[JRow]:
    total: Counter[JRow] = Counter()
    for block in value.values():
        total += Counter(adapter.joined_rows(block))
    return total


# ---- null-aware readers + the pandas oracle (m40 non-inner pattern) -----------------------------


def read_nullable_column(c: object) -> list[int | None]:
    """One joined column to Python ints with ``None`` for a null/masked entry — survives the
    option/masked rows the non-inner arms produce (a plain ``int()`` reader would crash)."""
    to_list = getattr(c, "to_list", None)
    if to_list is not None:  # awkward option array -> [.., None, ..]
        return [None if v is None else int(v) for v in to_list()]
    arr = np.ma.asanyarray(c)  # numpy masked (or plain) structured field
    return [None if arr[i] is np.ma.masked else int(arr[i]) for i in range(len(arr))]


def nullable_join_multiset(value: dict[int, object]) -> Counter[NJRow]:
    """The whole relational result as one null-aware multiset over ALL blocks (partitioning-
    independent — the join RESULT does not depend on how it is partitioned)."""
    total: Counter[NJRow] = Counter()
    for block in value.values():
        ks = read_nullable_column(block["__joinkey__"])  # type: ignore[index]
        lv = read_nullable_column(block["lval"])  # type: ignore[index]
        rv = read_nullable_column(block["rval"])  # type: ignore[index]
        total += Counter(zip(ks, lv, rv, strict=True))
    return total


def pandas_join_oracle(
    lk: list[int], lv: list[int], rk: list[int], rv: list[int], how: str
) -> Counter[NJRow]:
    """``pandas.merge`` on ``__joinkey__``: duplicating, null-preserving, key-coalescing — a wholly
    independent relational engine (no shared code with the kernels under test)."""
    import pandas as pd  # noqa: PLC0415  (optional oracle dep: only the relational module needs it)

    left = pd.DataFrame({"__joinkey__": lk, "lval": lv})
    right = pd.DataFrame({"__joinkey__": rk, "rval": rv})
    merged = pd.merge(left, right, on="__joinkey__", how=how)
    out: Counter[NJRow] = Counter()
    for _, row in merged.iterrows():
        key = int(row["__joinkey__"])  # coalesced: the merged key column never carries NaN here
        lval = None if pd.isna(row["lval"]) else int(row["lval"])
        rval = None if pd.isna(row["rval"]) else int(row["rval"])
        out[(key, lval, rval)] += 1
    return out


# ---- GatedShuffleBackend: mark stage-1 executions, hold stage-2 open (worker-death scenario) -----


class GatedShuffleBackend:
    """A delegating ``ShuffleBackend`` for the worker-death theme. ``partition`` (stage-1 only)
    drops a ``pmark-*`` file per call recording ``pid\\naddress`` — the RE-EXECUTION witness (more
    pmarks than src blocks proves a lost producer re-ran). ``from_wire`` (stage-2 only under
    repartition) drops a ``gstart-*`` file, then BLOCKS until ``gate_path`` exists — holding the
    gather open so the driver can kill a stage-1 holder mid-run deterministically (scenario
    construction; the assertions are file counts and content hashes, R0.10a). Data is delegated
    UNTOUCHED, so hashes stay comparable with a plain-backend local oracle run."""

    def __init__(self, inner: Any, mark_dir: str, gate_path: str | None) -> None:
        self.inner = inner
        self.mark_dir = mark_dir
        self.gate_path = gate_path
        self.identity = f"gated+{inner.identity}"

    def _mark(self, prefix: str) -> None:
        pid, addr = task_site()
        tmp = Path(self.mark_dir) / f".{prefix}-{uuid.uuid4().hex}"
        tmp.write_text(f"{pid}\n{addr}")
        os.replace(tmp, Path(self.mark_dir) / f"{prefix}-{uuid.uuid4().hex}")  # atomic publish

    def partition(self, block: Any, key_field: str, parts: int, **kwargs: Any) -> tuple[Any, ...]:
        self._mark("pmark")
        return tuple(self.inner.partition(block, key_field, parts, **kwargs))

    def from_wire(self, data: bytes) -> Any:
        if self.gate_path is not None:
            self._mark("gstart")
            deadline = time.monotonic() + 120.0
            while not os.path.exists(self.gate_path):
                if time.monotonic() > deadline:
                    raise RuntimeError("gate never opened — worker-death scenario misfired")
                time.sleep(0.05)
        return self.inner.from_wire(data)

    def concat(self, blocks: Sequence[Any]) -> Any:
        return self.inner.concat(blocks)

    def slice_rows(self, block: Any, start: int, stop: int) -> Any:
        return self.inner.slice_rows(block, start, stop)

    def estimated_bytes(self, block_or_form: object) -> int:
        return int(self.inner.estimated_bytes(block_or_form))

    def to_wire(self, block: Any) -> bytes:
        return bytes(self.inner.to_wire(block))


def mark_files(mark_dir: str, prefix: str) -> list[Path]:
    return sorted(p for p in Path(mark_dir).iterdir() if p.name.startswith(f"{prefix}-"))


def mark_addresses(mark_dir: str, prefix: str) -> list[str]:
    return [p.read_text().splitlines()[1] for p in mark_files(mark_dir, prefix)]


# ---- SpySubmitBackend: the seam tap (structure/plan-choice futures counting) --------------------


def _fn_name(fn: Any) -> str:
    while isinstance(fn, functools.partial):
        fn = fn.func
    return str(getattr(fn, "__name__", type(fn).__name__))


class SpySubmitBackend:
    """A delegating ``SubmitBackend`` recording every submitted task fn's name + key and every
    broadcast token — the §1.3.4 future-count gates read these (counts, never clocks)."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.submitted: list[tuple[str, str]] = []  # (fn name, key)
        self.broadcast_tokens: list[str] = []
        self.cancel_batches: list[int] = []

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
        self.submitted.append((_fn_name(fn), key))
        return self.inner.submit(
            fn, *args, key=key, retries=retries, priority=priority, resources=resources
        )

    def submitted_fn_names(self) -> Counter[str]:
        return Counter(name for name, _key in self.submitted)

    def broadcast(self, payload: bytes, *, token: str) -> Any:
        self.broadcast_tokens.append(token)
        return self.inner.broadcast(payload, token=token)

    def subscribe_events(self, topic: str, handler: Any) -> Any:
        return self.inner.subscribe_events(topic, handler)

    def cancel(self, futures: Sequence[Any]) -> None:
        futures = list(futures)
        self.cancel_batches.append(len(futures))
        self.inner.cancel(futures)

    def close(self) -> None:
        self.inner.close()

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        describe = getattr(self.inner, "describe_failure", None)
        return None if describe is None else describe(exc)


# ---- NoPeerBackend: the capability-gate stub (peer_data_movement=False, must receive NO work) ----


class NoPeerBackend:
    """A protocol-shaped stub modelling a head-node-routed backend (parsl-HTEX class): the §1.3.4
    gate must fire BEFORE any work reaches it — ``submit``/``broadcast`` count and refuse."""

    capabilities = SubmitCapabilities(
        peer_data_movement=False,
        scatter_broadcast=False,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=False,
        worker_file_cache=False,
    )

    def __init__(self) -> None:
        self.submit_attempts = 0
        self.broadcast_attempts = 0

    def n_workers(self) -> int:
        return 1

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        self.submit_attempts += 1
        raise AssertionError("the capability gate must refuse BEFORE submitting any work")

    def broadcast(self, payload: bytes, *, token: str) -> object:
        self.broadcast_attempts += 1
        raise AssertionError("the capability gate must refuse BEFORE broadcasting any payload")

    def subscribe_events(self, topic: str, handler: Any) -> Callable[[], None]:
        return lambda: None

    def cancel(self, futures: Sequence[Any]) -> None:
        return None

    def close(self) -> None:
        return None


# ---- DelayedEventBackend: deterministic async-transport model (F2a adaptive monitor drain) ------


class DelayedEventBackend:
    """A delegating ``SubmitBackend`` whose event DELIVERY lags emission by ``delay_s`` and whose
    unsubscribe DROPS undelivered events — the honest model of dask's asynchronous worker->driver
    event transport (an unsubscribed topic never delivers). Against it, an engine that returns from
    ``run()`` without draining trailing events loses them DETERMINISTICALLY: the driver path from
    the last result to the unsubscribe is microseconds against a ``delay_s`` of hundreds of
    milliseconds. The delay is scenario construction; every assertion is an event-set count
    (R0.10a)."""

    def __init__(self, inner: Any, delay_s: float = 0.5) -> None:
        self.inner = inner
        self.delay_s = delay_s
        self._lock = threading.Lock()
        self._dead_topics: set[str] = set()
        self._timers: list[threading.Timer] = []

    @property
    def capabilities(self) -> Any:
        return self.inner.capabilities

    def n_workers(self) -> int:
        return int(self.inner.n_workers())

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        return self.inner.submit(fn, *args, **kwargs)

    def broadcast(self, payload: bytes, *, token: str) -> object:
        return self.inner.broadcast(payload, token=token)

    def subscribe_events(self, topic: str, handler: Any) -> Callable[[], None]:
        def deliver(events: list[dict[str, object]]) -> None:
            with self._lock:
                if topic in self._dead_topics:
                    return  # unsubscribed: the transport will never deliver these
            handler(events)

        def delayed(events: list[dict[str, object]]) -> None:
            timer = threading.Timer(self.delay_s, deliver, args=(events,))
            timer.daemon = True
            with self._lock:
                self._timers.append(timer)
            timer.start()

        inner_unsub = self.inner.subscribe_events(topic, delayed)

        def unsub() -> None:
            with self._lock:
                self._dead_topics.add(topic)
                timers = list(self._timers)
            for t in timers:
                t.cancel()
            inner_unsub()

        return unsub

    def cancel(self, futures: Sequence[Any]) -> None:
        self.inner.cancel(futures)

    def close(self) -> None:
        self.inner.close()

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        describe = getattr(self.inner, "describe_failure", None)
        return None if describe is None else describe(exc)


# ---- F2 adaptive scenarios (duplicated from the m42 harness style, distinct names) --------------


def f2_partitions(n: int, tag: str) -> tuple[Any, ...]:
    from graphed.core.execution import Partition  # noqa: PLC0415  (kept local: harness stays light)

    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def adaptive_leaf_value(partition: Any, resources: object) -> int:
    return 7 * int(partition.entry_start) + 3


def add_ints(a: int, b: int) -> int:
    return a + b


def int_zero() -> int:
    return 0


@dataclass(frozen=True)
class AlwaysDieProcess:
    """Hard-kills its worker process on EVERY attempt at the poison uri (segfault/OOM stand-in) —
    with ``retries=0`` + a low allowed-failures budget the scheduler deterministically blames the
    task and the driver sees the backend's worker-death signal."""

    poison_uri: str

    def __call__(self, partition: Any, resources: object) -> int:
        if partition.uri == self.poison_uri:
            os._exit(1)
        return adaptive_leaf_value(partition, resources)


class BatchFeed:
    """Driver-side ``next_tasks``: hands out the given batches then ``None`` (DONE)."""

    def __init__(self, batches: Sequence[Sequence[Any]]) -> None:
        self.batches = [list(b) for b in batches]

    def __call__(self, ctx: Any) -> list[Any] | None:
        return self.batches.pop(0) if self.batches else None


class EventRecordingMonitor:
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


def f2_events_by_key(events: Sequence[Any]) -> dict[int, list[Any]]:
    out: dict[int, list[Any]] = {}
    for ev in events:
        out.setdefault(ev.key, []).append(ev)
    return out
