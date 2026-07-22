"""Shared harness for the m44 frozen suite: graphed's M38 ``WorkerTransport`` implemented ATOP
dask's worker-to-worker comm layer (m44-transport-plan §1), hosting the M38 peer reduction and the
M39-M41 shuffle/join engine on dask workers with an O(T+P) scheduler graph (no T*P pick tier).

Pinned execution contract (test-author decision — the implementer builds these surfaces; see the
m44 README for the clause-by-clause traceability):

Module ``graphed_executors.dask_backend.transport`` (plan §1.1, §1.5, §1.7 / Impl Target 1):

- ``GraphedTransportPlugin`` — a real ``distributed.WorkerPlugin`` with class attrs
  ``name = "graphed-transport"`` and ``idempotent = True`` (the m42 ``GraphedWorkerPlugin`` idiom,
  so a harness pre-registration and the engine's own registration coexist as ONE instance and a
  test-set injection seam survives into the run). ``setup`` registers the worker handlers
  ``graphed_transport_recv(src, epoch, data)`` / ``graphed_block_pull(epoch, digests)`` /
  ``graphed_transport_ping(nonce)`` and publishes itself at
  ``worker.extensions["graphed-transport"]``. Public plugin surface:
    * ``canary_ok: bool`` — True only after the §1.7 setup canary (self-RPC ping) succeeded;
    * ``stale_epoch_rejects: int`` — plugin-lifetime count of recv/pull messages rejected for an
      unknown or purged epoch (the P2P run_id guard);
    * ``inject_recv_failures: dict[str, int]`` — the §1.1 gated test seam: while the budget for a
      src address is > 0, ``graphed_transport_recv`` from that src DELIVERS the live-epoch message
      into the inbox normally, decrements the budget, then raises ``OSError`` — so the sender sees
      a comm-class failure, the REAL retry classifier engages, and the retried send produces a
      genuine at-least-once duplicate delivery (F10);
    * ``active_epochs() -> tuple[str, ...]`` — epochs with live per-run state (purge witness);
    * ``counters(epoch) -> Mapping[str, int]`` — per-epoch receiver-side witness counters (an
      unknown epoch returns an empty mapping) with AT LEAST the keys ``recv_invocations``,
      ``recv_on_loop`` (recv-handler invocations executed on the worker's event-loop thread),
      ``recv_duplicate_deliveries`` (deliveries whose exact payload bytes were already delivered
      in this epoch — content-digest keyed), ``sends_dropped`` (inbox-full rejections),
      ``bytes_served`` (block-plane bytes served via ``graphed_block_pull``), ``serve_pid``
      (os.getpid() of the serving process; 0 if this worker never served).
- ``make_transport_spec(epoch, worker_addresses, *, inbox_maxsize=None, overlay=None) -> spec`` —
  a PICKLABLE spec (epoch nonce, the ordered worker-address tuple, per-address outbox overlay,
  inbox size); ``overlay=None`` means every worker may send to every other worker and to
  ``"driver"`` (the engine passes its own bounded ``worker_outbox_addresses`` overlay).
- ``open_endpoint(spec) -> WorkerTransport`` — worker-task-side construction from
  ``get_worker().extensions["graphed-transport"]`` + the spec; idempotent per (worker, epoch):
  re-opening attaches to the SAME per-epoch inbox/state, so a later task can drain what an
  earlier task's peer sent.
- ``open_driver_endpoint(backend, spec) -> WorkerTransport`` — the reserved ``"driver"`` endpoint
  in the client process: ``address == "driver"``, ``peers() == spec worker addresses``; fed by
  ``backend.subscribe_events`` on the run topic ``graphed-transport-<epoch>``; ``broadcast`` is
  the reliable driver->all path (§1.1.3).
- ``graphed_transport_recv`` reply shape: ``{"accepted": True}`` on delivery,
  ``{"accepted": False}`` on inbox-full, ``{"accepted": False, "stale": True}`` for an unknown or
  purged epoch. ``send`` retry policy (§1.1 r2/r3): total attempt budget ``SEND_RETRIES = 5`` per
  send; a live handler answering ``accepted: False`` is the contract drop signal; comm-class
  failures are retried; exhaustion RAISES ``TransportDeliveryError`` (never a silent False).

Entry points (Impl Targets 2 and 5; signatures mirror the local engine minus
comms/store_root/faults/steal, plan §1.4 — the budget knobs resolve the plan's "budgets..." to the
local names, plus the F12 holder-plane knob):

    transport_run_plan(plan, backend, *, monitor=None, inbox_maxsize=None,
                       epoch_restarts_allowed=1) -> ExecResult-shaped result
    transport_run_repartition(backend, src_blocks, parts, *, dbackend, salt=0, n_tasks=None,
                              fetch_budget_bytes=None, disk_budget_bytes=None,
                              holder_budget_bytes=None, epoch_restarts_allowed=1) -> result
    transport_run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",),
                       how="inner", dbackend, broadcast=None, salt=0, mem_budget_bytes=None,
                       epoch_restarts_allowed=1) -> result

- Shuffle results carry the LOCAL engine's shape — ``.dest_block_hashes`` / ``.value`` /
  ``.witness`` (the imported ``ShuffleWitness`` counter names: every m39/m40/m41 counter keeps
  naming the same mechanism) — plus ``.transport`` (below). ``transport_run_plan`` returns an
  ``ExecResult``-shaped result (``.value``/``.n_partitions``/``.n_combines``) plus ``.transport``.
- ``result.transport`` (the m44 transport-plane witness, aggregated driver-side from task returns
  and the per-worker plugin counters):
    * ``epoch_nonces: Sequence[str]`` — the run's epochs in order; ``len == 1 + epoch_restarts``;
    * ``epoch_restarts: int`` — §1.5 whole-run restarts actually performed;
    * ``per_worker: Mapping[addr, Mapping[str, int]]`` — per worker address, merging the plugin
      counters above with the sender-endpoint counters ``sends_retried`` (retry attempts made by
      that worker's endpoint) and ``peer_sends`` (accepted sends to a non-driver peer), and the
      F12 holder-plane counters ``holder_spill_count`` / ``peak_holder_bytes``. Tests read keys
      with ``.get(key, 0)`` — richer mappings are fine, absent mechanisms are not.
- Scheduler graph (§1.4, F11): ALL engine tasks go through ``dbackend.submit`` (the m42 seam) as
  the pinned MODULE-LEVEL task fns ``_transport_map_task`` (T, strict-pinned to
  ``sorted_addrs[t % k]``), ``_transport_gather_task`` (P), ``_transport_gather_join`` /
  ``_transport_broadcast_join_part`` (join twins), ``_dask_peer_main`` (k peer actors,
  ``transport_run_plan``); every one strict-pinned (``workers=[one address]``,
  ``allow_other_workers=False`` at the dask layer). NO pick-shaped tier exists.
- Capability gate (§1.6): every entry point checks
  ``dbackend.capabilities.pin_to_worker and dbackend.capabilities.peer_data_movement`` FIRST and
  raises ``NotImplementedError`` naming the missing strict worker-pinning requirement and the
  "Phase 2" store-plane alternative BEFORE any submit/broadcast reaches the backend.
- Failure semantics (§1.5, F2 pinned empirically): worker-death runs are driven on clusters with
  ``distributed.scheduler.allowed-failures: 0`` — measured on distributed 2026.7.1: a
  strict-pinned task whose worker is hard-killed then raises ``distributed.scheduler.KilledWorker``
  promptly (<1 s) with ``last_worker`` = the victim; under the DEFAULT allowed-failures the future
  never errors (observed 90 s hang) because the pin can never be satisfied again — which is why
  every death/hang-risk test here carries a hard thread-join timeout (F2).

Two disciplines carried from the m42/m43 harnesses (duplicated with this module's own name —
frozen suites never import each other cross-directory): (1) everything a worker may unpickle is
MODULE-LEVEL here; (2) the implementation is imported ONLY through the deferred accessors below,
so pre-implementation the suite COLLECTS cleanly and every test FAILS in its body with the
right-reason ``ModuleNotFoundError`` (TEST_SANITY non-vacuity). All witnesses are counters,
content hashes, pids, thread ids, and worker addresses — never clocks (R0.10a); poll loops and
recv timeouts are scenario construction / hang guards, never assertions.
"""

from __future__ import annotations

import contextlib
import functools
import os
import threading
import time
import types
import uuid
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from graphed.core.execution import Partition, Plan, Task

from graphed_executors.submit.protocol import SubmitCapabilities

# the §1.1 send policy: total attempt budget per send (the local HttpTransport sender policy,
# graphed_executors/local/_transport.py:275-306, restated by plan §1.1 r2)
SEND_RETRIES = 5

# a joined output row, backend-independent: (key, left value, right value)
JRow = tuple[int, int, int]
# a null-aware joined row (non-inner arms): key always coalesced, an absent side's value is None
NJRow = tuple[int | None, int | None, int | None]

# ---- deferred accessors for the implementation under test ---------------------------------------


def transport_api() -> Any:
    """``graphed_executors.dask_backend.transport`` — absent pre-implementation: the right-reason
    ``ModuleNotFoundError`` for every transport-level test lands here."""
    import graphed_executors.dask_backend.transport as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def peer_api() -> Any:
    """``graphed_executors.dask_backend.transport_peer`` (Impl Target 2)."""
    import graphed_executors.dask_backend.transport_peer as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def shuffle44_api() -> Any:
    """``graphed_executors.dask_backend.transport_shuffle`` (Impl Target 5)."""
    import graphed_executors.dask_backend.transport_shuffle as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def transport_plan_run(*args: Any, **kwargs: Any) -> Any:
    """``transport_run_plan(plan, backend, *, monitor=None, inbox_maxsize=None,
    epoch_restarts_allowed=1)`` (contract pin above)."""
    return peer_api().transport_run_plan(*args, **kwargs)


def transport_repartition(*args: Any, **kwargs: Any) -> Any:
    """``transport_run_repartition(backend, src_blocks, parts, *, dbackend, ...)`` (contract pin)."""
    return shuffle44_api().transport_run_repartition(*args, **kwargs)


def transport_join(*args: Any, **kwargs: Any) -> Any:
    """``transport_run_join(backend, left, right, parts, *, dbackend, ...)`` (contract pin)."""
    return shuffle44_api().transport_run_join(*args, **kwargs)


def make_transport_spec(*args: Any, **kwargs: Any) -> Any:
    return transport_api().make_transport_spec(*args, **kwargs)


def open_driver_endpoint(backend: Any, spec: Any) -> Any:
    return transport_api().open_driver_endpoint(backend, spec)


def build_dask_backend(client: Any) -> Any:
    """A plain ``DaskBackend`` over a ready client (constructor already frozen by the m42 suite)."""
    from graphed_executors.dask_backend import DaskBackend  # noqa: PLC0415  (deferred: dask extra)

    return DaskBackend(client)


def install_transport_plugin(client: Any) -> None:
    """Register ``GraphedTransportPlugin`` (name='graphed-transport', idempotent) BEFORE a run so
    pinned micro-tasks can reach the plugin seam; the engine's own registration must then be the
    idempotent no-op that keeps this same instance alive (contract pin)."""
    client.register_plugin(transport_api().GraphedTransportPlugin())


# ---- LocalCluster fixture discipline (plan §3: tier A processes=False, tier B processes=True) ----


@contextmanager
def transport_cluster(
    n_workers: int = 2, *, processes: bool = True, allowed_failures: int | None = None
) -> Iterator[Any]:
    """Context-managed LocalCluster + Client, 1 thread per worker, random dashboard port.
    ``allowed_failures`` is set BEFORE the scheduler is built; the worker-death module pins it to 0
    (the F2-measured config under which a strict-pinned task on a dead worker errors PROMPTLY
    instead of hanging forever in no-worker state)."""
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


def sorted_worker_addresses(client: Any) -> tuple[str, ...]:
    """The F8 deterministic worker-address order every m44 ownership/pin decision is keyed on."""
    return tuple(sorted(client.scheduler_info()["workers"]))


def live_worker_pids(client: Any) -> dict[str, int]:
    """worker address -> pid for every live worker (the driver-vs-worker discrimination map)."""
    return dict(client.run(os.getpid))


def kill_worker(client: Any, address: str) -> None:
    """Hard-kill ONE worker process by address (the m43-validated ``os._exit`` idiom)."""
    with contextlib.suppress(Exception):  # the RPC dies with its target; on_error guards the rest
        client.run(os._exit, 1, workers=[address], on_error="ignore")


def poll_until(predicate: Callable[[], bool], timeout_s: float = 90.0) -> None:
    """Bounded driver-side poll; assertions on the state happen AFTER (never an assertion —
    R0.10a)."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


def run_bounded(fn: Callable[[], Any], timeout_s: float = 240.0) -> dict[str, Any]:
    """Run ``fn`` on a daemon thread with a HARD join timeout (F2): a hang surfaces as an
    AssertionError here, never as a hung test. Returns ``{"result": ...}`` or ``{"error": exc}``."""
    out: dict[str, Any] = {}

    def _drive() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    thread = threading.Thread(target=_drive, daemon=True)
    thread.start()
    thread.join(timeout_s)
    assert not thread.is_alive(), (
        f"HARD TIMEOUT (F2): the driven call did not finish within {timeout_s}s — under m44 "
        "failure semantics a worker death / lost message must surface as an error or an epoch "
        "restart, never a hang"
    )
    return out


# ---- pinned micro-tasks + client.run probes (the plugin seam) -----------------------------------


def pinned_call(client: Any, fn: Callable[..., Any], *args: Any, worker: str, timeout_s: float = 180.0) -> Any:
    """Run a module-level fn as a strict-pinned one-off task on ``worker`` and return its result
    (bounded — a hang surfaces as a TimeoutError, F2)."""
    future = client.submit(
        fn,
        *args,
        key=f"m44h-{getattr(fn, '__name__', 'fn')}-{uuid.uuid4().hex}",
        pure=False,
        retries=0,
        workers=[worker],
        allow_other_workers=False,
    )
    return future.result(timeout=timeout_s)


def _set_injection(src: str, n: int) -> bool:
    from distributed import get_worker  # noqa: PLC0415  (worker-side only)

    plugin = get_worker().extensions["graphed-transport"]
    plugin.inject_recv_failures[src] = int(n)
    return True


def set_inject_recv_failures(client: Any, worker: str, src: str, n: int) -> None:
    """Arm the §1.1 gated injection seam on ``worker``'s plugin for messages from ``src``."""
    assert pinned_call(client, _set_injection, src, n, worker=worker) is True


def probe_plugin(dask_worker: Any = None) -> dict[str, Any]:
    """client.run probe: plugin presence, canary, stale-reject count, registered graphed_* ops."""
    plugin = dask_worker.extensions.get("graphed-transport")
    return {
        "present": plugin is not None,
        "canary_ok": bool(getattr(plugin, "canary_ok", False)),
        "stale_epoch_rejects": int(getattr(plugin, "stale_epoch_rejects", 0)),
        "ops": sorted(op for op in dask_worker.handlers if str(op).startswith("graphed_")),
    }


def probe_active_epochs(dask_worker: Any = None) -> tuple[str, ...]:
    plugin = dask_worker.extensions.get("graphed-transport")
    return tuple(plugin.active_epochs()) if plugin is not None else ()


def probe_epoch_counters(epoch: str, dask_worker: Any = None) -> dict[str, int]:
    plugin = dask_worker.extensions.get("graphed-transport")
    return dict(plugin.counters(epoch)) if plugin is not None else {}


def freeze_worker_handlers(dask_worker: Any = None) -> bool:
    """The §1.7 drift scenario: make ``worker.handlers`` reject writes so the plugin's setup canary
    must fail LOUDLY at register time (scenario construction, run before registration)."""
    dask_worker.handlers = types.MappingProxyType(dict(dask_worker.handlers))
    return True


def fire_raw_recv(dest: str, epoch: str, payload: bytes) -> dict[str, Any]:
    """Worker-task-side: fire ONE raw ``graphed_transport_recv`` RPC at ``dest`` with an arbitrary
    epoch (the stale-guard probe) and return the handler's reply dict."""
    from distributed import get_worker  # noqa: PLC0415  (worker-side only)
    from distributed.utils import sync  # noqa: PLC0415

    worker = get_worker()

    async def _go() -> Any:
        return await worker.rpc(dest).graphed_transport_recv(
            src=worker.address, epoch=epoch, data=payload
        )

    return dict(sync(worker.loop, _go))


# ---- endpoint-level conformance task fns (module-level: spawn-safe) -----------------------------


def _ep(spec: Any) -> Any:
    from graphed_executors.dask_backend.transport import open_endpoint  # noqa: PLC0415  (worker-side)

    return open_endpoint(spec)


def conf_probe(spec: Any) -> dict[str, Any]:
    from graphed.core.execution import WorkerTransport  # noqa: PLC0415  (worker-side isinstance)

    ep = _ep(spec)
    return {
        "is_transport": isinstance(ep, WorkerTransport),
        "address": str(ep.address),
        "peers": tuple(ep.peers()),
        "empty_recv": ep.recv(timeout=0.05),
        "empty_poll": list(ep.poll()),
    }


def conf_send(spec: Any, dest: str, message: Any) -> bool:
    return bool(_ep(spec).send(dest, message))


def conf_recv(spec: Any, timeout_s: float) -> Any:
    return _ep(spec).recv(timeout=timeout_s)


def conf_poll(spec: Any) -> list[Any]:
    return list(_ep(spec).poll())


def conf_flood(spec: Any, dest: str, n: int) -> list[bool]:
    ep = _ep(spec)
    return [bool(ep.send(dest, ("flood", i))) for i in range(n)]


def conf_thread_probe(spec: Any, timeout_s: float) -> dict[str, Any]:
    """recv() from the TASK thread — must work off-loop and never run on the worker's IO loop."""
    from distributed import get_worker  # noqa: PLC0415  (worker-side only)

    worker = get_worker()
    ep = _ep(spec)
    got = ep.recv(timeout=timeout_s)
    return {"off_loop": threading.get_ident() != worker.thread_id, "got": got}


# ---- transport-witness helpers ------------------------------------------------------------------


def tw_per_worker(res: Any) -> dict[str, dict[str, int]]:
    return {addr: dict(counters) for addr, counters in dict(res.transport.per_worker).items()}


def tw_total(res: Any, key: str) -> int:
    return sum(int(counters.get(key, 0)) for counters in tw_per_worker(res).values())


# ---- real-backend adapters (the m39/m40 a2 discipline: BOTH backends, this suite's names) --------


@dataclass
class TransportAdapter:
    name: str
    backend: object
    make_block: Callable[[Sequence[int]], object]
    keys_of: Callable[[object], list[int]]
    make_side: Callable[[Sequence[int], str, Sequence[int]], object]
    side_rows: Callable[[object, str], list[tuple[int, int]]]
    joined_rows: Callable[[object], list[JRow]]


def _awkward_transport_adapter() -> TransportAdapter:
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
        return [int(k) for k in block["__joinkey__"].to_list()]  # type: ignore[index]

    def make_side(keys: Sequence[int], field: str, values: Sequence[int]) -> object:
        return ak.Array(
            {
                "__joinkey__": np.array(list(keys), dtype=np.uint64),
                field: np.array(list(values), dtype=np.int64),
            }
        )

    def side_rows(block: object, field: str) -> list[tuple[int, int]]:
        ks = [int(k) for k in block["__joinkey__"].to_list()]  # type: ignore[index]
        vs = [int(v) for v in block[field].to_list()]  # type: ignore[index]
        return list(zip(ks, vs, strict=True))

    def joined_rows(block: object) -> list[JRow]:
        ks = [int(k) for k in block["__joinkey__"].to_list()]  # type: ignore[index]
        lv = [int(v) for v in block["lval"].to_list()]  # type: ignore[index]
        rv = [int(v) for v in block["rval"].to_list()]  # type: ignore[index]
        return list(zip(ks, lv, rv, strict=True))

    return TransportAdapter(
        "awkward", AwkwardBackend(), make_block, keys_of, make_side, side_rows, joined_rows
    )


def _numpy_transport_adapter() -> TransportAdapter:
    from graphed.numpy import NumpyBackend  # noqa: PLC0415

    block_dt = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])

    def make_block(keys: Sequence[int]) -> object:
        block = np.zeros(len(keys), dtype=block_dt)
        block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
        block["v"] = np.arange(len(keys), dtype=np.int64)
        return block

    def keys_of(block: object) -> list[int]:
        return [int(k) for k in block["__joinkey__"]]  # type: ignore[index]

    def make_side(keys: Sequence[int], field: str, values: Sequence[int]) -> object:
        dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
        block = np.zeros(len(keys), dtype=dt)
        block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
        block[field] = np.array(list(values), dtype=np.int64)
        return block

    def side_rows(block: object, field: str) -> list[tuple[int, int]]:
        return list(
            zip([int(k) for k in block["__joinkey__"]], [int(v) for v in block[field]], strict=True)  # type: ignore[index]
        )

    def joined_rows(block: object) -> list[JRow]:
        return list(
            zip(
                [int(k) for k in block["__joinkey__"]],  # type: ignore[index]
                [int(v) for v in block["lval"]],  # type: ignore[index]
                [int(v) for v in block["rval"]],  # type: ignore[index]
                strict=True,
            )
        )

    return TransportAdapter(
        "numpy", NumpyBackend(), make_block, keys_of, make_side, side_rows, joined_rows
    )


def transport_adapters() -> list[TransportAdapter]:
    out: list[TransportAdapter] = []
    for factory in (_awkward_transport_adapter, _numpy_transport_adapter):
        with contextlib.suppress(Exception):  # a backend not installed drops from the parametrization
            out.append(factory())
    return out


def numpy_adapter() -> TransportAdapter:
    """The always-installed adapter, for transport-plane tests where the adapter is not the
    variable under test (death/epoch/block-plane themes — engine-plane, adapter-agnostic)."""
    return _numpy_transport_adapter()


# ---- scenario builders (the m39/m40/m41 shapes, carried) ----------------------------------------


def repartition_blocks(adapter: TransportAdapter, copies: int = 1) -> list[object]:
    """6 source blocks with overlapping key populations (multi-src multi-dest coalescing);
    ``copies`` multiplies ROWS, never block count (the row-independence knob)."""
    key_lists = [
        [0, 1, 2, 3, 4, 5, 6, 7],
        [7, 6, 5, 4, 3, 2, 1, 0],
        [10, 11, 12, 13, 10, 11, 12, 13],
        [100, 200, 300, 400],
        [1, 1, 2, 2, 3, 3],
        [42, 43, 44, 45, 46, 47, 48, 49],
    ]
    return [adapter.make_block(list(keys) * copies) for keys in key_lists]


def hot_key_blocks(adapter: TransportAdapter, hot_rows: int) -> list[object]:
    """A skewed scenario: nearly every row carries key 7 (one hot dest), plus a thin spread —
    the T3 disk-budget arbitration trigger."""
    return [
        adapter.make_block([7] * hot_rows),
        adapter.make_block([7] * hot_rows),
        adapter.make_block(list(range(24))),
    ]


def join_general_sides(adapter: TransportAdapter) -> tuple[list[object], list[object]]:
    """Multi-block both-large sides with overlapping keys PLUS a left-only key (13) and a
    right-only key (17) — real unmatched rows on BOTH sides for the non-inner arms."""
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


def join_equal_sides(adapter: TransportAdapter) -> tuple[list[object], list[object]]:
    """Byte-comparable sides: at ``parts=8`` the pinned rule says SHUFFLE while a live
    ``n_workers()==1`` recompute says BROADCAST — the F6 discrimination gap."""
    left = [
        adapter.make_side([0, 1, 2, 3, 4, 5, 6, 7], "lval", [10, 11, 12, 13, 14, 15, 16, 17]),
        adapter.make_side([8, 9, 10, 11, 0, 1, 2, 3], "lval", [18, 19, 20, 21, 22, 23, 24, 25]),
    ]
    right = [
        adapter.make_side([0, 2, 4, 6, 8, 10, 1, 3], "rval", [100, 101, 102, 103, 104, 105, 106, 107]),
        adapter.make_side([5, 7, 9, 11, 0, 2, 4, 6], "rval", [108, 109, 110, 111, 112, 113, 114, 115]),
    ]
    return left, right


def join_small_build_sides(adapter: TransportAdapter) -> tuple[list[object], list[object]]:
    """A tiny build side + a much larger multi-block probe side: the pinned rule
    (``build*parts < build+probe``, measured bytes) prefers BROADCAST at the tested ``parts=8`` —
    sized so ``48*8 = 384 < 48 + 864`` holds for both real backends' 16 B/row wire shape."""
    left = [adapter.make_side([1, 2, 3], "lval", [10, 20, 30])]
    probe_keys = [1, 1, 2, 3, 3, 3, 2, 2, 1, 3, 1, 2, 3, 1, 2, 3, 2, 1]
    right = [
        adapter.make_side(probe_keys, "rval", [100 * b + i for i in range(len(probe_keys))])
        for b in (1, 2, 3)
    ]
    return left, right


def join_hot_key_sides(
    adapter: TransportAdapter, n_left: int, n_right: int
) -> tuple[list[object], list[object]]:
    """A single hot key: the inner join emits ``n_left*n_right`` output rows in one dest — the
    B5 output-side blowup the join budget must cover."""
    left = [adapter.make_side([7] * n_left, "lval", list(range(n_left)))]
    right = [adapter.make_side([7] * n_right, "rval", list(range(100, 100 + n_right)))]
    return left, right


def side_bytes(adapter: TransportAdapter, blocks: Sequence[object]) -> int:
    be: Any = adapter.backend
    return sum(int(be.estimated_bytes(b)) for b in blocks)


# ---- test-authored oracles (route via the golden-pinned ``partition``, never the join kernel) ----


def route_oracle_dest_keys(
    adapter: TransportAdapter, src_blocks: Sequence[object], parts: int
) -> dict[int, list[int]]:
    """Per dest, the ``__joinkey__`` values routed there in ascending-src, within-src order — from
    the backend's own golden-pinned ``partition`` (routing is NOT re-implemented here)."""
    be: Any = adapter.backend
    per_dest: dict[int, list[int]] = {}
    for src in src_blocks:  # ascending src index
        for dest, sub in enumerate(be.partition(src, "__joinkey__", parts)):
            keys = adapter.keys_of(sub)
            if keys:
                per_dest.setdefault(dest, []).extend(keys)
    return per_dest


def result_dest_keys(adapter: TransportAdapter, value: dict[int, object]) -> dict[int, list[int]]:
    return {dest: adapter.keys_of(block) for dest, block in value.items()}


def total_result_rows(value: Mapping[int, object]) -> int:
    return sum(len(block) for block in value.values())  # type: ignore[arg-type]


def _per_dest_side(
    adapter: TransportAdapter, blocks: Sequence[object], field: str, parts: int
) -> dict[int, list[tuple[int, int]]]:
    be: Any = adapter.backend
    out: dict[int, list[tuple[int, int]]] = {}
    for block in blocks:  # ascending src index
        for dest, sub in enumerate(be.partition(block, "__joinkey__", parts)):
            rows = adapter.side_rows(sub, field)
            if rows:
                out.setdefault(dest, []).extend(rows)
    return out


def dup_join_all(
    adapter: TransportAdapter, left: Sequence[object], right: Sequence[object], parts: int
) -> Counter[JRow]:
    """DUPLICATING relational inner join as one multiset (the m40 §3.3 pin): a probe row with k
    build matches ⇒ k rows. Routes each side with the backend's ``partition`` only."""
    left_by_dest = _per_dest_side(adapter, left, "lval", parts)
    right_by_dest = _per_dest_side(adapter, right, "rval", parts)
    total: Counter[JRow] = Counter()
    for dest in set(left_by_dest) & set(right_by_dest):
        for k, lv in left_by_dest[dest]:
            for k2, rv in right_by_dest[dest]:
                if k == k2:
                    total[(k, lv, rv)] += 1
    return total


def observed_join_all(adapter: TransportAdapter, value: dict[int, object]) -> Counter[JRow]:
    total: Counter[JRow] = Counter()
    for block in value.values():
        total += Counter(adapter.joined_rows(block))
    return total


def read_nullable_column(c: object) -> list[int | None]:
    """One joined column to Python ints with ``None`` for a null/masked entry (the m40 option-type
    pin: a ``-1``-sentinel impl fails the multiset, a plain ``int()`` reader would crash)."""
    to_list = getattr(c, "to_list", None)
    if to_list is not None:  # awkward option array -> [.., None, ..]
        return [None if v is None else int(v) for v in to_list()]
    arr = np.ma.asanyarray(c)  # numpy masked (or plain) structured field
    return [None if arr[i] is np.ma.masked else int(arr[i]) for i in range(len(arr))]


def nullable_join_multiset(value: dict[int, object]) -> Counter[NJRow]:
    """The whole relational result as one null-aware multiset over ALL blocks (partitioning-
    independent)."""
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
    import pandas as pd  # noqa: PLC0415  (optional oracle dep: only the join-parity module needs it)

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


def side_columns(
    blocks: Sequence[object], adapter: TransportAdapter, field: str
) -> tuple[list[int], list[int]]:
    ks: list[int] = []
    vs: list[int] = []
    for block in blocks:
        for k, v in adapter.side_rows(block, field):
            ks.append(k)
            vs.append(v)
    return ks, vs


# ---- SpyDaskBackend: the submit seam tap (structure / plan-choice / pin gates) ------------------


def _fn_name(fn: Any) -> str:
    while isinstance(fn, functools.partial):
        fn = fn.func
    return str(getattr(fn, "__name__", type(fn).__name__))


class SpyDaskBackend:
    """A delegating wrapper over a real ``DaskBackend`` recording every ``submit`` (fn name, key,
    ``workers=`` pin) and every ``broadcast`` token; everything else (capabilities, ``_client``,
    accessors) delegates via ``__getattr__`` so BOTH sanctioned F1 worker-address paths keep
    working. The complexity/plan-choice gates read these records (counts, never clocks)."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.submitted: list[tuple[str, str, tuple[str, ...] | None]] = []
        self.broadcast_tokens: list[str] = []

    def __getattr__(self, name: str) -> Any:  # delegate the non-tapped surface
        return getattr(self.inner, name)

    def submit(self, fn: Any, /, *args: Any, key: str, workers: Sequence[str] | None = None, **kwargs: Any) -> Any:
        self.submitted.append((_fn_name(fn), key, None if workers is None else tuple(workers)))
        extra: dict[str, Any] = {} if workers is None else {"workers": workers}
        return self.inner.submit(fn, *args, key=key, **extra, **kwargs)

    def broadcast(self, payload: bytes, *, token: str) -> Any:
        self.broadcast_tokens.append(token)
        return self.inner.broadcast(payload, token=token)

    def submitted_fn_names(self) -> Counter[str]:
        return Counter(name for name, _key, _pin in self.submitted)

    def pins_of(self, fn_name: str) -> list[tuple[str, ...] | None]:
        return [pin for name, _key, pin in self.submitted if name == fn_name]


# ---- capability-gate stubs (FU2: discriminate WHICH flag fires; refuse on ANY touch) ------------


class _RefusingBackendBase:
    """Protocol-shaped stub: the §1.6 gate must fire BEFORE any work reaches it."""

    capabilities: SubmitCapabilities

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

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        return None


class PinlessBackend(_RefusingBackendBase):
    """The FU2 discriminator: ``peer_data_movement=True`` but ``pin_to_worker=False`` — exactly
    the frozen-m42 ``ThreadBackend`` flag shape, alive in the dask-free main matrix. The NEW m44
    gate (strict pinning) must fire on this stub even though the m43 gate (peer movement) passes."""

    capabilities = SubmitCapabilities(
        peer_data_movement=True,
        scatter_broadcast=False,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=False,
        worker_file_cache=False,
    )


class NoPeerTransportBackend(_RefusingBackendBase):
    """The m43-style head-node-routed stub: every flag False."""

    capabilities = SubmitCapabilities(
        peer_data_movement=False,
        scatter_broadcast=False,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=False,
        worker_file_cache=False,
    )


# ---- GatedTransportBackend: deterministic mid-run worker-death scenario (F9) --------------------


def task_site() -> tuple[int, str]:
    """(pid, worker address | 'driver') at the point of call."""
    try:
        from distributed import get_worker  # noqa: PLC0415  (absent or outside a worker -> driver)

        return os.getpid(), str(get_worker().address)
    except Exception:
        return os.getpid(), "driver"


class GatedTransportBackend:
    """A delegating ``ShuffleBackend`` for the worker-death theme (the m43 gated-kill mechanism,
    F9/F18): ``partition`` (stage-1 only) drops a ``pmark`` file per call recording
    ``pid\\naddress``; ``from_wire`` (gather-side only under repartition) drops a ``gstart`` mark
    then BLOCKS until ``gate_path`` exists — holding a gather open so the driver can kill a
    block-holding worker mid-run deterministically. Scenario construction only; every assertion is
    a file count, counter, or content hash (R0.10a). Data is delegated UNTOUCHED, so hashes stay
    comparable with a plain-backend local oracle run."""

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


# ---- peer-reduction plan builders (module-level: spawn-safe across worker processes) ------------


def peer_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def peer_leaf_value(partition: Partition, resources: object) -> int:
    return 7 * int(partition.entry_start) + 3


def add_ints(a: int, b: int) -> int:
    return a + b


def int_zero() -> int:
    return 0


def make_peer_plan(n: int, tag: str) -> Plan[int]:
    tasks = tuple(Task(i, p) for i, p in enumerate(peer_partitions(n, tag)))
    return Plan(process=peer_leaf_value, combine=add_ints, empty=int_zero, tasks=tasks)
