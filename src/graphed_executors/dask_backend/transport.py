"""graphed's M38 ``WorkerTransport`` implemented ATOP dask's worker-to-worker comm layer
(m44-transport-plan §1.1/§1.5/§1.7). The P2P-shuffle pattern: a ``WorkerPlugin`` registers custom
ops in ``worker.handlers`` and sends ride ``worker.rpc`` (the pooled ``ConnectionPool``).

This module subclasses ``distributed.WorkerPlugin`` and therefore imports distributed at module top;
it is imported ONLY lazily (from the deferring ``transport_peer``/``transport_shuffle`` function
bodies, and by-reference when a worker unpickles a task fn), so ``graphed_executors.dask_backend``
still leaves ``distributed`` out of ``sys.modules`` until a run actually starts (F13).

Three endpoints, all satisfying the ``WorkerTransport`` Protocol structurally:

- ``DaskWorkerTransport`` — the worker-side endpoint (peer reduction). ``send`` bridges the task
  thread to the worker IO loop via ``distributed.utils.sync`` and does a bounded at-least-once retry
  over ``worker.rpc(dest).graphed_transport_recv`` (the ``HttpTransport`` sender policy: 5 attempts,
  backoff, 5 s per attempt), raising ``TransportDeliveryError`` on exhaustion rather than a silent
  ``False`` on reduction-class traffic (hard constraint 8). ``send("driver", …)`` rides
  ``worker.log_event``.
- ``DriverTransport`` — the reserved ``"driver"`` endpoint in the client process (not a dask
  Server): worker→driver over ``subscribe_topic`` (fed by the workers' ``log_event``), driver→all
  over ``client.run`` (a reliable ``put_nowait`` on every worker).
- ``GraphedTransportPlugin`` — holds the per-epoch state (bounded inbox, digest-keyed block store
  with producer-local spill, witness counters) and the ``graphed_transport_recv`` /
  ``graphed_block_pull`` / ``graphed_transport_ping`` handlers. Handlers do ONLY an epoch check and
  a ``put_nowait`` / block read — never blocking work on the IO loop.

The block plane (``graphed_block_pull``) is the pull-model data plane for the shuffle engine: bulk
bytes travel worker→worker, never through the driver; the plugin counts ``bytes_served`` /
``serve_pid`` so the frozen suite can prove it.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import pickle
import queue
import threading
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import distributed
from distributed.protocol import to_serialize
from distributed.utils import sync

from ._transport_run import DRIVER, PLUGIN_NAME  # dask-free constants (single source)

# The §1.1 send policy (the local ``HttpTransport`` sender: _transport.py:275-306 — five
# at-least-once attempts + backoff, 5 s per attempt). dask's base RPC retries nothing
# (``comm.retry.count: 0``); P2P adds ``p2p.comm.retry.count: 10`` for exactly this gap.
SEND_RETRIES = 5
SEND_ATTEMPT_TIMEOUT_S = 5.0
PULL_ATTEMPT_TIMEOUT_S = 5.0
_SEND_BACKOFF_S = 0.05  # first inter-attempt sleep; doubles each retry (bounded by the attempt count)
_DEFAULT_INBOX_MAXSIZE = 1 << 20  # generous: reduction traffic is O(n_leaves/k + log k + steal chatter)
_CANARY_RPC_TIMEOUT_S = 2.0  # bound the setup self-RPC so a nanny-restart startup can never deadlock

__all__ = [
    "DaskWorkerTransport",
    "DriverTransport",
    "GraphedTransportPlugin",
    "TransportDeliveryError",
    "make_transport_spec",
    "open_driver_endpoint",
    "open_endpoint",
    "pull_blocks",
]


class TransportDeliveryError(Exception):
    """A ``send`` exhausted its bounded at-least-once retries against a live peer. Raised OUT of the
    pinned peer/gather task (never a silent ``False`` on reduction-class traffic) so it escalates to
    the §1.5 epoch restart / attributed ``StageError`` — the un-editable consumers ignore ``send``'s
    bool (``_peer.py:214``/``:453``), so raising is the only sound exhaustion semantics."""


def _driver_topic(epoch: str) -> str:
    return f"{PLUGIN_NAME}-{epoch}"


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---- the picklable spec shipped into every task ------------------------------------------------
@dataclass(frozen=True)
class _TransportSpec:
    """A picklable description of one run epoch: the ordered worker addresses (F8 sorted), an
    optional bounded per-address send overlay (``None`` ⇒ full mesh + driver), and the inbox size."""

    epoch: str
    addresses: tuple[str, ...]
    overlay: dict[str, tuple[str, ...]] | None = None
    inbox_maxsize: int = _DEFAULT_INBOX_MAXSIZE

    def peers_of(self, address: str) -> tuple[str, ...]:
        if self.overlay is not None:
            return self.overlay.get(address, ())
        return (*(a for a in self.addresses if a != address), DRIVER)


def make_transport_spec(
    epoch: str,
    worker_addresses: Sequence[str],
    *,
    inbox_maxsize: int | None = None,
    overlay: Mapping[str, Sequence[str]] | None = None,
) -> _TransportSpec:
    """Build the picklable spec. ``overlay=None`` means every worker may send to every other worker
    and to ``"driver"`` (the engine passes its own bounded ``worker_outbox_addresses`` overlay)."""
    ov = None if overlay is None else {a: tuple(v) for a, v in overlay.items()}
    return _TransportSpec(
        epoch=epoch,
        addresses=tuple(worker_addresses),
        overlay=ov,
        inbox_maxsize=int(inbox_maxsize) if inbox_maxsize is not None else _DEFAULT_INBOX_MAXSIZE,
    )


# ---- per-epoch worker-side run state ------------------------------------------------------------
_COUNTER_KEYS = (
    "recv_invocations",
    "recv_on_loop",
    "recv_duplicate_deliveries",
    "sends_dropped",
    "bytes_served",
    "serve_pid",
    "sends_retried",
    "peer_sends",
    "holder_spill_count",
    "peak_holder_bytes",
)


@dataclass
class _RunState:
    """One epoch's per-worker state: a bounded inbox, a digest-keyed block store (RAM + producer-local
    spill), and the witness counters. ``lock`` guards the block store (task thread writes, IO-loop
    handler serves); the inbox is a thread-safe ``queue.Queue`` and needs no extra lock."""

    inbox_maxsize: int
    spill_dir: str
    inbox: queue.Queue[tuple[str, Any]] = field(init=False)
    blocks: dict[str, bytes] = field(default_factory=dict)  # digest -> RAM-resident wire
    spilled: dict[str, str] = field(default_factory=dict)  # digest -> producer-local spill path
    seen_payload: set[str] = field(default_factory=set)  # content-digest of delivered recv payloads
    counters: dict[str, int] = field(default_factory=lambda: dict.fromkeys(_COUNTER_KEYS, 0))
    lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self) -> None:
        self.inbox = queue.Queue(maxsize=self.inbox_maxsize)


# ---- the plugin ---------------------------------------------------------------------------------
class GraphedTransportPlugin(distributed.WorkerPlugin):  # type: ignore[misc]
    """A ``distributed.WorkerPlugin`` registered once per client
    (``name='graphed-transport'``, ``idempotent=True`` — the m42 idiom, so a harness pre-registration
    and the engine's own registration are ONE instance). ``setup`` registers the three handlers, then
    runs the §1.7 canary (a self-RPC ping) so a drifted worker-handler seam fails LOUDLY at register
    time instead of corrupting a run."""

    name = PLUGIN_NAME
    idempotent = True

    async def setup(self, worker: Any) -> None:
        self._worker = worker
        self._runs: dict[str, _RunState] = {}
        self.inject_recv_failures: dict[str, int] = {}
        self.stale_epoch_rejects = 0
        self.canary_ok = False
        try:
            worker.handlers["graphed_transport_recv"] = self._on_recv
            worker.handlers["graphed_block_pull"] = self._on_block_pull
            worker.handlers["graphed_transport_ping"] = self._on_ping
        except Exception as exc:  # a read-only handlers dict (a future distributed) is real drift
            raise RuntimeError(
                f"graphed-transport: distributed worker-handler seam drifted (handlers not writable): {exc}"
            ) from exc
        worker.extensions[PLUGIN_NAME] = self
        # §1.7 canary: prove the handler seam actually DISPATCHES. On a running worker a self-RPC ping
        # exercises the full rpc->server->handler path; but when setup runs during worker STARTUP (a
        # nanny RESTART after a §1.5 death), the worker's server is not yet accepting self-connections,
        # so the self-RPC would block the whole startup (measured: the restarted worker then hangs every
        # client.run against it — F2). The self-RPC is therefore BOUNDED, falling back on timeout to a
        # direct handler-dispatch check (same seam, no socket): either way a live seam ends green and a
        # restart never deadlocks. Real drift (an unwritable ``worker.handlers``) already raised above,
        # before this point.
        probe = uuid.uuid4().hex
        dispatched = False
        try:
            reply = await asyncio.wait_for(
                worker.rpc(worker.address).graphed_transport_ping(nonce=probe),
                timeout=_CANARY_RPC_TIMEOUT_S,
            )
            dispatched = reply.get("nonce") == probe
        except Exception:  # server-not-yet-listening (startup) or a bounded timeout -> direct fallback
            dispatched = False
        if not dispatched:
            handler = worker.handlers.get("graphed_transport_ping")
            if handler is None or handler(nonce=probe).get("nonce") != probe:
                raise RuntimeError(
                    "graphed-transport: distributed worker-handler seam drifted "
                    "(canary handler did not dispatch/echo)"
                )
        self.canary_ok = True

    # -- handlers (run on the worker IO loop; put_nowait / block-read only, never blocking work) --
    def _on_ping(self, nonce: str) -> dict[str, Any]:
        return {"nonce": nonce}

    def _on_recv(self, src: str, epoch: str, data: bytes) -> dict[str, Any]:
        run = self._runs.get(epoch)
        if run is None:  # the P2P run_id guard: an unknown or purged epoch is refused
            self.stale_epoch_rejects += 1
            return {"accepted": False, "stale": True}
        run.counters["recv_invocations"] += 1
        if threading.get_ident() == getattr(self._worker, "thread_id", None):
            run.counters["recv_on_loop"] += 1
        raw = bytes(data)
        # the §1.1 gated injection seam (deliver-then-raise): deliver the live-epoch message, then
        # raise a comm-class OSError so the sender's REAL retry classifier engages and the retry
        # produces a genuine at-least-once duplicate (F10).
        inject = self.inject_recv_failures.get(src, 0)
        if inject > 0:
            self._deliver(run, src, raw)
            self.inject_recv_failures[src] = inject - 1
            raise OSError("graphed-test-injected")
        try:
            self._deliver(run, src, raw)
        except queue.Full:
            run.counters["sends_dropped"] += 1
            return {"accepted": False}
        return {"accepted": True}

    def _deliver(self, run: _RunState, src: str, raw: bytes) -> None:
        d = _digest(raw)
        if d in run.seen_payload:
            run.counters["recv_duplicate_deliveries"] += 1
        else:
            run.seen_payload.add(d)
        run.inbox.put_nowait((src, pickle.loads(raw)))  # raises queue.Full on a full inbox

    async def _on_block_pull(self, epoch: str, digests: Sequence[str]) -> dict[str, Any]:
        run = self._runs.get(epoch)
        if run is None:
            self.stale_epoch_rejects += 1
            return {"stale": True}
        # RAM-resident wires are read directly; disk-spilled wires are read off the IO loop so the
        # loop never blocks on file IO.
        ram: dict[str, bytes] = {}
        paths: list[tuple[str, str]] = []
        with run.lock:
            for d in digests:
                if d in run.blocks:
                    ram[d] = run.blocks[d]
                elif d in run.spilled:
                    paths.append((d, run.spilled[d]))
        disk: dict[str, bytes] = {}
        if paths:
            disk = await self._worker.loop.run_in_executor(None, _read_spill_files, paths)
        wires: list[Any] = []
        served = 0
        for d in digests:
            wire = ram.get(d)
            if wire is None:
                wire = disk[d]
            served += len(wire)
            wires.append(to_serialize(wire))
        with run.lock:  # evict-after-serve: the reader plane never holds producer store + gathered dest
            for d in digests:
                run.blocks.pop(d, None)
                path = run.spilled.pop(d, None)
                if path is not None:
                    Path(path).unlink(missing_ok=True)
        run.counters["bytes_served"] += served
        run.counters["serve_pid"] = os.getpid()
        return {"wires": wires}

    # -- lifecycle used by endpoints + engine (task thread) --
    def ensure_run(self, spec: _TransportSpec) -> _RunState:
        run = self._runs.get(spec.epoch)
        if run is None:
            spill = os.path.join(self._worker.local_directory, f"graphed-{spec.epoch}")
            os.makedirs(spill, exist_ok=True)
            run = _RunState(inbox_maxsize=spec.inbox_maxsize, spill_dir=spill)
            self._runs[spec.epoch] = run
        return run

    def store_block(self, epoch: str, digest: str, wire: bytes, *, to_disk: bool) -> None:
        run = self._runs[epoch]
        with run.lock:
            if to_disk:
                path = os.path.join(run.spill_dir, f"holder-{digest}")
                Path(path).write_bytes(wire)
                run.spilled[digest] = path
            else:
                run.blocks[digest] = wire

    def record(self, epoch: str, **counters: int) -> None:
        run = self._runs[epoch]
        for key, val in counters.items():
            if key == "peak_holder_bytes":
                run.counters[key] = max(run.counters[key], val)
            elif key == "serve_pid":
                run.counters[key] = val
            else:
                run.counters[key] += val

    def deliver_local(self, epoch: str, src: str, message: object) -> bool:
        """Put a message into an epoch's inbox from driver-driven control paths (broadcast/unicast)."""
        run = self._runs.get(epoch)
        if run is None:
            return False
        try:
            run.inbox.put_nowait((src, message))
        except queue.Full:
            run.counters["sends_dropped"] += 1
            return False
        return True

    def active_epochs(self) -> tuple[str, ...]:
        return tuple(self._runs)

    def counters(self, epoch: str) -> Mapping[str, int]:
        run = self._runs.get(epoch)
        return dict(run.counters) if run is not None else {}

    def purge(self, epoch: str) -> None:
        run = self._runs.pop(epoch, None)
        if run is not None:
            with run.lock:
                for path in run.spilled.values():
                    Path(path).unlink(missing_ok=True)
                run.blocks.clear()
                run.spilled.clear()
            import shutil  # noqa: PLC0415  (teardown only)

            shutil.rmtree(run.spill_dir, ignore_errors=True)


def _read_spill_files(paths: Sequence[tuple[str, str]]) -> dict[str, bytes]:
    return {d: Path(p).read_bytes() for d, p in paths}


def pull_blocks(epoch: str, holder: str, digests: Sequence[str]) -> list[bytes]:
    """Worker-task-side block-plane pull (the shuffle gather's data path): dial ``holder``'s
    ``graphed_block_pull`` over ``worker.rpc`` for a COALESCED batch of ``digests`` (one RPC per
    holder — the T2 incast bound), bridging the task thread to the IO loop via ``sync``. A dead holder
    surfaces as the comm-class error the §1.5 restart classifier recognizes; a purged epoch raises
    (never a silent empty), so a lost block can never be read as zero rows (hard constraint 8). No
    per-pull retry: a lost holder restarts the WHOLE run under a fresh epoch, not this one pull."""
    worker = distributed.get_worker()

    async def _go() -> list[bytes]:
        reply = await asyncio.wait_for(
            worker.rpc(holder).graphed_block_pull(epoch=epoch, digests=list(digests)),
            timeout=PULL_ATTEMPT_TIMEOUT_S,
        )
        if reply.get("stale"):
            raise RuntimeError(f"graphed_block_pull hit a stale/purged epoch {epoch} on holder {holder}")
        return [bytes(w) for w in reply["wires"]]

    out: list[bytes] = sync(worker.loop, _go)
    return out


def _get_plugin() -> GraphedTransportPlugin:
    worker = distributed.get_worker()
    plugin: GraphedTransportPlugin | None = worker.extensions.get(PLUGIN_NAME)
    if plugin is None:  # the engine registers the plugin before any run; a missing one is a real bug
        raise RuntimeError("graphed-transport plugin is not registered on this worker")
    return plugin


# ---- worker-side endpoint -----------------------------------------------------------------------
class DaskWorkerTransport:
    """A worker-side ``WorkerTransport``: ``send`` bridges the task thread to the IO loop and rides
    ``worker.rpc``; ``recv``/``poll`` drain the plugin inbox on the task thread (thread-safe queue)."""

    def __init__(self, plugin: GraphedTransportPlugin, spec: _TransportSpec) -> None:
        self._plugin = plugin
        self._spec = spec
        self._worker = plugin._worker
        self.address = self._worker.address
        self._run = plugin.ensure_run(spec)
        self.root_sent: Any = None  # captured when send("driver", ("root", v)) fires (the F3 fallback)

    def peers(self) -> tuple[str, ...]:
        return self._spec.peers_of(self.address)

    def send(self, dest: str, message: object) -> bool:
        if dest == DRIVER:
            if isinstance(message, tuple) and message and message[0] == "root":
                self.root_sent = message[1]
            payload = (self.address, pickle.dumps(message))
            self._worker.log_event(_driver_topic(self._spec.epoch), payload)
            return True
        return bool(sync(self._worker.loop, self._asend, dest, message))

    async def _asend(self, dest: str, message: object) -> bool:
        data = pickle.dumps(message)
        backoff = _SEND_BACKOFF_S
        for attempt in range(1, SEND_RETRIES + 1):
            try:
                reply = await asyncio.wait_for(
                    self._worker.rpc(dest).graphed_transport_recv(
                        src=self.address, epoch=self._spec.epoch, data=to_serialize(data)
                    ),
                    timeout=SEND_ATTEMPT_TIMEOUT_S,
                )
            except Exception as exc:  # comm-class failure (CommClosedError/OSError/timeout) -> retry
                if attempt >= SEND_RETRIES:
                    raise TransportDeliveryError(
                        f"send to {dest} exhausted {SEND_RETRIES} attempts: {type(exc).__name__}: {exc}"
                    ) from exc
                self._plugin.record(self._spec.epoch, sends_retried=1)
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            if reply.get("accepted"):
                self._plugin.record(self._spec.epoch, peer_sends=1)
                return True
            return False  # a live handler answered accepted:False (inbox full / stale) — the drop signal
        return False  # unreachable (the loop returns or raises), but keeps mypy total

    def broadcast(self, message: object) -> None:
        for dest in self.peers():
            if dest != DRIVER:
                self.send(dest, message)

    def poll(self) -> list[tuple[str, object]]:
        out: list[tuple[str, object]] = []
        while True:
            try:
                out.append(self._run.inbox.get_nowait())
            except queue.Empty:
                return out

    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        try:
            return self._run.inbox.get(timeout=timeout) if timeout is not None else self._run.inbox.get()
        except queue.Empty:
            return None

    def close(self) -> None:
        return None  # per-epoch state is torn down by the driver purge (§1.2)


def open_endpoint(spec: _TransportSpec) -> DaskWorkerTransport:
    """Worker-task-side construction from ``get_worker().extensions['graphed-transport']`` + the spec.
    Idempotent per (worker, epoch): re-opening attaches to the SAME inbox/state."""
    return DaskWorkerTransport(_get_plugin(), spec)


# ---- driver-side endpoint -----------------------------------------------------------------------
def _broadcast_put(epoch: str, message: object, dask_worker: Any = None) -> bool:
    plugin = dask_worker.extensions.get(PLUGIN_NAME)
    return plugin.deliver_local(epoch, DRIVER, message) if plugin is not None else False


def _ensure_run_probe(spec: _TransportSpec, dask_worker: Any = None) -> bool:
    """Create the epoch's per-worker inbox/state BEFORE any peer sends — the peer protocol has no
    barrier, so a fast worker can ship a boundary node before the owner's task has opened its
    endpoint; without a pre-created inbox that reduction message would be stale-rejected and LOST
    (hard constraint 8). The local executor builds every inbox up front for the same reason."""
    plugin = dask_worker.extensions.get(PLUGIN_NAME)
    if plugin is not None:
        plugin.ensure_run(spec)
    return True


class DriverTransport:
    """The reserved ``"driver"`` endpoint in the client process. Asymmetric by design (workers cannot
    ``rpc`` into the client): worker→driver arrives over ``subscribe_topic`` (fed by the workers'
    ``log_event``); driver→all/unicast is a reliable ``client.run`` ``put_nowait`` on the targets."""

    address = DRIVER

    def __init__(self, backend: Any, spec: _TransportSpec) -> None:
        self._client = backend._client
        self._spec = spec
        self._inbox: queue.Queue[tuple[str, object]] = queue.Queue()
        self._unsub = backend.subscribe_events(_driver_topic(spec.epoch), self._on_event)

    def _on_event(self, msg: Any) -> None:
        # subscribe_events hands back the worker's logged payload: (src_address, pickled message).
        src, data = msg
        self._inbox.put((str(src), pickle.loads(bytes(data))))

    def peers(self) -> tuple[str, ...]:
        return self._spec.addresses

    def _put(self, targets: Sequence[str], message: object) -> None:
        self._client.run(_broadcast_put, self._spec.epoch, message, workers=list(targets))

    def send(self, dest: str, message: object) -> bool:
        self._put([dest], message)  # rare driver->worker unicast (public client.run put_nowait)
        return True

    def broadcast(self, message: object) -> None:
        self._put(self._spec.addresses, message)

    def poll(self) -> list[tuple[str, object]]:
        out: list[tuple[str, object]] = []
        while True:
            try:
                out.append(self._inbox.get_nowait())
            except queue.Empty:
                return out

    def recv(self, timeout: float | None = None) -> tuple[str, object] | None:
        try:
            return self._inbox.get(timeout=timeout) if timeout is not None else self._inbox.get()
        except queue.Empty:
            return None

    def close(self) -> None:
        self._unsub()


def open_driver_endpoint(backend: Any, spec: _TransportSpec) -> DriverTransport:
    """The reserved ``"driver"`` endpoint over ``backend`` (a ``DaskBackend``): ``address == "driver"``,
    ``peers() == spec worker addresses``, fed by ``backend.subscribe_events`` on the run topic."""
    return DriverTransport(backend, spec)


def ensure_engine_plugins(client: Any) -> None:
    """Register ``GraphedWorkerPlugin`` (idempotent) — it backs the ``DaskBackend`` submit shim's
    ``WorkerEnv`` and the shim ``KeyError``s without it. ``GraphedTransportPlugin`` is registered by
    the caller (``dask_transport_setup`` / the harness) so its instance — and any armed test injection
    seam on it — is never disturbed mid-run by a re-registration."""
    from .plugin import GraphedWorkerPlugin  # noqa: PLC0415  (lazy: distributed extra)

    client.register_plugin(GraphedWorkerPlugin())


def dask_transport_setup(client: Any) -> None:
    """User-facing one-liner: register both engine plugins on a ready ``client`` before calling a
    ``transport_run_*`` entry point (the frozen suite calls it as ``install_transport_plugin`` for the
    transport half + relies on the engine's ``ensure_engine_plugins`` for the worker half)."""
    from .plugin import GraphedWorkerPlugin  # noqa: PLC0415  (lazy: distributed extra)

    client.register_plugin(GraphedWorkerPlugin())
    client.register_plugin(GraphedTransportPlugin())
