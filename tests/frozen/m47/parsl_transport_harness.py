"""Shared harness for the m47 frozen suite: the parsl HTTP **self-rendezvous worker transport**
(k persistent peers over an `EscalatingHttpTransport` dual-route plane), the **runtime reachability
probe** with ``on_unreachable={"error","fallback"}``, and the **parsl facade** reusing the shared
``common/facade.py`` resolver (parsl-backend-plan §1.4-§1.6, §2 m47, §3 m47 — plan FINAL at r3;
review minors N1/N2 honored: the reduce actor is ``process_and_reduce``, and ``send()`` must
special-case HTTP 503 BEFORE the generic retry).

Pinned execution contract (test-author restatement — the implementer builds exactly these module
paths, symbols, and observable behaviors; the m47 README carries the traceability map):

``graphed_executors.common.http_plane``  (stdlib-only; subclasses of ``local/_transport.py`` — the
edit-frozen base is NEVER modified; plan §1.4 block plane + design-A/design-B):

    TransportDeliveryError(Exception)     # same-NAMED parsl-side classes: the reused classifier
    PullTimeoutError(Exception)           # (common.transport_run.is_restart_worthy) matches by name
    SEND_RETRIES == 5                     # the frozen local sender policy, restated (:293-306)
    EscalatingHttpTransport(address, *, epoch, host="127.0.0.1", inbox_maxsize=None,
                            idle_deadline_s=None)
        # a graphed WorkerTransport on a DUAL-ROUTE ThreadingHTTPServer: POST /msg -> inbox
        # delivery, POST /pull -> block-store reads. .host/.port are OS-assigned at construction
        # (bind (host, 0)); .set_registry({address: (host, port)}); .peers(); .poll(); .close().
        # /msg WIRE ENVELOPE (pinned so the stale-epoch injector can speak it):
        #     POST body = pickle.dumps((sender_address, epoch, message))
        # send(dest, message) -> bool — INLINE in the caller's thread (no outbox, no sender
        # thread), bounded: SEND_RETRIES total attempts, 5 s/attempt, small backoff:
        #   * delivered (200)                     -> True
        #   * unknown dest or dest == self        -> False (no POST at all)
        #   * destination answered 503 (full inbox) -> False + sends_rejected += 1, NO retry —
        #     HTTPError code==503 special-cased BEFORE the generic retry (review N2); a
        #     retry-then-raise-on-503 implementation fails the flood arm
        #   * comm-class failure exhausting the retries -> RAISES TransportDeliveryError +
        #     send_failures += 1 (never the base class's silent ``drops += 1``)
        # recv(timeout)/poll() — inbox pop; recv RAISES TransportDeliveryError once
        # ``idle_deadline_s`` elapses with NO delivered traffic (design-A: the lost-``done``
        # backstop that guarantees a peer future always resolves); ``None`` disables the deadline.
        # broadcast(message) — attempts ALL registered peers, THEN raises the aggregated
        # TransportDeliveryError if any leg exhausted (never the base stop-at-first loop; one dead
        # peer cannot strand the live ones — design-A release netting).
        # Receiver-side /msg: ``inbox_maxsize=None`` (default) = unbounded never-refuse (the local
        # posture preserved); a set maxsize answers 503 on full + recv_rejects += 1; a mismatched
        # EPOCH in the envelope is NEVER delivered: rejected + stale_epoch_rejects += 1.
        # Counter attributes (ints, all start at 0): sends_retried (retry attempts made),
        # send_failures (exhaustion raises), sends_rejected (503 verdicts) — sender side;
        # recv_rejects (full-inbox 503s), stale_epoch_rejects — receiver side.

``graphed_executors.common.transport_run``  (§1.6 m47 move — the SAME objects the dask shim
re-exports): TransportWitness / TransportShuffleResult / TransportExecResult, merge_counters,
is_restart_worthy, pick_attributable, build_stage_error.
``graphed_executors.common.facade``: resolve_shuffle_method / _reject_transport_only_knobs — the
ONE function object `dask_backend.api` AND `parsl_backend.api` both re-export (identity-pinned).
``graphed_executors.common.transport_tasks``: the plane-parameterized m44 task bodies (the shared
map/gather/gather-join sequence whose budget counters the parity tests read).

``graphed_executors.parsl_backend.transport_peer``  (§1.4, m47 IT2):

    parsl_run_plan(plan, pbackend, *, monitor=None, workers=None, inbox_maxsize=None,
                   epoch_restarts_allowed=1, barrier_timeout_s=None,
                   inject_recv_failures=None) -> TransportExecResult-shaped
        # k = workers or pbackend.n_workers() at run start (an explicit over-ask is converted by
        # the barrier into a bounded attributed error, never a hang). Submits EXACTLY k peer tasks
        # (the module-level ``_parsl_peer_main`` — its RAW __name__ visible at the SubmitBackend
        # spy seam) + O(1) control, never per-leaf futures. Result: .value/.n_partitions/
        # .n_combines + .transport (TransportWitness: epoch_nonces, epoch_restarts, per_worker).
        # ``inject_recv_failures={dest_addr: {src_addr: budget}}`` is the pinned GATED TEST SEAM
        # (the m44 plugin-seam analog, spec-carried because parsl has no client.run): while the
        # budget is > 0, the dest peer's /msg route DELIVERS the live-epoch message into the inbox
        # normally, decrements the budget, then fails the HTTP response with a NON-503 comm-class
        # failure — the sender's REAL inline retry engages and the retried send lands a genuine
        # at-least-once duplicate (F10). Clock-free, deterministic.
    _parsl_peer_main            # the k-submitted peer actor (endpoint minted IN-TASK, hello ->
                                # barrier -> registry -> probe leg -> process_and_reduce (N1) or
                                # the shuffle body; returns its counters as the task result)

``graphed_executors.parsl_backend.transport_shuffle``  (m47 IT3):

    transport_run_repartition(backend, src_blocks, parts, *, pbackend, salt=0, workers=None,
                              fetch_budget_bytes=None, pull_timeout_s=None,
                              epoch_restarts_allowed=1, barrier_timeout_s=None,
                              registry_rewrite=None) -> TransportShuffleResult-shaped
    transport_run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",),
                       how="inner", pbackend, broadcast=None, salt=0, mem_budget_bytes=None,
                       workers=None, pull_timeout_s=None, epoch_restarts_allowed=1,
                       barrier_timeout_s=None) -> TransportShuffleResult-shaped
        # results carry the local engine's triple (.dest_block_hashes/.value/.witness with the
        # SHARED ShuffleWitness counter names) + .transport. Blocks move peer<->peer over /pull
        # ONLY (coalesced, evict-after-serve; a dead/slow holder raises PullTimeoutError — never a
        # partial result).

``graphed_executors.parsl_backend.api``  (§1.5, m47 IT4):

    run_repartition(backend, src_blocks, parts, *, pbackend, shuffle_method="auto", salt=0,
                    fetch_budget_bytes=None, pull_timeout_s=None, epoch_restarts_allowed=None,
                    workers=None, on_unreachable=None, registry_rewrite=None)
    run_join(backend, left, right, parts, *, on=("__joinkey__",), how="inner", pbackend,
             shuffle_method="auto", broadcast=None, salt=0, mem_budget_bytes=None,
             pull_timeout_s=None, epoch_restarts_allowed=None, workers=None, on_unreachable=None)
        # resolve_shuffle_method REUSED from common/facade (identity: parsl_backend.api.… IS
        # dask_backend.api.… IS common.facade.…). "auto" -> "tasks" on every parsl instance
        # (no parsl vector has pin AND peer). tasks -> the relay engine on HTEX / the m43 engine
        # on peer=True instances. Explicit "transport" -> the §1.4 engine gated on
        # ``pbackend.peer_transport`` + the reachability probe. Transport-only knobs
        # ({fetch_budget_bytes, pull_timeout_s, epoch_restarts_allowed, workers, on_unreachable,
        # registry_rewrite}) explicitly set while resolution lands on "tasks" -> ValueError naming
        # the knob BEFORE any submit. ``on_unreachable=None`` means "error".
    probe_peer_reachability(pbackend, k, *, timeout_s=5.0, registry_rewrite=None) -> ProbeReport
        # ProbeReport.ok: bool; ProbeReport.failed_pairs: sequence of (src_addr, dst_addr) —
        # empty iff ok. Rendezvous k peers, probe the overlay, RELEASE the peers (the pool is
        # usable afterwards), report. The user-facing pre-flight of directive #2.
    ParslBackend.peer_transport   # NEW in m47: True on HTEX instances (the §1.4 plane runs on
                                  # HTEX worker processes), False on TPE — where explicit
                                  # "transport" raises NotImplementedError naming "tasks"
                                  # (parsl_backend-private; NEVER a SubmitCapabilities field)

``graphed_executors.parsl_backend.launch`` (m47 delta): ``start_htex`` gains
``heartbeat_period: int | None = None`` (None = parsl default). MEASURED for this suite
(scratchpad ``hb_probe.py``, parsl 2026.7.20, direct submit): ``heartbeat_period=2`` compresses
the SIGKILL->WorkerLost latency from ~29.7 s to **1.65 s** and the pool survives (the watchdog
respawns the worker in place; the next submit returned in 0.01 s).

Pinned observability (how the frozen witnesses are read — counters/hashes/pids, never clocks):

- **Peer addresses are** ``"w0".."w{k-1}"`` (the exec-local scheme; sorted order == index order
  for the k <= 3 these tests use). ``"w0"`` owns leaf 0 and therefore the reduction root
  (make_bounds contiguous ranges). Producer-task t maps the CONTIGUOUS ascending src range
  (the frozen m43 ``_assign`` split); dest d is gathered by peer ``sorted_addrs[d % k]`` (the m44
  F8 round-robin). These pins are what make the injection seam addressable and the block-plane
  byte oracle computable test-side.
- ``result.transport.per_worker`` maps each peer address (and ``"driver"``) to counters, read with
  ``.get(key, 0)`` — richer mappings are fine, absent mechanisms are not. Pinned per-PEER keys:
  the ``process_and_reduce`` witness (``peer_sends``/``peer_recvs``/``n_combines``/``processed``),
  the sender counters (``sends_retried``/``send_failures``/``sends_rejected``), receiver
  ``recv_duplicate_deliveries`` (content-digest keyed per epoch) + ``stale_epoch_rejects``, the
  block plane (``bytes_served``, ``serve_pid``, ``pull_requests_served``,
  ``store_blocks_at_return``), and the rendezvous provenance (``endpoint_pid``, ``endpoint_port``,
  ``registry_size_at_receipt``). Pinned ``"driver"`` keys: ``recv_class_hello`` /
  ``recv_class_root`` / ``recv_class_node`` / ``recv_class_leaf`` (message-class receipt counts —
  the "driver saw only control traffic" witness).
- ``registry_rewrite`` is the pinned registry-POISONER seam (plan §3 m47 harness row): a callable
  applied to the assembled ``{address: (host, port)}`` book before the registry broadcast and the
  probe. The driver's OWN control legs keep the TRUE announced endpoints, so release and error
  attribution still reach the peers. Production callers never pass it.

Disciplines carried from the m44/m46 harnesses (REPLICATED, never imported cross-suite): (1)
everything an HTEX worker may unpickle is MODULE-LEVEL here and this directory is exported on
``PYTHONPATH`` before any HTEX starts (HTEX workers do not inherit pytest's sys.path — the m46
measured fact); (2) the implementation under test is imported ONLY through the deferred accessors,
so pre-implementation the suite COLLECTS cleanly and every test fails in-body with the
right-reason ``ModuleNotFoundError`` on the planned m47 modules (TEST_SANITY non-vacuity); (3)
every pool-touching call runs under a hard-timeout driver (a hang is a failure, never a hung CI
job); (4) witnesses are counters, content hashes, pids, and site provenance — never clocks; the
bounded waits here are scenario construction and hang guards, never assertions (R0.10a).
"""

from __future__ import annotations

import contextlib
import functools
import os
import pickle
import shutil
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.request
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

HARNESS_DIR = str(Path(__file__).resolve().parent)
REPO_ROOT = Path(__file__).resolve().parents[3]

# the frozen send policy (local/_transport.py:293-306), restated for the retry-arm budgets
SEND_RETRIES = 5
DRIVER = "driver"
W0, W1 = "w0", "w1"  # the pinned peer addresses at k=2 ("w0" owns leaf 0 -> the reduction root)

# a joined output row, backend-independent: (key, left value, right value)
JRow = tuple[int, int, int]
# a null-aware joined row (non-inner arms): key always coalesced, an absent side's value is None
NJRow = tuple[int | None, int | None, int | None]


# ---- deferred accessors for the implementation under test ---------------------------------------


def http_plane_api() -> Any:
    """``graphed_executors.common.http_plane`` — absent pre-implementation: the right-reason
    ``ModuleNotFoundError`` for every endpoint-level test lands here."""
    import graphed_executors.common.http_plane as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def transport_run_api() -> Any:
    """``graphed_executors.common.transport_run`` (the §1.6 m47 move)."""
    import graphed_executors.common.transport_run as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def peer47_api() -> Any:
    """``graphed_executors.parsl_backend.transport_peer`` (m47 IT2)."""
    import graphed_executors.parsl_backend.transport_peer as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def shuffle47_api() -> Any:
    """``graphed_executors.parsl_backend.transport_shuffle`` (m47 IT3)."""
    import graphed_executors.parsl_backend.transport_shuffle as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def papi() -> Any:
    """``graphed_executors.parsl_backend.api`` (m47 IT4)."""
    import graphed_executors.parsl_backend.api as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def launch47_api() -> Any:
    """``graphed_executors.parsl_backend.launch`` — m46-built, m47-extended (heartbeat_period)."""
    import graphed_executors.parsl_backend.launch as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def relay47_api() -> Any:
    """``graphed_executors.common.relay_engine`` — the m46 relay engine (the fallback target)."""
    import graphed_executors.common.relay_engine as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def make_parsl_backend(executor: Any) -> Any:
    import graphed_executors.parsl_backend as mod  # noqa: PLC0415  (deferred: m46-built substrate)

    return mod.ParslBackend(executor)


def make_endpoint(address: str, *, epoch: str, **kwargs: Any) -> Any:
    """``EscalatingHttpTransport(address, epoch=..., inbox_maxsize=..., idle_deadline_s=...)``."""
    return http_plane_api().EscalatingHttpTransport(address, epoch=epoch, **kwargs)


def delivery_error_cls() -> type[BaseException]:
    return http_plane_api().TransportDeliveryError  # type: ignore[no-any-return]


def plan_run(*args: Any, **kwargs: Any) -> Any:
    """``parsl_run_plan(plan, pbackend, *, ...)`` (contract pin above)."""
    return peer47_api().parsl_run_plan(*args, **kwargs)


def transport_repartition(*args: Any, **kwargs: Any) -> Any:
    """``transport_run_repartition(backend, src_blocks, parts, *, pbackend, ...)`` (contract pin)."""
    return shuffle47_api().transport_run_repartition(*args, **kwargs)


def transport_join(*args: Any, **kwargs: Any) -> Any:
    """``transport_run_join(backend, left, right, parts, *, pbackend, ...)`` (contract pin)."""
    return shuffle47_api().transport_run_join(*args, **kwargs)


def facade_repartition(*args: Any, **kwargs: Any) -> Any:
    return papi().run_repartition(*args, **kwargs)


def facade_join(*args: Any, **kwargs: Any) -> Any:
    return papi().run_join(*args, **kwargs)


def probe_reachability(*args: Any, **kwargs: Any) -> Any:
    return papi().probe_peer_reachability(*args, **kwargs)


def relay_repartition(*args: Any, **kwargs: Any) -> Any:
    return relay47_api().relay_run_repartition(*args, **kwargs)


def make_runner(backend: Any, *, retries: int = 3) -> Any:
    from graphed_executors.submit import SubmitRunner  # noqa: PLC0415  (frozen m42 substrate)

    return SubmitRunner(backend, retries=retries)


# ---- fixture substrate: HTEX / TPE pools through the PLANNED launch recipe ----------------------


def ensure_m47_harness_on_worker_path() -> None:
    """Prepend this directory to ``PYTHONPATH`` so spawned HTEX workers can unpickle this module's
    task fns by reference (measured load-bearing, the m46 precedent). Idempotent; deliberately not
    restored (process-level; restoring would race module-scoped pools across files)."""
    existing = os.environ.get("PYTHONPATH", "")
    if HARNESS_DIR not in existing.split(os.pathsep):
        os.environ["PYTHONPATH"] = HARNESS_DIR + (os.pathsep + existing if existing else "")


@contextmanager
def p2p_htex_pool(workers: int = 2, *, heartbeat_period: int | None = None) -> Iterator[Any]:
    """A started HTEX via the planned ``start_htex``. ``heartbeat_period`` engages the m47
    signature extension (the death-latency compression knob, measured working at 2 s); ``None``
    keeps the frozen m46 call shape so ordinary pools never depend on the extension."""
    ensure_m47_harness_on_worker_path()
    run_dir = tempfile.mkdtemp(prefix="graphed-m47-htex-")
    api = launch47_api()
    if heartbeat_period is None:
        executor = api.start_htex(workers=workers, run_dir=run_dir)
    else:
        executor = api.start_htex(workers=workers, run_dir=run_dir, heartbeat_period=heartbeat_period)
    try:
        yield executor
    finally:
        api.stop_htex(executor)
        shutil.rmtree(run_dir, ignore_errors=True)


@contextmanager
def p2p_tpe_pool(max_threads: int = 2) -> Iterator[Any]:
    """A started parsl ``ThreadPoolExecutor`` (harness-owned: parsl's public API, not the
    implementation under test)."""
    from parsl.executors import ThreadPoolExecutor  # noqa: PLC0415  (parsl modules importorskip first)

    executor = ThreadPoolExecutor(label=f"graphed-m47-tpe-{uuid.uuid4().hex[:8]}", max_threads=max_threads)
    executor.start()
    try:
        yield executor
    finally:
        executor.shutdown()


# ---- driver-side hard-timeout runner + bounded poll (F2 discipline) -----------------------------


def run_bounded(fn: Callable[[], Any], timeout_s: float = 240.0) -> Any:
    """Run ``fn`` on a daemon thread with a HARD join timeout: a hang surfaces as an assertion
    failure, never a hung CI job. Returns the value or re-raises the call's exception."""
    out: dict[str, Any] = {}

    def _drive() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    thread = threading.Thread(target=_drive, daemon=True)
    thread.start()
    thread.join(timeout_s)
    assert not thread.is_alive(), f"HARD TIMEOUT: the driven call did not finish within {timeout_s}s"
    if "error" in out:
        raise out["error"]
    return out["result"]


def poll_until(predicate: Callable[[], bool], timeout_s: float = 120.0) -> None:
    """Bounded driver-side poll (scenario construction; assertions happen AFTER — R0.10a)."""
    deadline = time.monotonic() + timeout_s
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


# ---- endpoint-plane helpers: ports, injectors, wiring -------------------------------------------


def closed_loopback_port() -> int:
    """A loopback port that was just bound and released — dialing it is a deterministic, instant
    connection-refused (the clock-free failure the registry-poisoner and exhaustion arms use)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def wire_endpoints(*endpoints: Any) -> None:
    """Hand every endpoint the full ``{address: (host, port)}`` registry (the build_http_transports
    idiom, over the endpoints' own OS-assigned ports)."""
    registry = {ep.address: (ep.host, ep.port) for ep in endpoints}
    for ep in endpoints:
        ep.set_registry(dict(registry))


def post_msg(host: str, port: int, sender: str, epoch: str, message: object) -> int:
    """The stale-epoch INJECTOR: speak the pinned /msg envelope directly —
    ``pickle.dumps((sender, epoch, message))`` — and return the HTTP status (an ``HTTPError``
    status is returned, not raised; a connection-level failure returns -1). Assertions live with
    the caller: the contract is delivery/counters, not the status line."""
    body = pickle.dumps((sender, epoch, message))
    req = urllib.request.Request(f"http://{host}:{port}/msg", data=body, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return int(resp.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)
    except (urllib.error.URLError, OSError):
        return -1


def poison_one(addr: str, port: int) -> Callable[[dict[str, tuple[str, int]]], dict[str, tuple[str, int]]]:
    """A ``registry_rewrite`` that repoints ``addr`` at a closed loopback port — the plan's
    registry-poisoner (connection-refused is deterministic, no clocks)."""

    def rewrite(registry: dict[str, tuple[str, int]]) -> dict[str, tuple[str, int]]:
        out = dict(registry)
        assert addr in out, f"poisoner misfired: {addr!r} not in the assembled registry {sorted(out)}"
        host, _real = out[addr]
        out[addr] = (host, port)
        return out

    return rewrite


# ---- transport-witness readers ------------------------------------------------------------------


def pw(res: Any) -> dict[str, dict[str, int]]:
    return {addr: dict(counters) for addr, counters in dict(res.transport.per_worker).items()}


def pw_total(res: Any, key: str) -> int:
    return sum(int(c.get(key, 0)) for addr, c in pw(res).items() if addr != DRIVER)


def peer_rows(res: Any) -> dict[str, dict[str, int]]:
    return {addr: c for addr, c in pw(res).items() if addr != DRIVER}


def driver_row(res: Any) -> dict[str, int]:
    return pw(res).get(DRIVER, {})


# ---- SpyP2PBackend: the SubmitBackend seam tap (raw fn names; the shim wraps after) -------------


def _fn_name(fn: Any) -> str:
    while isinstance(fn, functools.partial):
        fn = fn.func
    return str(getattr(fn, "__name__", type(fn).__name__))


class SpyP2PBackend:
    """A delegating ``SubmitBackend`` recording every submitted task fn's RAW name + key and every
    broadcast token (the m45/m46 idiom: delegation by ``__getattr__`` so capabilities /
    ``peer_transport`` / n_workers pass through untouched). The engine-shape and complexity gates
    read these — counts, never clocks."""

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


class RefusingP2PBackend:
    """Protocol-shaped stub carrying an explicit capability vector + ``peer_transport``: any facade
    path that must raise BEFORE work leaves it untouched (``submit``/``broadcast`` count and
    refuse — the m43/m44/m45 gate idiom)."""

    def __init__(self, capabilities: SubmitCapabilities, *, peer_transport: bool) -> None:
        self.capabilities = capabilities
        self.peer_transport = peer_transport
        self.submit_attempts = 0
        self.broadcast_attempts = 0

    def n_workers(self) -> int:
        return 2

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        self.submit_attempts += 1
        raise AssertionError("the facade must refuse BEFORE submitting any work")

    def broadcast(self, payload: bytes, *, token: str) -> object:
        self.broadcast_attempts += 1
        raise AssertionError("the facade must refuse BEFORE broadcasting any payload")

    def subscribe_events(self, topic: str, handler: Any) -> Callable[[], None]:
        return lambda: None

    def cancel(self, futures: Sequence[Any]) -> None:
        return None

    def close(self) -> None:
        return None

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        return None


def htex_floor_caps() -> SubmitCapabilities:
    """The all-seven-False "parsl floor" (the frozen m46 HTEX vector)."""
    return SubmitCapabilities(
        peer_data_movement=False,
        scatter_broadcast=False,
        pin_to_worker=False,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=False,
        worker_file_cache=False,
    )


# ---- module-level spawn-safe task fns (pickled BY REFERENCE; run on HTEX workers) ---------------


def p2p_double(x: int) -> int:
    """The pool-liveness probe: after a barrier error or a probe release, the slots must be free."""
    return 2 * x


def p2p_task_pid(idx: int) -> int:
    return os.getpid()


def p2p_conformance_probe(epoch: str) -> dict[str, Any]:
    """IN-TASK protocol conformance (plan §3 m47 conformance row): build TWO endpoints inside one
    HTEX worker task, wire them, and exercise the WorkerTransport obligations. Returns plain
    observations; the assertions live driver-side."""
    from graphed.core.execution import WorkerTransport  # noqa: PLC0415  (worker-side isinstance)

    from graphed_executors.common.http_plane import EscalatingHttpTransport  # noqa: PLC0415  (worker-side)

    e1 = EscalatingHttpTransport("pa", epoch=epoch)
    e2 = EscalatingHttpTransport("pb", epoch=epoch)
    try:
        registry = {"pa": (e1.host, e1.port), "pb": (e2.host, e2.port)}
        e1.set_registry(dict(registry))
        e2.set_registry(dict(registry))
        obs: dict[str, Any] = {
            "pid": os.getpid(),
            "is_transport": isinstance(e1, WorkerTransport),
            "port_pa": int(e1.port),
            "port_pb": int(e2.port),
            "peers_pa": tuple(e1.peers()),
            "empty_recv": e2.recv(timeout=0.05),
            "empty_poll": list(e2.poll()),
            "send_self": e1.send("pa", ("x",)),
            "send_unknown": e1.send("nope", ("x",)),
            "send_ok": e1.send("pb", ("hello", 1)),
        }
        got = e2.recv(timeout=30.0)
        obs["round_trip"] = None if got is None else (str(got[0]), tuple(got[1]))
        return obs
    finally:
        e1.close()
        e2.close()


# ---- peer-reduction plan builders (module-level: spawn-safe) ------------------------------------


def p2p_partitions(n: int, tag: str) -> tuple[Partition, ...]:
    return tuple(Partition(f"mem://{tag}/{i}", "", i, i + 1) for i in range(n))


def p2p_leaf_value(partition: Partition, resources: object) -> int:
    return 7 * int(partition.entry_start) + 3


def p2p_add(a: int, b: int) -> int:
    return a + b


def p2p_zero() -> int:
    return 0


def make_p2p_plan(n: int, tag: str) -> Plan[int]:
    tasks = tuple(Task(i, p) for i, p in enumerate(p2p_partitions(n, tag)))
    return Plan(process=p2p_leaf_value, combine=p2p_add, empty=p2p_zero, tasks=tasks)


def p2p_seq_value(n: int, tag: str) -> int:
    from graphed.core.execution import SequentialRunner  # noqa: PLC0415  (oracle substrate)

    return int(SequentialRunner().run(make_p2p_plan(n, tag)).value)


# ---- real-backend adapters (the m44 a2 discipline: BOTH backends, this suite's names) -----------


@dataclass
class P2PAdapter:
    name: str
    backend: object
    make_block: Callable[[Sequence[int]], object]
    keys_of: Callable[[object], list[int]]
    make_side: Callable[[Sequence[int], str, Sequence[int]], object]
    side_rows: Callable[[object, str], list[tuple[int, int]]]
    joined_rows: Callable[[object], list[JRow]]


def _awkward_p2p_adapter() -> P2PAdapter:
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

    return P2PAdapter("awkward", AwkwardBackend(), make_block, keys_of, make_side, side_rows, joined_rows)


def _numpy_p2p_adapter() -> P2PAdapter:
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

    return P2PAdapter("numpy", NumpyBackend(), make_block, keys_of, make_side, side_rows, joined_rows)


def p2p_adapters() -> list[P2PAdapter]:
    out: list[P2PAdapter] = []
    for factory in (_awkward_p2p_adapter, _numpy_p2p_adapter):
        with contextlib.suppress(Exception):  # a backend not installed drops from the parametrization
            out.append(factory())
    return out


def numpy_p2p_adapter() -> P2PAdapter:
    """The always-installed adapter for engine-plane themes where the adapter is not the variable
    under test (death/epoch/block-plane/shape)."""
    return _numpy_p2p_adapter()


# ---- scenario builders (the m39/m40/m41 lineage shapes, carried) --------------------------------

_KEY_LISTS = [
    [0, 1, 2, 3, 4, 5, 6, 7],
    [7, 6, 5, 4, 3, 2, 1, 0],
    [10, 11, 12, 13, 10, 11, 12, 13],
    [100, 200, 300, 400],
    [1, 1, 2, 2, 3, 3],
    [42, 43, 44, 45, 46, 47, 48, 49],
]


def repartition_blocks(adapter: P2PAdapter, copies: int = 1) -> list[object]:
    """6 source blocks with overlapping key populations (multi-src multi-dest coalescing);
    ``copies`` multiplies ROWS, never block count (the row-independence knob)."""
    return [adapter.make_block(list(keys) * copies) for keys in _KEY_LISTS]


def many_src_blocks(adapter: P2PAdapter, n_src: int) -> list[object]:
    """``n_src`` blocks by cycling the key lists — the T knob of the anti-super-linear ladder
    (varies BLOCK count, not row shape)."""
    return [adapter.make_block(list(_KEY_LISTS[i % len(_KEY_LISTS)])) for i in range(n_src)]


def join_sides(adapter: P2PAdapter) -> tuple[list[object], list[object]]:
    """Overlapping keys plus a LEFT-only key (13, two rows) and a RIGHT-only key (17, two rows):
    the non-inner arms have real unmatched rows on both sides. Pre-validated against the frozen
    LOCAL engine at parts=8: inner 16 rows (13/17 absent), left 18 (13 x2), right 18 (17 x2),
    outer 20 (both x2) — measured, not invented."""
    left = [
        adapter.make_side([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
        adapter.make_side([1, 1, 2, 2, 13, 13], "lval", [22, 23, 24, 25, 32, 33]),
    ]
    right = [
        adapter.make_side([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
        adapter.make_side([3, 3, 2, 17, 17, 4], "rval", [106, 107, 108, 116, 117, 111]),
    ]
    return left, right


def join_hot_key_sides(adapter: P2PAdapter, n_left: int, n_right: int) -> tuple[list[object], list[object]]:
    """A single hot key: the inner join emits ``n_left*n_right`` rows in one dest — the B5
    output-side blowup the join budget must cover (pre-validated: 64x64 at budget=output//8 ->
    local spilled 7 == chunks_read 7, peak 15360 <= 16384, rows 4096)."""
    left = [adapter.make_side([7] * n_left, "lval", list(range(n_left)))]
    right = [adapter.make_side([7] * n_right, "rval", list(range(100, 100 + n_right)))]
    return left, right


def side_bytes(adapter: P2PAdapter, blocks: Sequence[object]) -> int:
    be: Any = adapter.backend
    return sum(int(be.estimated_bytes(b)) for b in blocks)


# ---- test-authored oracles (route via the golden-pinned ``partition``, never the join kernel) ----


def route_oracle_dest_keys(
    adapter: P2PAdapter, src_blocks: Sequence[object], parts: int
) -> dict[int, list[int]]:
    be: Any = adapter.backend
    per_dest: dict[int, list[int]] = {}
    for src in src_blocks:  # ascending src index
        for dest, sub in enumerate(be.partition(src, "__joinkey__", parts)):
            keys = adapter.keys_of(sub)
            if keys:
                per_dest.setdefault(dest, []).extend(keys)
    return per_dest


def result_dest_keys(adapter: P2PAdapter, value: dict[int, object]) -> dict[int, list[int]]:
    return {dest: adapter.keys_of(block) for dest, block in value.items()}


def total_result_rows(value: Mapping[int, object]) -> int:
    return sum(len(block) for block in value.values())  # type: ignore[arg-type]


def contiguous_split(n_src: int, n_tasks: int) -> list[list[int]]:
    """The frozen contiguous-ascending producer assignment (the m43 ``_assign`` contract)."""
    base, rem = divmod(n_src, n_tasks)
    ranges: list[list[int]] = []
    start = 0
    for t in range(n_tasks):
        size = base + (1 if t < rem else 0)
        ranges.append(list(range(start, start + size)))
        start += size
    return ranges


def per_task_dest_wires(
    adapter: P2PAdapter, src_blocks: Sequence[object], parts: int, k: int, salt: int = 0
) -> dict[tuple[int, int], int]:
    """The INDEPENDENT fragment oracle: per (producer task t, dest d), the wire size of the
    coalesced fragment — the producer's owned srcs routed with the golden-pinned ``partition``,
    concatenated in ascending-src order, ``to_wire``'d (the frozen m39/m43 wire contract)."""
    be: Any = adapter.backend
    out: dict[tuple[int, int], int] = {}
    for t, owned in enumerate(contiguous_split(len(src_blocks), k)):
        per_dest: dict[int, list[Any]] = {}
        for s in owned:
            for d, sub in enumerate(be.partition(src_blocks[s], "__joinkey__", parts, salt=salt)):
                if len(sub):
                    per_dest.setdefault(d, []).append(sub)
        for d, subs in per_dest.items():
            out[(t, d)] = len(bytes(be.to_wire(be.concat(subs))))
    return out


def cross_fragment_bytes(
    adapter: P2PAdapter, src_blocks: Sequence[object], parts: int, k: int
) -> tuple[int, int]:
    """``(total cross-peer bytes, cross fragment count)`` under the pinned ownership rules:
    producer task t runs on peer index t (contiguous split), dest d is gathered by peer d % k (F8).
    A fragment crosses iff d % k != t — exactly the bytes the /pull plane must serve."""
    wires = per_task_dest_wires(adapter, src_blocks, parts, k)
    cross = {(t, d): v for (t, d), v in wires.items() if d % k != t}
    return sum(cross.values()), len(cross)


def _per_dest_side(
    adapter: P2PAdapter, blocks: Sequence[object], field: str, parts: int
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
    adapter: P2PAdapter, left: Sequence[object], right: Sequence[object], parts: int
) -> Counter[JRow]:
    """DUPLICATING relational inner join as one multiset (the m40 §3.3 pin): a probe row with k
    build matches => k rows. Routes each side with the backend's ``partition`` only — the engine
    under test cannot share a join-kernel bug with this oracle."""
    left_by_dest = _per_dest_side(adapter, left, "lval", parts)
    right_by_dest = _per_dest_side(adapter, right, "rval", parts)
    total: Counter[JRow] = Counter()
    for dest in set(left_by_dest) & set(right_by_dest):
        for k_, lv in left_by_dest[dest]:
            for k2, rv in right_by_dest[dest]:
                if k_ == k2:
                    total[(k_, lv, rv)] += 1
    return total


def observed_join_all(adapter: P2PAdapter, value: dict[int, object]) -> Counter[JRow]:
    total: Counter[JRow] = Counter()
    for block in value.values():
        total += Counter(adapter.joined_rows(block))
    return total


def read_nullable_column(c: object) -> list[int | None]:
    """One joined column to Python ints with ``None`` for a null/masked entry (the m40 option-type
    pin: a sentinel implementation fails the multiset against real ``None`` rows)."""
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


def key_row_count(multiset: Counter[NJRow], key: int) -> int:
    return sum(v for (k_, _l, _r), v in multiset.items() if k_ == key)


# ---- GatedP2PShuffleBackend: deterministic mid-run worker-death scenario (the m43/m44 gate) -----


class GatedP2PShuffleBackend:
    """A delegating ``ShuffleBackend`` for the worker-death theme (the m43/m44 gated-kill
    mechanism, clock-free): ``partition`` (map-phase only) drops a ``pmark`` file per call
    recording the caller's pid; ``from_wire`` (gather-side only under repartition) drops a
    ``gstart`` mark then BLOCKS until ``gate_path`` exists — holding a gather open so the driver
    can SIGKILL a block-holding worker process mid-run deterministically. Ships to HTEX peers via
    the spec (spawn-picklable: plain attributes + a delegate). Data is delegated UNTOUCHED, so
    hashes stay comparable with a plain-backend local oracle run."""

    def __init__(self, inner: Any, mark_dir: str, gate_path: str | None) -> None:
        self.inner = inner
        self.mark_dir = mark_dir
        self.gate_path = gate_path
        self.identity = f"gated+{inner.identity}"

    def _mark(self, prefix: str) -> None:
        tmp = Path(self.mark_dir) / f".{prefix}-{uuid.uuid4().hex}"
        tmp.write_text(str(os.getpid()))
        os.replace(tmp, Path(self.mark_dir) / f"{prefix}-{uuid.uuid4().hex}")  # atomic publish

    def partition(self, block: Any, key_field: str, parts: int, **kwargs: Any) -> tuple[Any, ...]:
        self._mark("pmark")
        return tuple(self.inner.partition(block, key_field, parts, **kwargs))

    def from_wire(self, data: bytes) -> Any:
        if self.gate_path is not None:
            self._mark("gstart")
            deadline = time.monotonic() + 180.0
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


def mark_pids(mark_dir: str, prefix: str) -> list[int]:
    return [int(p.read_text()) for p in sorted(Path(mark_dir).iterdir()) if p.name.startswith(f"{prefix}-")]
