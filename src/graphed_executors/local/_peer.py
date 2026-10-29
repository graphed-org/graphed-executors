"""Peer reduction (M38), worker side: the per-worker actor and its pool entry points.

:func:`process_and_reduce` drives one worker's :class:`_PeerActor`, which processes and steals
leaves and settles them into the :class:`PeerReducer` of :mod:`._peer_tree`; the
``*_peer_actor`` functions are the picklable entry points the executors submit.
"""

from __future__ import annotations

import contextlib
import time
from collections import deque
from collections.abc import Callable, Sequence
from typing import Any, Generic, TypeVar

from graphed.core.execution import (
    LocalResources,
    Partition,
    TaskEvent,
    TaskPhase,
    emit_task,
    partition_label,
)
from graphed.debug import StageError

from ._peer_tree import (
    DRIVER,
    ERROR_EVENT_DRAIN_S,
    OUTBOX_EXIT_WAIT_S,
    Outbox,
    PeerControl,
    PeerReducer,
    collect_peer_root,
    lifeline_neighbors,
    make_bounds,
    release_workers,
    run_peer_worker,
    worker_of,
    worker_outbox_addresses,
)
from ._pinned_pool import exit_with_driver
from ._transport import HttpTransport, QueueTransport

R = TypeVar("R")

# the reduction core in ._peer_tree is re-exported: callers import the whole protocol from here
__all__ = [
    "DRIVER",
    "ERROR_EVENT_DRAIN_S",
    "MAX_STEAL_BACKOFF",
    "OUTBOX_EXIT_WAIT_S",
    "PROFILE_FLUSH_INTERVAL",
    "STEAL_DELAY",
    "Outbox",
    "PeerControl",
    "PeerReducer",
    "collect_peer_root",
    "http_driver_handshake",
    "http_peer_actor",
    "ipc_peer_actor",
    "lifeline_neighbors",
    "make_bounds",
    "peer_pool_init",
    "pinned_peer_actor",
    "pinned_peer_init",
    "pooled_peer_actor",
    "process_and_reduce",
    "release_workers",
    "released",
    "run_peer_worker",
    "slice_items",
    "worker_of",
    "worker_outbox_addresses",
]


# Only steal after a worker has been idle this long — long enough that a BALANCED run (all workers
# finish within ~ms of each other) completes before any steal fires, so stealing is near-free on
# the common case; short enough that a genuinely imbalanced load (a straggler idle for 100s of ms)
# still rebalances promptly. Without this, idle workers fire spurious steals near the end of
# balanced runs and add coordination tail latency for no benefit.
STEAL_DELAY = 0.01
# After the first request, back off exponentially up to this cap while requests keep being DENIED,
# so an idle worker during a balanced run's reduction tail doesn't storm peers with steal-requests
# (the denial traffic alone — even with zero successful steals — measurably slowed uniform runs). A
# successful steal resets the backoff, so a genuine straggler is still drained promptly.
MAX_STEAL_BACKOFF = 0.1
# Serialize the off-thread profiler at most ~1/s (its sampler keeps running continuously; only the
# flush + ship is throttled), never per leaf — the M37 R20.7 discipline.
PROFILE_FLUSH_INTERVAL = 1.0


def slice_items(
    partitions: Sequence[Partition], bounds: list[int], worker_addresses: tuple[str, ...]
) -> dict[str, list[tuple[int, Partition]]]:
    """Split the key-ordered partitions into each worker's leaf range.

    Split the key-ordered partitions into each worker's ``[(leaf, partition), ...]`` by leaf range.
    """
    out: dict[str, list[tuple[int, Partition]]] = {a: [] for a in worker_addresses}
    for leaf, part in enumerate(partitions):
        out[worker_addresses[worker_of(leaf, bounds)]].append((leaf, part))
    return out


def process_and_reduce(
    address: str,
    transport: Any,
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    process: Callable[[Partition, Any], R],
    combine: Callable[[R, R], R],
    items: Sequence[tuple[int, Partition]],
    resources: LocalResources,
    *,
    steal: bool = True,
    steal_peers: tuple[str, ...] | None = None,
    emit: bool = False,
    profiler_factory: Callable[[], Any] | None = None,
    prebuffered: Sequence[tuple[str, Any]] = (),
    close_resources: bool = True,
    release: Any = None,
    monitor_factory: Callable[[], Any] | None = None,
    lean: bool = False,
    keys: Sequence[int] | None = None,
    complete: bool = False,
) -> dict[str, int]:
    """Run the per-worker actor.

    Run ``process`` on its leaves, peer-reduce over the transport, and return witness counters.
    Runs in a thread (ThreadExecutor) or a worker process (ProcessExecutor).

    **Work-stealing** (``steal=True``, the default): a worker that drains its own leaves steals
    from a busy peer. Stealing redistributes only the ``process`` work — the leaf's **owner still
    settles it** into the reduction (a thief ships the computed partial back as a ``leaf``
    message), so the fixed tree and the result are **unchanged**: stealing moves *where* a leaf is
    computed, never *where* it reduces. The loop interleaves a non-blocking inbox drain between
    leaves, so a busy worker answers steal requests; every send is handed to the :class:`Outbox`
    thread, so the actor never blocks writing to a peer (the R20.7 rule) and its own inbox is
    always being drained — the property that makes a send cycle between peers impossible for
    partials larger than the OS pipe buffer. The actor also RETURNS in bounded time: a peer that
    crashed stops reading, so the exit wait for queued sends is capped (``OUTBOX_EXIT_WAIT_S``) and
    an abandoned write dies with the worker process.

    ``resources`` (its ``open_once`` cache) is supplied by the caller; ``close_resources=False``
    keeps it open across runs (a persistent pool reusing file handles, like the hub path).

    ``release`` is the read end of the run's one-way pipe, whose only write end the driver holds.
    It reads EOF once the driver closes that end on any exit of the run, or dies, and the actor
    then leaves as on ``done``: the driver needs no worker address to fire it.

    ``emit`` events carry ``keys[leaf]`` (the leaf index without ``keys``); ``lean`` drops STARTED
    and the label; a ``monitor_factory`` builds this actor's own monitor, which then gets the task
    events and profile trees that would otherwise ship to the driver. ``complete`` makes a failing
    actor wait, bounded, for its queued sends (its last event batch included) before it raises.
    """
    actor: _PeerActor[R] = _PeerActor(
        address,
        transport,
        n,
        bounds,
        worker_addresses,
        process,
        combine,
        items,
        steal=steal,
        steal_peers=steal_peers,
        emit=emit,
        prebuffered=prebuffered,
        monitor_factory=monitor_factory,
        lean=lean,
        keys=keys,
    )
    # off-thread profiler parity with the hub: a worker samples its task thread and ships the
    # flamegraph tree to the driver (forwarded to monitor.on_profile), throttled so the serialize
    # is never per-leaf.
    profiler = None
    last_flush = time.monotonic()
    if profiler_factory is not None:
        with contextlib.suppress(Exception):  # a profiler that won't start just disables sampling
            profiler = profiler_factory()
            profiler.start()
    try:
        while not actor.done:
            if released(release):  # the driver's run ended, however it ended
                break
            # a failed send (an unpicklable partial) never reaches the root
            if actor.outbox.error is not None:
                raise actor.outbox.error
            actor.drain()
            if actor.done:
                break
            if emit:
                # batch off-path: one send per leaf-ish, negligible for coarse partitions
                actor.ship_events()
            if profiler is not None and time.monotonic() - last_flush >= PROFILE_FLUSH_INTERVAL:
                with contextlib.suppress(Exception):
                    actor.ship_profile(profiler.flush())
                last_flush = time.monotonic()
            actor.step(resources)
        if emit:
            # final flush after `done` — the driver drains until workers finish
            actor.ship_events()
        if profiler is not None:
            with contextlib.suppress(Exception):
                actor.ship_profile(profiler.stop())  # final sample tree + join the sampler thread
        # let the queued sends land, but never hang here
        send_error = actor.outbox.close(OUTBOX_EXIT_WAIT_S)
    except BaseException:
        # this run is already failing: don't wait on sends to a peer that may never read again
        # (unless the monitor asked for complete events), and don't let a send failure displace the
        # actor's own exception.
        actor.outbox.close(OUTBOX_EXIT_WAIT_S if complete else 0.0)
        raise
    finally:
        if close_resources:
            resources.close()
    if send_error is not None:
        raise send_error
    return actor.witness()


class _PeerActor(Generic[R]):
    """One worker's peer-reduction state, driven a step at a time by :func:`process_and_reduce`.

    It owns the leaves left to process (its own and stolen ones), the reducer it settles them
    into, the steal backoff, and the run-control tags the driver sent.
    """

    def __init__(
        self,
        address: str,
        transport: Any,
        n: int,
        bounds: list[int],
        worker_addresses: tuple[str, ...],
        process: Callable[[Partition, Any], R],
        combine: Callable[[R, R], R],
        items: Sequence[tuple[int, Partition]],
        *,
        steal: bool,
        steal_peers: tuple[str, ...] | None,
        emit: bool,
        prebuffered: Sequence[tuple[str, Any]],
        monitor_factory: Callable[[], Any] | None,
        lean: bool,
        keys: Sequence[int] | None,
    ) -> None:
        self.address = address
        self.transport = transport
        self.outbox = Outbox(transport)  # the reducer's node/root sends go through it too
        self.reducer: PeerReducer[R] = PeerReducer(
            address, self.outbox, n, bounds, worker_addresses, combine
        )
        self._bounds = bounds
        self._worker_addresses = worker_addresses
        self._process = process
        self._steal = steal
        self._emit = emit
        self._lean = lean
        self._keys = keys
        self.mine: deque[tuple[int, Partition]] = deque(items)  # leaves to PROCESS (own + stolen)
        # dedup settled nodes/leaves (a reliable transport may retry)
        self.seen: set[tuple[int, int]] = set()
        # victims this worker may steal from: the bounded lifeline overlay when given (O(log N), so
        # the IPC registry stays sub-quadratic), else every peer (the in-process / full-mesh case).
        self.peers = (
            steal_peers
            if steal_peers is not None
            else tuple(a for a in worker_addresses if a != address)
        )
        self.me = worker_addresses.index(address)
        # asked: steal-requests I sent
        self.stats = {"steals": 0, "given": 0, "asked": 0, "processed": 0}
        self.victim = 0
        # when this worker ran out of local work (gates the steal delay)
        self.idle_since: float | None = None
        # current wait between steal-requests; grows on denial, resets on a grant
        self.backoff = STEAL_DELAY
        self.next_steal_at = 0.0  # monotonic time of the next allowed steal-request
        self.pending = [payload for _sender, payload in prebuffered]
        self.done = False
        self.paused = self.cancelled = False  # the driver's run-control tags (plan-A2 A2-2)
        self.events: list[TaskEvent] = []  # M37 monitor events, batched to the driver off-path
        self.monitor = None
        if monitor_factory is not None:
            with contextlib.suppress(Exception):  # a monitor that won't build disables telemetry
                self.monitor = monitor_factory()

    def emit_event(
        self, phase: TaskPhase, leaf: int, part: Partition, error: str | None = None
    ) -> None:
        if self._lean and phase is TaskPhase.STARTED:
            return
        ev = TaskEvent(
            phase,
            leaf if self._keys is None else self._keys[leaf],
            self.address,
            time.perf_counter(),
            "" if self._lean else partition_label(part),
            part.n_entries,
            error=error,
        )
        if self.monitor is not None:
            emit_task(self.monitor, ev)
        else:
            self.events.append(ev)

    def ship_events(self) -> None:
        if self.events:
            # the driver forwards them to the monitor
            self.outbox.send(DRIVER, ("events", self.events.copy()))
            self.events.clear()

    def ship_profile(self, payload: bytes | None) -> None:
        if payload and self.monitor is not None:
            with contextlib.suppress(Exception):
                self.monitor.on_profile(self.address, payload)
        elif payload:
            self.outbox.send(DRIVER, ("profile", self.address, payload))

    def owner(self, leaf: int) -> str:
        return self._worker_addresses[worker_of(leaf, self._bounds)]

    def settle_leaf(self, leaf: int, value: R) -> None:
        # idempotent: own-processing and a thief's `leaf` can't double-count
        if (0, leaf) not in self.seen:
            self.seen.add((0, leaf))
            self.reducer.settle(0, leaf, value)

    def handle(self, payload: Any) -> None:
        tag = payload[0]
        if tag == "node":
            _, lvl, pos, val = payload
            if (lvl, pos) not in self.seen:
                self.seen.add((lvl, pos))
                self.reducer.peer_recvs += 1
                self.reducer.settle(lvl, pos, val)
        elif tag == "leaf":  # a partial (computed by a thief) for a leaf I OWN -> I settle it
            self.settle_leaf(payload[1], payload[2])
        elif tag == "steal_req":
            self._grant(payload[1])
        elif tag == "steal_resp":
            _, granted, grant = payload
            if grant not in self.seen:
                self.seen.add(grant)
                self.mine.extend(granted)
                self.stats["steals"] += len(granted)
        elif tag == "pause":
            self.paused = True
        elif tag == "resume":
            self.paused = False
        elif tag == "cancel" and not self.cancelled:
            # Between leaves, so nothing is in flight here: hand in the count and the parked nodes.
            self.cancelled = True
            self.mine.clear()
            self.outbox.send(
                DRIVER,
                ("cancelled", self.address, self.stats["processed"], self.reducer.hand_in()),
            )
        elif tag == "done":
            self.done = True

    def _grant(self, thief: str) -> None:
        # Hand ONE leaf from the FAR end (steal-one, Blumofe-Leiserson / Cilk). NOT steal-half:
        # under many idle thieves hitting one victim, serialized "half each" drains the victim
        # geometrically (keeps W/2^k) and over-concentrates work on the first thief -> worse
        # makespan for our coarse, independent partitions. Steal-one lets k thieves each take
        # one leaf fairly with no cascade; steals are cheap relative to a partition, so the
        # extra steal attempts cost ~nothing. (`len > 1`: never give away the leaf I'm about to
        # run.) Each grant carries its own id (a negative level, never a node's), so the thief
        # keeps a response retried by an at-least-once transport once, and a leaf granted twice
        # twice. Only an owned leaf is granted: the thief returns it to its owner, and the
        # victim is the only owner guaranteed to be in the thief's bounded overlay.
        if self._steal and len(self.mine) > 1 and self.owner(self.mine[-1][0]) == self.address:
            self.stats["given"] += 1
            self.outbox.send(
                thief, ("steal_resp", [self.mine.pop()], (-1 - self.me, self.stats["given"]))
            )
        else:
            self.outbox.send(thief, ("steal_resp", [], (-1 - self.me, 0)))

    def drain(self) -> None:
        """Handle the buffered messages, then a cheap non-blocking drain between leaves."""
        while self.pending and not self.done:
            self.handle(self.pending.pop(0))
        for _sender, payload in self.transport.poll():
            self.handle(payload)
            if self.done:
                break

    def step(self, resources: LocalResources) -> None:
        """Process one leaf, or ask a peer for work, or wait briefly for a message."""
        if self.mine and not (self.paused or self.cancelled):
            # have work again (incl. a successful steal) -> reset steal backoff
            self.idle_since = None
            self.backoff = STEAL_DELAY
            self._process_leaf(resources)
            return
        if self._steal and self.peers and not (self.paused or self.cancelled):
            # no local work: request a steal once idle past STEAL_DELAY, then back off on each
            # denial so a balanced run's reduction tail isn't flooded with steal-requests.
            now = time.monotonic()
            if self.idle_since is None:
                self.idle_since, self.next_steal_at, self.backoff = (
                    now,
                    now + STEAL_DELAY,
                    STEAL_DELAY,
                )
            if now >= self.next_steal_at:
                self.outbox.send(
                    self.peers[self.victim % len(self.peers)], ("steal_req", self.address)
                )
                self.stats["asked"] += 1
                self.victim += 1
                self.next_steal_at = now + self.backoff
                self.backoff = min(self.backoff * 2, MAX_STEAL_BACKOFF)
            got = self.transport.recv(timeout=0.01)  # stay responsive to node/leaf/done while idle
        else:
            got = self.transport.recv(timeout=0.05)
        if got is not None:
            self.handle(got[1])

    def _process_leaf(self, resources: LocalResources) -> None:
        leaf, part = self.mine.popleft()
        if self._emit:
            self.emit_event(TaskPhase.STARTED, leaf, part)
        try:
            partial = self._process(part, resources)  # HOT PATH (read + compute)
        except BaseException as exc:
            if self._emit:
                msg = str(exc) if isinstance(exc, StageError) else f"{type(exc).__name__}: {exc}"
                self.emit_event(TaskPhase.ERRORED, leaf, part, error=msg)
                # surface the error to the dashboard before the run tears down
                self.ship_events()
            raise
        self.stats["processed"] += 1
        if self._emit:
            self.emit_event(TaskPhase.FINISHED, leaf, part)
        if self.owner(leaf) == self.address:
            self.settle_leaf(leaf, partial)  # my leaf -> reduce here
        else:
            # stolen -> back to its owner
            self.outbox.send(self.owner(leaf), ("leaf", leaf, partial))

    def witness(self) -> dict[str, int]:
        return {
            "n_combines": self.reducer.n_combines,
            "peer_sends": self.reducer.peer_sends,
            "peer_recvs": self.reducer.peer_recvs,
            **self.stats,
        }


# A per-WORKER-PROCESS resource cache reused across run()s of a persistent pool, so a worker
# reopens a file at most once over its whole life (file locality across runs — the hub path's
# _proc_resources does the same). One peer actor runs at a time per process, so no intra-process
# concurrency.
_peer_proc_resources: LocalResources | None = None


def _worker_proc_resources() -> LocalResources:
    global _peer_proc_resources
    if _peer_proc_resources is None:
        _peer_proc_resources = LocalResources()
    return _peer_proc_resources


# ---- identity-pinned worker specialisation (M38 P7), driven by PinnedProcessPool ----------------
#
# An identity-pinned worker is spawned ONCE, owns ``address`` for life, and inherits ONLY its inbox
# + the outboxes of its O(log N) overlay peers (so the IPC registry is sub-quadratic, not O(N²)). A
# later run whose merge targets fall outside that overlay sends those nodes through the driver. The
# pool delivers each run's ``_pinned_peer_actor`` call over a per-worker control channel SEPARATE
# from the reduction transport, so reduction/steal messages that race ahead simply wait in the
# transport inbox (no buffering hack) and the witness/error come back as the call's Future result.

# (address, transport, steal_peers) per worker
_pinned: tuple[str, Any, tuple[str, ...]] | None = None


def pinned_peer_init(
    address: str, inbox: Any, outboxes: dict[str, Any], steal_peers: tuple[str, ...]
) -> None:
    """``PinnedProcessPool`` per-worker initializer.

    Stash this worker's identity + its inherited transport (inbox + the O(log N) outboxes) +
    its steal lifelines as a process global.
    """
    global _pinned
    _pinned = (
        address,
        QueueTransport(address, inbox, outboxes, via=outboxes[DRIVER]),
        steal_peers,
    )


def pinned_peer_actor(
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    process: Callable[[Partition, Any], Any],
    combine: Callable[[Any, Any], Any],
    items: Sequence[tuple[int, Partition]],
    steal: bool = True,
    emit: bool = False,
    profiler_factory: Callable[[], Any] | None = None,
    monitor_factory: Callable[[], Any] | None = None,
    lean: bool = False,
    keys: Sequence[int] | None = None,
    complete: bool = False,
    release: Any = None,
) -> dict[str, int]:
    """One run of the pinned worker.

    Peer-reduce its leaves over the inherited transport, return the witness (the call's Future
    carries it back, and re-raises a worker error intact — M6/M7).
    """
    assert _pinned is not None, "pinned_peer_init must run as the pool worker initializer"
    address, transport, steal_peers = _pinned
    return process_and_reduce(
        address,
        transport,
        n,
        bounds,
        worker_addresses,
        process,
        combine,
        items,
        _worker_proc_resources(),
        steal=steal,
        steal_peers=steal_peers,
        emit=emit,
        profiler_factory=profiler_factory,
        monitor_factory=monitor_factory,
        lean=lean,
        keys=keys,
        complete=complete,
        close_resources=False,
        release=release,
    )


# ---- full-registry IPC actor (small/medium w): the ProcessPoolExecutor path ---------------------
#
# For small/medium worker counts the all-inherit registry (every worker inherits every inbox) is
# the fast, simplest IPC path — a ``ProcessPoolExecutor`` whose initializer hands each worker the
# full registry of SimpleQueue inboxes. The O(N²) inheritance is a non-issue while N is well under
# the per-process fd limit; the executor switches to the bounded identity-pinned pool
# (PinnedProcessPool) only when N approaches that limit (large many-core machines) — see
# ``ProcessExecutor._peer_ipc``.

_peer_registry: dict[str, Any] | None = None


def peer_pool_init(registry: dict[str, Any]) -> None:
    """``ProcessPoolExecutor`` initializer.

    Stash the inherited full registry so each actor resolves its own inbox + the peers'
    outboxes by address (no Manager server).
    """
    global _peer_registry
    _peer_registry = registry
    exit_with_driver()


def pooled_peer_actor(
    address: str,
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    process: Callable[[Partition, Any], Any],
    combine: Callable[[Any, Any], Any],
    items: Sequence[tuple[int, Partition]],
    steal: bool = True,
    emit: bool = False,
    profiler_factory: Callable[[], Any] | None = None,
    monitor_factory: Callable[[], Any] | None = None,
    lean: bool = False,
    keys: Sequence[int] | None = None,
    complete: bool = False,
    release: Any = None,
) -> dict[str, int]:
    """Picklable pool entry point.

    Resolve this actor's inbox/outboxes from the inherited full registry (set by
    :func:`peer_pool_init`) keyed by ``address``, then delegate to :func:`ipc_peer_actor`. Any
    worker can serve any address because every worker inherited the full registry at spawn.
    """
    assert _peer_registry is not None, "peer_pool_init must run as the pool initializer"
    inbox = _peer_registry[address]
    outboxes = {a: q for a, q in _peer_registry.items() if a != address}
    return ipc_peer_actor(
        address,
        inbox,
        outboxes,
        n,
        bounds,
        worker_addresses,
        process,
        combine,
        items,
        steal=steal,
        emit=emit,
        profiler_factory=profiler_factory,
        monitor_factory=monitor_factory,
        lean=lean,
        keys=keys,
        complete=complete,
        release=release,
    )


def ipc_peer_actor(
    address: str,
    inbox: Any,
    outboxes: dict[str, Any],
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    process: Callable[[Partition, Any], Any],
    combine: Callable[[Any, Any], Any],
    items: Sequence[tuple[int, Partition]],
    steal: bool = True,
    emit: bool = False,
    profiler_factory: Callable[[], Any] | None = None,
    monitor_factory: Callable[[], Any] | None = None,
    lean: bool = False,
    keys: Sequence[int] | None = None,
    complete: bool = False,
    release: Any = None,
) -> dict[str, int]:
    """Module-level (picklable) IPC actor.

    Build the queue transport from the ``inbox``/``outboxes`` handed in, then process +
    peer-reduce. The ``ProcessExecutor`` peer path uses :func:`pinned_peer_actor` (transport
    from the per-worker init); this explicit-queue form is used to exercise the actor
    in-process. Reuses the process resource cache across runs (file locality).
    """
    transport = QueueTransport(address, inbox, outboxes)
    return process_and_reduce(
        address,
        transport,
        n,
        bounds,
        worker_addresses,
        process,
        combine,
        items,
        _worker_proc_resources(),
        steal=steal,
        emit=emit,
        profiler_factory=profiler_factory,
        monitor_factory=monitor_factory,
        lean=lean,
        keys=keys,
        complete=complete,
        close_resources=False,
        release=release,
    )


def released(release: Any) -> bool:
    """Whether the run's release pipe has been closed by its driver.

    Whether the run's release pipe (see :func:`process_and_reduce`) has been closed by its driver.
    """
    if release is None:
        return False
    try:
        return bool(release.poll())
    except OSError:  # Windows reports the closed write end as a broken pipe, not as EOF
        return True


def http_peer_actor(
    address: str,
    driver_host: str,
    driver_port: int,
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    process: Callable[[Partition, Any], Any],
    combine: Callable[[Any, Any], Any],
    items: Sequence[tuple[int, Partition]],
    steal: bool = True,
    emit: bool = False,
    profiler_factory: Callable[[], Any] | None = None,
    monitor_factory: Callable[[], Any] | None = None,
    lean: bool = False,
    keys: Sequence[int] | None = None,
    complete: bool = False,
    release: Any = None,
) -> dict[str, int]:
    """Module-level (picklable) HTTP actor for ``ProcessExecutor``.

    Bind a loopback server, announce ``(host, port)`` to the driver, wait for the assembled
    registry (buffering any node hand-offs that race ahead), then process + peer-reduce. Proves
    the transport works across real processes.
    """
    transport = HttpTransport(address)
    transport.set_registry({DRIVER: (driver_host, driver_port)})
    transport.send(DRIVER, ("hello", address, transport.host, transport.port))
    prebuffered: list[tuple[str, Any]] = []
    registry: dict[str, tuple[str, int]] | None = None
    while registry is None:
        got = transport.recv(timeout=0.2)
        if got is None:
            if released(release):  # the driver left before the handshake
                transport.close()
                raise RuntimeError("the driver left before sending the registry")
            continue
        payload: Any = got[1]
        if payload[0] == "registry":
            registry = payload[1]
        else:
            prebuffered.append((got[0], payload))  # a node/done that raced ahead — keep it
    transport.set_registry(registry)
    try:
        return process_and_reduce(
            address,
            transport,
            n,
            bounds,
            worker_addresses,
            process,
            combine,
            items,
            _worker_proc_resources(),
            steal=steal,
            emit=emit,
            profiler_factory=profiler_factory,
            monitor_factory=monitor_factory,
            lean=lean,
            keys=keys,
            complete=complete,
            prebuffered=prebuffered,
            close_resources=False,
            release=release,
        )
    finally:
        transport.close()


def http_driver_handshake(
    driver_transport: HttpTransport, worker_addresses: tuple[str, ...], *, timeout_s: float = 30.0
) -> None:
    """Driver side of the HTTP discovery.

    Gather each worker's announced ``(host, port)``, assemble the full registry, and send it to
    every worker so they can address each other.
    """
    registry: dict[str, tuple[str, int]] = {DRIVER: (driver_transport.host, driver_transport.port)}
    deadline = time.monotonic() + timeout_s
    while len(registry) <= len(worker_addresses):
        got = driver_transport.recv(timeout=0.1)
        if got is not None:
            payload: Any = got[1]
            if payload[0] == "hello":
                _, addr, host, port = payload
                registry[addr] = (host, port)
                continue
        if time.monotonic() >= deadline:
            raise TimeoutError("HTTP peer workers did not all announce in time")
    driver_transport.set_registry(registry)
    for addr in worker_addresses:
        driver_transport.send(addr, ("registry", registry))
