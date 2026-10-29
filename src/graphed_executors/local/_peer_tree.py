"""Peer reduction (M38): the tree reduction runs **across the workers, off the driver**.

The driver is no longer the combine hub. Each worker owns a contiguous **leaf range** and reduces
it with the lazy index tree (:mod:`_reduce`); the partials that straddle a range boundary are
handed **worker→worker** over a :class:`graphed.core.execution.WorkerTransport`. The driver only
collects the final root.

Why it stays **bit-for-bit identical** to the existing (hub) path — even for non-associative float
combines like histogram addition: every node keeps its **global** ``(level, pos)`` identity in the
one fixed ``plan_tree`` (even position = left operand, odd = right, unpaired carries up).
Distributing the *combines* across workers never changes the *grouping*, so the result is the same
down to the last ULP. Determinism is independent of message timing — a parked node waits for its
fixed sibling, whoever sends it whenever.

Ownership routing (the segment-tree merge): node ``(level, pos)`` is owned by the worker holding
its leftmost leaf ``pos << level``. The parent of a pair is owned by the **even** child's owner, so
a worker that forms an **odd** node it doesn't own ships it to that owner; an **even** node parks
until its odd sibling arrives (locally or from a peer). Only the O(log N) boundary nodes ever cross
the wire. Termination is a driver ``done`` broadcast once the root (owned by worker 0) arrives — no
fragile message counting, no barrier.

This module holds the reduction itself (topology, :class:`PeerReducer`, the sends) and its
driver side; :mod:`._peer` holds the per-worker actor and its pool entry points.
"""

from __future__ import annotations

import bisect
import contextlib
import queue
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Generic, TypeVar

from graphed.core import RunControl, RunState

from ._reduce import LazyReducer, running_fold
from ._transport import HttpTransport

R = TypeVar("R")
_MISSING: Any = object()

DRIVER = "driver"

# How long an exiting endpoint waits for its queued sends to land. A healthy send completes in
# microseconds (every live peer and the driver are reading); the bound exists only for the crash
# case, where a peer stopped reading for good and the parked pipe write can never complete — the
# actor must still return so the pool can be torn down, and the driver must still reach its own
# teardown.
OUTBOX_EXIT_WAIT_S = 5.0
# How long a failed run's driver keeps forwarding event batches that land after the failure
# surfaced.
ERROR_EVENT_DRAIN_S = 0.5


def make_bounds(n: int, n_workers: int) -> list[int]:
    """Contiguous leaf-range boundaries: worker ``w`` owns leaves ``[bounds[w], bounds[w+1])``.

    Use ``n_workers <= n`` so every range is non-empty (the executor clamps).
    """
    return [(i * n) // n_workers for i in range(n_workers + 1)]


def worker_of(leaf: int, bounds: list[int]) -> int:
    """Return the worker index owning ``leaf`` (the range containing it)."""
    return max(0, min(len(bounds) - 2, bisect.bisect_right(bounds, leaf) - 1))


# ---- bounded communication overlay (M38 P7): O(log N) degree, not all-to-all ------------------
#
# To keep the IPC registry sub-quadratic (so a worker inherits O(log N) inbox handles, not all N),
# and to keep the HTTP transport's connection count sub-quadratic on large farms, each worker talks
# to only O(log N) peers: its *reduction* partners (the binomial-style segment-tree send targets)
# plus a fixed *hypercube lifeline* overlay for work-stealing. The lifeline graph (X10 GLB /
# Saraswat et al.) is a low-degree, low-diameter, symmetric hypercube — work still routes anywhere,
# scaling to thousands of workers — and hierarchical/lifeline victim selection scales better than
# random all-to-all (HotSLAW).


def lifeline_neighbors(idx: int, w: int) -> set[int]:
    """Hypercube lifeline neighbours of worker ``idx`` among ``w`` workers.

    ``idx ^ 2^k`` for each ``2^k < w`` (skipping out-of-range when ``w`` is not a power of
    two). **Symmetric** (so a steal request and its response use the same edge), degree and
    diameter O(log w).
    """
    out: set[int] = set()
    k = 1
    while k < w:
        nb = idx ^ k
        if nb < w:
            out.add(nb)
        k <<= 1
    return out


class _EdgeRecorder:
    """A :class:`graphed.core.execution.WorkerTransport`-shaped stub.

    Used only to compute the static reduction topology: it records each ``send`` destination and
    queues ``node`` hand-offs so a value-free in-process replay of the reduction propagates fully.
    Reusing the real :meth:`PeerReducer.settle` guarantees the topology never diverges from the
    runtime reduction.
    """

    def __init__(
        self, address: str, edges: dict[str, set[str]], pending: deque[tuple[str, Any]]
    ) -> None:
        self.address = address
        self._edges = edges
        self._pending = pending

    def send(self, dest: str, message: Any) -> bool:
        self._edges.setdefault(self.address, set()).add(dest)
        if message[0] == "node":
            self._pending.append((dest, message))
        return True

    def poll(self) -> list[tuple[str, Any]]:
        return []

    def recv(self, timeout: float | None = None) -> tuple[str, Any] | None:
        return None

    def broadcast(self, message: Any) -> None: ...
    def peers(self) -> tuple[str, ...]:
        return ()

    def close(self) -> None: ...


def reduction_edges(
    n: int, bounds: list[int], worker_addresses: tuple[str, ...]
) -> dict[str, set[str]]:
    """Per worker, the set of addresses it sends reduction partials to (incl.

    ``DRIVER`` for the root holder). A static, value-free replay of the real reduction —
    deterministic from ``(n, bounds)``.
    """
    edges: dict[str, set[str]] = {}
    pending: deque[tuple[str, Any]] = deque()
    reducers = {
        a: PeerReducer(
            a, _EdgeRecorder(a, edges, pending), n, bounds, worker_addresses, lambda x, y: 1
        )
        for a in worker_addresses
    }
    for leaf in range(n):
        reducers[worker_addresses[worker_of(leaf, bounds)]].settle(0, leaf, 1)
    while pending:
        dest, message = pending.popleft()
        reducers[dest].settle(message[1], message[2], message[3])
    return edges


def worker_outbox_addresses(
    n: int, bounds: list[int], worker_addresses: tuple[str, ...]
) -> dict[str, set[str]]:
    """For each worker address, the bounded set of peer addresses it must be able to send to.

    Reduction targets + symmetric hypercube lifelines + ``DRIVER``. O(log N) per worker, so the IPC
    registry a worker inherits (its inbox + these peers' inboxes) is O(log N), total O(N log N).
    """
    redges = reduction_edges(n, bounds, worker_addresses)
    w = len(worker_addresses)
    idx = {a: i for i, a in enumerate(worker_addresses)}
    out: dict[str, set[str]] = {}
    for a in worker_addresses:
        peers = set(redges.get(a, set()))  # reduction targets (may include DRIVER)
        peers |= {worker_addresses[j] for j in lifeline_neighbors(idx[a], w)}  # steal lifelines
        peers.add(DRIVER)  # events + witness + (for w0) the root
        peers.discard(a)
        out[a] = peers
    return out


class PeerReducer(Generic[R]):
    """Per-worker reduction state.

    Feed local partials + peer nodes, keep/route/park by ownership, bubbling up the one global
    fixed tree. Worker 0 forms and ships the root to the driver.
    """

    def __init__(
        self,
        address: str,
        transport: Any,
        n: int,
        bounds: list[int],
        worker_addresses: tuple[str, ...],
        combine: Callable[[R, R], R],
    ) -> None:
        self.address = address
        self._t = transport
        self.n = n
        self._bounds = bounds
        self._workers = worker_addresses
        self._combine = combine
        # parked nodes awaiting a sibling (the frontier)
        self._present: dict[tuple[int, int], R] = {}
        self.n_combines = 0  # combines this worker performed (witness: combines are distributed)
        self.peer_sends = 0  # nodes shipped worker->worker (witness: the reduction is off-driver)
        self.peer_recvs = 0  # peer nodes consumed (witness: hand-offs were actually received)
        self.root: R | None = None
        self.have_root = False
        self.forward = False  # after a cancel: every node goes to the driver instead of settling

    def hand_in(self) -> list[tuple[int, int, R]]:
        """Stop settling (a cancel).

        Return the parked nodes and forward every later one to the driver.
        """
        self.forward = True
        parked = [(level, pos, value) for (level, pos), value in self._present.items()]
        self._present.clear()
        return parked

    def _level_size(self, level: int) -> int:
        return (self.n + (1 << level) - 1) >> level

    def _owner(self, level: int, pos: int) -> str:
        return self._workers[worker_of(pos << level, self._bounds)]

    def settle(self, level: int, pos: int, value: R) -> None:
        """Place a node and bubble it up.

        Combine with present siblings, route odd nodes owned by a peer to that peer, park
        nodes still missing a sibling. Same (level,pos)/left-right rule as the flat tree,
        so the grouping is identical regardless of who runs the combine or when.
        """
        if self.forward:
            self._t.send(DRIVER, ("item", level, pos, value))
            return
        present = self._present
        while True:
            if self._level_size(level) == 1:  # the global root (worker 0 only ever reaches it)
                self.root = value
                self.have_root = True
                self._t.send(DRIVER, ("root", value))
                return
            if pos % 2 == 0:
                # unpaired (last of an odd level) -> carry up
                if pos + 1 >= self._level_size(level):
                    level, pos = level + 1, pos >> 1
                    continue
                other = present.pop((level, pos + 1), _MISSING)
                if other is _MISSING:
                    present[(level, pos)] = value  # park: wait for the odd sibling (local or peer)
                    return
                left, right = value, other
            else:
                # parent is owned by the even child's owner
                parent_owner = self._owner(level, pos - 1)
                if parent_owner != self.address:
                    self._t.send(parent_owner, ("node", level, pos, value))  # off-driver hand-off
                    self.peer_sends += 1
                    return
                other = present.pop((level, pos - 1), _MISSING)
                if other is _MISSING:
                    present[(level, pos)] = value  # park: wait for my own even sibling to form
                    return
                left, right = other, value
            value = self._combine(left, right)
            self.n_combines += 1
            level, pos = level + 1, pos >> 1


def run_peer_worker(
    reducer: PeerReducer[R],
    transport: Any,
    local_items: Iterable[tuple[int, R]],
    *,
    poll_timeout: float = 0.05,
    prebuffered: Sequence[tuple[str, Any]] = (),
) -> None:
    """Run one worker.

    Settle every local ``(leaf, partial)``, then drain peer hand-offs until the driver
    broadcasts ``done`` (sent once the root has arrived — all combines are then complete).

    ``prebuffered`` are messages already received off the transport (e.g. ``node`` hand-offs that
    raced ahead of the HTTP discovery handshake); they are processed before resuming live ``recv``.
    """
    for leaf, partial in local_items:
        reducer.settle(0, leaf, partial)
    # dedup by node identity (a reliable transport may retry-send)
    seen: set[tuple[int, int]] = set()
    pending = list(prebuffered)
    while True:
        if pending:
            item: tuple[str, Any] | None = pending.pop(0)
        else:
            item = transport.recv(timeout=poll_timeout)
        if item is None:
            continue
        payload: Any = item[1]
        tag = payload[0]
        if tag == "node":
            _, level, pos, value = payload
            if (level, pos) in seen:  # a duplicate hand-off must not combine twice
                continue
            seen.add((level, pos))
            reducer.peer_recvs += 1
            reducer.settle(level, pos, value)
        elif tag == "done":
            return


def collect_peer_root(
    driver_transport: Any,
    empty: Callable[[], R],
    n: int,
    *,
    poll_timeout: float = 0.05,
    timeout_s: float | None = None,
) -> R:
    """Driver side: wait for worker 0's root, then broadcast ``done`` to release the workers.

    For an empty plan there is no root — return the identity. ``timeout_s`` (if set) bounds the
    wait so a lost root surfaces as a ``TimeoutError`` instead of hanging.
    """
    if n == 0:
        driver_transport.broadcast(("done",))
        return empty()

    deadline = None if timeout_s is None else time.monotonic() + timeout_s
    root: R = _MISSING
    while root is _MISSING:
        got = driver_transport.recv(timeout=poll_timeout)
        if got is not None:
            payload: Any = got[1]
            if payload[0] == "root":
                root = payload[1]
                break
        if deadline is not None and time.monotonic() >= deadline:
            driver_transport.broadcast(("done",))  # release workers before surfacing the failure
            raise TimeoutError(f"peer reduction did not produce a root within {timeout_s}s")
    driver_transport.broadcast(("done",))
    return root


class Outbox:
    """One daemon thread per endpoint that performs EVERY send on the caller's behalf.

    The IPC inbox write is **synchronous** (``PipeInbox`` over ``SimpleQueue``, no feeder thread),
    so a message larger than the OS pipe buffer (~64 KB) — any real reduction partial — parks the
    writer until the peer reads. A caller that sends on its own thread is not reading its own inbox
    while parked, and work-stealing supplies the sideways/upward edge that closes a wait cycle: a
    thief returning a ``leaf`` UP while the owner sends a ``node`` DOWN, or a tiny ``steal_req``
    stuck on the victim inbox's write lock behind a third worker's big write. Enqueueing instead
    keeps the actor in ``poll``/``recv``, so every inbox of a LIVE peer stays drained. One thread ⇒
    send order preserved.

    A peer that CRASHED, though, never reads again, and a write already parked in its pipe cannot
    be interrupted. So the thread is a daemon and :meth:`close` waits only a bounded time: whoever
    is shutting down returns on schedule and the abandoned write dies with the process. That is the
    whole guarantee — sends are asynchronous and exit is bounded, NOT that every queued message is
    delivered when a peer has stopped reading.
    """

    def __init__(self, transport: Any) -> None:
        self._t = transport
        self._q: queue.Queue[tuple[str, Any] | None] = queue.Queue()
        self._error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"graphed-outbox-{getattr(transport, 'address', '')}",
            daemon=True,
        )
        self._thread.start()

    def send(self, dest: str, message: Any) -> None:
        # deliberately no bool: the transport's own accepted/dropped verdict happens later, on the
        # outbox thread, so there is nothing truthful to hand back here.
        self._q.put((dest, message))

    @property
    def error(self) -> BaseException | None:
        """The first send failure, once one happened."""
        return self._error

    def _run(self) -> None:
        while True:
            item = self._q.get()
            if item is None:
                return
            if self._error is not None:
                # transport already failed: keep draining so the caller's exit wait can't block
                continue
            try:
                self._t.send(*item)
            except BaseException as exc:  # re-raised in the caller once the queue is drained
                self._error = exc

    def close(self, timeout: float) -> BaseException | None:
        """Stop the thread, waiting at most ``timeout`` for the queued sends to land.

        The ``root`` and the final events must land on a healthy exit. Returns a send
        failure for the caller to raise.
        """
        self._q.put(None)
        if timeout > 0:
            self._thread.join(timeout)
        return self._error


def release_workers(transport: Any) -> None:
    """Deliver the reduction's ``done`` OFF the caller's thread, one thread per destination.

    Same blocking-write hazard as :class:`Outbox`, seen from the driver: a worker that crashed
    stops reading its inbox, a peer's parked partial still holds that inbox's cross-process write
    lock, and the driver's own ``done`` then waits on the lock forever — with the worker error
    already in hand and the pool teardown one line away. Per destination, not one broadcast thread,
    because that one thread would park on the dead worker and leave every LIVE worker behind it
    unreleased — which is what the teardown is waiting for. A live worker is polling, so it is
    released immediately.

    An :class:`HttpTransport` already buffers every send onto its own sender thread, so there the
    release is enqueued on THIS thread instead: a helper thread could otherwise enqueue *after* the
    caller's ``close()`` has drained the outbound queue, losing the release exactly when it is
    needed.
    """

    def fire(dest: str) -> None:
        with contextlib.suppress(Exception):  # e.g. a registry closed under us during teardown
            transport.send(dest, ("done",))

    inline = isinstance(transport, HttpTransport)
    for dest in transport.peers():
        if inline:
            fire(dest)
        else:
            threading.Thread(
                target=fire, args=(dest,), name=f"graphed-release-{dest}", daemon=True
            ).start()


_TAGS = {RunState.RUNNING: "resume", RunState.PAUSED: "pause", RunState.CANCELLED: "cancel"}


class PeerControl(Generic[R]):
    """The driver side of a controlled peer run (plan-A2 A2-2), used on every driver poll.

    :meth:`relay` tells every worker a state change, through one :class:`Outbox` per destination
    (a worker that stopped reading parks only its own sender; one thread keeps pause→resume in
    order), or inline on an :class:`HttpTransport`, whose sends are already off-thread.
    :meth:`take` collects the workers' cancel hand-ins and folds them once they cover every
    processed leaf.
    """

    def __init__(
        self,
        control: RunControl,
        transport: Any,
        n: int,
        combine: Callable[[R, R], R],
        empty: Callable[[], R],
    ) -> None:
        self._control = control
        self._t = transport
        self._workers = transport.peers()
        self._n = n
        self._combine = combine
        self._empty = empty
        self._told = RunState.RUNNING  # the workers start running
        self._outboxes: dict[str, Outbox] = {}
        self._processed: dict[str, int] = {}  # per worker, from its `cancelled` hand-in
        self._items: dict[tuple[int, int], R] = {}  # one frontier item per node
        self._covered = 0

    def relay(self) -> bool:
        """Send the workers the control's state if it changed; True while PAUSED."""
        state = self._control.state
        if state is not self._told and self._told is not RunState.CANCELLED:
            self._told = state
            if not self._outboxes and not isinstance(self._t, HttpTransport):
                self._outboxes = {dest: Outbox(self._t) for dest in self._workers}
            for dest in self._workers:
                self._outboxes.get(dest, self._t).send(dest, (_TAGS[state],))
        return state is RunState.PAUSED

    def take(self, payload: Any) -> tuple[R, int] | None:
        """Consume a hand-in message.

        Once complete, the fold of the completed leaves and their count.

        Items cover completed leaves only, each at most once, so covered == processed means every
        completed leaf is in hand.
        """
        if payload[0] == "cancelled":
            _, address, processed, parked = payload
            self._processed[address] = processed  # a worker sends one; a retried copy is identical
            for level, pos, value in parked:
                self._add(level, pos, value)
        elif payload[0] == "item":
            self._add(*payload[1:])
        if len(self._processed) < len(self._workers) or self._covered != sum(
            self._processed.values()
        ):
            return None
        # The fixed tree first, so sibling nodes the workers stopped settling combine as it
        # combines them.
        tree: LazyReducer[R] = LazyReducer(self._n, self._combine, self._empty)
        for (level, pos), value in self._items.items():
            tree.feed(pos, value, level)
        return running_fold(iter(tree.frontier()), self._combine, self._empty)[0], self._covered

    def _add(self, level: int, pos: int, value: R) -> None:
        if (level, pos) not in self._items:
            self._items[(level, pos)] = value
            self._covered += min((pos + 1) << level, self._n) - (pos << level)

    def close(self, timeout: float) -> None:
        """Stop the senders, waiting up to ``timeout`` each for queued tags.

        Call before ``done``.
        """
        for box in self._outboxes.values():
            box.close(timeout)
        self._outboxes.clear()
