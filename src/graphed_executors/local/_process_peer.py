"""M38 peer reduction across spawned worker processes: the pools, the actors, the root wait.

:class:`_ProcessPeerBase` runs a peer plan on worker processes over IPC (a full-registry stdlib
pool, or an identity-pinned :class:`PinnedProcessPool`) or loopback HTTP, and keeps that pool
across runs when the executor is persistent. The hub pool, the dashboard collector and the process
broadcast live on its subclass in :mod:`.executors`.
"""

from __future__ import annotations

import contextlib
import multiprocessing
import os
import shutil
import signal
import tempfile
import threading
import time
import weakref
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, wait
from concurrent.futures import ProcessPoolExecutor as _StdProcessPool
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any, Generic, TypeVar, cast

from graphed import WorkerLostError
from graphed.core import ExecResult, Partition, Plan, RunControl, chunk_label
from graphed.core.execution import complete_events
from graphed.core.worker_warnings import process_id

from ._driver import _BaseExecutor
from ._peer import (
    ERROR_EVENT_DRAIN_S,
    OUTBOX_EXIT_WAIT_S,
    Outbox,
    PeerControl,
    http_driver_handshake,
    http_peer_actor,
    lifeline_neighbors,
    peer_pool_init,
    pinned_peer_actor,
    pinned_peer_init,
    pooled_peer_actor,
    release_workers,
    worker_outbox_addresses,
)
from ._pinned_pool import PinnedProcessPool, exit_with_driver
from ._transport import HttpTransport, PipeInbox, QueueTransport

R = TypeVar("R")

# Monitored peer runs drain trailing worker events while waiting for the futures to finish; a
# coarse poll cadence here is a fixed per-run tail (the no-monitor path blocks on f.result() to
# avoid exactly that). 2 ms keeps the tail negligible (~1 % on a sub-second pass) without
# busy-spinning.
_PEER_MONITOR_DRAIN_POLL_S = 0.002


def _drain_queue(q: Any) -> None:
    """Discard any straggler messages left in a reused peer inbox (a clean run leaves none)."""
    with contextlib.suppress(Exception):
        while True:
            q.get_nowait()


def _close_registry(registry: dict[str, Any]) -> None:
    """Tear down a peer inbox registry (PipeInbox/SimpleQueue — no feeder thread to join)."""
    for q in registry.values():
        with contextlib.suppress(Exception):
            q.close()


def _worker_processes(pool: Any) -> list[BaseProcess]:
    """Return ``pool``'s worker processes, which the stdlib pool's public API does not name."""
    if pool is None:
        return []
    return pool.workers if isinstance(pool, PinnedProcessPool) else list(pool._processes.values())


# a spawned worker's exit when the script it re-imported calls run(): the driver names the fix
_UNGUARDED_EXIT = 83
_UNGUARDED = """run() was called while a pool worker re-imported your script.
  Put the run under:  if __name__ == "__main__":"""


def _bootstrapping() -> bool:
    """Whether this process is a spawned worker still importing its parent's main."""
    return bool(getattr(multiprocessing.current_process(), "_inheriting", False))


@dataclass(frozen=True)
class _InFlight(Generic[R]):
    """``fn`` that names its work in a file while it runs.

    The file outlives a killed worker, so the driver can say what it died on. ``label`` names
    work that has no partition; a chunk is named by the one it is called with.
    """

    # ponytail: a file per call; a shared-memory slot per worker if 1 ms chunks ever matter
    fn: Callable[..., R]
    directory: str
    label: str | None = None

    def __call__(self, *args: Any) -> R:
        marker = Path(self.directory, f"{os.getpid()}.{threading.get_ident()}")
        with contextlib.suppress(OSError):  # diagnostics never fail a chunk
            marker.write_text(self.label or chunk_label(args[0]))
        try:
            return self.fn(*args)
        finally:
            with contextlib.suppress(OSError):
                marker.unlink()


class _ProcessPeerBase(_BaseExecutor):
    """Peer reduction on spawned worker processes, reused across runs when ``persistent=True``.

    The IPC pool is the subclass's fixed choice, ``_peer_pool_is_pinned`` (no silent switch): a
    full-registry stdlib pool whose every worker inherits every inbox (O(N²) fds, fine while N is
    well under the fd limit), or a :class:`PinnedProcessPool` whose worker inherits only its inbox
    and its O(log N) overlay peers (reduction targets, hypercube steal lifelines, the driver). Both
    bound stealing to the lifelines. HTTP always uses a plain stdlib pool.
    """

    #: Which pool the peer-reduction IPC path builds. Subclasses set this.
    _peer_pool_is_pinned: bool = False
    #: How long a freshly spawned pool worker may take to boot and answer the driver.
    _worker_start_timeout_s = 60.0

    # The persistent peer state; both paths reuse a pool of the same width (a pinned worker reaches
    # a merge target its spawn-time overlay lacks through the driver).
    _peer_pool: _StdProcessPool | PinnedProcessPool | None = None
    _peer_nworkers = 0
    _peer_pinned = False  # which IPC pool the live one is
    _peer_registry: dict[str, Any] | None = None  # the inbox per address
    _peer_driver: QueueTransport | None = None
    _last_peer_relays = 0  # nodes the driver forwarded in the last pinned run

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._in_flight = ""
        # a worker re-importing main exits in run(), and a kill before then would leak a dir
        if not _bootstrapping():
            self._make_in_flight()

    def _make_in_flight(self) -> None:
        # per executor, not per run: a persistent pool's broadcast token hashes the path
        self._in_flight = tempfile.mkdtemp(prefix="graphed-in-flight-")
        weakref.finalize(self, shutil.rmtree, self._in_flight, ignore_errors=True)

    def run(self, plan: Plan[R]) -> ExecResult[R]:
        # what stdlib spawn itself checks, but it reports it in every worker's traceback
        if _bootstrapping():
            os._exit(_UNGUARDED_EXIT)
        if not self._in_flight:  # built during a worker's bootstrap, run after it
            self._make_in_flight()
        return super().run(plan)

    def _watched(self, fn: Callable[..., R], label: str | None = None) -> Callable[..., R]:
        return _InFlight(fn, self._in_flight, label)

    def _busy(self) -> bool:
        return bool(os.listdir(self._in_flight))

    def _run_once(self, plan: Plan[R], control: RunControl | None) -> ExecResult[R]:
        # work a shutdown cut off left its marker, which would read as running from now on
        shutil.rmtree(self._in_flight, ignore_errors=True)
        os.makedirs(self._in_flight, exist_ok=True)
        return super()._run_once(plan, control)

    def _lost_in(self, workers: Sequence[BaseProcess]) -> WorkerLostError | None:
        """Return the error for the first dead one of ``workers``, ``None`` if all are alive.

        Raise :class:`RuntimeError` instead when one exited for an unguarded script.
        """
        exited = [p for p in workers if p.exitcode is not None]
        if not exited:
            return None
        if any(p.exitcode == _UNGUARDED_EXIT for p in exited):
            raise RuntimeError(_UNGUARDED) from None
        # a broken stdlib pool SIGTERMs the survivors; the worker that broke it exited otherwise
        dead = next((p for p in exited if p.exitcode != -signal.SIGTERM), exited[0])
        chunk = "no chunk (a combine, or idle)"
        for marker in Path(self._in_flight).glob(f"{dead.pid}.*"):
            with contextlib.suppress(OSError):
                chunk = marker.read_text() or chunk
        return WorkerLostError(partition=chunk, worker=f"pid {dead.pid}")

    def _raise_lost(self, pool: Any) -> None:
        lost = self._lost_in(_worker_processes(pool))
        if lost is not None:
            raise lost

    @contextlib.contextmanager
    def _lost_workers(self, pool: Any) -> Iterator[None]:
        try:
            yield
        except BrokenProcessPool as exc:
            if not getattr(pool, "_broken", False):  # a chunk's own nested pool, not this one
                raise
            workers = _worker_processes(pool)
            # either joins them, so every exit code is in
            if self._kept_pool is pool:
                self._release_kept_pool()
            else:
                pool.shutdown()
            lost = self._lost_in(workers)
            if lost is None:
                raise
            raise lost from exc

    def _close_peer(self) -> None:
        if self._peer_pool is not None:
            self._peer_pool.shutdown(wait=True)
            self._peer_pool = None
        if self._peer_registry is not None:
            _close_registry(self._peer_registry)
            self._peer_registry = None
        self._peer_driver = None
        self._peer_nworkers = 0
        self._peer_pinned = False

    def _check_fd_budget(self, w: int) -> None:
        """Warn when a ``w``-worker IPC pool would strain the fd limit; this pool never does."""

    def _peer_execute(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        ctx = multiprocessing.get_context("spawn")
        try:
            if (self._comms or "ipc") == "http":
                return self._peer_http(plan, n, w, bounds, worker_addrs, items, ctx, control)
            return self._peer_ipc(plan, n, w, bounds, worker_addrs, items, ctx, control)
        except BaseException:
            # A failed run leaves messages in flight that a bounded exit deliberately abandoned: a
            # peer's >64 KB partial parked in the crashed worker's pipe, the driver's own `done`.
            # They land AFTER run() returns, so a reused pool cannot be cleaned by draining at the
            # start of the next run — that run would take the stale `done` as its first message and
            # its worker would leave the actor before reducing anything. Drop the persistent peer
            # state instead; the next run respawns. Bounded because every exit closes the run's
            # release pipe: the surviving actors return within ``OUTBOX_EXIT_WAIT_S``.
            self._close_peer()
            raise

    def _ipc_pool(
        self, n: int, w: int, bounds: list[int], worker_addrs: tuple[str, ...], ctx: Any
    ) -> tuple[_StdProcessPool | PinnedProcessPool, dict[str, Any], QueueTransport]:
        """Return the IPC pool, its inbox registry and the driver's transport for this run.

        A persistent executor reuses the live pool when it matches; anything else respawns.
        """
        pinned = self._peer_pool_is_pinned
        if (
            self._persistent
            and self._peer_pool is not None
            and self._peer_pinned == pinned
            and self._peer_nworkers == w
            and self._peer_registry is not None
            and self._peer_driver is not None
        ):
            for q in self._peer_registry.values():  # a clean run leaves no straggler
                _drain_queue(q)
            return self._peer_pool, self._peer_registry, self._peer_driver
        self._close_peer()
        addrs = ("driver", *worker_addrs)
        registry = {a: PipeInbox(ctx) for a in addrs}  # no per-queue feeder thread
        driver_t = QueueTransport(
            "driver", registry["driver"], {a: registry[a] for a in addrs if a != "driver"}
        )
        pool: _StdProcessPool | PinnedProcessPool
        if pinned:
            outbox = worker_outbox_addresses(n, bounds, worker_addrs)  # address -> its peers
            wi = {a: i for i, a in enumerate(worker_addrs)}
            init_args = [
                (
                    a,
                    registry[a],  # this worker's inbox
                    {t: registry[t] for t in outbox[a]},  # only its O(log N) outboxes (+ driver)
                    tuple(worker_addrs[j] for j in lifeline_neighbors(wi[a], w)),  # lifelines
                )
                for a in worker_addrs
            ]
            pool = PinnedProcessPool(w, ctx, pinned_peer_init, init_args, driver=process_id())
        else:
            pool = _StdProcessPool(
                max_workers=w,
                mp_context=ctx,
                initializer=peer_pool_init,
                initargs=(registry, process_id()),
            )
        if self._persistent:
            self._peer_pool, self._peer_registry, self._peer_driver = pool, registry, driver_t
            self._peer_nworkers, self._peer_pinned = w, pinned
        return pool, registry, driver_t

    def _peer_ipc(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        ctx: Any,
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        self._check_fd_budget(w)
        pool, registry, driver_t = self._ipc_pool(n, w, bounds, worker_addrs, ctx)
        factory = self._peer_profiler_factory()
        # the run's release: EOF once the driver closes it
        release_r, release_w = ctx.Pipe(duplex=False)
        try:
            with self._lost_workers(pool):
                if isinstance(pool, PinnedProcessPool):
                    futs = [
                        pool.submit(
                            pinned_peer_actor,
                            n,
                            bounds,
                            worker_addrs,
                            plan.process,
                            plan.combine,
                            items[a],
                            self._steal,
                            self._run_monitor is not None,
                            factory,
                            self._run_push,  # the pinned pool takes no task keywords
                            self._run_lean,
                            self._run_keys,
                            complete_events(self._run_monitor),
                            release_r,
                            worker=i,
                        )
                        for i, a in enumerate(worker_addrs)
                    ]
                else:
                    futs = [
                        pool.submit(
                            pooled_peer_actor,
                            a,
                            n,
                            bounds,
                            worker_addrs,
                            plan.process,
                            plan.combine,
                            items[a],
                            self._steal,
                            self._run_monitor is not None,
                            factory,
                            monitor_factory=self._run_push,
                            lean=self._run_lean,
                            keys=self._run_keys,
                            complete=complete_events(self._run_monitor),
                            release=release_r,
                        )
                        for a in worker_addrs
                    ]
                return self._collect_peer(driver_t, plan, n, futs, pool, control)
        finally:
            release_w.close()  # first: the shutdown below joins actors that only this ends
            release_r.close()
            if not self._persistent:
                pool.shutdown()
                _close_registry(registry)

    def _peer_http(
        self,
        plan: Plan[R],
        n: int,
        w: int,
        bounds: list[int],
        worker_addrs: tuple[str, ...],
        items: dict[str, list[tuple[int, Partition]]],
        ctx: Any,
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        # HTTP across processes: each worker binds its own loopback server and announces its port
        # to the driver (known up front), which assembles + broadcasts the registry — real sockets,
        # the path a distributed scheduler takes. persistent=True reuses the worker POOL (spawn
        # paid once); the loopback servers + discovery handshake are re-done per run (workers are
        # per-submit actors).
        reuse = (
            self._persistent
            and self._peer_pool is not None
            # HTTP always uses a plain stdlib pool, never the pinned pool
            and not self._peer_pinned
            and self._peer_nworkers == w
        )
        if reuse:
            pool = cast("_StdProcessPool", self._peer_pool)
        else:
            self._close_peer()
            pool = _StdProcessPool(
                max_workers=w,
                mp_context=ctx,
                initializer=exit_with_driver,
                initargs=(process_id(),),
            )
            if self._persistent:
                self._peer_pool, self._peer_nworkers = pool, w
        assert pool is not None
        driver_t = HttpTransport("driver")
        factory = self._peer_profiler_factory()
        # the run's release: EOF once the driver closes it
        release_r, release_w = ctx.Pipe(duplex=False)
        try:
            with self._lost_workers(pool):
                futs = [
                    pool.submit(
                        http_peer_actor,
                        a,
                        driver_t.host,
                        driver_t.port,
                        n,
                        bounds,
                        worker_addrs,
                        plan.process,
                        plan.combine,
                        items[a],
                        self._steal,
                        self._run_monitor is not None,
                        factory,
                        monitor_factory=self._run_push,
                        lean=self._run_lean,
                        keys=self._run_keys,
                        complete=complete_events(self._run_monitor),
                        release=release_r,
                    )
                    for a in worker_addrs
                ]
                http_driver_handshake(
                    driver_t,
                    worker_addrs,
                    timeout_s=self._worker_start_timeout_s,
                    check=lambda: self._raise_lost(pool),
                )
                return self._collect_peer(driver_t, plan, n, futs, pool, control)
        finally:
            release_w.close()  # first: the shutdown below joins actors that only this ends
            release_r.close()
            driver_t.close()
            if not self._persistent:
                pool.shutdown()

    def _collect_peer(
        self,
        driver_t: Any,
        plan: Plan[R],
        n: int,
        futs: list[Future[Any]],
        pool: Any = None,
        control: RunControl | None = None,
    ) -> tuple[R, int]:
        """Wait for the root while watching the worker futures and processes.

        A worker exception means the root will never form, so we must detect it PROMPTLY
        (not after the 300s safety timeout) and re-raise it intact (a picklable
        ``StageError``, M6 obligation). Also records each worker's witness stats.
        """
        ctl = (
            PeerControl(control, driver_t, n, plan.combine, plan.empty)
            if control is not None
            else None
        )
        forward: Outbox | None = None  # asynchronous, so a >64 KB node never parks this loop
        self._last_peer_relays = 0

        def via(payload: Any) -> None:
            nonlocal forward
            forward = forward or Outbox(driver_t)
            forward.send(payload[1], payload[2])
            self._last_peer_relays += 1

        def failed() -> bool:
            for f in futs:
                if f.done() and f.exception() is not None:
                    f.result()
            # Nothing above sees a pinned worker's crash, or the crash of the last worker a stdlib
            # pool spawned: submit wakes the pool's watcher before adding that process.
            self._raise_lost(pool)
            return False

        try:
            out = self._await_peer_root(driver_t, n, ctl, failed, via)
        except BaseException as exc:
            # EVERY exit releases the workers — including a KeyboardInterrupt delivered while
            # parked in ``recv``. An unreleased actor waits for ``done`` forever, and the pool
            # shutdown that follows (the non-persistent ``finally``, the persistent discard) then
            # never returns.
            if ctl is not None:
                ctl.close(0.0)
            if forward is not None:
                forward.close(0.0)
            release_workers(driver_t)
            if isinstance(exc, Exception) and complete_events(self._run_monitor):
                self._drain_failing_batch(driver_t)
            raise
        if ctl is not None:  # a queued tag lands in this run's inboxes before `done`
            ctl.close(OUTBOX_EXIT_WAIT_S)
        if forward is not None:
            forward.close(OUTBOX_EXIT_WAIT_S)
        release_workers(driver_t)
        self._last_peer_witness = self._finish_peer(driver_t, futs, pool)
        assert out is not None  # failed() raises instead of reporting
        return out

    def _drain_failing_batch(self, driver_t: Any) -> None:
        """Forward what lands within ``ERROR_EVENT_DRAIN_S`` after a worker failed.

        The failing actor's last batch can land after the future check saw its failure.
        """
        with contextlib.suppress(Exception):
            deadline = time.monotonic() + ERROR_EVENT_DRAIN_S
            while time.monotonic() < deadline:
                got = driver_t.recv(timeout=0.05)
                if got is None:
                    break
                self._forward_peer_events(got[1])

    def _finish_peer(
        self, driver_t: Any, futs: list[Future[Any]], pool: Any = None
    ) -> list[dict[str, int]]:
        """Wait for the released workers and return their witness stats, re-raising a failure."""
        if self._run_monitor is None:
            # Fast path (no monitor): workers see ``done`` immediately (the transport wakes on the
            # message) and return, so BLOCK on the futures' completion instead of polling
            # ``f.done()`` on a 20 ms cadence. That poll granularity was a fixed ~20-30 ms tail on
            # EVERY run — under 1 % of a long job but ~15 % of a sub-second interactive pass (the
            # measured peer-vs-hub regression on the ADL benchmark was almost entirely this join).
            # ``f.result()`` is woken the instant a worker finishes and re-raises a worker error
            # intact (M6). ``wait`` wakes on completion too; its timeout only paces the liveness
            # check, since a dead pinned worker's future never resolves.
            while wait(futs, timeout=0.05).not_done:
                self._raise_lost(pool)
            return [f.result() for f in futs]
        # Monitored: drain trailing events until every worker has finished (a late
        # FINISHED/ERRORED, shipped after the root formed, must not be lost) — we cannot just block
        # on f.result() here because a worker could still be shipping events, so we drain
        # concurrently. The IPC transport write is synchronous (a worker's trailing telemetry is
        # readable the instant it returns), so a FINE cadence has no tail: a coarse cadence was a
        # ~20 ms over-wait per run = ~13 % of a sub-second interactive pass, the bulk of the
        # measured monitor-attached overhead. HTTP delivers asynchronously (a worker's end-of-run
        # profile POST can still be in flight when its future completes), so it keeps the coarse
        # grace to let that final POST land before we stop draining.
        drain_poll = 0.02 if isinstance(driver_t, HttpTransport) else _PEER_MONITOR_DRAIN_POLL_S
        while not all(f.done() for f in futs):
            got = driver_t.recv(timeout=drain_poll)
            if got is not None:
                self._forward_peer_events(got[1])
            self._raise_lost(pool)
        for _sender, payload in driver_t.poll():
            self._forward_peer_events(payload)
        return [f.result() for f in futs]  # propagate any error even on success
