"""m44 (b)+(c): the M38 peer reduction hosted on dask workers over ``DaskWorkerTransport``.

m44-transport-plan §1.3. One long-running, strict-pinned, seceded peer task per worker runs the
UNCHANGED ``process_and_reduce`` actor loop; the driver runs the UNCHANGED ``collect_peer_root``
loop over a ``DriverTransport`` (fed by the workers' ``log_event``), always with an explicit
``timeout_s`` so a lost root can never hang it (F3: on ``TimeoutError`` the caller falls back to
the peer task's returned root).

Scheduler graph: exactly k ``_dask_peer_main`` actor tasks (leaves ride the broadcast payload —
no per-leaf futures) + O(1) control (``client.run`` broadcast/purge). Determinism is
``PeerReducer.settle``'s fixed ``(level, pos)`` ownership, independent of dask timing; the same
fixed tree makes the result bit-for-bit the ``SequentialRunner`` baseline.

This module imports the reduction kernels from ``graphed_executors.local._peer`` UNCHANGED and
defers the dask-touching ``.transport`` import into function bodies, so it imports on the dask-free
matrix (F13).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from graphed.core.execution import LocalResources, Plan
from graphed.services import require_bound

from graphed_executors.common import refuse_staged
from graphed_executors.common.transport_run import empty_run, finalized, harvest, run_epochs
from graphed_executors.local._peer import (
    collect_peer_root,
    make_bounds,
    process_and_reduce,
    slice_items,
    worker_outbox_addresses,
)

from ._transport_run import (
    TransportExecResult,
    TransportWitness,
    collect_and_purge,
    pick_attributable,
    require_pin,
    sorted_addresses,
)

# generous vs a sub-second in-cluster reduction; a peer error triggers restart
_ROOT_TIMEOUT_S = 30.0
_MISSING: Any = object()


def _select_root(driver_root: Any, results: dict[str, Any], missing: Any) -> Any:
    """Resolve the reduction root after a clean run.

    If the driver's ``collect_peer_root`` returned one (``driver_root is not missing``, even a
    genuinely-``None`` root), it is authoritative. Otherwise fall back to the F3
    belt-and-braces: the first peer that CAPTURED a root (``has_root``). If NO peer captured
    one, RAISE a ``TransportDeliveryError`` (R2): the reduction was aborted before producing a
    root (e.g. ``collect_peer_root`` timed out and broadcast ``('done',)``, ending the actors)
    — returning ``plan.empty()`` here would silently substitute the identity value for the real
    answer (hard constraint 8). The raise is restart-worthy, so it escalates through §1.5,
    never a silent wrong result.
    """
    if driver_root is not missing:
        return driver_root
    for r in results.values():
        if r.get("has_root"):
            return r["root"]
    # deferred: distributed-touching
    from .transport import TransportDeliveryError

    raise TransportDeliveryError(
        f"peer reduction completed with no captured root across {len(results)} peer(s): the "
        "driver root "
        "collection did not deliver a root and no peer future carried one (the reduction was "
        "aborted "
        "before the root — a lost/withheld root must never default to the identity value)"
    )


def _dask_peer_main(
    spec: Any,
    process: Any,
    combine: Any,
    items: Any,
    n: int,
    bounds: list[int],
    worker_addresses: tuple[str, ...],
    steal_peers: tuple[str, ...],
) -> dict[str, Any]:
    """One worker's peer actor (module-level: spawn-safe).

    Build the ``DaskWorkerTransport`` from the plugin + spec, ``secede`` (free the pool slot
    for the long-running loop), then run ``process_and_reduce`` UNCHANGED over this endpoint
    and this worker's owned leaf slice. Returns the reducer witness plus the root value the
    endpoint captured (the §1.1.3 belt-and-braces fallback: worker 0 also ships the root to the
    driver over ``log_event``).
    """
    import distributed  # worker-side only

    from .transport import open_endpoint

    endpoint = open_endpoint(spec)
    # research §5: free the ThreadPool slot; the worker keeps serving handlers
    distributed.secede()
    counters = process_and_reduce(
        endpoint.address,
        endpoint,
        n,
        bounds,
        tuple(worker_addresses),
        process,
        combine,
        items,
        LocalResources(),
        steal=True,
        steal_peers=tuple(steal_peers),
        emit=False,
    )
    # ``has_root`` is a plain bool (survives the wire): ``endpoint.root_sent`` may be a
    # genuinely-``None`` root, which the ``_NO_ROOT`` sentinel — a live object identity that does
    # NOT survive pickling — can only disambiguate here, in the endpoint's own process (R2).
    captured = endpoint.has_root
    return {
        "address": endpoint.address,
        "has_root": captured,
        "root": endpoint.root_sent if captured else None,
        "counters": counters,
    }


def transport_run_plan(
    plan: Plan[Any],
    backend: Any,
    *,
    monitor: Any = None,
    inbox_maxsize: int | None = None,
    root_timeout_s: float = _ROOT_TIMEOUT_S,
    epoch_restarts_allowed: int = 1,
) -> TransportExecResult:
    """Run ``plan`` as an M38 peer reduction hosted on ``backend`` (a ``DaskBackend``).

    ``monitor`` is accepted for signature parity but peer-mode telemetry over the transport is
    not wired for m44 (emit=False) — a documented limitation. ``root_timeout_s`` bounds the
    driver's ``collect_peer_root`` wait (R4; default ``_ROOT_TIMEOUT_S``) — raise it for a
    legitimately long reduction, since a lost root now RAISES rather than defaulting to the
    identity value (R2). Failure semantics (§1.5): an exhausted send / worker death / no
    captured root restarts the whole run under a fresh epoch up to ``epoch_restarts_allowed``,
    else surfaces as an attributed ``StageError``.
    """
    refuse_staged(plan, "transport_run_plan", "DaskBackend(client)")
    require_bound(plan)  # resolves no services: an unbound plan is refused before any task
    require_pin(backend)
    tasks = sorted(plan.tasks, key=lambda t: t.key)
    n = len(tasks)
    if n == 0:
        return empty_run(plan)
    witness = TransportWitness()

    from .transport import ensure_engine_plugins

    # the shim needs graphed-worker; idempotent for transport
    ensure_engine_plugins(backend.client)
    addresses = sorted_addresses(backend)
    k = max(1, min(len(addresses), n))
    worker_addrs = tuple(addresses[:k])
    bounds = make_bounds(n, k)
    peers = _PeerLayout(
        n,
        bounds,
        worker_addrs,
        slice_items([t.partition for t in tasks], bounds, worker_addrs),
        {a: tuple(v) for a, v in worker_outbox_addresses(n, bounds, worker_addrs).items()},
    )
    return run_epochs(
        lambda _n, nonce: _reduce_epoch(
            backend, plan, nonce, peers, witness, inbox_maxsize, root_timeout_s
        ),
        restarts_allowed=epoch_restarts_allowed,
        backend=backend,
        witness=witness,
    )


@dataclass(frozen=True)
class _PeerLayout:
    """Which worker reduces which leaves, and the overlay it sends on; fixed across epochs."""

    n: int
    bounds: list[int]
    worker_addrs: tuple[str, ...]
    items: dict[str, Any]
    overlay: dict[str, tuple[str, ...]]


def _reduce_epoch(
    backend: Any,
    plan: Plan[Any],
    nonce: str,
    peers: _PeerLayout,
    witness: TransportWitness,
    inbox_maxsize: int | None,
    root_timeout_s: float,
) -> TransportExecResult | BaseException:
    """Run one epoch: its result, or the failure ``run_epochs`` classifies."""
    from .transport import (
        TransportDeliveryError,
        _ensure_run_probe,
        make_transport_spec,
        open_driver_endpoint,
    )

    spec = make_transport_spec(
        nonce, peers.worker_addrs, inbox_maxsize=inbox_maxsize, overlay=peers.overlay
    )
    driver_ep = open_driver_endpoint(backend, spec)
    # pre-create every worker's inbox so an early boundary-node send can never be stale-rejected
    # (hard constraint 8); the peer tasks then attach to the SAME per-epoch state.
    backend.client.run(_ensure_run_probe, spec, workers=list(peers.worker_addrs))
    futures = {
        addr: backend.submit(
            _dask_peer_main,
            spec,
            plan.process,
            plan.combine,
            peers.items[addr],
            peers.n,
            peers.bounds,
            peers.worker_addrs,
            peers.overlay.get(addr, ()),
            key=f"graphed-transport-{nonce}-peer-{i}",
            workers=[addr],
            retries=0,
        )
        for i, addr in enumerate(peers.worker_addrs)
    }
    root: Any = _MISSING
    try:
        root = collect_peer_root(driver_ep, plan.empty, peers.n, timeout_s=root_timeout_s)
    except TimeoutError:
        pass  # F3: a lost root falls back to the peer future's returned root (below)
    finally:
        driver_ep.close()
    results, errs = harvest(futures)
    collect_and_purge(backend.client, nonce, witness.per_worker)
    if errs:
        return pick_attributable(errs, backend)
    try:
        root = _select_root(root, results, _MISSING)
    except TransportDeliveryError as exc:  # no root captured: restart-worthy, like a lost send
        return exc
    n = peers.n
    return TransportExecResult(finalized(plan, root), n, max(0, n - 1), witness)
