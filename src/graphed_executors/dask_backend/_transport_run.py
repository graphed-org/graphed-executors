"""Dask-FREE driver-side helpers shared by the m44 worker-transport entry points
(``transport_peer`` / ``transport_shuffle``).

This module imports NOTHING from dask/distributed — the ``client.run`` probe fns take the injected
``dask_worker`` and only read ``worker.extensions`` (plain attribute access), and the capability
gate reads only ``backend.capabilities``. Keeping the gate + result/witness shapes + probes here is
what lets ``transport_peer``/``transport_shuffle`` import cleanly on the dask-free main matrix (F13):
they import THIS at module top and defer ``.transport`` (which subclasses ``distributed.WorkerPlugin``)
into their function bodies.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

# duplicated here (not imported from ``.transport``, which pulls in distributed) so this module and
# the probes stay dask-free; ``.transport`` imports these two names FROM here (single source).
PLUGIN_NAME = "graphed-transport"
DRIVER = "driver"

_PHASE2 = "the Phase 2 store-plane data movement"


def require_pin(dbackend: Any) -> None:
    """Every entry point checks this FIRST, before any submit/broadcast: the worker-transport engine
    needs STRICT worker pinning (``pin_to_worker``) + peer data movement, so it refuses loudly on a
    backend that lacks either (``ThreadBackend``: ``pin_to_worker=False``) and names the Phase-2
    store-plane alternative."""
    caps = dbackend.capabilities
    if not (caps.pin_to_worker and caps.peer_data_movement):
        raise NotImplementedError(
            "the dask worker-transport engine requires strict worker pinning "
            "(capabilities.pin_to_worker) and peer data movement — this backend provides neither in "
            f"full; route the block plane through {_PHASE2} instead (Phase 2)."
        )


def sorted_addresses(dbackend: Any) -> tuple[str, ...]:
    """The F8 deterministic worker-address order every ownership/pin decision keys on (same as the
    harness ``sorted(client.scheduler_info()['workers'])``). Same-package private ``_client`` access."""
    return tuple(sorted(dbackend._client.scheduler_info()["workers"]))


@dataclass
class TransportWitness:
    """The m44 transport-plane witness (``result.transport``): the run epochs, restarts performed,
    and the per-worker plugin + sender/holder counters (aggregated driver-side before purge)."""

    epoch_nonces: list[str] = field(default_factory=list)
    epoch_restarts: int = 0
    per_worker: dict[str, dict[str, int]] = field(default_factory=dict)


@dataclass
class TransportShuffleResult:
    """Carries the local engine's shape (``.dest_block_hashes``/``.value``/``.witness`` — the imported
    ``ShuffleWitness`` counter names) plus the m44 transport-plane witness."""

    dest_block_hashes: dict[int, str]
    value: dict[int, Any]
    witness: Any  # a graphed_executors.local.shuffle.ShuffleWitness
    transport: TransportWitness


@dataclass
class TransportExecResult:
    """``ExecResult``-shaped (``.value``/``.n_partitions``/``.n_combines``) + the transport witness."""

    value: Any
    n_partitions: int
    n_combines: int
    transport: TransportWitness


# ---- client.run probes (dask-free: they read the injected dask_worker's extensions) -------------
def _counters_probe(epoch: str, dask_worker: Any = None) -> dict[str, int]:
    plugin = dask_worker.extensions.get(PLUGIN_NAME)
    return dict(plugin.counters(epoch)) if plugin is not None else {}


def _purge_probe(epoch: str, dask_worker: Any = None) -> bool:
    plugin = dask_worker.extensions.get(PLUGIN_NAME)
    if plugin is not None:
        plugin.purge(epoch)
    return True


def merge_counters(per_worker: dict[str, dict[str, int]], counters: Mapping[str, Mapping[str, int]]) -> None:
    for addr, c in counters.items():
        acc = per_worker.setdefault(addr, {})
        for key, val in c.items():
            if key == "peak_holder_bytes":
                acc[key] = max(acc.get(key, 0), int(val))
            elif key == "serve_pid":
                if val:
                    acc[key] = int(val)
            else:
                acc[key] = acc.get(key, 0) + int(val)


def collect_and_purge(client: Any, epoch: str, per_worker: dict[str, dict[str, int]]) -> None:
    """Aggregate every worker's per-epoch plugin counters into ``per_worker``, THEN purge the epoch's
    per-worker state (inboxes, block stores, spill dirs) — the §1.2 teardown witness. Counter
    collection failures are swallowed (witnesses, not correctness); the purge always runs."""
    with contextlib.suppress(Exception):
        merge_counters(per_worker, client.run(_counters_probe, epoch))
    with contextlib.suppress(Exception):
        client.run(_purge_probe, epoch)


# ---- failure classification / attribution -------------------------------------------------------
def _in_chain(exc: BaseException | None, name: str) -> bool:
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if type(exc).__name__ == name:
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def is_restart_worthy(exc: BaseException | None, dbackend: Any) -> bool:
    """A run restarts under a fresh epoch on ANY of: an exhausted-send ``TransportDeliveryError``, a
    block-plane ``PullTimeoutError`` (R4 — a timed-out holder is classified LOST: the whole-run restart
    re-runs the producers onto the survivors, which recovers a dead holder and rides a transient-slow
    one; a caller who knows the batch is legitimately large widens ``pull_timeout_s`` first), a
    ``describe_failure``-recognized ``KilledWorker``, or a bare comm loss to a dead holder."""
    if exc is None:
        return False
    if _in_chain(exc, "TransportDeliveryError") or _in_chain(exc, "PullTimeoutError"):
        return True
    describe = getattr(dbackend, "describe_failure", None)
    if describe is not None and describe(exc) is not None:
        return True
    return _in_chain(exc, "CommClosedError") or _in_chain(exc, "KilledWorker")


def pick_attributable(errs: Mapping[Any, BaseException], dbackend: Any) -> BaseException:
    """Prefer a ``describe_failure``-attributable ``KilledWorker`` (names the victim worker) over a
    generic comm/delivery failure, so the surfaced ``StageError`` names the true cause."""
    describe = getattr(dbackend, "describe_failure", None)
    if describe is not None:
        for exc in errs.values():
            if describe(exc) is not None:
                return exc
    return next(iter(errs.values()))


def build_stage_error(exc: BaseException, dbackend: Any) -> Any:
    """Wrap a run-fatal failure as an attributed ``StageError`` (never a raw ``KilledWorker`` /
    ``TransportDeliveryError`` to the user, §1.5 / m42 precedent). A ``KilledWorker`` names the victim
    worker via ``describe_failure``; anything else keeps its own type + message."""
    from graphed.debug import SourceFrame, StageError  # noqa: PLC0415  (dask-free; error path only)

    describe = getattr(dbackend, "describe_failure", None)
    info = describe(exc) if describe is not None else None
    if info is not None:
        key, last_worker = info
        return StageError(
            op="run",
            frames=(SourceFrame(filename=str(key), lineno=0),),
            input_forms=(),
            partition=str(key),
            cause_type="KilledWorker",
            cause_message=(
                f"worker {last_worker} died (segfault/OOM/preemption suspected; note: blame can be "
                "unfair under co-located tasks)"
            ),
            opt_level=0,
        )
    return StageError(
        op="run",
        frames=(SourceFrame(filename="transport-run", lineno=0),),
        input_forms=(),
        partition="transport-run",
        cause_type=type(exc).__name__,
        cause_message=str(exc),
        opt_level=0,
    )
