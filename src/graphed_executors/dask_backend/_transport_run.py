"""Dask-model-specific driver helpers for the m44 worker-transport entry points
(``transport_peer`` / ``transport_shuffle``), plus a re-export shim over the moved shared types.

The witness/result dataclasses + the failure classifier MOVED to
:mod:`graphed_executors.common.transport_run` (plan §1.6 m47) so the parsl peer-exchange engine
shares them; this module re-exports the SAME objects (identity pinned by the frozen m47 imports
test) and KEEPS the dask-only pieces: ``require_pin`` (reads ``backend.capabilities``),
``sorted_addresses``/``_counters_probe``/``_purge_probe``/``collect_and_purge`` (touch a dask
``Client``). It still imports NOTHING from dask/distributed — the ``client.run`` probe fns take the
injected ``dask_worker`` and read only plain attributes (F13).
"""

from __future__ import annotations

import contextlib
from typing import Any

from graphed_executors.common.transport_run import (
    TransportExecResult,
    TransportShuffleResult,
    TransportWitness,
    build_stage_error,
    is_restart_worthy,
    merge_counters,
    pick_attributable,
)

__all__ = [
    "DRIVER",
    "PLUGIN_NAME",
    "TransportExecResult",
    "TransportShuffleResult",
    "TransportWitness",
    "build_stage_error",
    "collect_and_purge",
    "is_restart_worthy",
    "merge_counters",
    "pick_attributable",
    "require_pin",
    "sorted_addresses",
]

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
    harness ``sorted(client.scheduler_info()['workers'])``). Same-package private ``_client`` access.

    An EMPTY read is treated as a stale snapshot, not a real state: ``Client.scheduler_info()`` returns
    the client's ``_scheduler_identity`` cache, refreshed asynchronously, so on a slow runner it can
    momentarily report zero workers even though the scheduler has them. Left unguarded, the callers do
    ``k = max(1, len(addresses))`` then ``addresses[i % k]`` — indexing an EMPTY tuple → ``IndexError:
    tuple index out of range`` (the m44 zero-partition CI-only failure). A live cluster always has
    workers, so on empty we force the client to observe >= 1 (clock-free: distributed's own bounded
    wait, no sleep here) and re-read."""
    client = dbackend._client
    workers = client.scheduler_info()["workers"]
    if not workers:
        with contextlib.suppress(Exception):
            client.wait_for_workers(1, timeout=30)
        workers = client.scheduler_info()["workers"]
    return tuple(sorted(workers))


# ---- client.run probes (dask-free: they read the injected dask_worker's extensions) -------------
def _counters_probe(epoch: str, dask_worker: Any = None) -> dict[str, int]:
    plugin = dask_worker.extensions.get(PLUGIN_NAME)
    return dict(plugin.counters(epoch)) if plugin is not None else {}


def _purge_probe(epoch: str, dask_worker: Any = None) -> bool:
    plugin = dask_worker.extensions.get(PLUGIN_NAME)
    if plugin is not None:
        plugin.purge(epoch)
    return True


def collect_and_purge(client: Any, epoch: str, per_worker: dict[str, dict[str, int]]) -> None:
    """Aggregate every worker's per-epoch plugin counters into ``per_worker``, THEN purge the epoch's
    per-worker state (inboxes, block stores, spill dirs) — the §1.2 teardown witness. Counter
    collection failures are swallowed (witnesses, not correctness); the purge always runs."""
    with contextlib.suppress(Exception):
        merge_counters(per_worker, client.run(_counters_probe, epoch))
    with contextlib.suppress(Exception):
        client.run(_purge_probe, epoch)
