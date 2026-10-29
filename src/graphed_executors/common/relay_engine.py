r"""The relay (as-tasks/workflow) shuffle engine (plan §1.3, m46 IT2).

Directive #2's workflow engine, shaped for a head-node-routed broker (e.g. a parsl HTEX cluster
without worker<->worker reachability, where the m43 ``_require_peer`` gate refuses the peer-shuffle
engine). Backend-generic: it runs over ANY :class:`SubmitBackend`, including the all-False "parsl
floor".

Shape (repartition): ``T = min(n_workers, n_src)`` submits of :func:`_dask_map_write` (the m43 body
UNCHANGED) -> the driver resolves the ``_MapOut``\ s at a barrier (the map payloads arrive at the
submit host exactly once) -> the driver regroups per dest by calling :func:`_dask_pick` LOCALLY (a
plain dict lookup — ZERO submits) -> ``P`` submits of :func:`_dask_gather` with the picked wire
fragments as concrete args (data leaves the submit host once). The scheduler sees **T + P tasks,
zero picks**, and the total data crosses the driver exactly twice — the optimal head-node shape
(the m43 ``T·P`` pick tier would re-ship each producer's whole payload per dest, ~P-fold
driver-traffic amplification). Join mirrors :func:`dask_run_join`: two aligned map phases, driver
regroup, P ``_dask_gather_join`` submits; the broadcast path reuses
:func:`_dask_broadcast_join_part` + the once-only unmatched-build tail unchanged.

Because the task bodies, kernels, ``_WorkerStore``, and result assembly are the SAME objects the
dask shim gates, ``dest_block_hashes`` are byte-identical to the local engine and to the m43
engine — only the pick tier moves driver-side. The whole-barrier driver residency (≈ total shuffle
bytes) IS head-node routing — the m43-refused pathology, accepted here as the workflow engine's
defining cost and the reason the m47 transport engine exists. That engagement is made per-run
observable through :class:`RelayShuffleWitness` (``head_node_routed=True`` +
``driver_relay_bytes``), never docs-only.

# ponytail: whole-barrier residency; stream dests incrementally if driver RSS ever matters.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from graphed.core import ShuffleBackend
from graphed.core.execution import JoinBackend

from graphed_executors.common.tasks_engine import (
    RelayShuffleWitness,
    _join,
    _JoinSpec,
    _repartition,
)
from graphed_executors.local._shuffle_kernels import ShuffleResult

if TYPE_CHECKING:
    from graphed_executors.submit import SubmitRunner

__all__ = ["RelayShuffleWitness", "relay_run_join", "relay_run_repartition"]


def relay_run_repartition(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    *,
    runner: SubmitRunner,
    salt: int = 0,
) -> ShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dests as the relay workflow.

    T producers -> driver barrier + local regroup -> P gathers, ZERO pick submits.
    ``dest_block_hashes`` are byte-identical across runs and equal to the local
    ``run_repartition`` on identical inputs; ``salt`` folds into the pinned route.
    """
    return _repartition(backend, src_blocks, parts, runner, salt, relay=True)


def relay_run_join(
    backend: JoinBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    *,
    on: Sequence[str] = ("__joinkey__",),
    how: str = "inner",
    runner: SubmitRunner,
    broadcast: bool | None = None,
    salt: int = 0,
    mem_budget_bytes: int | None = None,
) -> ShuffleResult:
    """Distributed relational hash JOIN as the relay workflow.

    ``broadcast=None`` lets the pinned cost rule choose, keyed on ``parts`` (F6 — never the
    live worker count, so 1-vs-2-worker runs agree); ``True``/``False`` honour a plan-recorded
    choice. ``mem_budget_bytes`` bounds the gather-join working set via the reused
    ``join_with_budget`` spill.
    """
    spec = _JoinSpec(tuple(on), how, salt, mem_budget_bytes)
    return _join(backend, left_blocks, right_blocks, parts, spec, runner, broadcast, relay=True)
