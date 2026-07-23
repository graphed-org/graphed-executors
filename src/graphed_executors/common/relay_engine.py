"""The relay (as-tasks/workflow) shuffle engine (plan §1.3, m46 IT2).

Directive #2's workflow engine, shaped for a head-node-routed broker (e.g. a parsl HTEX cluster
without worker<->worker reachability, where the m43 ``_require_peer`` gate refuses the peer-shuffle
engine). Backend-generic: it runs over ANY :class:`SubmitBackend`, including the all-False "parsl
floor".

Shape (repartition): ``T = min(n_workers, n_src)`` submits of :func:`_dask_map_write` (the m43 body
UNCHANGED) -> the driver resolves the ``_MapOut``\\ s at a barrier (the map payloads arrive at the
submit host exactly once) -> the driver regroups per dest by calling :func:`_dask_pick` LOCALLY (a
plain dict lookup — ZERO submits) -> ``P`` submits of :func:`_dask_gather` with the picked wire
fragments as concrete args (data leaves the submit host once). The scheduler sees **T + P tasks,
zero picks**, and the total data crosses the driver exactly twice — the optimal head-node shape (the
m43 ``T·P`` pick tier would re-ship each producer's whole payload per dest, ~P-fold driver-traffic
amplification). Join mirrors :func:`dask_run_join`: two aligned map phases, driver regroup, P
``_dask_gather_join`` submits; the broadcast path reuses :func:`_dask_broadcast_join_part` + the
once-only unmatched-build tail unchanged.

Because the task bodies, kernels, ``_WorkerStore``, and result assembly are the SAME objects the
dask shim gates, ``dest_block_hashes`` are byte-identical to the local engine and to the m43
engine — only the pick tier moves driver-side. The whole-barrier driver residency (≈ total shuffle
bytes) IS head-node routing — the m43-refused pathology, accepted here as the workflow engine's
defining cost and the reason the m47 transport engine exists. That engagement is made per-run
observable through :class:`RelayShuffleWitness` (``head_node_routed=True`` + ``driver_relay_bytes``),
never docs-only.

# ponytail: whole-barrier residency; stream dests incrementally if driver RSS ever matters.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from graphed.core import ShuffleBackend
from graphed.core.execution import JoinBackend
from graphed.shuffle import broadcast_join_choice

from graphed_executors.common.tasks_engine import (
    DaskShuffleWitness,
    _dask_broadcast_join_part,
    _dask_gather,
    _dask_gather_join,
    _dask_map_write,
    _dask_pick,
    _JoinOut,
    _MapOut,
    _result,
    _skey,
)
from graphed_executors.local.shuffle import ShuffleResult, _assign, _sha256_hex

if TYPE_CHECKING:
    from graphed_executors.submit import SubmitRunner


@dataclass
class RelayShuffleWitness(DaskShuffleWitness):
    """The :class:`DaskShuffleWitness` mechanism counters PLUS the head-node-routing witnesses
    (design-M2): ``head_node_routed`` is True on every relay result, and ``driver_relay_bytes`` is
    the Σ of all map-payload wire sizes resolved at the driver barrier — an unlabeled relay or a
    stubbed tally fails the frozen witness."""

    head_node_routed: bool = True
    driver_relay_bytes: int = 0


def relay_run_repartition(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    *,
    runner: SubmitRunner,
    salt: int = 0,
) -> ShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dests as the relay workflow (T producers ->
    driver barrier + local regroup -> P gathers, ZERO pick submits). ``dest_block_hashes`` are
    byte-identical across runs and equal to the local ``run_repartition`` on identical inputs;
    ``salt`` folds into the pinned route."""
    submit = runner.backend.submit
    n_src = len(src_blocks)
    t = min(runner.backend.n_workers(), n_src) if n_src else 1
    nonce = uuid.uuid4().hex[:8]

    # submit ALL producer maps first (so they run in parallel across the pool), THEN resolve the
    # barrier: all _MapOuts are retained regardless, so resolving inside the submit loop would make
    # map compute needlessly T-fold sequential.
    map_futs = [
        submit(
            _dask_map_write,
            backend,
            [src_blocks[s] for s in owned],
            parts,
            salt,
            key=_skey(nonce, "map", i),
        )
        for i, owned in enumerate(_assign(n_src, t))
    ]
    map_outs = [cast("_MapOut", f.result()) for f in map_futs]
    driver_relay_bytes = sum(len(w) for mo in map_outs for w in mo.payload.values())

    # driver-local regroup: _dask_pick is a dict lookup, submitted NOWHERE (the T·P amplification the
    # relay exists to avoid); the P gathers take the picked wire fragments as concrete args.
    gather_futs = {
        dest: submit(
            _dask_gather,
            backend,
            dest,
            *[_dask_pick(mo, dest) for mo in map_outs],
            key=_skey(nonce, "gather", dest),
        )
        for dest in range(parts)
    }

    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    gather_sites: dict[int, tuple[int, str]] = {}
    for dest in range(parts):
        out = cast("Any", gather_futs[dest].result())
        if out is None:
            continue
        value[out.dest] = out.block
        dest_block_hashes[out.dest] = out.hash
        gather_sites[out.dest] = out.site

    producer_sites: dict[int, tuple[int, str]] = {}
    blocks_per: dict[int, int] = {}
    peak = 0
    for i, mo in enumerate(map_outs):
        producer_sites[i] = mo.site
        blocks_per[i] = mo.blocks
        peak = max(peak, mo.peak)

    witness = RelayShuffleWitness(
        n_producer_tasks=t,
        blocks_per_producer_task=blocks_per,
        peak_writer_buffer_bytes=peak,
        producer_sites=producer_sites,
        gather_sites=gather_sites,
        driver_relay_bytes=driver_relay_bytes,
    )
    return _result(dest_block_hashes, value, witness)


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
    """Distributed relational hash JOIN as the relay workflow. ``broadcast=None`` lets the pinned
    cost rule choose, keyed on ``parts`` (F6 — never the live worker count, so 1-vs-2-worker runs
    agree); ``True``/``False`` honour a plan-recorded choice. ``mem_budget_bytes`` bounds the
    gather-join working set via the reused ``_join_with_budget`` spill."""
    build_bytes = sum(backend.estimated_bytes(b) for b in left_blocks)
    probe_bytes = sum(backend.estimated_bytes(b) for b in right_blocks)
    chosen = broadcast_join_choice(build_bytes, probe_bytes, parts) if broadcast is None else bool(broadcast)
    if chosen:
        return _relay_broadcast_join(backend, left_blocks, right_blocks, on, how, mem_budget_bytes, runner)
    return _relay_shuffle_join(
        backend, left_blocks, right_blocks, parts, on, how, salt, mem_budget_bytes, runner
    )


def _relay_shuffle_join(
    backend: Any,
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    on: Sequence[str],
    how: str,
    salt: int,
    mem_budget: int | None,
    runner: SubmitRunner,
) -> ShuffleResult:
    submit = runner.backend.submit
    on_t = tuple(on)
    nonce = uuid.uuid4().hex[:8]
    n_left, n_right = len(left_blocks), len(right_blocks)
    t_l = min(runner.backend.n_workers(), n_left) if n_left else 0
    t_r = min(runner.backend.n_workers(), n_right) if n_right else 0

    # submit ALL map tasks (both sides) first so the whole map phase runs in parallel across the
    # pool, THEN resolve the barrier — resolving inside the submit loops would make the left phase
    # run fully before the right and each side T-fold sequential, for zero memory benefit.
    left_futs = (
        [
            submit(
                _dask_map_write,
                backend,
                [left_blocks[s] for s in owned],
                parts,
                salt,
                key=_skey(nonce, "lmap", i),
            )
            for i, owned in enumerate(_assign(n_left, t_l))
        ]
        if t_l
        else []
    )
    right_futs = (
        [
            submit(
                _dask_map_write,
                backend,
                [right_blocks[s] for s in owned],
                parts,
                salt,
                key=_skey(nonce, "rmap", i),
            )
            for i, owned in enumerate(_assign(n_right, t_r))
        ]
        if t_r
        else []
    )
    left_outs = [cast("_MapOut", f.result()) for f in left_futs]
    right_outs = [cast("_MapOut", f.result()) for f in right_futs]
    driver_relay_bytes = sum(len(w) for mo in (*left_outs, *right_outs) for w in mo.payload.values())

    # F1 schema carriers for one-sided dests (None when a whole side is partitionless)
    left_carrier = left_blocks[0] if left_blocks else None
    right_carrier = right_blocks[0] if right_blocks else None
    gather_futs = {
        dest: submit(
            _dask_gather_join,
            backend,
            dest,
            on_t,
            how,
            mem_budget,
            left_carrier,
            right_carrier,
            t_l,
            *[_dask_pick(lo, dest) for lo in left_outs],
            *[_dask_pick(ro, dest) for ro in right_outs],
            key=_skey(nonce, "gjoin", dest),
        )
        for dest in range(parts)
    }

    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    gather_sites: dict[int, tuple[int, str]] = {}
    peak_join = spilled_total = rows_total = 0
    next_key = parts  # spilled sub-partitions get fresh keys beyond the dest range (mirrors m43/local)
    for dest in range(parts):
        out = cast("_JoinOut | None", gather_futs[dest].result())
        if out is None:
            continue
        gather_sites[dest] = out.site
        peak_join = max(peak_join, out.peak)
        spilled_total += out.spilled
        rows_total += out.rows
        if len(out.chunks) == 1:
            value[dest], dest_block_hashes[dest] = out.chunks[0], out.hashes[0]
        else:
            for ch, h in zip(out.chunks, out.hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1

    witness = RelayShuffleWitness(
        n_producer_tasks=max(t_l, t_r),
        broadcast_chosen=False,
        peak_join_bytes=peak_join,
        join_spilled_partitions=spilled_total,
        join_output_rows=rows_total,
        gather_sites=gather_sites,
        driver_relay_bytes=driver_relay_bytes,
    )
    return _result(dest_block_hashes, value, witness)


def _relay_broadcast_join(
    backend: Any,
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    runner: SubmitRunner,
) -> ShuffleResult:
    submit = runner.backend.submit
    on_t = tuple(on)
    nonce = uuid.uuid4().hex[:8]
    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    if not left_blocks:
        return _result(
            dest_block_hashes,
            value,
            RelayShuffleWitness(broadcast_chosen=True, n_producer_tasks=1, driver_relay_bytes=0),
        )

    build_concat = backend.concat(list(left_blocks))
    build_wire = bytes(backend.to_wire(build_concat))
    # the build side crosses the driver once (broadcast returns the payload on a scatter_broadcast=False
    # backend) — head-node routing, tallied like the map barrier so the witness is honest on this arm too.
    handle = runner.backend.broadcast(build_wire, token=f"bjoin-{nonce}-{_sha256_hex(build_wire)[:12]}")
    per_block_how = {"inner": "inner", "left": "inner", "right": "right", "outer": "right"}[how]
    track_unmatched = how in ("left", "outer")

    part_futs = [
        submit(
            _dask_broadcast_join_part,
            backend,
            handle,
            probe_block,
            on_t,
            per_block_how,
            mem_budget,
            track_unmatched,
            None,
            key=_skey(nonce, "bpart", pidx),
        )
        for pidx, probe_block in enumerate(right_blocks)
    ]

    gather_sites: dict[int, tuple[int, str]] = {}
    peak_join = spilled_total = rows_total = 0
    matched: set[int] = set()
    next_key = len(right_blocks)  # spilled sub-partitions get fresh keys beyond the pidx range
    for pidx, f in enumerate(part_futs):
        out = cast("_JoinOut", f.result())
        gather_sites[pidx] = out.site
        peak_join = max(peak_join, out.peak)
        spilled_total += out.spilled
        rows_total += out.rows
        if out.matched is not None:
            matched.update(out.matched)
        if len(out.chunks) == 1:
            value[pidx], dest_block_hashes[pidx] = out.chunks[0], out.hashes[0]
        else:
            for ch, h in zip(out.chunks, out.hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1

    if track_unmatched:  # emit the never-matched build rows exactly once (F2 of _run_broadcast_join)
        unmatched = tuple(sorted(set(range(len(build_concat))) - matched))
        if unmatched and right_blocks:  # empty probe -> no schema carrier: the tail is dropped (no crash)
            tail = cast(
                "_JoinOut",
                submit(
                    _dask_broadcast_join_part,
                    backend,
                    handle,
                    right_blocks[0],
                    on_t,
                    "left",
                    mem_budget,
                    False,
                    unmatched,
                    key=_skey(nonce, "btail", 0),
                ).result(),
            )
            peak_join = max(peak_join, tail.peak)
            spilled_total += tail.spilled
            rows_total += tail.rows
            for ch, h in zip(tail.chunks, tail.hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1

    witness = RelayShuffleWitness(
        n_producer_tasks=len(right_blocks) or 1,
        broadcast_chosen=True,
        peak_join_bytes=peak_join,
        join_spilled_partitions=spilled_total,
        join_output_rows=rows_total,
        gather_sites=gather_sites,
        driver_relay_bytes=len(build_wire),
    )
    return _result(dest_block_hashes, value, witness)
