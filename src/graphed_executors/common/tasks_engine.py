"""The m43 as-tasks shuffle / relational join engine as a NATIVE future graph.

Plan §1.3.4, §2 m43; moved to ``common/`` verbatim in m46, §1.6. Backend-generic: it runs over any
:class:`SubmitBackend` whose ``peer_data_movement`` capability holds — dask (via the
``dask_backend.shuffle`` re-export shim) and a parsl ``ThreadPoolExecutor`` (shared-memory threads
report ``peer_data_movement=True``). The names are preserved verbatim: the frozen m43 Counter gates
pin the submitted ``_dask_*`` function names, and a moved function object keeps its ``__name__``.

The local two-phase engine computes every partition/join kernel **in the driver** (its ``cluster``
duck-type only farms out block STORAGE — ``local/shuffle.py:364-377``), so running it over a
dask-backed cluster object would distribute storage but not compute. m43 instead expresses
map-write/gather as a dask future graph that REUSES the local per-task kernels and plan-level
contracts, and RETIRES the announcement/manifest/steal machinery — under dask the future graph IS
completeness (a gather *depends on* its producers; the scheduler tracks who holds what and workers
fetch deps peer-to-peer) and the scheduler owns stealing.

Graph shape (plan §1.3.4)::

    stage-1: T = min(n_workers, n_src) producer futures   ``_dask_map_write`` (worker-side coalesce
                 -> dict[dest -> wire bytes]                 + to_wire; records its (pid, address))
    pick:    T·P selector futures                          ``_dask_pick`` (runs ON the holder;
                 -> one dest's wire bytes                     scopes each gather dep to one block)
    stage-2: P gather / gather-join futures                ``_dask_gather`` / ``_dask_gather_join``
                 (ascending-producer-task concat = the deterministic merge; worker-side sha256)
    root:    the driver assembles the ShuffleResult (value, dest_block_hashes, DaskShuffleWitness)

Determinism: ``assign`` gives producer tasks contiguous ascending src ranges and gathers concat in
ascending-task order, so a dest's rows assemble in ascending-src order regardless of worker count —
``dest_block_hashes`` are byte-identical across two dask runs AND equal to the local engine's on
the same inputs (the headline cross-engine gate).

This module imports NOTHING from dask/distributed (worker identity comes from the submit-engine's
``current_env()`` seam), so it imports cleanly in the main matrix and the capability gate refuses a
non-peer backend before any work is submitted.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from graphed.core import ShuffleBackend
from graphed.core.execution import JoinBackend

from graphed_executors.local._shuffle_kernels import (
    BROADCAST_PER_BLOCK_HOW,
    JoinTally,
    ShuffleResult,
    ShuffleWitness,
    assign,
    choose_broadcast,
    coalesce_task,
    empty_build_join,
    join_dest,
    join_probe_block,
    sha256_hex,
)
from graphed_executors.submit.engine import current_env

if TYPE_CHECKING:
    from graphed_executors.submit import SubmitRunner


# ---- witness: only the mechanisms that exist under dask (§1.3.4 (f); retired ones are ABSENT)
@dataclass
class DaskShuffleWitness:
    """The dask future-graph mechanism counters.

    The local ``ShuffleWitness``'s announcement/manifest/steal counters are RETIRED — under
    dask the future graph is completeness and the scheduler owns stealing, so those mechanisms
    (and their counters) do not exist here.
    """

    n_producer_tasks: int = 0  # T
    # task -> non-empty dests <= P
    blocks_per_producer_task: dict[int, int] = field(default_factory=dict)
    peak_writer_buffer_bytes: int = 0  # the reused ``coalesce_task`` streaming bound
    broadcast_chosen: bool = False  # the honoured F6 plan choice
    peak_join_bytes: int = 0  # reused ``join_with_budget`` (B5)
    join_spilled_partitions: int = 0
    join_output_rows: int = 0
    # task -> (pid, address)
    producer_sites: dict[int, tuple[int, str]] = field(default_factory=dict)
    # block key -> (pid, address)
    gather_sites: dict[int, tuple[int, str]] = field(default_factory=dict)


def _result(
    dest_block_hashes: dict[int, str], value: dict[int, Any], witness: DaskShuffleWitness
) -> ShuffleResult:
    # Reuse the local ShuffleResult dataclass; its ``witness`` field is typed ``ShuffleWitness``
    # but m43 deliberately carries the reduced ``DaskShuffleWitness`` (§1.3.4 (f)) — the cast
    # bridges the reuse without weakening either type.
    return ShuffleResult(
        dest_block_hashes=dest_block_hashes,
        value=value,
        partitions=[],
        witness=cast("ShuffleWitness", witness),
    )


def _require_peer(backend: Any) -> None:
    """Both entry points refuse a head-node-routed backend (parsl-HTEX class) BEFORE any submit.

    Routing every block through the driver is the exact pathology §1.3.4 rejects.
    """
    if not backend.capabilities.peer_data_movement:
        raise NotImplementedError(
            "shuffle on a backend without peer data movement requires the store-handle data plane "
            "— Phase 2"
        )


def _skey(nonce: str, *parts: object) -> str:
    """Return a namespaced, per-run-nonced, collision-proof task key.

    The ``graphed-`` prefix can never collide with a user string arg (dask/dask#9969) and the
    nonce re-executes across two runs.
    """
    return "graphed-shuffle-" + nonce + "-" + "-".join(str(p) for p in parts)


def _site() -> tuple[int, str]:
    """(pid, worker address) recorded AT EXECUTION.

    The worker address via the submit-engine's ``current_env()`` seam (no dask import), so a
    driver-side kernel loop would leak the driver pid.
    """
    return os.getpid(), current_env().worker


class _WorkerStore:
    """A minimal ``cluster`` stand-in for the reused ``join_with_budget`` disk-spill.

    Its only use of ``cluster`` is ``store_dir(node_i)``. The budgeted join's radix partitions
    spill to a worker-local temp dir and are read back (the kernel's F5 contract);
    ``store_dir`` is never touched when the budget is ``None`` (the kernel returns before
    spilling), so no temp dir is created then.
    """

    def __init__(self) -> None:
        self._dir: str | None = None

    def store_dir(self, node_i: int) -> str:
        if self._dir is None:
            base = tempfile.mkdtemp(prefix="graphed-dask-join-")
            (Path(base) / "objects").mkdir(exist_ok=True)
            self._dir = base
        return self._dir

    def cleanup(self) -> None:
        if self._dir is not None:
            shutil.rmtree(self._dir, ignore_errors=True)
            self._dir = None


# ---- module-level spawn-safe task fns (pickled BY REFERENCE; run on dask workers) ---------------
@dataclass
class _MapOut:
    """One producer task's output.

    ``dest -> wire bytes`` (the pick indexes it) + the driver-side metadata fragment (non-empty
    dest count, the writer peak, and where it ran).
    """

    payload: dict[int, bytes]
    blocks: int
    peak: int
    site: tuple[int, str]


@dataclass
class _GatherOut:
    dest: int
    block: Any
    hash: str
    site: tuple[int, str]


@dataclass
class _JoinOut:
    dest: int
    chunks: list[Any]
    hashes: list[str]
    peak: int
    spilled: int
    rows: int
    site: tuple[int, str]
    matched: tuple[int, ...] | None = None
    read: int = 0


def _dask_map_write(backend: Any, owned_blocks: Sequence[Any], parts: int, salt: int) -> _MapOut:
    """Route + coalesce one producer task's owned src blocks into <= P dest blocks.

    Then to_wire each (worker-side). Reuses ``coalesce_task`` (ascending-src merge, O(P·rg) writer
    buffer) verbatim.
    """
    per_dest, peak = coalesce_task(backend, owned_blocks, parts, salt)
    payload = {dest: bytes(backend.to_wire(block)) for dest, block in per_dest.items()}
    return _MapOut(payload=payload, blocks=len(per_dest), peak=peak, site=_site())


def _dask_pick(map_out: _MapOut, dest: int) -> bytes | None:
    """Select one dest's wire bytes from a producer's output.

    Runs ON the holder (dask locality), so a gather's dep is scoped to a single block — it
    never pulls a producer's whole P-dict.
    """
    return map_out.payload.get(dest)


def _dask_gather(backend: Any, dest: int, *picks: bytes | None) -> _GatherOut | None:
    """Gather one dest across all producers in ascending-task order.

    That order is also ascending-src (the deterministic merge); then concat, and sha256 the wire
    bytes (worker-side).
    """
    blocks = [backend.from_wire(w) for w in picks if w is not None]
    if not blocks:
        return None  # no producer wrote this dest
    gathered = backend.concat(blocks)
    return _GatherOut(dest, gathered, sha256_hex(backend.to_wire(gathered)), _site())


def _dask_gather_join(
    backend: Any,
    dest: int,
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    left_carrier: Any,
    right_carrier: Any,
    n_left: int,
    *picks: bytes | None,
) -> _JoinOut | None:
    """Gather one dest's co-partitioned build/probe blocks (ascending-task order) and join them.

    Spilling under ``mem_budget``, one-sided dests included (the shared ``join_dest``, F1).
    """
    build_side = [backend.from_wire(w) for w in picks[:n_left] if w is not None]
    probe_side = [backend.from_wire(w) for w in picks[n_left:] if w is not None]
    store = _WorkerStore()
    try:
        joined = join_dest(
            backend,
            build_side,
            probe_side,
            on,
            how,
            mem_budget,
            (left_carrier, right_carrier),
            store,
            0,
        )
    finally:
        store.cleanup()
    if joined is None:
        return None
    chunks, hashes, peak, spilled, rows, read = joined
    return _JoinOut(dest, chunks, hashes, peak, spilled, rows, _site(), read=read)


def _dask_broadcast_join_part(
    backend: Any,
    build_wire: bytes,
    probe_block: Any,
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    track_unmatched: bool,
    take_indices: tuple[int, ...] | None,
) -> _JoinOut:
    """Join one probe block against the (broadcast) whole build side.

    The large side is NEVER shuffled. Mirrors ``_run_broadcast_join``'s per-block body:
    ``take_indices`` restricts the build to its unmatched rows (the once-only how=left/outer
    tail); ``track_unmatched`` returns the build indices this block matched so the driver can
    emit the never-matched build rows exactly once.
    """
    build = backend.from_wire(build_wire)
    if take_indices is not None:
        build = backend.take(build, list(take_indices))
    store = _WorkerStore()
    try:
        matched, (chunks, hashes, peak, spilled, rows, read) = join_probe_block(
            backend,
            build,
            probe_block,
            on,
            how,
            mem_budget,
            store,
            0,
            track_unmatched=track_unmatched,
        )
    finally:
        store.cleanup()
    return _JoinOut(-1, chunks, hashes, peak, spilled, rows, _site(), matched, read)


@dataclass
class TaskJoinTally(JoinTally):
    """A :class:`JoinTally` the task engines file their ``_JoinOut`` returns into."""

    def add_out(self, key: int | None, out: Any) -> None:
        """File one task's ``_JoinOut``, recording the build rows it matched."""
        if out.matched is not None:
            self.matched.update(out.matched)
        self.add(key, (out.chunks, out.hashes, out.peak, out.spilled, out.rows, out.read))


# ---- the shared driver: the peer engine picks on the holders, the relay on the driver ----------
@dataclass
class RelayShuffleWitness(DaskShuffleWitness):
    """The :class:`DaskShuffleWitness` mechanism counters PLUS the head-node-routing witnesses.

    ``head_node_routed`` is True on every relay result, and ``driver_relay_bytes`` is the Σ of all
    map-payload wire sizes resolved at the driver barrier (design-M2) — an unlabeled relay or a
    stubbed tally fails the frozen witness.
    """

    head_node_routed: bool = True
    driver_relay_bytes: int = 0


def _witness(relay_bytes: int, *, relay: bool, **fields: Any) -> DaskShuffleWitness:
    if relay:
        return RelayShuffleWitness(**fields, driver_relay_bytes=relay_bytes)
    return DaskShuffleWitness(**fields)


def _submit_maps(
    runner: SubmitRunner,
    backend: Any,
    blocks: Sequence[Any],
    parts: int,
    salt: int,
    t: int,
    nonce: str,
    tag: str,
) -> list[Any]:
    """Submit ``t`` producer maps over contiguous ascending ranges of ``blocks``."""
    return [
        runner.backend.submit(
            _dask_map_write,
            backend,
            [blocks[s] for s in owned],
            parts,
            salt,
            key=_skey(nonce, tag, i),
        )
        for i, owned in enumerate(assign(len(blocks), t) if blocks or t else [])
    ]


def _pick_tier(
    runner: SubmitRunner,
    map_futs: list[Any],
    parts: int,
    nonce: str,
    tag: str,
    *,
    relay: bool,
) -> tuple[list[list[Any]], int, list[_MapOut] | None]:
    """Return each dest's picks in ascending-producer order, the driver-relayed bytes, the maps.

    The maps are the ones the relay resolved, so its caller never pulls them twice (``None`` on
    the peer engine).

    The peer engine submits one pick per (producer, dest), which runs ON the holder so a gather
    pulls one block. The relay resolves every map at a driver barrier and picks locally — zero
    pick submits, the T·P tier's ~P-fold driver-traffic amplification avoided.
    """
    if relay:
        outs = [cast("_MapOut", f.result()) for f in map_futs]
        relay_bytes = sum(len(w) for mo in outs for w in mo.payload.values())
        return [[_dask_pick(mo, dest) for mo in outs] for dest in range(parts)], relay_bytes, outs
    picks = {
        (i, dest): runner.backend.submit(_dask_pick, f, dest, key=_skey(nonce, tag, i, dest))
        for i, f in enumerate(map_futs)
        for dest in range(parts)
    }
    return [[picks[(i, dest)] for i in range(len(map_futs))] for dest in range(parts)], 0, None


def _repartition(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    runner: SubmitRunner,
    salt: int,
    *,
    relay: bool,
) -> ShuffleResult:
    n_src = len(src_blocks)
    t = min(runner.backend.n_workers(), n_src) if n_src else 1
    nonce = uuid.uuid4().hex[:8]
    map_futs = _submit_maps(runner, backend, src_blocks, parts, salt, t, nonce, "map")
    picks, relay_bytes, map_outs = _pick_tier(runner, map_futs, parts, nonce, "pick", relay=relay)
    gather_futs = [
        runner.backend.submit(
            _dask_gather, backend, dest, *picks[dest], key=_skey(nonce, "gather", dest)
        )
        for dest in range(parts)
    ]

    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    gather_sites: dict[int, tuple[int, str]] = {}
    for fut in gather_futs:
        out = cast("_GatherOut | None", fut.result())
        if out is not None:
            value[out.dest] = out.block
            dest_block_hashes[out.dest] = out.hash
            gather_sites[out.dest] = out.site

    # small metadata pull; on the peer engine the payloads stay worker-side for the gathers
    if map_outs is None:
        map_outs = [cast("_MapOut", f.result()) for f in map_futs]
    witness = _witness(
        relay_bytes,
        relay=relay,
        n_producer_tasks=t,
        blocks_per_producer_task={i: mo.blocks for i, mo in enumerate(map_outs)},
        peak_writer_buffer_bytes=max((mo.peak for mo in map_outs), default=0),
        producer_sites={i: mo.site for i, mo in enumerate(map_outs)},
        gather_sites=gather_sites,
    )
    return _result(dest_block_hashes, value, witness)


@dataclass(frozen=True)
class _JoinSpec:
    on: tuple[str, ...]
    how: str
    salt: int
    mem_budget: int | None


def _join(
    backend: JoinBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    spec: _JoinSpec,
    runner: SubmitRunner,
    *,
    broadcast: bool | None,
    relay: bool,
) -> ShuffleResult:
    if choose_broadcast(backend, left_blocks, right_blocks, parts, broadcast=broadcast):
        return _broadcast_join(backend, left_blocks, right_blocks, spec, runner, relay=relay)
    return _shuffle_join(backend, left_blocks, right_blocks, parts, spec, runner, relay=relay)


def _shuffle_join(
    backend: Any,
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    spec: _JoinSpec,
    runner: SubmitRunner,
    *,
    relay: bool,
) -> ShuffleResult:
    nonce = uuid.uuid4().hex[:8]
    n_workers = runner.backend.n_workers()
    t_l = min(n_workers, len(left_blocks)) if left_blocks else 0
    t_r = min(n_workers, len(right_blocks)) if right_blocks else 0
    # submit ALL maps (both sides) before any pick tier resolves them, so the whole map phase runs
    # in parallel across the pool
    left_maps = _submit_maps(runner, backend, left_blocks, parts, spec.salt, t_l, nonce, "lmap")
    right_maps = _submit_maps(runner, backend, right_blocks, parts, spec.salt, t_r, nonce, "rmap")
    left_picks, left_relayed, _ = _pick_tier(runner, left_maps, parts, nonce, "lpick", relay=relay)
    right_picks, right_relayed, _ = _pick_tier(
        runner, right_maps, parts, nonce, "rpick", relay=relay
    )
    # F1 schema carriers for one-sided dests (None when a whole side is partitionless)
    left_carrier = left_blocks[0] if left_blocks else None
    right_carrier = right_blocks[0] if right_blocks else None
    gather_futs = [
        runner.backend.submit(
            _dask_gather_join,
            backend,
            dest,
            spec.on,
            spec.how,
            spec.mem_budget,
            left_carrier,
            right_carrier,
            t_l,
            *left_picks[dest],
            *right_picks[dest],
            key=_skey(nonce, "gjoin", dest),
        )
        for dest in range(parts)
    ]

    tally = TaskJoinTally(next_key=parts)
    gather_sites: dict[int, tuple[int, str]] = {}
    for dest, fut in enumerate(gather_futs):
        out = cast("_JoinOut | None", fut.result())
        if out is not None:
            gather_sites[dest] = out.site
            tally.add_out(dest, out)
    witness = _witness(
        left_relayed + right_relayed,
        relay=relay,
        n_producer_tasks=max(t_l, t_r),
        broadcast_chosen=False,
        peak_join_bytes=tally.peak,
        join_spilled_partitions=tally.spilled,
        join_output_rows=tally.rows,
        gather_sites=gather_sites,
    )
    return _result(tally.hashes, tally.value, witness)


def _broadcast_join(
    backend: Any,
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    spec: _JoinSpec,
    runner: SubmitRunner,
    *,
    relay: bool,
) -> ShuffleResult:
    if not left_blocks:
        kept = empty_build_join(backend, right_blocks, spec.how)
        return _result(
            kept.hashes,
            kept.value,
            _witness(
                0,
                relay=relay,
                broadcast_chosen=True,
                n_producer_tasks=1,
                join_output_rows=kept.rows,
            ),
        )
    submit = runner.backend.submit
    nonce = uuid.uuid4().hex[:8]
    build_concat = backend.concat(list(left_blocks))
    build_wire = bytes(backend.to_wire(build_concat))
    # on a scatter_broadcast=False backend the build side crosses the driver once: the relay
    # tallies it like the map barrier
    handle = runner.backend.broadcast(
        build_wire, token=f"bjoin-{nonce}-{sha256_hex(build_wire)[:12]}"
    )
    per_block_how = BROADCAST_PER_BLOCK_HOW[spec.how]
    track_unmatched = spec.how in ("left", "outer")
    part_futs = [
        submit(
            _dask_broadcast_join_part,
            backend,
            handle,
            probe_block,
            spec.on,
            per_block_how,
            spec.mem_budget,
            track_unmatched,
            None,
            key=_skey(nonce, "bpart", pidx),
        )
        for pidx, probe_block in enumerate(right_blocks)
    ]

    tally = TaskJoinTally(next_key=len(right_blocks))
    gather_sites: dict[int, tuple[int, str]] = {}
    for pidx, fut in enumerate(part_futs):
        out = cast("_JoinOut", fut.result())
        gather_sites[pidx] = out.site
        tally.add_out(pidx, out)

    # emit the never-matched build rows exactly once (F2); an empty probe has no schema carrier,
    # so the tail is dropped there, as the local engine does
    unmatched = tally.unmatched(len(build_concat)) if track_unmatched else ()
    if unmatched and right_blocks:
        tail = submit(
            _dask_broadcast_join_part,
            backend,
            handle,
            right_blocks[0],
            spec.on,
            "left",
            spec.mem_budget,
            False,
            unmatched,
            key=_skey(nonce, "btail", 0),
        )
        tally.add_out(None, cast("_JoinOut", tail.result()))

    witness = _witness(
        len(build_wire),
        relay=relay,
        n_producer_tasks=len(right_blocks) or 1,
        broadcast_chosen=True,
        peak_join_bytes=tally.peak,
        join_spilled_partitions=tally.spilled,
        join_output_rows=tally.rows,
        gather_sites=gather_sites,
    )
    return _result(tally.hashes, tally.value, witness)


# ---- public entry points (plan §2 m43) ----------------------------------------------------------
def dask_run_repartition(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    *,
    runner: SubmitRunner,
    salt: int = 0,
) -> ShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dests as a dask future graph.

    T producers -> T·P picks -> P gathers. ``dest_block_hashes`` are byte-identical across runs
    and equal to the local ``run_repartition`` on identical inputs. ``salt`` folds into the
    pinned route (skew).
    """
    _require_peer(runner.backend)
    return _repartition(backend, src_blocks, parts, runner, salt, relay=False)


def dask_run_join(
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
    """Distributed relational hash JOIN of two block sets as a dask future graph.

    ``broadcast=None`` lets the pinned cost rule choose, keyed on ``parts`` (F6 — never the
    live worker count); ``True``/``False`` honour a plan-recorded choice. ``mem_budget_bytes``
    bounds the gather-join working set by spilling duplicated OUTPUT partitions (the reused
    ``join_with_budget``).
    """
    _require_peer(runner.backend)
    spec = _JoinSpec(tuple(on), how, salt, mem_budget_bytes)
    return _join(
        backend, left_blocks, right_blocks, parts, spec, runner, broadcast=broadcast, relay=False
    )
