"""Shuffle / relational join on dask as a NATIVE future graph (plan §1.3.4, §2 m43).

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

Determinism: ``_assign`` gives producer tasks contiguous ascending src ranges and gathers concat in
ascending-task order, so a dest's rows assemble in ascending-src order regardless of worker count —
``dest_block_hashes`` are byte-identical across two dask runs AND equal to the local engine's on the
same inputs (the headline cross-engine gate).

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
from graphed.shuffle import broadcast_join_choice

from graphed_executors.local.shuffle import (
    ShuffleResult,
    ShuffleWitness,
    _assign,
    _coalesce_task,
    _join_with_budget,
    _sha256_hex,
)
from graphed_executors.submit.engine import current_env

if TYPE_CHECKING:
    from graphed_executors.submit import SubmitRunner


# ---- witness: only the mechanisms that exist under dask (§1.3.4 (f); retired ones are ABSENT) ----
@dataclass
class DaskShuffleWitness:
    """The dask future-graph mechanism counters. The local ``ShuffleWitness``'s
    announcement/manifest/steal counters are RETIRED — under dask the future graph is completeness
    and the scheduler owns stealing, so those mechanisms (and their counters) do not exist here."""

    n_producer_tasks: int = 0  # T
    blocks_per_producer_task: dict[int, int] = field(default_factory=dict)  # task -> non-empty dests <= P
    peak_writer_buffer_bytes: int = 0  # the reused ``_coalesce_task`` streaming bound
    broadcast_chosen: bool = False  # the honoured F6 plan choice
    peak_join_bytes: int = 0  # reused ``_join_with_budget`` (B5)
    join_spilled_partitions: int = 0
    join_output_rows: int = 0
    producer_sites: dict[int, tuple[int, str]] = field(default_factory=dict)  # task -> (pid, address)
    gather_sites: dict[int, tuple[int, str]] = field(default_factory=dict)  # block key -> (pid, address)


def _result(
    dest_block_hashes: dict[int, str], value: dict[int, Any], witness: DaskShuffleWitness
) -> ShuffleResult:
    # Reuse the local ShuffleResult dataclass; its ``witness`` field is typed ``ShuffleWitness`` but
    # m43 deliberately carries the reduced ``DaskShuffleWitness`` (§1.3.4 (f)) — the cast bridges the
    # reuse without weakening either type.
    return ShuffleResult(
        dest_block_hashes=dest_block_hashes,
        value=value,
        partitions=[],
        witness=cast("ShuffleWitness", witness),
    )


def _require_peer(backend: Any) -> None:
    """Both entry points refuse a head-node-routed backend (parsl-HTEX class) BEFORE any submit —
    routing every block through the driver is the exact pathology §1.3.4 rejects."""
    if not backend.capabilities.peer_data_movement:
        raise NotImplementedError(
            "shuffle on a backend without peer data movement requires the store-handle data plane — Phase 2"
        )


def _skey(nonce: str, *parts: object) -> str:
    """A namespaced, per-run-nonced, collision-proof task key: the ``graphed-`` prefix can never
    collide with a user string arg (dask/dask#9969) and the nonce re-executes across two runs."""
    return "graphed-shuffle-" + nonce + "-" + "-".join(str(p) for p in parts)


def _site() -> tuple[int, str]:
    """(pid, worker address) recorded AT EXECUTION — the worker address via the submit-engine's
    ``current_env()`` seam (no dask import), so a driver-side kernel loop would leak the driver pid."""
    return os.getpid(), current_env().worker


class _WorkerStore:
    """A minimal ``cluster`` stand-in for the reused ``_join_with_budget`` disk-spill (its only use of
    ``cluster`` is ``store_dir(node_i)``). The budgeted join's radix partitions spill to a worker-local
    temp dir and are read back (the kernel's F5 contract); ``store_dir`` is never touched when the
    budget is ``None`` (the kernel returns before spilling), so no temp dir is created then."""

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


# ---- module-level spawn-safe task fns (pickled BY REFERENCE; run on dask workers) ----------------
@dataclass
class _MapOut:
    """One producer task's output: ``dest -> wire bytes`` (the pick indexes it) + the driver-side
    metadata fragment (non-empty dest count, the writer peak, and where it ran)."""

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


def _dask_map_write(backend: Any, owned_blocks: Sequence[Any], parts: int, salt: int) -> _MapOut:
    """Route + coalesce one producer task's owned src blocks into <= P dest blocks, then to_wire each
    (worker-side). Reuses ``_coalesce_task`` (ascending-src merge, O(P·rg) writer buffer) verbatim."""
    per_dest, peak = _coalesce_task(backend, owned_blocks, parts, salt)
    payload = {dest: bytes(backend.to_wire(block)) for dest, block in per_dest.items()}
    return _MapOut(payload=payload, blocks=len(per_dest), peak=peak, site=_site())


def _dask_pick(map_out: _MapOut, dest: int) -> bytes | None:
    """Select one dest's wire bytes from a producer's output. Runs ON the holder (dask locality), so a
    gather's dep is scoped to a single block — it never pulls a producer's whole P-dict."""
    return map_out.payload.get(dest)


def _dask_gather(backend: Any, dest: int, *picks: bytes | None) -> _GatherOut | None:
    """Gather one dest across all producers in ascending-task order (== ascending-src, the
    deterministic merge), concat, and sha256 the wire bytes (worker-side)."""
    blocks = [backend.from_wire(w) for w in picks if w is not None]
    if not blocks:
        return None  # no producer wrote this dest
    gathered = backend.concat(blocks)
    return _GatherOut(dest, gathered, _sha256_hex(backend.to_wire(gathered)), _site())


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
    """Gather one dest's co-partitioned build/probe blocks (ascending-task order) and join them,
    spilling under ``mem_budget`` via the reused ``_join_with_budget``. F1 one-sided-dest handling
    mirrors ``_run_shuffle_join``: a build-only dest under how=left/outer null-fills against the probe
    schema carrier (``right_carrier``), and vice versa; a partitionless absent side (carrier ``None``)
    keeps the present rows as-is."""
    build_side = [backend.from_wire(w) for w in picks[:n_left] if w is not None]
    probe_side = [backend.from_wire(w) for w in picks[n_left:] if w is not None]
    if not build_side and not probe_side:
        return None
    if not build_side and how not in ("right", "outer"):
        return None  # probe-only dest, but how keeps no unmatched-probe rows
    if not probe_side and how not in ("left", "outer"):
        return None  # build-only dest, but how keeps no unmatched-build rows

    if build_side and probe_side:
        build, probe, how_here = backend.concat(build_side), backend.concat(probe_side), how
    elif build_side:
        if right_carrier is None:  # partitionless probe side -> keep the build rows (never drop)
            blk = backend.concat(build_side)
            return _JoinOut(dest, [blk], [_sha256_hex(backend.to_wire(blk))], 0, 0, len(blk), _site())
        build, probe, how_here = backend.concat(build_side), right_carrier, "left"
    else:
        if left_carrier is None:  # partitionless build side -> keep the probe rows (never drop)
            blk = backend.concat(probe_side)
            return _JoinOut(dest, [blk], [_sha256_hex(backend.to_wire(blk))], 0, 0, len(blk), _site())
        build, probe, how_here = left_carrier, backend.concat(probe_side), "right"

    store = _WorkerStore()
    try:
        chunks, hashes, peak, spilled, rows, _read = _join_with_budget(
            backend, build, probe, on, how_here, mem_budget, store, 0
        )
    finally:
        store.cleanup()
    return _JoinOut(dest, chunks, hashes, peak, spilled, rows, _site())


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
    """Join one probe block against the (broadcast) whole build side — the large side is NEVER
    shuffled. Mirrors ``_run_broadcast_join``'s per-block body: ``take_indices`` restricts the build to
    its unmatched rows (the once-only how=left/outer tail); ``track_unmatched`` returns the build
    indices this block matched so the driver can emit the never-matched build rows exactly once."""
    build = backend.from_wire(build_wire)
    if take_indices is not None:
        build = backend.take(build, list(take_indices))
    matched: tuple[int, ...] | None = None
    if track_unmatched:
        b_idx, _p_idx = backend.match_indices(build, probe_block, on=list(on), how="inner")
        matched = tuple(int(x) for x in b_idx)
    store = _WorkerStore()
    try:
        chunks, hashes, peak, spilled, rows, _read = _join_with_budget(
            backend, build, probe_block, on, how, mem_budget, store, 0
        )
    finally:
        store.cleanup()
    return _JoinOut(-1, chunks, hashes, peak, spilled, rows, _site(), matched)


# ---- public entry points (plan §2 m43) ----------------------------------------------------------
def dask_run_repartition(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    *,
    runner: SubmitRunner,
    salt: int = 0,
) -> ShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dests as a dask future graph (T producers ->
    T·P picks -> P gathers). ``dest_block_hashes`` are byte-identical across runs and equal to the
    local ``run_repartition`` on identical inputs. ``salt`` folds into the pinned route (skew)."""
    _require_peer(runner.backend)
    submit = runner.backend.submit
    n_src = len(src_blocks)
    t = min(runner.backend.n_workers(), n_src) if n_src else 1
    nonce = uuid.uuid4().hex[:8]

    map_futs = [
        submit(
            _dask_map_write, backend, [src_blocks[s] for s in owned], parts, salt, key=_skey(nonce, "map", i)
        )
        for i, owned in enumerate(_assign(n_src, t))
    ]
    picks = {
        (i, dest): submit(_dask_pick, f, dest, key=_skey(nonce, "pick", i, dest))
        for i, f in enumerate(map_futs)
        for dest in range(parts)
    }
    gather_futs = {
        dest: submit(
            _dask_gather,
            backend,
            dest,
            *[picks[(i, dest)] for i in range(t)],
            key=_skey(nonce, "gather", dest),
        )
        for dest in range(parts)
    }

    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    gather_sites: dict[int, tuple[int, str]] = {}
    for dest in range(parts):
        out = cast("_GatherOut | None", gather_futs[dest].result())
        if out is None:
            continue
        value[out.dest] = out.block
        dest_block_hashes[out.dest] = out.hash
        gather_sites[out.dest] = out.site

    producer_sites: dict[int, tuple[int, str]] = {}
    blocks_per: dict[int, int] = {}
    peak = 0
    for i, f in enumerate(map_futs):  # small metadata pull; payloads stay worker-side for the gathers
        mo = cast("_MapOut", f.result())
        producer_sites[i] = mo.site
        blocks_per[i] = mo.blocks
        peak = max(peak, mo.peak)

    witness = DaskShuffleWitness(
        n_producer_tasks=t,
        blocks_per_producer_task=blocks_per,
        peak_writer_buffer_bytes=peak,
        producer_sites=producer_sites,
        gather_sites=gather_sites,
    )
    return _result(dest_block_hashes, value, witness)


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
    """Distributed relational hash JOIN of two block sets as a dask future graph. ``broadcast=None``
    lets the pinned cost rule choose, keyed on ``parts`` (F6 — never the live worker count);
    ``True``/``False`` honour a plan-recorded choice. ``mem_budget_bytes`` bounds the gather-join
    working set by spilling duplicated OUTPUT partitions (the reused ``_join_with_budget``)."""
    _require_peer(runner.backend)
    build_bytes = sum(backend.estimated_bytes(b) for b in left_blocks)
    probe_bytes = sum(backend.estimated_bytes(b) for b in right_blocks)
    chosen = broadcast_join_choice(build_bytes, probe_bytes, parts) if broadcast is None else bool(broadcast)
    if chosen:
        return _broadcast_join(backend, left_blocks, right_blocks, on, how, mem_budget_bytes, runner)
    return _shuffle_join(backend, left_blocks, right_blocks, parts, on, how, salt, mem_budget_bytes, runner)


def _shuffle_join(
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

    left_maps = (
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
    right_maps = (
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
    left_picks = {
        (i, dest): submit(_dask_pick, f, dest, key=_skey(nonce, "lpick", i, dest))
        for i, f in enumerate(left_maps)
        for dest in range(parts)
    }
    right_picks = {
        (i, dest): submit(_dask_pick, f, dest, key=_skey(nonce, "rpick", i, dest))
        for i, f in enumerate(right_maps)
        for dest in range(parts)
    }
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
            *[left_picks[(i, dest)] for i in range(t_l)],
            *[right_picks[(i, dest)] for i in range(t_r)],
            key=_skey(nonce, "gjoin", dest),
        )
        for dest in range(parts)
    }

    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    gather_sites: dict[int, tuple[int, str]] = {}
    peak_join = spilled_total = rows_total = 0
    next_key = parts  # spilled sub-partitions get fresh keys beyond the dest range (mirrors local)
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

    witness = DaskShuffleWitness(
        n_producer_tasks=max(t_l, t_r),
        broadcast_chosen=False,
        peak_join_bytes=peak_join,
        join_spilled_partitions=spilled_total,
        join_output_rows=rows_total,
        gather_sites=gather_sites,
    )
    return _result(dest_block_hashes, value, witness)


def _broadcast_join(
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
            dest_block_hashes, value, DaskShuffleWitness(broadcast_chosen=True, n_producer_tasks=1)
        )

    build_concat = backend.concat(list(left_blocks))
    build_wire = bytes(backend.to_wire(build_concat))
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
        if unmatched and right_blocks:  # empty probe -> no schema carrier: local drops the tail (no crash)
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

    witness = DaskShuffleWitness(
        n_producer_tasks=len(right_blocks) or 1,
        broadcast_chosen=True,
        peak_join_bytes=peak_join,
        join_spilled_partitions=spilled_total,
        join_output_rows=rows_total,
        gather_sites=gather_sites,
    )
    return _result(dest_block_hashes, value, witness)
