"""Plane-parameterized worker-transport task bodies (plan §1.6 m47 move).

The m44 map / gather / gather-join / broadcast-part compute bodies + the reader-plane sizing replay,
moved here VERBATIM from ``dask_backend/transport_shuffle.py`` and parameterized by an injected
**block plane** (``store_block`` / ``record`` on the holder side; ``pull_blocks`` on the puller
side) so BOTH the dask worker-transport engine and the m47 parsl peer-exchange engine run the SAME
bodies — the frozen budget goldens read the counters these bodies produce (``common.transport_tasks``
gates on the dask job; the parsl job executes them too but does not gate).

``dask_backend/transport_shuffle.py`` keeps module-level wrappers with the exact submit names
(``_transport_map_task`` / ``_transport_gather_task`` / ``_transport_gather_join`` /
``_transport_broadcast_join_part``) binding a dask plane — the m45 dispatch spy records those
``__name__``\\ s. This module imports NOTHING from dask/distributed/parsl (the ``common/`` rule)."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from graphed_executors.common.tasks_engine import _WorkerStore  # the m43 join-spill dir (dask-free)
from graphed_executors.local.shuffle import (
    ShuffleWitness,
    _coalesce_task,
    _join_with_budget,
    _sha256_hex,
    _stage2_gather,
)


# ---- driver-side fragment / result carriers ----------------------------------------------------
@dataclass
class _MapFrag:
    """One producer's driver-side fragment: ``dest -> (digest, wire_size)`` (the sizes drive the
    reader replay), the non-empty dest count, and the reused writer-buffer peak. The wires themselves
    stay in the holder's block store — never returned through the driver."""

    manifest: dict[int, tuple[str, int]]
    blocks: int
    peak_writer: int


@dataclass
class _GatherOut:
    dest: int
    value: Any
    hash: str


@dataclass
class _JoinOut:
    dest: int
    chunks: list[Any]
    hashes: list[str]
    peak: int
    spilled: int
    rows: int
    read: int
    matched: tuple[int, ...] | None = None


# ---- the compute bodies (a `plane` abstracts the dask plugin vs the parsl http endpoint) ---------
def map_task_body(
    plane: Any, backend: Any, owned_blocks: Sequence[Any], parts: int, salt: int, holder_budget: int | None
) -> _MapFrag:
    """Stage-1 producer: coalesce this task's owned src blocks into <= P dest wires (reused
    ``_coalesce_task``: ascending-src merge, O(P*rg) writer buffer) and REGISTER each in the holder's
    block store via ``plane.store_block``. F12 holder plane: a dest wire that pushes producer-local
    RAM past ``holder_budget`` spills to disk (``holder_spill_count`` / ``peak_holder_bytes``).
    Returns only the manifest + sizes (bytes stay resident for the gather to pull)."""
    per_dest, peak_writer = _coalesce_task(backend, owned_blocks, parts, salt)
    manifest: dict[int, tuple[str, int]] = {}
    resident = 0
    peak_holder = 0
    spills = 0
    for dest in sorted(per_dest):
        wire = bytes(backend.to_wire(per_dest[dest]))
        digest = _sha256_hex(wire)
        resident += len(wire)
        peak_holder = max(peak_holder, resident)
        to_disk = holder_budget is not None and resident > holder_budget
        plane.store_block(digest, wire, to_disk=to_disk)
        if to_disk:  # spilled off the RAM budget -> back under the cap (peak already charged this wire)
            resident -= len(wire)
            spills += 1
        manifest[dest] = (digest, len(wire))
    plane.record(holder_spill_count=spills, peak_holder_bytes=peak_holder)
    return _MapFrag(manifest=manifest, blocks=len(per_dest), peak_writer=peak_writer)


def pull_ordered(plane: Any, backend: Any, pulls: Sequence[tuple[str, str]], timeout_s: float) -> list[Any]:
    """Pull one side's fragments for a dest, COALESCED per holder (one request per holder — the T2
    bound), then decode them back in the caller's ascending-task order (the deterministic merge).
    Byte-identical fragments (same digest from >1 task) share one wire but are re-materialised per
    manifest entry (parity with ``_stage2_gather``)."""
    by_holder: dict[str, list[str]] = {}
    for holder_addr, digest in pulls:
        by_holder.setdefault(holder_addr, []).append(digest)
    wires: dict[str, bytes] = {}
    for holder_addr, digests in by_holder.items():
        got = plane.pull_blocks(holder_addr, digests, timeout_s=timeout_s)
        wires.update(zip(digests, got, strict=True))
    return [backend.from_wire(wires[digest]) for _holder, digest in pulls]


def gather_task_body(
    plane: Any, backend: Any, dest: int, pulls: Sequence[tuple[str, str]], timeout_s: float
) -> _GatherOut | None:
    """Stage-2 gather: pull this dest's fragments (ascending-task = ascending-src merge), concat, and
    sha256 the wire bytes — byte-identical to the local engine's ``dest_block_hashes``."""
    blocks = pull_ordered(plane, backend, pulls, timeout_s)
    if not blocks:
        return None
    gathered = backend.concat(blocks)
    return _GatherOut(dest, gathered, _sha256_hex(bytes(backend.to_wire(gathered))))


def gather_join_body(
    plane: Any,
    backend: Any,
    dest: int,
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    left_carrier: Any,
    right_carrier: Any,
    left_pulls: Sequence[tuple[str, str]],
    right_pulls: Sequence[tuple[str, str]],
    timeout_s: float,
) -> _JoinOut | None:
    """Gather one dest's co-partitioned build/probe fragments and join them under ``mem_budget`` via
    the reused ``_join_with_budget``. F1 one-sided-dest handling mirrors ``_run_shuffle_join``: a
    build-only dest under how=left/outer null-fills against the probe carrier, and vice versa; a
    partitionless absent side (carrier ``None``) keeps the present rows as-is (never dropped)."""
    build_side = pull_ordered(plane, backend, left_pulls, timeout_s)
    probe_side = pull_ordered(plane, backend, right_pulls, timeout_s)
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
            return _JoinOut(dest, [blk], [_sha256_hex(bytes(backend.to_wire(blk)))], 0, 0, len(blk), 0)
        build, probe, how_here = backend.concat(build_side), right_carrier, "left"
    else:
        if left_carrier is None:  # partitionless build side -> keep the probe rows (never drop)
            blk = backend.concat(probe_side)
            return _JoinOut(dest, [blk], [_sha256_hex(bytes(backend.to_wire(blk)))], 0, 0, len(blk), 0)
        build, probe, how_here = left_carrier, backend.concat(probe_side), "right"

    store = _WorkerStore()
    try:
        chunks, hashes, peak, spilled, rows, read = _join_with_budget(
            backend, build, probe, on, how_here, mem_budget, store, 0
        )
    finally:
        store.cleanup()
    return _JoinOut(dest, chunks, hashes, peak, spilled, rows, read)


def broadcast_join_part_body(
    backend: Any,
    build_wire: bytes,
    probe_block: Any,
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    track_unmatched: bool,
    take_indices: tuple[int, ...] | None,
) -> _JoinOut:
    """Join one probe block against the (broadcast-resolved) whole build side — the large side is
    NEVER shuffled. ``take_indices`` restricts the build to its unmatched rows (the once-only tail);
    ``track_unmatched`` returns the build indices this block matched so the driver emits the
    never-matched build rows exactly once."""
    build = backend.from_wire(build_wire)
    if take_indices is not None:
        build = backend.take(build, list(take_indices))
    matched: tuple[int, ...] | None = None
    if track_unmatched:
        b_idx, _p_idx = backend.match_indices(build, probe_block, on=list(on), how="inner")
        matched = tuple(int(x) for x in b_idx)
    store = _WorkerStore()
    try:
        chunks, hashes, peak, spilled, rows, read = _join_with_budget(
            backend, build, probe_block, on, how, mem_budget, store, 0
        )
    finally:
        store.cleanup()
    return _JoinOut(-1, chunks, hashes, peak, spilled, rows, read, matched)


# ---- reader-plane driver-side sizing replay (the §1.4 budget-parity decision) -------------------
@dataclass
class _Sized:
    n: int


class _SizingBackend:
    """A size-only ``ShuffleBackend`` stand-in: a block IS its wire length, so replaying
    ``_stage2_gather`` reproduces the reader-plane byte accounting WITHOUT moving or decoding data."""

    def from_wire(self, wire: bytes) -> _Sized:
        return _Sized(len(wire))

    def concat(self, blocks: Sequence[_Sized]) -> _Sized:
        return _Sized(sum(b.n for b in blocks))

    def to_wire(self, block: _Sized) -> bytes:
        return b"\x00" * block.n

    def estimated_bytes(self, block: _Sized) -> int:
        return block.n


class _SizingCluster:
    """A size-only ``cluster`` stand-in keyed by REAL worker addresses. ``get`` returns zero-filled
    bytes of the manifest-recorded size; the disk-spill writes land in a throwaway temp tree — the
    byte accounting is the witness, not the files."""

    def __init__(self, addresses: Sequence[str], size_of: Mapping[str, int]) -> None:
        self._addrs = tuple(addresses)
        self._size = size_of
        self._root = tempfile.mkdtemp(prefix="gx-t44-replay-")
        self._dirs = [os.path.join(self._root, f"node-{i}") for i in range(len(self._addrs))]
        for d in self._dirs:
            os.makedirs(os.path.join(d, "objects"), exist_ok=True)

    def addr(self, i: int) -> str:
        return self._addrs[i]

    def store_dir(self, i: int) -> str:
        return self._dirs[i]

    def get(self, i: int, digest: str) -> bytes:
        return b"\x00" * self._size[digest]

    def evict(self, i: int, digest: str) -> None:
        return None

    def close(self) -> None:
        shutil.rmtree(self._root, ignore_errors=True)


def replay_reader_plane(
    manifests: dict[int, dict[int, tuple[str, int]]],
    parts: int,
    n_tasks: int,
    k: int,
    addresses: Sequence[str],
    size_of: Mapping[str, int],
    *,
    fetch_budget: int | None,
    disk_budget: int | None,
) -> ShuffleWitness:
    """Reproduce the reference reader/disk-plane counters by running the imported ``_stage2_gather``
    over the fragment SIZES (size-only backend + worker-address-keyed size-only cluster) — byte-
    identical to the local engine on identical inputs/budgets because the kernel is reused verbatim."""
    witness = ShuffleWitness(n_producer_tasks=n_tasks)
    cluster = _SizingCluster(addresses, size_of)
    try:
        _stage2_gather(
            _SizingBackend(),  # type: ignore[arg-type]
            parts,
            n_tasks,
            k,
            manifests,
            cluster,
            witness,
            fetch_budget_bytes=fetch_budget,
            disk_budget_bytes=disk_budget,
        )
    finally:
        cluster.close()
    return witness
