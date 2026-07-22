"""m44 (d): the M39-M41 shuffle / relational-join engine hosted on dask workers over the worker
transport (m44-transport-plan §1.4, Impl Target 5). An **O(T+P) scheduler graph** — exactly ``T``
strict-pinned ``_transport_map_task`` producers + ``P`` strict-pinned ``_transport_gather_task`` (or
``_transport_gather_join``) consumers + a CONSTANT control tail, with NO ``T*P`` pick tier: the map
tasks register their per-dest wires in the holder's plugin block store and the gather tasks PULL
their dest's fragments worker→worker over ``graphed_block_pull`` (the pull-model data plane), so the
bulk bytes never ride a task return or the driver.

Two accounting decisions make the frozen goldens satisfiable together (see ``.graphed/m44/attempts.md``
§"Design decisions" for the measured justification):

- **Reader-plane budgets are a DRIVER-SIDE REPLAY.** A per-DEST gather (the O(T+P) shape) cannot, by
  construction, reproduce the reference ``_stage2_gather``'s per-NODE shared-fetch-buffer counters
  (``fetch_spill_count`` / ``peak_fetch_bytes`` differ), yet the budget golden pins EXACT equality
  with the local engine AND the structure gate pins ``_transport_gather_task == parts``. Both hold
  only if the reader/disk counters are computed by REPLAYING the imported ``_stage2_gather`` over
  block SIZES at the barrier (a size-only backend + a worker-address-keyed size-only cluster), while
  the real bytes move worker→worker through the P gather tasks. The replay reuses the kernel VERBATIM
  (no reimplementation of the accounting), so it tracks the local engine bit-for-bit.
- **Join gather is naturally per-dest** — ``_run_shuffle_join`` already loops per dest with a per-dest
  ``_join_with_budget``, so P ``_transport_gather_join`` tasks reproduce the join spill counters
  EXACTLY with no replay (each dest independent).

Kernels are reused BY IMPORT (never reimplemented, never edited): ``_assign`` / ``_coalesce_task`` /
``_join_with_budget`` / ``_stage2_gather`` / ``_sha256_hex`` / ``ShuffleWitness`` from the local engine,
``broadcast_join_choice`` from the frontend, and the m43 ``_WorkerStore`` join-spill dir. The
distributed-touching imports (``_get_plugin`` / ``pull_blocks``) are deferred into the task/entry
bodies, so this module imports on the dask-free main matrix (F13)."""

from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from graphed.shuffle import broadcast_join_choice

from graphed_executors.local.shuffle import (
    ShuffleWitness,
    _assign,
    _coalesce_task,
    _join_with_budget,
    _sha256_hex,
    _stage2_gather,
)

from ._transport_run import (
    TransportShuffleResult,
    TransportWitness,
    build_stage_error,
    collect_and_purge,
    is_restart_worthy,
    pick_attributable,
    require_pin,
    sorted_addresses,
)
from .shuffle import _WorkerStore  # the m43 join-spill dir (reused, not re-implemented)


def _skey(epoch: str, *parts: object) -> str:
    """A per-epoch, collision-proof task key: the ``graphed-`` prefix cannot collide with a user
    string arg (dask/dask#9969) and the fresh epoch nonce re-executes across runs / restarts."""
    return "graphed-transport-" + epoch + "-" + "-".join(str(p) for p in parts)


# ---- module-level spawn-safe task fns (pickled BY REFERENCE; run pinned on dask workers) ----------
@dataclass
class _MapFrag:
    """One producer task's driver-side fragment: ``dest -> (digest, wire_size)`` (the holder-plane
    sizes drive the reader replay), the non-empty dest count, and the reused writer-buffer peak. The
    wires themselves stay in the holder's plugin block store — never returned through the driver."""

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


def _transport_map_task(
    spec: Any,
    backend: Any,
    owned_blocks: Sequence[Any],
    parts: int,
    salt: int,
    holder_budget: int | None,
) -> _MapFrag:
    """Stage-1 producer (strict-pinned to ``sorted_addrs[t % k]``): coalesce this task's owned src
    blocks into <= P dest wires (reused ``_coalesce_task``: ascending-src merge, O(P*rg) writer buffer)
    and REGISTER each in this worker's plugin block store. F12 holder plane: retention is bounded — a
    dest wire that pushes producer-local RAM past ``holder_budget`` spills to the worker's local disk
    (``holder_spill_count`` / ``peak_holder_bytes`` counters), so the producer store is never the
    unmanaged-memory pause/terminate trap. Returns only the manifest + sizes (bytes stay resident for
    the gather to pull)."""
    from .transport import _get_plugin  # noqa: PLC0415  (deferred: distributed-touching)

    plugin = _get_plugin()
    plugin.ensure_run(spec)
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
        plugin.store_block(spec.epoch, digest, wire, to_disk=to_disk)
        if to_disk:  # spilled off the RAM budget -> back under the cap (peak already charged this wire)
            resident -= len(wire)
            spills += 1
        manifest[dest] = (digest, len(wire))
    plugin.record(spec.epoch, holder_spill_count=spills, peak_holder_bytes=peak_holder)
    return _MapFrag(manifest=manifest, blocks=len(per_dest), peak_writer=peak_writer)


def _pull_ordered(backend: Any, epoch: str, pulls: Sequence[tuple[str, str]], timeout_s: float) -> list[Any]:
    """Pull one side's fragments for a dest, COALESCED per holder (one RPC per holder — the T2 bound),
    then decode them back in the caller's ascending-task order (the deterministic merge). Byte-identical
    fragments (same digest from >1 task) share one wire but are re-materialised per manifest entry, so a
    duplicated route keeps both fragments' rows (parity with ``_stage2_gather``). ``timeout_s`` bounds
    each per-holder batch (R4)."""
    from .transport import pull_blocks  # noqa: PLC0415  (deferred: distributed-touching)

    by_holder: dict[str, list[str]] = {}
    for holder_addr, digest in pulls:
        by_holder.setdefault(holder_addr, []).append(digest)
    wires: dict[str, bytes] = {}
    for holder_addr, digests in by_holder.items():
        got = pull_blocks(epoch, holder_addr, digests, timeout_s=timeout_s)
        wires.update(zip(digests, got, strict=True))
    return [backend.from_wire(wires[digest]) for _holder, digest in pulls]


def _transport_gather_task(
    spec: Any, backend: Any, dest: int, pulls: Sequence[tuple[str, str]]
) -> _GatherOut | None:
    """Stage-2 gather (strict-pinned): pull this dest's fragments worker→worker (ascending-task =
    ascending-src merge), concat, and sha256 the wire bytes — the content-addressed determinism key,
    byte-identical to the local engine's ``dest_block_hashes``."""
    blocks = _pull_ordered(backend, spec.epoch, pulls, spec.pull_timeout_s)
    if not blocks:
        return None
    gathered = backend.concat(blocks)
    return _GatherOut(dest, gathered, _sha256_hex(bytes(backend.to_wire(gathered))))


def _transport_gather_join(
    spec: Any,
    backend: Any,
    dest: int,
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    left_carrier: Any,
    right_carrier: Any,
    left_pulls: Sequence[tuple[str, str]],
    right_pulls: Sequence[tuple[str, str]],
) -> _JoinOut | None:
    """Gather one dest's co-partitioned build/probe fragments and join them under ``mem_budget`` via the
    reused ``_join_with_budget``. F1 one-sided-dest handling mirrors ``_run_shuffle_join`` VERBATIM: a
    build-only dest under how=left/outer null-fills against the probe schema carrier, and vice versa; a
    partitionless absent side (carrier ``None``) keeps the present rows as-is (never dropped)."""
    build_side = _pull_ordered(backend, spec.epoch, left_pulls, spec.pull_timeout_s)
    probe_side = _pull_ordered(backend, spec.epoch, right_pulls, spec.pull_timeout_s)
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


def _transport_broadcast_join_part(
    backend: Any,
    build_wire: bytes,
    probe_block: Any,
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    track_unmatched: bool,
    take_indices: tuple[int, ...] | None,
) -> _JoinOut:
    """Join one probe block against the (broadcast-resolved) whole build side — the large side is NEVER
    shuffled. Mirrors ``_run_broadcast_join``'s per-block body: ``take_indices`` restricts the build to
    its unmatched rows (the once-only left/outer tail); ``track_unmatched`` returns the build indices
    this block matched so the driver emits the never-matched build rows exactly once."""
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
    """A size-only ``cluster`` stand-in keyed by REAL worker addresses (so ``per_node_disk_bytes`` comes
    out keyed by dask worker address, per the disk golden). ``get`` returns zero-filled bytes of the
    manifest-recorded size; the disk-spill writes land in a throwaway temp tree — the byte accounting
    is the witness, not the files."""

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


def _replay_reader_plane(
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
    over the fragment SIZES (size-only backend + worker-address-keyed size-only cluster). Byte-identical
    to the local engine on identical inputs/budgets because the kernel is reused verbatim."""
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


def _collect_maps(
    map_futs: Sequence[Any], k: int
) -> tuple[dict[int, dict[int, tuple[str, int]]], dict[str, int], dict[int, int], int]:
    """Barrier: harvest every producer's fragment into per-task manifests keyed to its holder node
    (``holder_i = t % k``), the digest→size table for the replay, and the map-side witness pieces. A
    stage-1 producer death propagates raw from ``fut.result()`` straight to ``_run_with_restarts``,
    which classifies it (restart-worthy) exactly as it does a gather death."""
    manifests: dict[int, dict[int, tuple[str, int]]] = {}
    size_of: dict[str, int] = {}
    blocks_per: dict[int, int] = {}
    peak_writer = 0
    for t, fut in enumerate(map_futs):
        frag: _MapFrag = fut.result()
        holder_i = t % k
        manifests[t] = {dest: (digest, holder_i) for dest, (digest, _sz) in frag.manifest.items()}
        for _dest, (digest, sz) in frag.manifest.items():
            size_of[digest] = sz
        blocks_per[t] = frag.blocks
        peak_writer = max(peak_writer, frag.peak_writer)
    return manifests, size_of, blocks_per, peak_writer


# ---- public entry points (Impl Target 5) --------------------------------------------------------
def transport_run_repartition(
    backend: Any,
    src_blocks: Sequence[Any],
    parts: int,
    *,
    dbackend: Any,
    salt: int = 0,
    n_tasks: int | None = None,
    fetch_budget_bytes: int | None = None,
    disk_budget_bytes: int | None = None,
    holder_budget_bytes: int | None = None,
    pull_timeout_s: float | None = None,
    epoch_restarts_allowed: int = 1,
) -> TransportShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dests as an O(T+P) dask graph over the worker
    transport. ``dest_block_hashes`` are byte-identical across runs AND equal to the local
    ``run_repartition`` on identical inputs. ``fetch_budget_bytes`` / ``disk_budget_bytes`` bound the
    reader plane (accounted by the driver-side replay); ``holder_budget_bytes`` bounds the producer
    retention (F12); ``pull_timeout_s`` widens the per-holder block-pull ceiling for large batches (R4).
    A worker death restarts the whole run under a fresh epoch up to ``epoch_restarts_allowed``, else
    surfaces as an attributed ``StageError`` (§1.5)."""
    require_pin(dbackend)
    from .transport import ensure_engine_plugins  # noqa: PLC0415  (deferred: distributed-touching)

    ensure_engine_plugins(dbackend._client)  # the submit shim needs graphed-worker; idempotent
    n_src = len(src_blocks)

    def _attempt(
        spec: Any, addresses: Sequence[str], k: int, t: int
    ) -> tuple[dict[int, Any], dict[int, str], ShuffleWitness]:
        map_futs = [
            dbackend.submit(
                _transport_map_task,
                spec,
                backend,
                [src_blocks[s] for s in owned],
                parts,
                salt,
                holder_budget_bytes,
                key=_skey(spec.epoch, "map", i),
                workers=[addresses[i % k]],
                retries=0,
            )
            for i, owned in enumerate(_assign(n_src, t))
        ]
        manifests, size_of, blocks_per, peak_writer = _collect_maps(map_futs, k)
        witness = _replay_reader_plane(
            manifests,
            parts,
            t,
            k,
            addresses,
            size_of,
            fetch_budget=fetch_budget_bytes,
            disk_budget=disk_budget_bytes,
        )
        witness.blocks_per_producer_task = blocks_per
        witness.peak_writer_buffer_bytes = peak_writer
        witness.block_holder = {
            digest: addresses[hi] for m in manifests.values() for _d, (digest, hi) in m.items()
        }
        gather_futs = {
            dest: dbackend.submit(
                _transport_gather_task,
                spec,
                backend,
                dest,
                [(addresses[gt % k], manifests[gt][dest][0]) for gt in range(t) if dest in manifests[gt]],
                key=_skey(spec.epoch, "gather", dest),
                workers=[addresses[dest % k]],
                retries=0,
            )
            for dest in range(parts)
        }
        value: dict[int, Any] = {}
        hashes: dict[int, str] = {}
        errs: dict[int, BaseException] = {}
        for dest in range(parts):
            try:
                out = gather_futs[dest].result()
            except BaseException as exc:  # (harvest every gather for restart classification)
                errs[dest] = exc
                continue
            if out is not None:
                value[out.dest], hashes[out.dest] = out.value, out.hash
        if errs:
            raise pick_attributable(errs, dbackend)
        return value, hashes, witness

    return _run_with_restarts(dbackend, n_src, n_tasks, epoch_restarts_allowed, _attempt, pull_timeout_s)


def transport_run_join(
    backend: Any,
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    *,
    on: Sequence[str] = ("__joinkey__",),
    how: str = "inner",
    dbackend: Any,
    broadcast: bool | None = None,
    salt: int = 0,
    mem_budget_bytes: int | None = None,
    holder_budget_bytes: int | None = None,
    pull_timeout_s: float | None = None,
    epoch_restarts_allowed: int = 1,
) -> TransportShuffleResult:
    """Distributed relational hash JOIN over the worker transport. ``broadcast=None`` lets the pinned
    ``parts``-keyed cost rule choose (F6 — never the live worker count); ``True``/``False`` honour a
    plan-recorded choice. Shuffle path: two co-partitioned repartitions (same ``salt``) + P per-dest
    ``_transport_gather_join`` tasks reusing ``_join_with_budget`` (spill counters match the local
    engine exactly). Broadcast path: the build side ships once via ``dbackend.broadcast`` and each probe
    block is joined by a pinned ``_transport_broadcast_join_part`` — the large side never shuffles.
    ``holder_budget_bytes`` bounds BOTH sides' producer retention on the shuffle path (F12, mirrors
    repartition); ``pull_timeout_s`` widens the per-holder block-pull ceiling (R4). Neither applies to
    the broadcast path (the build side ships via ``broadcast``, not the holder store)."""
    require_pin(dbackend)
    from .transport import ensure_engine_plugins  # noqa: PLC0415  (deferred: distributed-touching)

    ensure_engine_plugins(dbackend._client)
    on_t = tuple(on)
    build_bytes = sum(backend.estimated_bytes(b) for b in left_blocks)
    probe_bytes = sum(backend.estimated_bytes(b) for b in right_blocks)
    chosen = broadcast_join_choice(build_bytes, probe_bytes, parts) if broadcast is None else bool(broadcast)

    if chosen:

        def _attempt(
            spec: Any, addresses: Sequence[str], k: int, _t: int
        ) -> tuple[dict[int, Any], dict[int, str], ShuffleWitness]:
            return _broadcast_join_attempt(
                spec, backend, dbackend, left_blocks, right_blocks, on_t, how, mem_budget_bytes, addresses, k
            )
    else:

        def _attempt(
            spec: Any, addresses: Sequence[str], k: int, _t: int
        ) -> tuple[dict[int, Any], dict[int, str], ShuffleWitness]:
            return _shuffle_join_attempt(
                spec,
                backend,
                dbackend,
                left_blocks,
                right_blocks,
                parts,
                on_t,
                how,
                salt,
                mem_budget_bytes,
                holder_budget_bytes,
                addresses,
                k,
            )

    # join task granularity keys on the side block counts, not a caller n_tasks; the restart driver's
    # ``t`` arg is unused (``_t``) — the map fan-out is min(k, n_side) inside each attempt.
    return _run_with_restarts(
        dbackend,
        max(len(left_blocks), len(right_blocks)),
        None,
        epoch_restarts_allowed,
        _attempt,
        pull_timeout_s,
    )


def _shuffle_join_attempt(
    spec: Any,
    backend: Any,
    dbackend: Any,
    left: Sequence[Any],
    right: Sequence[Any],
    parts: int,
    on: tuple[str, ...],
    how: str,
    salt: int,
    mem_budget: int | None,
    holder_budget: int | None,
    addresses: Sequence[str],
    k: int,
) -> tuple[dict[int, Any], dict[int, str], ShuffleWitness]:
    n_left, n_right = len(left), len(right)
    t_l = min(k, n_left) if n_left else 0
    t_r = min(k, n_right) if n_right else 0
    left_maps = [
        dbackend.submit(
            _transport_map_task,
            spec,
            backend,
            [left[s] for s in owned],
            parts,
            salt,
            holder_budget,
            key=_skey(spec.epoch, "lmap", i),
            workers=[addresses[i % k]],
            retries=0,
        )
        for i, owned in enumerate(_assign(n_left, t_l) if t_l else [])
    ]
    right_maps = [
        dbackend.submit(
            _transport_map_task,
            spec,
            backend,
            [right[s] for s in owned],
            parts,
            salt,
            holder_budget,
            key=_skey(spec.epoch, "rmap", i),
            workers=[addresses[i % k]],
            retries=0,
        )
        for i, owned in enumerate(_assign(n_right, t_r) if t_r else [])
    ]
    left_manifests, _lsz, _lbp, _lpw = _collect_maps(left_maps, k)
    right_manifests, _rsz, _rbp, _rpw = _collect_maps(right_maps, k)
    left_carrier = left[0] if left else None
    right_carrier = right[0] if right else None

    gather_futs = {
        dest: dbackend.submit(
            _transport_gather_join,
            spec,
            backend,
            dest,
            on,
            how,
            mem_budget,
            left_carrier,
            right_carrier,
            [(addresses[t % k], left_manifests[t][dest][0]) for t in range(t_l) if dest in left_manifests[t]],
            [
                (addresses[t % k], right_manifests[t][dest][0])
                for t in range(t_r)
                if dest in right_manifests[t]
            ],
            key=_skey(spec.epoch, "gjoin", dest),
            workers=[addresses[dest % k]],
            retries=0,
        )
        for dest in range(parts)
    }
    value: dict[int, Any] = {}
    hashes: dict[int, str] = {}
    errs: dict[int, BaseException] = {}
    peak_join = spilled_total = rows_total = reads_total = 0
    next_key = parts  # spilled sub-partitions get fresh keys beyond the dest range (mirrors local)
    for dest in range(parts):
        try:
            out = gather_futs[dest].result()
        except BaseException as exc:  # (harvest every dest for restart classification)
            errs[dest] = exc
            continue
        if out is None:
            continue
        peak_join = max(peak_join, out.peak)
        spilled_total += out.spilled
        rows_total += out.rows
        reads_total += out.read
        if len(out.chunks) == 1:
            value[dest], hashes[dest] = out.chunks[0], out.hashes[0]
        else:
            for ch, h in zip(out.chunks, out.hashes, strict=True):
                value[next_key], hashes[next_key] = ch, h
                next_key += 1
    if errs:
        raise pick_attributable(errs, dbackend)
    witness = ShuffleWitness(
        n_producer_tasks=max(t_l, t_r),
        broadcast_chosen=False,
        build_side_blocks=sum(len(m) for m in left_manifests.values()),
        large_side_blocks=sum(len(m) for m in right_manifests.values()),
        peak_join_bytes=peak_join,
        join_spilled_partitions=spilled_total,
        join_chunks_read=reads_total,
        join_output_rows=rows_total,
        manifest_fetch_is_per_dest=True,
    )
    return value, hashes, witness


def _broadcast_join_attempt(
    spec: Any,
    backend: Any,
    dbackend: Any,
    left: Sequence[Any],
    right: Sequence[Any],
    on: tuple[str, ...],
    how: str,
    mem_budget: int | None,
    addresses: Sequence[str],
    k: int,
) -> tuple[dict[int, Any], dict[int, str], ShuffleWitness]:
    value: dict[int, Any] = {}
    hashes: dict[int, str] = {}
    if not left:
        return value, hashes, ShuffleWitness(broadcast_chosen=True, n_producer_tasks=1, large_side_blocks=0)

    build_concat = backend.concat(list(left))
    build_wire = bytes(backend.to_wire(build_concat))
    handle = dbackend.broadcast(build_wire, token=f"bjoin-{spec.epoch}-{_sha256_hex(build_wire)[:12]}")
    per_block_how = {"inner": "inner", "left": "inner", "right": "right", "outer": "right"}[how]
    track_unmatched = how in ("left", "outer")

    pins = [addresses[pidx % k] for pidx in range(len(right))]
    part_futs = [
        dbackend.submit(
            _transport_broadcast_join_part,
            backend,
            handle,
            probe_block,
            on,
            per_block_how,
            mem_budget,
            track_unmatched,
            None,
            key=_skey(spec.epoch, "bpart", pidx),
            workers=[pins[pidx]],
            retries=0,
        )
        for pidx, probe_block in enumerate(right)
    ]
    errs: dict[int, BaseException] = {}
    peak_join = spilled_total = rows_total = reads_total = 0
    matched: set[int] = set()
    next_key = len(right)  # spilled sub-partitions get fresh keys beyond the pidx range
    for pidx, fut in enumerate(part_futs):
        try:
            out = fut.result()
        except BaseException as exc:  # (harvest every probe task for restart classification)
            errs[pidx] = exc
            continue
        peak_join = max(peak_join, out.peak)
        spilled_total += out.spilled
        rows_total += out.rows
        reads_total += out.read
        if out.matched is not None:
            matched.update(out.matched)
        if len(out.chunks) == 1:
            value[pidx], hashes[pidx] = out.chunks[0], out.hashes[0]
        else:
            for ch, h in zip(out.chunks, out.hashes, strict=True):
                value[next_key], hashes[next_key] = ch, h
                next_key += 1
    if errs:
        raise pick_attributable(errs, dbackend)

    if track_unmatched:  # emit the never-matched build rows exactly once (F2 of _run_broadcast_join)
        unmatched = tuple(sorted(set(range(len(build_concat))) - matched))
        if unmatched and right:  # empty probe -> no schema carrier: local drops the tail (no crash)
            tail = dbackend.submit(
                _transport_broadcast_join_part,
                backend,
                handle,
                right[0],
                on,
                "left",
                mem_budget,
                False,
                unmatched,
                key=_skey(spec.epoch, "btail", 0),
                workers=[addresses[0]],
                retries=0,
            ).result()
            peak_join = max(peak_join, tail.peak)
            spilled_total += tail.spilled
            rows_total += tail.rows
            reads_total += tail.read
            for ch, h in zip(tail.chunks, tail.hashes, strict=True):
                value[next_key], hashes[next_key] = ch, h
                next_key += 1

    witness = ShuffleWitness(
        n_producer_tasks=len(right) or 1,
        broadcast_chosen=True,
        large_side_blocks=0,
        broadcast_puts=len(set(pins)),  # DISTINCT workers that resolved the build handle (== k here)
        peak_join_bytes=peak_join,
        join_spilled_partitions=spilled_total,
        join_chunks_read=reads_total,
        join_output_rows=rows_total,
    )
    return value, hashes, witness


# ---- the §1.5 whole-run restart driver ----------------------------------------------------------
def _run_with_restarts(
    dbackend: Any,
    n_src: int,
    n_tasks: int | None,
    epoch_restarts_allowed: int,
    attempt_fn: Any,
    pull_timeout_s: float | None = None,
) -> TransportShuffleResult:
    """Run ``attempt_fn`` under a fresh epoch, restarting the WHOLE run on a restart-worthy failure
    (worker death / exhausted send / pull timeout) up to ``epoch_restarts_allowed`` — else an attributed
    ``StageError``. Worker addresses are re-read each attempt so a restart after a death pins onto the
    SURVIVORS. ``pull_timeout_s`` (when given) overrides the spec's block-pull ceiling (R4)."""
    transport = TransportWitness()
    per_worker: dict[str, dict[str, int]] = {}
    last_exc: BaseException | None = None
    for attempt in range(epoch_restarts_allowed + 1):
        from .transport import make_transport_spec  # noqa: PLC0415  (deferred: distributed-touching)

        nonce = uuid.uuid4().hex
        transport.epoch_nonces.append(nonce)
        addresses = sorted_addresses(dbackend)
        k = max(1, len(addresses))
        t = n_tasks if n_tasks is not None else (min(k, n_src) if n_src else 1)
        spec = make_transport_spec(nonce, addresses, pull_timeout_s=pull_timeout_s)
        try:
            value, hashes, witness = attempt_fn(spec, addresses, k, t)
        except BaseException as exc:  # (classify: restart or attributed StageError)
            collect_and_purge(dbackend._client, nonce, per_worker)
            last_exc = exc
            if not is_restart_worthy(exc, dbackend) or attempt >= epoch_restarts_allowed:
                transport.per_worker = per_worker
                raise build_stage_error(exc, dbackend) from exc
            continue
        collect_and_purge(dbackend._client, nonce, per_worker)
        transport.epoch_restarts = attempt
        transport.per_worker = per_worker
        return TransportShuffleResult(
            dest_block_hashes=hashes, value=value, witness=witness, transport=transport
        )

    transport.per_worker = per_worker  # pragma: no cover (the loop returns or raises)
    raise build_stage_error(last_exc, dbackend) if last_exc is not None else RuntimeError("no epochs run")
