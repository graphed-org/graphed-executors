"""The shuffle kernels every exchange engine shares: routing, coalescing, the budgeted join.

The local two-phase executor (:mod:`.shuffle`), the task engine and the transport engines all route
rows with :func:`coalesce_task`, assign producer-tasks with :func:`assign`, join a dest with
:func:`join_with_budget`, and report through :class:`ShuffleWitness` / :class:`ShuffleResult`.
"""

from __future__ import annotations

import hashlib
import time
import zlib
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from graphed.core import ShuffleBackend
from graphed.shuffle import (
    broadcast_join_choice,
    join_blocks,  # the generic radix-hash join kernel over a JoinBackend
)

#: one parquet row-group's worth of writer-buffer staging (the documented O(P*rg) memory knob,
#: §5.1).
ROW_GROUP_BYTES: int = 1 << 20  # 1 MiB
#: the pinned §4 route hash the golden vectors require (measured against a non-crypto alt below).
PINNED_ROUTING_HASH: str = "sha256"
#: fixed on-wire size charged per per-dest manifest entry (hash + holder address) — keeps the
#: metadata accounting O(N*P) linear in P (guidance 1).
MANIFEST_ENTRY_BYTES: int = 80


#: :func:`join_with_budget`'s ``(chunks, chunk_hashes, peak_output_bytes, spilled_partitions,
#: output_rows, chunks_read)``
JoinResult = tuple[list[Any], list[str], int, int, int, int]


def sha256_hex(data: bytes) -> str:
    """Return the content address of ``data`` (the hex sha256 every block is keyed by)."""
    return hashlib.sha256(data).hexdigest()


# ---- public result / fault / witness types ------------------------------------------------------
@dataclass(frozen=True)
class ShuffleFaults:
    """Deterministic fault injection (never a timing race, R0.10a).

    Announcement drop/duplicate, a forced steal (relocate >=1 producer-task to a non-owner
    node), and a fixed manifest-push drop count (drop the first N thief->owner pushes of each
    stolen manifest, then let retry-until-ack redeliver).
    """

    drop_all_announcements: bool = False
    duplicate_all_announcements: bool = False
    force_steal: bool = False
    manifest_push_drops: int = 0


@dataclass
class ShuffleWitness:
    """The mechanism counters the frozen suite asserts on (never just the result)."""

    n_producer_tasks: int = 0
    blocks_per_producer_task: dict[int, int] = field(default_factory=dict)
    blocks_of_task: dict[int, set[str]] = field(default_factory=dict)
    manifest_owner: dict[int, str] = field(default_factory=dict)  # task -> owner node address
    # block hash -> holding node address
    block_holder: dict[str, str] = field(default_factory=dict)
    node_store_dirs: dict[str, str] = field(default_factory=dict)  # node address -> Store dir
    node_hosts: list[str] = field(default_factory=list)  # the announced (routable) node hosts
    announcements_sent: int = 0
    announcements_dropped: int = 0
    manifest_put_attempts: int = 0
    manifest_put_acks: int = 0
    manifest_bytes: int = 0
    manifest_fetch_is_per_dest: bool = False
    steals: int = 0
    stolen_tasks: tuple[int, ...] = ()
    cross_node_fetches: int = 0
    peak_writer_buffer_bytes: int = 0
    # ---- M40 distributed-join counters ----
    build_side_blocks: int = 0  # shuffle blocks written by the build (left) side's map-write
    # shuffle blocks written by the probe (right) side — 0 under broadcast
    large_side_blocks: int = 0
    # times the build side was replicated to a node (== n_nodes under broadcast)
    broadcast_puts: int = 0
    # the plan-recorded broadcast-vs-shuffle choice the executor honoured
    broadcast_chosen: bool = False
    # the gather-join kernel's peak live working-set bytes (the B5 spill gate)
    peak_join_bytes: int = 0
    # radix output partitions spilled to the node Store (spill engaged)
    join_spilled_partitions: int = 0
    # spilled chunks READ BACK off disk (F5 — must equal join_spilled_partitions)
    join_chunks_read: int = 0
    join_output_rows: int = 0  # total relational (duplicating) output rows produced
    # ---- M41 fetch/gather (reader) plane + cluster-seam counters ----
    # peak live bytes in the FETCH-ACCUMULATION buffer (bounded to fetch_budget_bytes + one block);
    # NOT total reader RAM — reassembly (read-back + concat) still holds the whole dest resident.
    peak_fetch_bytes: int = 0
    fetch_spilled_bytes: int = 0  # wire bytes the fetch spill wrote to node Stores on disk (T1/T3)
    fetch_spill_count: int = 0  # number of fetch-plane spill flushes (spill engaged)
    # COALESCED fetch ops (one per (gather,holder) node-pair); also the cluster-sim fetch count.
    bulk_fetch_count: int = 0
    # mean bytes per coalesced fetch = bytes_transferred / bulk_fetch_count
    bytes_per_fetch: float = 0.0
    # total UNIQUE payload bytes moved through fetch() (re-fetch of a present digest adds 0).
    bytes_transferred: int = 0
    # node addr -> on-disk fetch-spill bytes (agrees with du of that node's Store dir).
    per_node_disk_bytes: dict[str, int] = field(default_factory=dict)
    disk_budget_bytes: int = 0  # the configured per-node Store disk cap (echoes the input)
    # times the per-node cap forced a spill onto another node (T3)
    disk_backpressure_events: int = 0
    served_pid: int = 0  # os.getpid() of the CHILD that served the last cluster-sim fetch (c.2)
    served_host: str = ""  # the routable host that served the last cluster-sim fetch (c.2)


@dataclass
class ShuffleResult:
    # dest_pid -> sha256(gathered block wire bytes) (the §7.1 key)
    """The outcome of a shuffle: dest block hashes, values, partitions and the witness."""

    dest_block_hashes: dict[int, str]
    value: dict[int, Any]  # dest_pid -> gathered backend block (content correctness)
    partitions: list[Any]  # coalesced/split blocks (run_repartition_by_size)
    witness: ShuffleWitness


# ---- routing-hash measurement (benchmark guidance 2) --------------------------------------------
def routing_hash_measurement() -> dict[str, float]:
    """MEASURE the pinned sha256 route hash against a non-cryptographic alternative.

    On packed-u64 keys (guidance 2 — never invent numbers, R0.11). Returns ``{name: seconds}``;
    ``PINNED_ROUTING_HASH`` records the chosen one (sha256, which the golden vectors require).
    Non-crypto hashes are faster, but the process-independent B2 requirement is met by either — so
    the choice is documented, not silently optimized away from the golden rule.
    """
    keys = [k.to_bytes(8, "big") for k in range(4096)]
    t0 = time.perf_counter()
    for kb in keys:
        int.from_bytes(hashlib.sha256(kb).digest()[:8], "big")
    sha256_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    for kb in keys:
        zlib.crc32(kb)
    crc32_s = time.perf_counter() - t0
    return {"sha256": sha256_s, "crc32": crc32_s}


# ---- producer-task assignment (T ~ W, contiguous ascending src_pid ranges) ----------------------
def assign(n_src: int, n_tasks: int) -> list[list[int]]:
    """Split ``n_src`` src_pids into ``n_tasks`` CONTIGUOUS ascending ranges.

    A producer-task thus coalesces several src_pids (T ~ W, not T = N) and the gather's
    ascending-task order is exactly the ascending-src_pid merge order (§4.0).
    """
    base, rem = divmod(n_src, n_tasks)
    ranges: list[list[int]] = []
    start = 0
    for t in range(n_tasks):
        size = base + (1 if t < rem else 0)
        ranges.append(list(range(start, start + size)))
        start += size
    return ranges


# ---- stage 1: coalescing map-write --------------------------------------------------------------
def coalesce_task(
    backend: ShuffleBackend[Any, Any], owned: Sequence[Any], parts: int, salt: int = 0
) -> tuple[dict[int, Any], int]:
    """Route + coalesce one producer-task's owned src blocks into <= P dest blocks.

    One per non-empty dest. Streams sub-blocks through P per-dest writers, flushing a writer at
    ``ROW_GROUP_BYTES``, so the peak live writer-buffer bytes are O(P*rg) (guidance 3). Rows
    for a dest are kept in ascending src order (the deterministic merge). ``salt`` folds into
    the pinned route (M40 skew handling; both join sides pass the SAME salt, so co-partitioning
    — and thus the join result — is preserved).
    """
    per_dest_subs: dict[int, list[Any]] = {}
    live: dict[int, int] = {}  # dest -> live writer-buffer bytes not yet flushed to the block file
    peak = 0
    for src in owned:  # ascending src_pid
        subs = backend.partition(src, "__joinkey__", parts, salt=salt)
        for dest in range(parts):
            sub = subs[dest]
            if len(sub) == 0:
                continue
            per_dest_subs.setdefault(dest, []).append(sub)
            live[dest] = live.get(dest, 0) + backend.estimated_bytes(sub)
            if live[dest] >= ROW_GROUP_BYTES:
                live[dest] = 0  # flushed a row-group to the block file (off the RAM budget)
            peak = max(peak, sum(live.values()))
    per_dest = {dest: backend.concat(subs) for dest, subs in per_dest_subs.items()}
    return per_dest, peak


# ---- the gather-join kernel (M40) ---------------------------------------------------------------
def join_with_budget(
    backend: ShuffleBackend[Any, Any],
    build: Any,
    probe: Any,
    on: Sequence[str],
    how: str,
    budget: int | None,
    cluster: Any,
    node_i: int,
) -> JoinResult:
    """Gather-join one dest's co-partitioned ``build``/``probe`` blocks with the generic kernel.

    As a LIST of joined sub-partition blocks (one when unbudgeted). With a budget it
    radix-sub-partitions the PROBE, sizing each chunk so its DUPLICATED output fits ``budget``
    (trap 2 — the budget covers output), and SPILLS each partition's wire to the node Store **on
    disk**, DROPPING it from RAM — so the kernel's live RAM is one chunk, never a full-RAM concat
    of the whole dest. Each spilled chunk is then READ BACK off disk to reconstruct the return
    value (F5 — a real reader, not dead write-only I/O), and ``chunks_read`` witnesses the reader
    actually ran. The one-chunk-at-a-time RAM bound is the JOIN+SPILL loop above (each ``joined``
    is dropped once its wire spills); the read-back list IS the return value, so all chunks are
    resident on the way out — the bound is the spill, not the read. The caller keeps the chunks as
    separate result blocks (their union is the dest's join). Returns ``(chunks, chunk_hashes,
    peak_output_bytes, spilled_partitions, output_rows, chunks_read)``.
    """
    on_l = list(on)
    if budget is None or len(probe) == 0:
        joined = join_blocks(backend, build, probe, on=on_l, how=how)
        return (
            [joined],
            [sha256_hex(backend.to_wire(joined))],
            backend.estimated_bytes(joined),
            0,
            len(joined),
            0,  # nothing spilled -> nothing to read back
        )

    n_probe = len(probe)
    # Size the probe chunk from a ONE-ROW sample so we NEVER materialise the whole duplicated
    # output to measure it (that would defeat the budget). A denser-than-predicted chunk is caught
    # + halved below.
    sample = join_blocks(backend, build, backend.slice_rows(probe, 0, 1), on=on_l, how=how)
    per_row = max(1, backend.estimated_bytes(sample))
    chunk = max(1, budget // per_row)
    obj_dir = Path(cluster.store_dir(node_i)) / "objects"

    hashes: list[str] = []
    peak = 0
    spilled = 0
    rows = 0
    off = 0
    while off < n_probe:
        c = min(chunk, n_probe - off)
        joined = join_blocks(
            backend, build, backend.slice_rows(probe, off, off + c), on=on_l, how=how
        )
        jb = backend.estimated_bytes(joined)
        if jb >= budget and c > 1:
            # a denser radix partition than the sample predicted -> shrink + retry
            chunk = max(1, c // 2)
            continue
        wire = backend.to_wire(joined)
        digest = sha256_hex(wire)
        (obj_dir / digest).write_bytes(wire)  # SPILL the radix partition to the node Store on DISK
        peak = max(peak, jb)
        spilled += 1
        rows += len(joined)
        del wire, joined  # free the wire AND the joined object from RAM — the real streaming bound
        hashes.append(digest)
        off += c

    # READ EACH SPILLED CHUNK BACK off disk to reconstruct the value — the reader F5 requires, so
    # the spill is load-bearing I/O instead of write-once-never-read.
    chunks = [backend.from_wire((obj_dir / digest).read_bytes()) for digest in hashes]
    return chunks, hashes, peak, spilled, rows, len(hashes)


def choose_broadcast(
    backend: ShuffleBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    *,
    broadcast: bool | None,
) -> bool:
    """Honour a plan-recorded ``broadcast`` choice, else apply the pinned cost rule.

    F6: the rule is keyed on ``parts``, never the live worker count, so the same logical join
    makes the same choice however the pool is sized.
    """
    if broadcast is not None:
        return bool(broadcast)
    build_bytes = sum(backend.estimated_bytes(b) for b in left_blocks)
    probe_bytes = sum(backend.estimated_bytes(b) for b in right_blocks)
    return broadcast_join_choice(build_bytes, probe_bytes, parts)


#: the per-probe-block ``how`` of a broadcast join: an unmatched build row is emitted once, after
#: every block (F2), so ``left``/``outer`` keep only their probe-side half per block.
BROADCAST_PER_BLOCK_HOW = {"inner": "inner", "left": "inner", "right": "right", "outer": "right"}


def join_dest(
    backend: ShuffleBackend[Any, Any],
    build_side: list[Any],
    probe_side: list[Any],
    on: Sequence[str],
    how: str,
    budget: int | None,
    carriers: tuple[Any, Any],
    cluster: Any,
    node_i: int,
) -> JoinResult | None:
    """Join one dest's gathered build/probe blocks; ``None`` when ``how`` keeps none of its rows.

    F1: a dest that got rows from ONE side is not dropped — how=left/outer keeps a build-only
    dest, how=right/outer a probe-only one. The absent side's schema carrier (``carriers`` holds
    the first left and right input block, ``None`` for a partitionless side) stands in, with the
    internal ``how`` forced to the one direction being filled: by the routing invariant a row
    sharing a key with this dest would itself route here, so only this dest's rows emit,
    null-filled against the carrier's schema (``backend.take`` null-fills a zero-row carrier too).
    With no carrier the present rows pass through as-is.
    """
    if not build_side and not probe_side:
        return None
    if not build_side and how not in ("right", "outer"):
        return None  # probe-only dest, but how keeps no unmatched-probe rows
    if not probe_side and how not in ("left", "outer"):
        return None  # build-only dest, but how keeps no unmatched-build rows
    left_carrier, right_carrier = carriers
    if build_side and probe_side:
        build, probe, how_here = backend.concat(build_side), backend.concat(probe_side), how
    elif build_side:
        if right_carrier is None:
            return _pass_through(backend, build_side)
        build, probe, how_here = backend.concat(build_side), right_carrier, "left"
    else:
        if left_carrier is None:
            return _pass_through(backend, probe_side)
        build, probe, how_here = left_carrier, backend.concat(probe_side), "right"
    return join_with_budget(backend, build, probe, on, how_here, budget, cluster, node_i)


def _pass_through(backend: ShuffleBackend[Any, Any], blocks: list[Any]) -> JoinResult:
    blk = backend.concat(blocks)
    return [blk], [sha256_hex(backend.to_wire(blk))], 0, 0, len(blk), 0


@dataclass
class JoinTally:
    """Join results filed under block keys, with the join counters summed over them.

    A result's lone chunk is filed under its key (stable for determinism); spilled
    sub-partitions, or a result with no key, take fresh keys counting up from ``next_key`` (their
    union is that key's join).
    """

    next_key: int
    value: dict[int, Any] = field(default_factory=dict)
    hashes: dict[int, str] = field(default_factory=dict)
    peak: int = 0
    spilled: int = 0
    rows: int = 0
    read: int = 0
    #: build-row indices any broadcast probe block matched
    matched: set[int] = field(default_factory=set)

    def add(self, key: int | None, joined: JoinResult) -> None:
        """File one :func:`join_with_budget` result."""
        chunks, hashes, peak, spilled, rows, read = joined
        self.peak = max(self.peak, peak)
        self.spilled += spilled
        self.rows += rows
        self.read += read
        if key is not None and len(chunks) == 1:
            self.value[key], self.hashes[key] = chunks[0], hashes[0]
            return
        for ch, h in zip(chunks, hashes, strict=True):
            self.value[self.next_key], self.hashes[self.next_key] = ch, h
            self.next_key += 1

    def add_out(self, key: int | None, out: Any) -> None:
        """File one task's ``_JoinOut``, recording the build rows it matched."""
        if out.matched is not None:
            self.matched.update(out.matched)
        self.add(key, (out.chunks, out.hashes, out.peak, out.spilled, out.rows, out.read))

    def unmatched(self, n_build: int) -> tuple[int, ...]:
        """Return the build rows no broadcast probe block matched, ascending."""
        return tuple(sorted(set(range(n_build)) - self.matched))


# ---- stage 2: gather (M41: coalesced fetch + reader-plane RAM/disk budgets) ---------------------
def _spill_node(
    witness: ShuffleWitness,
    disk_budget_bytes: int | None,
    k: int,
    gather_i: int,
    nbytes: int,
    cluster: Any,
) -> int:
    """Pick the node whose Store a fetch-spilled block lands on.

    Default is the gather node; if that node's on-disk Store would exceed ``disk_budget_bytes``
    (the T3 skew cap), REDISTRIBUTE to another node still under budget (round-robin), counting
    a backpressure event — so one hot dest never blows a single node's disk (Dask P2P #8433).
    Never drops data: if every node is full it falls back to the gather node (still counting
    the event) rather than lose the block.
    """
    if disk_budget_bytes is None:
        return gather_i
    here = cluster.addr(gather_i)
    if witness.per_node_disk_bytes.get(here, 0) + nbytes <= disk_budget_bytes:
        return gather_i
    for j in range(k):
        if witness.per_node_disk_bytes.get(cluster.addr(j), 0) + nbytes <= disk_budget_bytes:
            witness.disk_backpressure_events += 1
            return j
    witness.disk_backpressure_events += 1
    return gather_i


def stage2_gather(
    backend: ShuffleBackend[Any, Any],
    parts: int,
    n_tasks: int,
    k: int,
    manifests: dict[int, dict[int, tuple[str, int]]],
    cluster: Any,
    witness: ShuffleWitness,
    *,
    fetch_budget_bytes: int | None = None,
    disk_budget_bytes: int | None = None,
) -> tuple[dict[int, Any], dict[int, str]]:
    """Gather each dest in ascending-task (== ascending-src_pid) order — the deterministic merge.

    It has two M41 reader-plane hardenings over the M39 per-dest concat:

    - **T2 incast avoidance:** cross-node pulls are COALESCED to one bulk fetch per (gather,
      holder) node-pair (``bulk_fetch_count <= K*K``), instead of one ``cluster.get`` per (task,
      dest) block.
    - **T1 RAM budget + spill:** when the live FETCH-ACCUMULATION buffer crosses
      ``fetch_budget_bytes`` the buffered wires STREAM to a node-local Store on disk
      (byte-backpressure), so that buffer stays ``<= budget + one block`` (``peak_fetch_bytes``).
      Reassembly (read-back + concat) still holds the whole dest resident — T1 bounds the *fetch*
      plane, not total reader RAM. **T3 disk budget:** the spill target respects a per-node
      ``disk_budget_bytes`` cap, redistributing under skew.

    Distinct from the M40 join spill (``join_with_budget`` bounds the JOIN OUTPUT) and the M39
    writer flush (``ROW_GROUP_BYTES``): this is the third, reader-plane spill. Content + order are
    preserved (spill/read-back is FIFO in ascending-task order), so ``dest_block_hashes`` stay
    byte-identical.
    """
    witness.manifest_fetch_is_per_dest = True
    if disk_budget_bytes is not None:
        witness.disk_budget_bytes = disk_budget_bytes
    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    # unique on-disk spill names: hot-key blocks can be byte-identical, so a content-address would
    # dedup REAL bytes and undercount the independent `du` disk cross-check.
    spill_seq = 0

    for gather_i in range(k):
        dests_here = [d for d in range(parts) if d % k == gather_i]
        by_holder = _blocks_by_holder(dests_here, n_tasks, manifests, witness)
        resident: dict[tuple[int, int], bytes] = {}  # (dest, task) -> wire held in RAM
        # (dest, task) -> spilled wire path on a node Store
        spilled: dict[tuple[int, int], Path] = {}
        live_bytes = 0

        for holder_i in sorted(by_holder):
            for key, wire in _pull_holder(
                cluster, holder_i, gather_i, by_holder[holder_i], witness
            ):
                resident[key] = wire
                live_bytes += len(wire)
                witness.peak_fetch_bytes = max(witness.peak_fetch_bytes, live_bytes)
                if fetch_budget_bytes is not None and live_bytes >= fetch_budget_bytes:
                    spill_seq = _spill_resident(
                        resident,
                        spilled,
                        spill_seq,
                        witness,
                        disk_budget_bytes,
                        k,
                        gather_i,
                        cluster,
                    )
                    live_bytes = 0

        for dest in dests_here:
            gathered = _reassemble(backend, dest, n_tasks, resident, spilled)
            if gathered is not None:
                value[dest] = gathered
                dest_block_hashes[dest] = sha256_hex(backend.to_wire(gathered))

    if witness.bulk_fetch_count:
        witness.bytes_per_fetch = witness.bytes_transferred / witness.bulk_fetch_count
    return value, dest_block_hashes


def _pull_holder(
    cluster: Any,
    holder_i: int,
    gather_i: int,
    blocks: list[tuple[int, int, str]],
    witness: ShuffleWitness,
) -> Iterator[tuple[tuple[int, int], bytes]]:
    """Yield each ``(dest, task)`` wire pulled from one holder: ONE coalesced fetch (T2).

    Byte-identical hot-key blocks (steal can co-locate them) share one content address, so each
    digest is billed to ``bytes_transferred`` once and evicted once, after all its refs are
    pulled — a mid-loop evict would KeyError the next ref.
    """
    witness.bulk_fetch_count += 1
    billed: set[str] = set()
    for dest, t, digest in blocks:
        wire = cluster.get(holder_i, digest)  # from its holder (the thief if stolen)
        if digest not in billed:
            witness.bytes_transferred += len(wire)
            billed.add(digest)
        if holder_i != gather_i:
            witness.cross_node_fetches += 1  # the M39 per-block witness, kept for cross-check
        yield (dest, t), wire
    for digest in billed:
        cluster.evict(holder_i, digest)


def _blocks_by_holder(
    dests: list[int],
    n_tasks: int,
    manifests: dict[int, dict[int, tuple[str, int]]],
    witness: ShuffleWitness,
) -> dict[int, list[tuple[int, int, str]]]:
    """Group the ``(dest, task, digest)`` blocks one gather node needs by the node holding each."""
    by_holder: dict[int, list[tuple[int, int, str]]] = {}
    for dest in dests:
        # ascending task -> ascending src_pid: the deterministic merge order
        for t in range(n_tasks):
            # per-dest manifest GET (.../{task}/{dest}), O(N*P) metadata
            entry = manifests[t].get(dest)
            if entry is None:
                continue
            digest, holder_i = entry
            witness.manifest_bytes += MANIFEST_ENTRY_BYTES
            by_holder.setdefault(holder_i, []).append((dest, t, digest))
    return by_holder


def _spill_resident(
    resident: dict[tuple[int, int], bytes],
    spilled: dict[tuple[int, int], Path],
    spill_seq: int,
    witness: ShuffleWitness,
    disk_budget_bytes: int | None,
    k: int,
    gather_i: int,
    cluster: Any,
) -> int:
    """Stream every buffered wire to a node Store on disk and empty the buffer.

    Returns the next spill sequence number.
    """
    for key, w in resident.items():
        node_i = _spill_node(witness, disk_budget_bytes, k, gather_i, len(w), cluster)
        path = Path(cluster.store_dir(node_i)) / "objects" / f"fetchspill-{spill_seq:08d}"
        spill_seq += 1
        path.write_bytes(w)
        spilled[key] = path
        witness.fetch_spilled_bytes += len(w)
        addr = cluster.addr(node_i)
        witness.per_node_disk_bytes[addr] = witness.per_node_disk_bytes.get(addr, 0) + len(w)
    witness.fetch_spill_count += 1
    resident.clear()
    return spill_seq


def _reassemble(
    backend: ShuffleBackend[Any, Any],
    dest: int,
    n_tasks: int,
    resident: dict[tuple[int, int], bytes],
    spilled: dict[tuple[int, int], Path],
) -> Any:
    """Concat ``dest``'s blocks in ascending-task order from RAM or spill; None if it got none."""
    blocks: list[Any] = []
    for t in range(n_tasks):
        key = (dest, t)
        if key in resident:
            wire = resident[key]
        elif key in spilled:
            wire = spilled[key].read_bytes()
        else:
            continue
        blocks.append(backend.from_wire(wire))
    return backend.concat(blocks) if blocks else None
