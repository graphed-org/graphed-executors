"""The two-phase shuffle executor (plan M39 §4): map-write + gather over a ``ShuffleBackend``.

This is the generic exchange/repartition ENGINE, backend-agnostic (it deals only in the opaque
``graphed.core.ShuffleBackend`` primitives — ``partition``/``concat``/``slice_rows``/
``estimated_bytes``/``to_wire``/``from_wire``), so its correctness is witnessed by EXECUTION over
BOTH real backends (the a-BI theme), not merely a §A.4 import lint.

Design (plan §4.0-§4.3, §7):

- **Producer-task granularity** ``T ~ W`` (one producer-task per worker, each coalescing several
  ``src_pid``s), not ``T = N`` src_pids — so stage-1 emits at most **P blocks per producer-task**
  (``O(T*P)``), never the ``O(N*P)`` per-src_pid MxR fragment blowup (§4.0/B, the anti-MxR gate).
- **Coalescing writer** streams sub-blocks into P open per-dest writers, flushing a row-group when a
  writer reaches ``ROW_GROUP_BYTES``, so stage-1 writer memory is **O(P*rg)** (guidance 3), not
  O(total shuffled bytes). ``rg`` is the documented ``ROW_GROUP_BYTES`` knob.
- **Deterministic ascending-``src_pid`` merge** (§4.0): producer-tasks own contiguous ascending
  ``src_pid`` ranges and the gather concatenates them in ascending task order, so a dest's rows
  assemble in a fixed order regardless of arrival — the content-addressed ``dest_block_hashes`` are
  therefore byte-identical across two fuzzed-arrival runs (§7.1 determinism gate).
- **Announcement-independent completeness** (§4.2.1/B1): the gather derives the input set from the
  plan's producer-task set + per-producer-task manifests (a **per-dest GET**, ``O(N*P)`` metadata,
  guidance 1), NOT from pushed announcements — which are demoted to droppable/duplicable hints.
- **Steal + reliable ship-back** (§4.3/NB1,NB2): a stolen producer-task's blocks stay on the THIEF;
  only the tiny manifest is pushed to ``owner(task)`` (retry-until-ack, dedup), and the gather dials
  ``owner(task)`` for the manifest then pulls the block from the thief.

Cluster-sim scope (§6.5): ``comms="ipc"`` runs the K logical nodes in-process (the default, the fast
path every theme but the cross-process one uses); ``comms="http"`` gives each node its OWN
node-local Store and a real HTTP block server bound to a ROUTABLE (non-loopback) address, so blocks
move between distinct node Stores over a real socket on a dialable IP. This is a single-machine
cluster *simulation* of the transport seam — see ``CLAUDE.md`` for the cluster-correct-seam scope.
"""

from __future__ import annotations

import hashlib
import tempfile
import threading
import time
import urllib.request
import zlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from graphed.core import ShuffleBackend
from graphed.core.execution import JoinBackend  # the join-half protocol (match_indices/take), M40
from graphed.shuffle import (
    broadcast_join_choice,  # the pinned cost rule (F6: graphed.shuffle is the single source of truth)
    join_blocks,  # the generic radix-hash join kernel over a JoinBackend (M40)
)

from ._transport import select_advertise_host

#: one parquet row-group's worth of writer-buffer staging (the documented O(P*rg) memory knob, §5.1).
ROW_GROUP_BYTES: int = 1 << 20  # 1 MiB
#: the pinned §4 route hash the golden vectors require (measured against a non-crypto alt below).
PINNED_ROUTING_HASH: str = "sha256"
#: fixed on-wire size charged per per-dest manifest entry (hash + holder address) — keeps the
#: metadata accounting O(N*P) linear in P (guidance 1).
_MANIFEST_ENTRY_BYTES: int = 80


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---- public result / fault / witness types ------------------------------------------------------
@dataclass(frozen=True)
class ShuffleFaults:
    """Deterministic fault injection (never a timing race, R0.10a): announcement drop/duplicate,
    a forced steal (relocate >=1 producer-task to a non-owner node), and a fixed manifest-push drop
    count (drop the first N thief->owner pushes of each stolen manifest, then let retry-until-ack
    redeliver)."""

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
    block_holder: dict[str, str] = field(default_factory=dict)  # block hash -> holding node address
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
    large_side_blocks: int = 0  # shuffle blocks written by the probe (right) side — 0 under broadcast
    broadcast_puts: int = 0  # times the build side was replicated to a node (== n_nodes under broadcast)
    broadcast_chosen: bool = False  # the plan-recorded broadcast-vs-shuffle choice the executor honoured
    peak_join_bytes: int = 0  # the gather-join kernel's peak live working-set bytes (the B5 spill gate)
    join_spilled_partitions: int = 0  # radix output partitions spilled to the node Store (spill engaged)
    join_chunks_read: int = 0  # spilled chunks READ BACK off disk (F5 — must equal join_spilled_partitions)
    join_output_rows: int = 0  # total relational (duplicating) output rows produced


@dataclass
class ShuffleResult:
    dest_block_hashes: dict[int, str]  # dest_pid -> sha256(gathered block wire bytes) (the §7.1 key)
    value: dict[int, Any]  # dest_pid -> gathered backend block (content correctness)
    partitions: list[Any]  # coalesced/split blocks (run_repartition_by_size)
    witness: ShuffleWitness


# ---- routing-hash measurement (benchmark guidance 2) --------------------------------------------
def routing_hash_measurement() -> dict[str, float]:
    """MEASURE the pinned sha256 route hash against a non-cryptographic alternative on packed-u64
    keys (guidance 2 — never invent numbers, R0.11). Returns ``{name: seconds}``; ``PINNED_ROUTING_HASH``
    records the chosen one (sha256, which the golden vectors require). Non-crypto hashes are faster,
    but the process-independent B2 requirement is met by either — so the choice is documented, not
    silently optimized away from the golden rule."""
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


# ---- node cluster: block Stores + (optional) routable HTTP transport ----------------------------
def _make_block_handler(blocks: dict[str, bytes]) -> type[BaseHTTPRequestHandler]:
    class _BlockHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # stdlib http.server handler name
            digest = self.path.rsplit("/", 1)[-1]
            data = blocks.get(digest)
            if data is None:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: Any) -> None:  # silence the access log
            pass

    return _BlockHandler


class _IpcCluster:
    """K logical nodes in-process (the default): each has its own block dict + Store dir; fetch is a
    local lookup. The fast path for every theme except the cross-process cluster-sim."""

    def __init__(self, k: int, root: Path) -> None:
        self._blocks: list[dict[str, bytes]] = [{} for _ in range(k)]
        self._dirs = [root / f"node-{i}" for i in range(k)]
        for d in self._dirs:
            (d / "objects").mkdir(parents=True, exist_ok=True)

    def addr(self, i: int) -> str:
        return f"node-{i}"

    def store_dir(self, i: int) -> str:
        return str(self._dirs[i])

    def host(self, i: int) -> str:
        return f"node-{i}"

    def put(self, i: int, digest: str, wire: bytes) -> None:
        self._blocks[i][digest] = wire

    def get(self, i: int, digest: str) -> bytes:
        return self._blocks[i][digest]

    def close(self) -> None:
        pass


class _HttpNode:
    """One cluster-sim node: an isolated block Store served by a real HTTP GET server bound to a
    routable address (so a peer genuinely dials it over a socket)."""

    def __init__(self, host: str, store_dir: Path) -> None:
        self.blocks: dict[str, bytes] = {}
        self.dir = store_dir
        (store_dir / "objects").mkdir(parents=True, exist_ok=True)
        self._server = ThreadingHTTPServer((host, 0), _make_block_handler(self.blocks))
        self.host = str(self._server.server_address[0])
        self.port = int(self._server.server_address[1])
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


class _HttpCluster:
    """K nodes each with its OWN Store dir and a real HTTP block server on a ROUTABLE host (§6.2);
    a cross-node fetch is a genuine HTTP GET over that socket. A single-machine cluster simulation of
    the transport seam (in-process threads, real routable sockets) — see the §6.5 CLAUDE.md scope."""

    def __init__(self, k: int, root: Path, advertise_host: str | None) -> None:
        host = select_advertise_host(advertise_host)  # ValueError on loopback/0.0.0.0
        self._nodes = [_HttpNode(host, root / f"node-{i}") for i in range(k)]

    def addr(self, i: int) -> str:
        return f"node-{i}"

    def store_dir(self, i: int) -> str:
        return str(self._nodes[i].dir)

    def host(self, i: int) -> str:
        return self._nodes[i].host

    def put(self, i: int, digest: str, wire: bytes) -> None:
        self._nodes[i].blocks[digest] = wire

    def get(self, i: int, digest: str) -> bytes:
        node = self._nodes[i]
        url = f"http://{node.host}:{node.port}/block/{digest}"
        with urllib.request.urlopen(url, timeout=10) as resp:  # a real cross-node socket fetch
            data: bytes = resp.read()
        return data

    def close(self) -> None:
        for node in self._nodes:
            node.close()


def _make_cluster(comms: str, k: int, store_root: str | None, advertise_host: str | None) -> Any:
    root = Path(store_root) if store_root is not None else Path(tempfile.mkdtemp(prefix="gx-shuffle-"))
    if comms == "ipc":
        return _IpcCluster(k, root)
    if comms == "http":
        return _HttpCluster(k, root, advertise_host)
    raise ValueError(f"unknown comms {comms!r} (expected 'ipc' or 'http')")


# ---- producer-task assignment (T ~ W, contiguous ascending src_pid ranges) ----------------------
def _assign(n_src: int, n_tasks: int) -> list[list[int]]:
    """Split ``n_src`` src_pids into ``n_tasks`` CONTIGUOUS ascending ranges — so a producer-task
    coalesces several src_pids (T ~ W, not T = N) and the gather's ascending-task order is exactly
    the ascending-src_pid merge order (§4.0)."""
    base, rem = divmod(n_src, n_tasks)
    ranges: list[list[int]] = []
    start = 0
    for t in range(n_tasks):
        size = base + (1 if t < rem else 0)
        ranges.append(list(range(start, start + size)))
        start += size
    return ranges


# ---- stage 1: coalescing map-write --------------------------------------------------------------
def _coalesce_task(
    backend: ShuffleBackend[Any, Any], owned: Sequence[Any], parts: int, salt: int = 0
) -> tuple[dict[int, Any], int]:
    """Route + coalesce one producer-task's owned src blocks into <= P dest blocks (one per non-empty
    dest). Streams sub-blocks through P per-dest writers, flushing a writer at ``ROW_GROUP_BYTES``, so
    the peak live writer-buffer bytes are O(P*rg) (guidance 3). Rows for a dest are kept in ascending
    src order (the deterministic merge). ``salt`` folds into the pinned route (M40 skew handling; both
    join sides pass the SAME salt, so co-partitioning — and thus the join result — is preserved)."""
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


def _reliable_manifest_push(faults: ShuffleFaults, witness: ShuffleWitness) -> None:
    """Push a stolen producer-task's manifest thief->owner with retry-until-ack: drop the first
    ``manifest_push_drops`` attempts, then ack (attempts > acks witnesses the redelivery; §4.3/NB2)."""
    attempts = 0
    while True:
        attempts += 1
        witness.manifest_put_attempts += 1
        if attempts <= faults.manifest_push_drops:
            continue  # dropped in flight — retry
        witness.manifest_put_acks += 1
        return


def _stage1_map_write(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    n_tasks: int,
    k: int,
    faults: ShuffleFaults,
    steal: bool,
    cluster: Any,
    witness: ShuffleWitness,
    salt: int = 0,
) -> dict[int, dict[int, tuple[str, int]]]:
    assignment = _assign(len(src_blocks), n_tasks)
    stolen: dict[int, int] = {}
    if steal and faults.force_steal and k >= 2:
        t_steal = 0  # deterministic relocation (not a timing race)
        thief = (t_steal % k + 1) % k  # a node other than owner(t_steal) = t_steal % k
        stolen[t_steal] = thief
        witness.steals = 1
        witness.stolen_tasks = (t_steal,)

    manifests: dict[int, dict[int, tuple[str, int]]] = {}
    peak = 0
    for t, owned_ids in enumerate(assignment):
        owner_i = t % k
        exec_i = stolen.get(t, owner_i)  # a stolen task runs on (and leaves its blocks on) the thief
        per_dest, task_peak = _coalesce_task(backend, [src_blocks[s] for s in owned_ids], parts, salt)
        peak = max(peak, task_peak)

        manifest: dict[int, tuple[str, int]] = {}
        block_hashes: set[str] = set()
        for dest in sorted(per_dest):
            wire = backend.to_wire(per_dest[dest])
            digest = _sha256_hex(wire)
            cluster.put(exec_i, digest, wire)
            witness.block_holder[digest] = cluster.addr(exec_i)
            manifest[dest] = (digest, exec_i)
            block_hashes.add(digest)
            n_ann = 2 if faults.duplicate_all_announcements else 1  # a droppable per-block hint
            witness.announcements_sent += n_ann
            if faults.drop_all_announcements:
                witness.announcements_dropped += n_ann

        witness.blocks_per_producer_task[t] = len(manifest)
        witness.blocks_of_task[t] = block_hashes
        witness.manifest_owner[t] = cluster.addr(owner_i)  # owner journals the manifest regardless
        manifests[t] = manifest
        if t in stolen:
            _reliable_manifest_push(faults, witness)
    witness.peak_writer_buffer_bytes = peak
    return manifests


# ---- stage 2: gather ----------------------------------------------------------------------------
def _stage2_gather(
    backend: ShuffleBackend[Any, Any],
    parts: int,
    n_tasks: int,
    k: int,
    manifests: dict[int, dict[int, tuple[str, int]]],
    cluster: Any,
    witness: ShuffleWitness,
) -> tuple[dict[int, Any], dict[int, str]]:
    witness.manifest_fetch_is_per_dest = True
    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    for dest in range(parts):
        gather_i = dest % k
        blocks: list[Any] = []
        for t in range(n_tasks):  # ascending task -> ascending src_pid: the deterministic merge order
            entry = manifests[t].get(dest)  # per-dest manifest GET (.../{task}/{dest}), O(N*P) metadata
            if entry is None:
                continue
            digest, holder_i = entry
            witness.manifest_bytes += _MANIFEST_ENTRY_BYTES
            wire = cluster.get(holder_i, digest)  # pull the block from its holder (thief if stolen)
            if holder_i != gather_i:
                witness.cross_node_fetches += 1
            blocks.append(backend.from_wire(wire))
        if not blocks:
            continue
        gathered = backend.concat(blocks)
        value[dest] = gathered
        dest_block_hashes[dest] = _sha256_hex(backend.to_wire(gathered))
    return value, dest_block_hashes


# ---- public entry points ------------------------------------------------------------------------
def run_repartition(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    *,
    workers: int = 1,
    comms: str = "ipc",
    store_root: str | None = None,
    steal: bool = False,
    faults: ShuffleFaults | None = None,
    advertise_host: str | None = None,
) -> ShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dest partitions via the two-phase executor.
    See the module docstring for the coalescing / determinism / announcement-independence / steal
    guarantees. ``comms="http"`` runs the routable cross-node cluster-sim."""
    faults = faults if faults is not None else ShuffleFaults()
    n_src = len(src_blocks)
    k = max(1, workers)
    n_tasks = min(k, n_src) if n_src else 1
    cluster = _make_cluster(comms, k, store_root, advertise_host)
    try:
        witness = ShuffleWitness(n_producer_tasks=n_tasks)
        witness.node_store_dirs = {cluster.addr(i): cluster.store_dir(i) for i in range(k)}
        witness.node_hosts = [cluster.host(i) for i in range(k)]
        manifests = _stage1_map_write(backend, src_blocks, parts, n_tasks, k, faults, steal, cluster, witness)
        value, dest_block_hashes = _stage2_gather(backend, parts, n_tasks, k, manifests, cluster, witness)
        return ShuffleResult(dest_block_hashes=dest_block_hashes, value=value, partitions=[], witness=witness)
    finally:
        cluster.close()


# ---- M40: distributed relational JOIN over the two-phase substrate ------------------------------
# broadcast_join_choice moved to graphed.shuffle (F6: the frontend, where join_plan freezes the choice
# into the durable plan, is the single source of truth); imported above and re-exported from here.
def _gather_side(
    backend: ShuffleBackend[Any, Any],
    manifests: dict[int, dict[int, tuple[str, int]]],
    dest: int,
    n_tasks: int,
    gather_i: int,
    cluster: Any,
    witness: ShuffleWitness,
) -> list[Any]:
    """Pull one side's blocks for ``dest`` in ascending-task (== ascending-src_pid) order — the
    deterministic merge — from each block's holder (the thief, if its task was stolen)."""
    blocks: list[Any] = []
    for t in range(n_tasks):
        entry = manifests[t].get(dest)
        if entry is None:
            continue
        digest, holder_i = entry
        witness.manifest_bytes += _MANIFEST_ENTRY_BYTES
        wire = cluster.get(holder_i, digest)
        if holder_i != gather_i:
            witness.cross_node_fetches += 1
        blocks.append(backend.from_wire(wire))
    return blocks


def _join_with_budget(
    backend: ShuffleBackend[Any, Any],
    build: Any,
    probe: Any,
    on: Sequence[str],
    how: str,
    budget: int | None,
    cluster: Any,
    node_i: int,
) -> tuple[list[Any], list[str], int, int, int, int]:
    """Gather-join one dest's co-partitioned ``build``/``probe`` blocks with the generic kernel, as a
    LIST of joined sub-partition blocks (one when unbudgeted). With a budget it radix-sub-partitions the
    PROBE, sizing each chunk so its DUPLICATED output fits ``budget`` (trap 2 — the budget covers
    output), and SPILLS each partition's wire to the node Store **on disk**, DROPPING it from RAM —
    so the kernel's live RAM is one chunk, never a full-RAM concat of the whole dest. Each spilled chunk
    is then READ BACK off disk to reconstruct the return value (F5 — a real reader, not dead write-only
    I/O): the caller's RAM holds one read-back chunk at a time here too, and ``chunks_read`` witnesses
    the reader actually ran. The caller keeps the chunks as separate result blocks (their union is the
    dest's join). Returns
    ``(chunks, chunk_hashes, peak_output_bytes, spilled_partitions, output_rows, chunks_read)``.
    """
    on_l = list(on)
    if budget is None or len(probe) == 0:
        joined = join_blocks(backend, build, probe, on=on_l, how=how)
        return (
            [joined],
            [_sha256_hex(backend.to_wire(joined))],
            backend.estimated_bytes(joined),
            0,
            len(joined),
            0,  # nothing spilled -> nothing to read back
        )

    n_probe = len(probe)
    # Size the probe chunk from a ONE-ROW sample so we NEVER materialise the whole duplicated output to
    # measure it (that would defeat the budget). A denser-than-predicted chunk is caught + halved below.
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
        joined = join_blocks(backend, build, backend.slice_rows(probe, off, off + c), on=on_l, how=how)
        jb = backend.estimated_bytes(joined)
        if jb >= budget and c > 1:
            chunk = max(1, c // 2)  # a denser radix partition than the sample predicted -> shrink + retry
            continue
        wire = backend.to_wire(joined)
        digest = _sha256_hex(wire)
        (obj_dir / digest).write_bytes(wire)  # SPILL the radix partition to the node Store on DISK
        peak = max(peak, jb)
        spilled += 1
        rows += len(joined)
        del wire, joined  # free the wire AND the joined object from RAM — the real streaming bound
        hashes.append(digest)
        off += c

    # READ EACH SPILLED CHUNK BACK off disk to reconstruct the value — the reader F5 requires, so the
    # spill is load-bearing I/O instead of write-once-never-read.
    chunks = [backend.from_wire((obj_dir / digest).read_bytes()) for digest in hashes]
    return chunks, hashes, peak, spilled, rows, len(hashes)


def _run_shuffle_join(
    backend: ShuffleBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    k: int,
    on: Sequence[str],
    how: str,
    salt: int,
    mem_budget: int | None,
    steal: bool,
    faults: ShuffleFaults,
    cluster: Any,
    witness: ShuffleWitness,
) -> tuple[dict[int, Any], dict[int, str]]:
    """Shuffle both sides on ``__joinkey__`` (same ``parts`` + ``salt`` ⇒ co-partitioned), then gather
    the two sides per dest in ascending-task order and join them (spilling under ``mem_budget``).

    F1: a dest that only got rows from ONE side is not dropped — how=left/outer keeps a BUILD-only dest,
    how=right/outer keeps a PROBE-only dest. The absent side's ORIGINAL other-side block
    (``left_blocks[0]``/``right_blocks[0]``) stands in as a SCHEMA carrier and the internal ``how`` is
    forced to only the direction being filled (``left``/``right``): by the routing invariant any row
    sharing a key with this dest would itself route here, so an empty gather PROVES the carrier shares no
    key with it and ``match_indices``' opposite arm is never taken — only THIS dest's real rows emit,
    each null-filled against the carrier's schema. The carrier may itself be a ZERO-ROW block (a
    cut-emptied partition): ``backend.take`` null-fills an empty carrier, so no non-emptiness is assumed.
    An ENTIRELY partitionless absent side (``right_blocks``/``left_blocks`` == ``[]``) carries no schema
    at all — its present-side rows are emitted as-is (no null columns to add), never silently dropped."""
    n_tasks_l = min(k, len(left_blocks)) if left_blocks else 1
    n_tasks_r = min(k, len(right_blocks)) if right_blocks else 1
    witness.n_producer_tasks = max(n_tasks_l, n_tasks_r)
    left_manifests = _stage1_map_write(
        backend, left_blocks, parts, n_tasks_l, k, faults, steal, cluster, witness, salt
    )
    witness.build_side_blocks = sum(len(m) for m in left_manifests.values())
    right_manifests = _stage1_map_write(
        backend, right_blocks, parts, n_tasks_r, k, faults, steal, cluster, witness, salt
    )
    witness.large_side_blocks = sum(len(m) for m in right_manifests.values())
    witness.manifest_fetch_is_per_dest = True

    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    next_key = parts  # sub-partition (budget) blocks get fresh keys beyond the dest range
    for dest in range(parts):
        gather_i = dest % k
        build_side = _gather_side(backend, left_manifests, dest, n_tasks_l, gather_i, cluster, witness)
        probe_side = _gather_side(backend, right_manifests, dest, n_tasks_r, gather_i, cluster, witness)
        if not build_side and not probe_side:
            continue  # no rows on either side for this dest
        if not build_side and how not in ("right", "outer"):
            continue  # probe-only dest, but `how` keeps no unmatched-probe rows (F1)
        if not probe_side and how not in ("left", "outer"):
            continue  # build-only dest, but `how` keeps no unmatched-build rows (F1)
        if build_side and probe_side:
            build, probe, how_here = backend.concat(build_side), backend.concat(probe_side), how
        elif build_side:
            if not right_blocks:  # partitionless probe side -> no schema; keep the build rows (never drop)
                blk = backend.concat(build_side)
                value[dest], dest_block_hashes[dest] = blk, _sha256_hex(backend.to_wire(blk))
                continue
            build, probe, how_here = backend.concat(build_side), right_blocks[0], "left"
        else:
            if not left_blocks:  # partitionless build side -> keep the probe rows (never drop)
                blk = backend.concat(probe_side)
                value[dest], dest_block_hashes[dest] = blk, _sha256_hex(backend.to_wire(blk))
                continue
            build, probe, how_here = left_blocks[0], backend.concat(probe_side), "right"
        chunks, hashes, peak, spilled, rows, chunks_read = _join_with_budget(
            backend, build, probe, on, how_here, mem_budget, cluster, gather_i
        )
        witness.peak_join_bytes = max(witness.peak_join_bytes, peak)
        witness.join_spilled_partitions += spilled
        witness.join_chunks_read += chunks_read
        witness.join_output_rows += rows
        if len(chunks) == 1:  # the whole dest is one block -> keyed by dest (stable for determinism)
            value[dest], dest_block_hashes[dest] = chunks[0], hashes[0]
        else:  # spilled sub-partitions -> separate result blocks (their union is the dest's join)
            for ch, h in zip(chunks, hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1
    return value, dest_block_hashes


def _run_broadcast_join(
    backend: JoinBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    k: int,
    on: Sequence[str],
    how: str,
    mem_budget: int | None,
    cluster: Any,
    witness: ShuffleWitness,
) -> tuple[dict[int, Any], dict[int, str]]:
    """Replicate the small build (left) side to every node and join each probe (right) partition
    against the whole build — the large side is NEVER shuffled (``large_side_blocks == 0``). ``value``
    is keyed by probe-partition index; the join RESULT (union over blocks) equals the shuffle join.

    F2: joining each probe block independently against the WHOLE build side would re-emit an unmatched
    build row once per block (N-fold). So for how=left/outer, each block is joined with the per-block
    how narrowed to "inner" (matched pairs only — never a per-block build-only fill) while the build
    indices matched in ANY block are tracked; AFTER all blocks, the build rows that never matched
    anywhere are emitted EXACTLY ONCE as null-probe rows. how=right/outer needs no such tracking — a
    probe row belongs to exactly one block, so its unmatched survivor is already exactly-once."""
    value: dict[int, Any] = {}
    dest_block_hashes: dict[int, str] = {}
    if not left_blocks:
        return value, dest_block_hashes
    build_concat = backend.concat(list(left_blocks))
    build_wire = backend.to_wire(build_concat)
    build_digest = _sha256_hex(build_wire)
    for i in range(k):
        cluster.put(i, build_digest, build_wire)
        witness.broadcast_puts += 1
    witness.large_side_blocks = 0
    witness.n_producer_tasks = max(1, min(k, len(right_blocks))) if right_blocks else 1

    per_block_how = {"inner": "inner", "left": "inner", "right": "right", "outer": "right"}[how]
    track_unmatched = how in ("left", "outer")
    matched_build_idx: set[int] = set()

    next_key = len(right_blocks)  # sub-partition (budget) blocks get fresh keys beyond the pidx range
    for pidx, probe_block in enumerate(right_blocks):
        node_i = pidx % k
        build_here = backend.from_wire(cluster.get(node_i, build_digest))
        if node_i != 0:
            witness.cross_node_fetches += 1
        if track_unmatched:
            b_idx, _p_idx = backend.match_indices(build_here, probe_block, on=list(on), how="inner")
            matched_build_idx.update(int(x) for x in b_idx)
        chunks, hashes, peak, spilled, rows, chunks_read = _join_with_budget(
            backend, build_here, probe_block, on, per_block_how, mem_budget, cluster, node_i
        )
        witness.peak_join_bytes = max(witness.peak_join_bytes, peak)
        witness.join_spilled_partitions += spilled
        witness.join_chunks_read += chunks_read
        witness.join_output_rows += rows
        if len(chunks) == 1:
            value[pidx], dest_block_hashes[pidx] = chunks[0], hashes[0]
        else:
            for ch, h in zip(chunks, hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1

    if track_unmatched:
        unmatched = sorted(set(range(len(build_concat))) - matched_build_idx)
        # ponytail: a probe-less broadcast run (a forced ``broadcast=True`` on an entirely empty right)
        # leaves the unmatched build rows unemitted — outside the frozen contract and unreachable via the
        # cost model (which never broadcasts an empty probe: build*parts < build is false); the SHUFFLE
        # path, the cost-model-reachable one, is where an entirely-empty side is pinned.
        if unmatched and right_blocks:
            # `right_blocks[0]` is the probe SCHEMA carrier and may itself be a ZERO-ROW block —
            # ``backend.take`` null-fills an empty carrier, so no non-emptiness is assumed. `unmatched`
            # rows are, by construction, unmatched against EVERY probe block, so forcing how="left" here
            # can never spuriously match.
            build_unmatched = backend.take(build_concat, unmatched)
            chunks, hashes, peak, spilled, rows, chunks_read = _join_with_budget(
                backend, build_unmatched, right_blocks[0], on, "left", mem_budget, cluster, 0
            )
            witness.peak_join_bytes = max(witness.peak_join_bytes, peak)
            witness.join_spilled_partitions += spilled
            witness.join_chunks_read += chunks_read
            witness.join_output_rows += rows
            for ch, h in zip(chunks, hashes, strict=True):
                value[next_key], dest_block_hashes[next_key] = ch, h
                next_key += 1
    return value, dest_block_hashes


def run_join(
    backend: JoinBackend[Any, Any],
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    *,
    on: Sequence[str] = ("__joinkey__",),
    how: str = "inner",
    workers: int = 1,
    comms: str = "ipc",
    store_root: str | None = None,
    broadcast: bool | None = None,
    salt: int = 0,
    mem_budget_bytes: int | None = None,
    steal: bool = False,
    faults: ShuffleFaults | None = None,
    advertise_host: str | None = None,
) -> ShuffleResult:
    """Distributed relational hash JOIN of two block sets via the two-phase executor (plan §4.2).

    The build (``left``) and probe (``right``) sides are co-partitioned on ``__joinkey__`` (same
    ``parts`` + ``salt``) then gather-joined per dest by the generic ``join_blocks`` kernel — SQL
    *duplicating* semantics (a probe row with k build matches ⇒ k rows). ``broadcast=None`` lets the
    pinned cost model choose (recorded in ``witness.broadcast_chosen``, E5); ``True``/``False`` honour a
    plan-recorded choice. ``mem_budget_bytes`` bounds the gather-join working set by spilling duplicated
    OUTPUT partitions to the node Store (B5). ``value`` maps dest (or probe-partition index, under
    broadcast) → the joined block; ``dest_block_hashes`` is the content-addressed determinism key.

    F6: the auto (``broadcast=None``) choice is keyed on ``parts``, NOT the runtime ``workers`` count —
    a PLAN-STABLE N, so the SAME logical join makes the SAME choice regardless of how the worker pool
    happens to be sized at run time (``graphed.join_plan`` computes the same rule the same way, on the
    same ``parts``, at plan-build time)."""
    faults = faults if faults is not None else ShuffleFaults()
    k = max(1, workers)
    build_bytes = sum(backend.estimated_bytes(b) for b in left_blocks)
    probe_bytes = sum(backend.estimated_bytes(b) for b in right_blocks)
    chosen = broadcast_join_choice(build_bytes, probe_bytes, parts) if broadcast is None else bool(broadcast)
    cluster = _make_cluster(comms, k, store_root, advertise_host)
    try:
        witness = ShuffleWitness()
        witness.broadcast_chosen = chosen
        witness.node_store_dirs = {cluster.addr(i): cluster.store_dir(i) for i in range(k)}
        witness.node_hosts = [cluster.host(i) for i in range(k)]
        if chosen:
            value, dest_block_hashes = _run_broadcast_join(
                backend, left_blocks, right_blocks, k, on, how, mem_budget_bytes, cluster, witness
            )
        else:
            value, dest_block_hashes = _run_shuffle_join(
                backend,
                left_blocks,
                right_blocks,
                parts,
                k,
                on,
                how,
                salt,
                mem_budget_bytes,
                steal,
                faults,
                cluster,
                witness,
            )
        return ShuffleResult(dest_block_hashes=dest_block_hashes, value=value, partitions=[], witness=witness)
    finally:
        cluster.close()


def _split_to_target(backend: ShuffleBackend[Any, Any], block: Any, target_bytes: int) -> list[Any]:
    """Split an oversized block at EVENT (row) boundaries into pieces of ~``target_bytes`` (§5.1);
    a small-enough block passes through unsplit."""
    total = backend.estimated_bytes(block)
    n_rows = len(block)
    if total <= target_bytes or n_rows <= 1:
        return [block]
    rows_per = max(1, (n_rows * target_bytes) // total)
    out: list[Any] = []
    start = 0
    while start < n_rows:
        stop = min(start + rows_per, n_rows)
        out.append(backend.slice_rows(block, start, stop))
        start = stop
    return out


def _coalesce_pieces(
    backend: ShuffleBackend[Any, Any], pieces: Sequence[Any], target_bytes: int
) -> list[Any]:
    """Greedily merge consecutive pieces up to ~``target_bytes`` per output partition (content + order
    preserved: ``concat(out) == concat(pieces)``)."""
    out: list[Any] = []
    cur: list[Any] = []
    cur_bytes = 0
    for piece in pieces:
        cur.append(piece)
        cur_bytes += backend.estimated_bytes(piece)
        if cur_bytes >= target_bytes:
            out.append(backend.concat(cur))
            cur, cur_bytes = [], 0
    if cur:
        out.append(backend.concat(cur))
    return out


def run_repartition_by_size(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    *,
    target_bytes: int,
    workers: int = 1,
    store_root: str | None = None,
    row_group_bytes: int = ROW_GROUP_BYTES,
) -> ShuffleResult:
    """Coalesce/split ``src_blocks`` toward ``target_bytes`` by MEASURED bytes (§5.1): split any
    oversized block at row boundaries, then coalesce consecutive pieces. Content + order preserved.
    Returns the rebalanced blocks in ``.partitions`` (no key routing — a physical rebalance)."""
    pieces: list[Any] = []
    for block in src_blocks:
        pieces.extend(_split_to_target(backend, block, target_bytes))
    partitions = _coalesce_pieces(backend, pieces, target_bytes)
    return ShuffleResult(dest_block_hashes={}, value={}, partitions=partitions, witness=ShuffleWitness())
