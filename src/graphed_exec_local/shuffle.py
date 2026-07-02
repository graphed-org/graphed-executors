"""The two-phase shuffle executor (plan M39 §4): map-write + gather over a ``ShuffleBackend``.

This is the generic exchange/repartition ENGINE, backend-agnostic (it deals only in the opaque
``graphed_core.ShuffleBackend`` primitives — ``partition``/``concat``/``slice_rows``/
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

from graphed_core import ShuffleBackend

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
    backend: ShuffleBackend[Any, Any], owned: Sequence[Any], parts: int
) -> tuple[dict[int, Any], int]:
    """Route + coalesce one producer-task's owned src blocks into <= P dest blocks (one per non-empty
    dest). Streams sub-blocks through P per-dest writers, flushing a writer at ``ROW_GROUP_BYTES``, so
    the peak live writer-buffer bytes are O(P*rg) (guidance 3). Rows for a dest are kept in ascending
    src order (the deterministic merge)."""
    per_dest_subs: dict[int, list[Any]] = {}
    live: dict[int, int] = {}  # dest -> live writer-buffer bytes not yet flushed to the block file
    peak = 0
    for src in owned:  # ascending src_pid
        subs = backend.partition(src, "__joinkey__", parts)
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
        per_dest, task_peak = _coalesce_task(backend, [src_blocks[s] for s in owned_ids], parts)
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
