"""The two-phase shuffle executor (plan M39 §4): map-write + gather over a ``ShuffleBackend``.

This is the generic exchange/repartition ENGINE, backend-agnostic (it deals only in the opaque
``graphed.core.ShuffleBackend`` primitives — ``partition``/``concat``/``slice_rows``/
``estimated_bytes``/``to_wire``/``from_wire``), so its correctness is witnessed by EXECUTION over
BOTH real backends (the a-BI theme), not merely a §A.4 import lint.

Design (plan §4.0-§4.3, §7):

- **Producer-task granularity** ``T ~ W`` (one producer-task per worker, each coalescing several
  ``src_pid``s), not ``T = N`` src_pids — so stage-1 emits at most **P blocks per producer-task**
  (``O(T*P)``), never the ``O(N*P)`` per-src_pid MxR fragment blowup (§4.0/B, the anti-MxR gate).
- **Coalescing writer** streams sub-blocks into P open per-dest writers, flushing a row-group when
  a writer reaches ``ROW_GROUP_BYTES``, so stage-1 writer memory is **O(P*rg)** (guidance 3), not
  O(total shuffled bytes). ``rg`` is the documented ``ROW_GROUP_BYTES`` knob.
- **Deterministic ascending-``src_pid`` merge** (§4.0): producer-tasks own contiguous ascending
  ``src_pid`` ranges and the gather concatenates them in ascending task order, so a dest's rows
  assemble in a fixed order regardless of arrival — the content-addressed ``dest_block_hashes`` are
  therefore byte-identical across two fuzzed-arrival runs (§7.1 determinism gate).
- **Announcement-independent completeness** (§4.2.1/B1): the gather derives the input set from the
  plan's producer-task set + per-producer-task manifests (a **per-dest GET**, ``O(N*P)`` metadata,
  guidance 1), NOT from pushed announcements — which are demoted to droppable/duplicable hints.
- **Steal + reliable ship-back** (§4.3/NB1,NB2): a stolen producer-task's blocks stay on the THIEF;
  only the tiny manifest is pushed to ``owner(task)`` (retry-until-ack, dedup), and the gather
  dials ``owner(task)`` for the manifest then pulls the block from the thief.

Cluster-sim scope (§6.5): ``comms="ipc"`` runs the K logical nodes in-process (the default, the
fast path every theme but the cross-process one uses); ``comms="http"`` gives each node its OWN
node-local Store and a real HTTP block server bound to a ROUTABLE (non-loopback) address, so blocks
move between distinct node Stores over a real socket on a dialable IP. This is a single-machine
cluster *simulation* of the transport seam — see ``CLAUDE.md`` for the cluster-correct-seam scope.
"""

from __future__ import annotations

import os
import tempfile
import threading
import urllib.request
from collections.abc import Sequence
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from graphed.core import ShuffleBackend
from graphed.core.execution import JoinBackend  # the join-half protocol (match_indices/take), M40

# the pinned cost rule (F6: graphed.shuffle is the single source of truth)
from graphed.shuffle import broadcast_join_choice

from ._routable_store import RoutableClusterSim, launch_routable_cluster
from ._shuffle_kernels import (
    BROADCAST_PER_BLOCK_HOW,
    MANIFEST_ENTRY_BYTES,
    PINNED_ROUTING_HASH,
    ROW_GROUP_BYTES,
    JoinTally,
    ShuffleFaults,
    ShuffleResult,
    ShuffleWitness,
    assign,
    choose_broadcast,
    coalesce_task,
    join_dest,
    join_probe_block,
    join_with_budget,
    routing_hash_measurement,
    sha256_hex,
    stage2_gather,
)
from ._transport import LookupFreeHTTPServer, select_advertise_host

__all__ = [
    "PINNED_ROUTING_HASH",
    "ROW_GROUP_BYTES",
    "RoutableClusterSim",
    "ShuffleFaults",
    "ShuffleResult",
    "ShuffleWitness",
    "broadcast_join_choice",
    "launch_routable_cluster",
    "routing_hash_measurement",
    "run_join",
    "run_repartition",
    "run_repartition_by_size",
]


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
    """K logical nodes in-process (the default): each has its own block dict + Store dir.

    Fetch is a local lookup. The fast path for every theme except the cross-process
    cluster-sim.
    """

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

    def evict(self, i: int, digest: str) -> None:
        """Drop a consumed block from the in-RAM node store.

        Each repartition-gather block is fetched exactly once, so evicting after the fetch
        keeps the reader plane from holding the producer store AND the gathered dest
        resident at once.
        """
        self._blocks[i].pop(digest, None)

    def close(self) -> None:
        pass


class _HttpNode:
    """One cluster-sim node.

    An isolated block Store served by a real HTTP GET server bound to a routable address (so a
    peer genuinely dials it over a socket).
    """

    def __init__(self, host: str, store_dir: Path) -> None:
        self.blocks: dict[str, bytes] = {}
        self.dir = store_dir
        (store_dir / "objects").mkdir(parents=True, exist_ok=True)
        self._server = LookupFreeHTTPServer((host, 0), _make_block_handler(self.blocks))
        self.host = str(self._server.server_address[0])
        self.port = int(self._server.server_address[1])
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


class _HttpCluster:
    """K nodes each with its OWN Store dir and a real HTTP block server on a ROUTABLE host (§6.2).

    A cross-node fetch is a genuine HTTP GET over that socket. A single-machine cluster
    simulation of the transport seam (in-process threads, real routable sockets) — see the §6.5
    CLAUDE.md scope.
    """

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

    def evict(self, i: int, digest: str) -> None:
        """Drop a consumed block from a node's block Store (see :meth:`_IpcCluster.evict`)."""
        self._nodes[i].blocks.pop(digest, None)

    def close(self) -> None:
        for node in self._nodes:
            node.close()


def _make_cluster(
    comms: str, k: int, store_root: str | os.PathLike[str] | None, advertise_host: str | None
) -> Any:
    root = (
        Path(store_root)
        if store_root is not None
        else Path(tempfile.mkdtemp(prefix="gx-shuffle-"))
    )
    if comms == "ipc":
        return _IpcCluster(k, root)
    if comms == "http":
        return _HttpCluster(k, root, advertise_host)
    raise ValueError(f"unknown comms {comms!r} (expected 'ipc' or 'http')")


# ---- stage 1: coalescing map-write --------------------------------------------------------------
def _reliable_manifest_push(faults: ShuffleFaults, witness: ShuffleWitness) -> None:
    """Push a stolen producer-task's manifest thief->owner with retry-until-ack.

    Drop the first ``manifest_push_drops`` attempts, then ack (attempts > acks witnesses the
    redelivery; §4.3/NB2).
    """
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
    assignment = assign(len(src_blocks), n_tasks)
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
        # a stolen task runs on (and leaves its blocks on) the thief
        exec_i = stolen.get(t, owner_i)
        per_dest, task_peak = coalesce_task(
            backend, [src_blocks[s] for s in owned_ids], parts, salt
        )
        peak = max(peak, task_peak)

        manifest: dict[int, tuple[str, int]] = {}
        block_hashes: set[str] = set()
        for dest in sorted(per_dest):
            wire = backend.to_wire(per_dest[dest])
            digest = sha256_hex(wire)
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


# ---- public entry points ------------------------------------------------------------------------
def run_repartition(
    backend: ShuffleBackend[Any, Any],
    src_blocks: Sequence[Any],
    parts: int,
    *,
    workers: int = 1,
    comms: str = "ipc",
    store_root: str | os.PathLike[str] | None = None,
    steal: bool = False,
    faults: ShuffleFaults | None = None,
    advertise_host: str | None = None,
    fetch_budget_bytes: int | None = None,
    disk_budget_bytes: int | None = None,
) -> ShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dest partitions via the two-phase executor.

    See the module docstring for the coalescing / determinism / announcement-independence /
    steal guarantees. ``comms="http"`` runs the routable cross-node cluster-sim.

    M41 reader-plane knobs (both default off, so M39/M40 behaviour is unchanged):
    ``fetch_budget_bytes`` caps the RAM the gather holds — over it, fetched wires stream-spill to a
    node-local Store on disk (:func:`stage2_gather`); ``disk_budget_bytes`` caps each node's
    Store, redistributing the spill across nodes under skew (T3).
    """
    faults = faults if faults is not None else ShuffleFaults()
    n_src = len(src_blocks)
    k = max(1, workers)
    n_tasks = min(k, n_src) if n_src else 1
    cluster = _make_cluster(comms, k, store_root, advertise_host)
    try:
        witness = ShuffleWitness(n_producer_tasks=n_tasks)
        witness.node_store_dirs = {cluster.addr(i): cluster.store_dir(i) for i in range(k)}
        witness.node_hosts = [cluster.host(i) for i in range(k)]
        manifests = _stage1_map_write(
            backend, src_blocks, parts, n_tasks, k, faults, steal, cluster, witness
        )
        value, dest_block_hashes = stage2_gather(
            backend,
            parts,
            n_tasks,
            k,
            manifests,
            cluster,
            witness,
            fetch_budget_bytes=fetch_budget_bytes,
            disk_budget_bytes=disk_budget_bytes,
        )
        return ShuffleResult(
            dest_block_hashes=dest_block_hashes, value=value, partitions=[], witness=witness
        )
    finally:
        cluster.close()


# ---- M40: distributed relational JOIN over the two-phase substrate ------------------------------
# broadcast_join_choice moved to graphed.shuffle (F6: the frontend, where join_plan freezes the
# choice into the durable plan, is the single source of truth); imported above and re-exported from
# here.
def _gather_side(
    backend: ShuffleBackend[Any, Any],
    manifests: dict[int, dict[int, tuple[str, int]]],
    dest: int,
    n_tasks: int,
    gather_i: int,
    cluster: Any,
    witness: ShuffleWitness,
) -> list[Any]:
    """Pull one side's blocks for ``dest`` in ascending-task (== ascending-src_pid) order.

    The deterministic merge — from each block's holder (the thief, if its task was stolen).
    """
    blocks: list[Any] = []
    for t in range(n_tasks):
        entry = manifests[t].get(dest)
        if entry is None:
            continue
        digest, holder_i = entry
        witness.manifest_bytes += MANIFEST_ENTRY_BYTES
        wire = cluster.get(holder_i, digest)
        if holder_i != gather_i:
            witness.cross_node_fetches += 1
        blocks.append(backend.from_wire(wire))
    return blocks


def _record_join(witness: ShuffleWitness, tally: JoinTally) -> None:
    for name, value in tally.counters().items():
        setattr(witness, name, value)


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
    """Shuffle both sides on ``__joinkey__`` (same ``parts`` + ``salt`` ⇒ co-partitioned).

    Then gather the two sides per dest in ascending-task order and join them (spilling under
    ``mem_budget``), one-sided dests included (:func:`join_dest`, F1).
    """
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

    carriers = (left_blocks[0] if left_blocks else None, right_blocks[0] if right_blocks else None)
    tally = JoinTally(next_key=parts)
    for dest in range(parts):
        gather_i = dest % k
        build_side = _gather_side(
            backend, left_manifests, dest, n_tasks_l, gather_i, cluster, witness
        )
        probe_side = _gather_side(
            backend, right_manifests, dest, n_tasks_r, gather_i, cluster, witness
        )
        joined = join_dest(
            backend, build_side, probe_side, on, how, mem_budget, carriers, cluster, gather_i
        )
        if joined is not None:
            tally.add(dest, joined)
    _record_join(witness, tally)
    return tally.value, tally.hashes


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
    """Replicate the small build (left) side to every node.

    Then join each probe (right) partition against the whole build — the large side is NEVER
    shuffled (``large_side_blocks == 0``). ``value`` is keyed by probe-partition index; the join
    RESULT (union over blocks) equals the shuffle join.

    F2: joining each probe block independently against the WHOLE build side would re-emit an
    unmatched build row once per block (N-fold). So for how=left/outer, each block is joined with
    the per-block how narrowed to "inner" (matched pairs only — never a per-block build-only fill)
    while the build indices matched in ANY block are tracked; AFTER all blocks, the build rows that
    never matched anywhere are emitted EXACTLY ONCE as null-probe rows. how=right/outer needs no
    such tracking — a probe row belongs to exactly one block, so its unmatched survivor is already
    exactly-once.
    """
    if not left_blocks:
        return {}, {}
    build_concat = backend.concat(list(left_blocks))
    build_wire = backend.to_wire(build_concat)
    build_digest = sha256_hex(build_wire)
    for i in range(k):
        cluster.put(i, build_digest, build_wire)
        witness.broadcast_puts += 1
    witness.large_side_blocks = 0
    witness.n_producer_tasks = max(1, min(k, len(right_blocks))) if right_blocks else 1

    per_block_how = BROADCAST_PER_BLOCK_HOW[how]
    track_unmatched = how in ("left", "outer")
    tally = JoinTally(next_key=len(right_blocks))
    for pidx, probe_block in enumerate(right_blocks):
        node_i = pidx % k
        build_here = backend.from_wire(cluster.get(node_i, build_digest))
        if node_i != 0:
            witness.cross_node_fetches += 1
        matched, joined = join_probe_block(
            backend,
            build_here,
            probe_block,
            on,
            per_block_how,
            mem_budget,
            cluster,
            node_i,
            track_unmatched=track_unmatched,
        )
        tally.matched.update(matched or ())
        tally.add(pidx, joined)

    unmatched = tally.unmatched(len(build_concat)) if track_unmatched else ()
    # ponytail: a probe-less broadcast run (a forced ``broadcast=True`` on an entirely empty
    # right) leaves the unmatched build rows unemitted — outside the frozen contract and
    # unreachable via the cost model (which never broadcasts an empty probe: build*parts <
    # build is false); the SHUFFLE path, the cost-model-reachable one, is where an
    # entirely-empty side is pinned.
    if unmatched and right_blocks:
        # `right_blocks[0]` is the probe SCHEMA carrier (a zero-row one is null-filled by
        # ``backend.take``); `unmatched` rows match no probe block, so how="left" cannot match.
        build_unmatched = backend.take(build_concat, list(unmatched))
        joined = join_with_budget(
            backend, build_unmatched, right_blocks[0], on, "left", mem_budget, cluster, 0
        )
        tally.add(None, joined)
    _record_join(witness, tally)
    return tally.value, tally.hashes


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
    store_root: str | os.PathLike[str] | None = None,
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
    *duplicating* semantics (a probe row with k build matches ⇒ k rows). ``broadcast=None`` lets
    the pinned cost model choose (recorded in ``witness.broadcast_chosen``, E5); ``True``/``False``
    honour a plan-recorded choice. ``mem_budget_bytes`` bounds the gather-join working set by
    spilling duplicated OUTPUT partitions to the node Store (B5). ``value`` maps dest (or
    probe-partition index, under broadcast) → the joined block; ``dest_block_hashes`` is the
    content-addressed determinism key.

    F6: the auto (``broadcast=None``) choice is keyed on ``parts``, NOT the runtime ``workers``
    count — a PLAN-STABLE N, so the SAME logical join makes the SAME choice regardless of how the
    worker pool happens to be sized at run time. (``graphed.join_plan`` applies the same
    ``parts``-keyed cost rule at plan-build time, but on partition COUNTS — the only size it has
    before any data is read — whereas here the rule sees measured BYTES; the two can differ when
    per-partition size is skewed.)
    """
    faults = faults if faults is not None else ShuffleFaults()
    k = max(1, workers)
    chosen = choose_broadcast(backend, left_blocks, right_blocks, parts, broadcast=broadcast)
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
        return ShuffleResult(
            dest_block_hashes=dest_block_hashes, value=value, partitions=[], witness=witness
        )
    finally:
        cluster.close()


def _split_to_target(
    backend: ShuffleBackend[Any, Any], block: Any, target_bytes: int
) -> list[Any]:
    """Split an oversized block at EVENT (row) boundaries into pieces of ~``target_bytes`` (§5.1).

    A small-enough block passes through unsplit.
    """
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
    """Greedily merge consecutive pieces up to ~``target_bytes`` per output partition.

    Content + order preserved: ``concat(out) == concat(pieces)``.
    """
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
    store_root: str | os.PathLike[str] | None = None,
    row_group_bytes: int = ROW_GROUP_BYTES,
) -> ShuffleResult:
    """Coalesce/split ``src_blocks`` toward ``target_bytes`` by MEASURED bytes (§5.1).

    Split any oversized block at row boundaries, then coalesce consecutive pieces. Content +
    order preserved. Returns the rebalanced blocks in ``.partitions`` (no key routing — a
    physical rebalance).
    """
    pieces: list[Any] = []
    for block in src_blocks:
        pieces.extend(_split_to_target(backend, block, target_bytes))
    partitions = _coalesce_pieces(backend, pieces, target_bytes)
    return ShuffleResult(
        dest_block_hashes={}, value={}, partitions=partitions, witness=ShuffleWitness()
    )
