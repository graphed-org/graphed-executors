"""M41 T4: the Phase-2 ClusterExecutor launch seam, a genuine CROSS-PROCESS routable Store sim.

The core CONTRACT (AddressTable/NodeStore/ClusterExecutor) lives in graphed.core.execution; here
the concrete node-local Store + mock launcher satisfy it BEHIND those types (§A.4). Multi-host
launch stays deferred (Phase-2): this is a single-machine simulation — K genuine CHILD processes
(spawn), each serving its Store over a real socket on a ROUTABLE (non-loopback) address,
registering into the core AddressTable. Scope honesty (§3(e)): throughput is measured on this
sim, never a cluster wall-clock SLA.
"""

from __future__ import annotations

import multiprocessing
import os
import urllib.request
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from graphed.core.execution import AddressTable  # the M41 cluster-seam address table (data-only)

from ._shuffle_kernels import ShuffleWitness, sha256_hex
from ._transport import LookupFreeHTTPServer, select_advertise_host


def _store_server_handler(
    objects_dir: Path, served_pid: int, served_host: str
) -> type[BaseHTTPRequestHandler]:
    """Return the node-local Store's bulk data-plane endpoints (§4.3).

    ``GET /blob/{hash}`` + ``PUT /blob`` (content-addressed). A GET response carries
    ``X-Served-Pid``/``X-Served-Host`` so the consumer witnesses which CHILD (not the driver
    thread) served the fetch, and over what routable host.
    """

    class _StoreHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # stdlib http.server handler name — GET /blob/{hash}
            path = objects_dir / self.path.rsplit("/", 1)[-1]
            if not path.is_file():
                self.send_response(404)
                self.end_headers()
                return
            data = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("X-Served-Pid", str(served_pid))
            self.send_header("X-Served-Host", served_host)
            self.end_headers()
            self.wfile.write(data)

        def do_PUT(self) -> None:  # PUT /blob -> content-addressed store; returns the stored hash
            n = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(n) if n else b""
            digest = sha256_hex(body)
            (objects_dir / digest).write_bytes(body)
            self.send_response(200)
            self.send_header("Content-Length", str(len(digest)))
            self.end_headers()
            self.wfile.write(digest.encode())

        def log_message(self, *args: Any) -> None:  # silence the access log
            pass

    return _StoreHandler


def _routable_store_child(
    node_id: int, store_root: str, advertise_host: str, ready_q: Any
) -> None:
    """Run a spawned CHILD (its own PID).

    Stand up a node-local Store served over a real socket on the ROUTABLE ``advertise_host``,
    then announce ``(node_id, host, port, os.getpid())`` back to the driver so the launcher
    registers the address with the child's pid as provenance.
    """
    store_dir = Path(store_root) / f"node-{node_id}"
    objects = store_dir / "objects"
    objects.mkdir(parents=True, exist_ok=True)
    # ValueError on loopback/0.0.0.0 — never bind non-routable
    host = select_advertise_host(advertise_host)
    server = LookupFreeHTTPServer((host, 0), _store_server_handler(objects, os.getpid(), host))
    ready_q.put(
        (node_id, str(server.server_address[0]), int(server.server_address[1]), os.getpid())
    )
    server.serve_forever()


class _RoutableNodeStore:
    """A concrete :class:`graphed.core.execution.NodeStore`.

    ``fetch(digest)`` GETs a blob from a routable CHILD and STREAMS it (chunked,
    byte-backpressured — never the whole blob resident) into the driver-side consumer Store on
    disk. **Idempotent**: a re-fetch of a digest already in the consumer Store returns it
    without moving bytes again (``bytes_transferred`` unchanged).
    """

    def __init__(
        self, host: str, port: int, consumer_objects: Path, witness: ShuffleWitness
    ) -> None:
        self._host = host
        self._port = port
        self._consumer = consumer_objects
        self._witness = witness

    def fetch(self, digest: str) -> bytes:
        target = self._consumer / digest
        if target.is_file():  # idempotent: already streamed in — no new bytes move
            return target.read_bytes()
        url = f"http://{self._host}:{self._port}/blob/{digest}"
        moved = 0
        with urllib.request.urlopen(url, timeout=30) as resp:
            served_pid = int(resp.headers.get("X-Served-Pid", "0"))
            served_host = str(resp.headers.get("X-Served-Host", ""))
            with open(target, "wb") as out:
                while True:
                    chunk = resp.read(1 << 16)  # 64 KiB — bound the reader buffer, stream to disk
                    if not chunk:
                        break
                    out.write(chunk)
                    moved += len(chunk)
        # UNIQUE payload bytes (idempotent re-fetch adds 0)
        self._witness.bytes_transferred += moved
        self._witness.bulk_fetch_count += 1
        self._witness.served_pid = served_pid
        self._witness.served_host = served_host
        return target.read_bytes()


class RoutableClusterSim:
    """Handle over a launched cross-process routable-Store cluster (the (c.2)/(e) frozen API).

    ``put`` ships a blob to a child's Store (PUT /blob); ``fetch`` pulls it back THROUGH the
    core ``NodeStore.fetch`` seam, streaming into the consumer Store. ``close`` tears the
    children down cleanly (no leaked processes/ports).
    """

    def __init__(
        self,
        procs: list[Any],
        address_table: AddressTable,
        node_ids: tuple[int, ...],
        child_pids: tuple[int, ...],
        consumer_dir: Path,
        witness: ShuffleWitness,
        stores: dict[int, _RoutableNodeStore],
    ) -> None:
        """Hold the launched nodes' address table, pids and Stores."""
        self.driver_pid = os.getpid()
        self.address_table = address_table
        self.node_ids = node_ids
        self.child_pids = child_pids
        self.consumer_store_dir = str(consumer_dir)
        self.witness = witness
        self._procs = procs
        self._stores = stores

    def put(self, node_id: int, data: bytes) -> str:
        """PUT /blob to the CHILD hosting ``node_id`` -> the content hash it stored under."""
        host, port = self.address_table.lookup(node_id)
        req = urllib.request.Request(f"http://{host}:{port}/blob", data=data, method="PUT")
        with urllib.request.urlopen(req, timeout=30) as resp:
            digest: bytes = resp.read()
        return digest.decode()

    def fetch(self, node_id: int, digest: str) -> bytes:
        """GET /blob from ``node_id`` through the core ``NodeStore.fetch`` seam (idempotent)."""
        return self._stores[node_id].fetch(digest)

    def close(self) -> None:
        """Terminate and join the node processes."""
        for p in self._procs:
            p.terminate()
        for p in self._procs:
            p.join(timeout=5)


def launch_routable_cluster(
    n_nodes: int, *, store_root: str, advertise_host: str
) -> RoutableClusterSim:
    """Mock Phase-2 launcher.

    Spawn ``n_nodes`` CHILD processes (multiprocessing 'spawn'), each serving a node-local
    Store on the ROUTABLE ``advertise_host``, and register each announced ``(host, port)`` —
    tagged with the CHILD's pid as provenance — into the core :class:`AddressTable`. Returns a
    :class:`RoutableClusterSim` whose ``fetch`` streams blobs through the core
    ``NodeStore.fetch`` seam into a driver-side consumer Store. Multi-host launch stays
    deferred — this is a single-machine sim.
    """
    host = select_advertise_host(advertise_host)  # reject loopback/wildcard up front
    ctx = multiprocessing.get_context("spawn")
    ready_q: Any = ctx.Queue()
    root = Path(store_root)
    consumer_objects = root / "consumer" / "objects"
    consumer_objects.mkdir(parents=True, exist_ok=True)
    procs: list[Any] = []
    for nid in range(n_nodes):
        p = ctx.Process(
            target=_routable_store_child, args=(nid, str(root), host, ready_q), daemon=True
        )
        p.start()
        procs.append(p)
    address_table = AddressTable()
    witness = ShuffleWitness()
    child_pids: list[int] = []
    stores: dict[int, _RoutableNodeStore] = {}
    for _ in range(n_nodes):
        # each child announces its routable (host,port,pid)
        nid, bhost, bport, pid = ready_q.get(timeout=60)
        address_table.register(nid, bhost, bport, registered_by=pid)
        child_pids.append(pid)
        stores[nid] = _RoutableNodeStore(bhost, bport, consumer_objects, witness)
    return RoutableClusterSim(
        procs,
        address_table,
        tuple(range(n_nodes)),
        tuple(child_pids),
        root / "consumer",
        witness,
        stores,
    )
