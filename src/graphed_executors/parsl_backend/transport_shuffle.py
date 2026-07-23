"""m47 (a): the M39-M41 shuffle / relational-join engine hosted on k parsl peers (plan §1.4, IT3).

``transport_run_repartition`` / ``transport_run_join`` seat ``k`` persistent peers (one per worker
slot), each of which — INSIDE its task — maps its owned src range into its endpoint's block store,
announces a manifest to the driver, awaits the driver's aggregated pull assignment, then PULLS its
owned dests' fragments peer→peer over ``/pull`` (coalesced one request per holder, evict-after-serve)
and gathers/joins them. Bytes move peer↔peer over the block plane; only the final dest blocks + the
counters ride the task return (once, like the m44 dask returns). Reader-plane fetch budgets are a
driver-side replay of the imported ``_stage2_gather`` (``common.transport_tasks.replay_reader_plane``,
the SAME code the dask job gates); join budgets are the real per-dest ``_join_with_budget``.

Reuses the driver primitives (barrier / registry / probe / restart) from ``transport_peer``; imports
NOTHING from parsl or ``dask_backend`` (F13 / G9)."""

from __future__ import annotations

import time
import uuid
from typing import Any

from graphed_executors.common.http_plane import PullTimeoutError
from graphed_executors.common.transport_run import (
    TransportShuffleResult,
    TransportWitness,
    is_restart_worthy,
    pick_attributable,
)
from graphed_executors.common.transport_tasks import gather_join_body, map_task_body, replay_reader_plane
from graphed_executors.local.shuffle import ShuffleWitness, _assign, _sha256_hex

from .transport_peer import (
    DRIVER,
    ProbeUnreachable,
    _barrier_and_registry,
    _barrier_stage_error,
    _BarrierTimeout,
    _bump_driver,
    _cleanup_after_barrier,
    _collect_probes,
    _future_errored,
    _merge_peer,
    _open_driver_endpoint,
    _release,
    _resolve_k,
    _sender_counters,
    _worker_addrs,
    sorted_addrs_seen,
)

_MSG_WAIT_S = 120.0  # bound on a peer's assign/done wait (a lost driver message never hangs the task)
_COLLECT_S = 120.0  # bound on the driver's manifest/gathered collection


# ---- the parsl block plane over the peer's http endpoint (self-fragments read locally) -----------
class _HttpBlockPlane:
    """The ``map_task_body`` / ``gather*`` plane over an ``EscalatingHttpTransport``: ``store_block``
    puts into the endpoint's /pull store; ``pull_blocks`` reads SELF-owned fragments locally (never
    over HTTP, so they are not billed to ``bytes_served``) and CROSS fragments over the real /pull
    route (billed at the serving holder)."""

    def __init__(self, ep: Any) -> None:
        self._ep = ep

    def store_block(self, digest: str, wire: bytes, *, to_disk: bool) -> None:
        self._ep.put_block(digest, wire)  # holder retention is RAM-only for m47 (no test wires holder spill)

    def record(self, **counters: int) -> None:
        return None  # the parsl block-plane witnesses live on the endpoint, not the plugin record seam

    def pull_blocks(self, holder_addr: str, digests: Any, *, timeout_s: float) -> list[bytes]:
        if holder_addr == self._ep.address:  # self fragment: local read + evict, never over HTTP
            out: list[bytes] = []
            for digest in digests:
                wire = self._ep.take_local(digest)
                if wire is None:
                    raise PullTimeoutError(f"self-fragment {digest[:8]} missing from the local store")
                out.append(wire)
            return out
        got = self._ep.pull(holder_addr, list(digests), timeout_s=timeout_s)
        return [got[d] for d in digests]


# ---- the peer shuffle body (called INSIDE _parsl_peer_main) --------------------------------------
def _peer_shuffle_body(
    ep: Any,
    spec: dict[str, Any],
    registry: dict[str, tuple[str, int]],
    prebuffered: list[Any],
    base: dict[str, Any],
) -> dict[str, Any]:
    backend = spec["backend"]
    parts = spec["parts"]
    salt = spec["salt"]
    pull_timeout_s = spec.get("pull_timeout_s") or 30.0
    holder_budget = spec.get("holder_budget_bytes")
    plane = _HttpBlockPlane(ep)
    kind = spec["kind"]
    sides = (
        {"src": spec["owned"]}
        if kind == "repartition"
        else {"left": spec["owned_left"], "right": spec["owned_right"]}
    )

    manifest_out: dict[str, dict[int, tuple[str, int]]] = {}
    for name, blocks in sides.items():
        frag = map_task_body(plane, backend, blocks, parts, salt, holder_budget)
        manifest_out[name] = dict(frag.manifest)
    ep.send(DRIVER, ("manifest", ep.address, manifest_out))

    assign = _wait_msg(ep, "assign", prebuffered)  # {dest: {side: [(holder_addr, digest), ...]}}
    if kind == "repartition":
        value, hashes, jw = _gather_repartition(plane, backend, assign, pull_timeout_s)
    else:
        value, hashes, jw = _gather_join(plane, backend, assign, spec, pull_timeout_s)

    ep.send(DRIVER, ("gathered", ep.address))
    _wait_done(ep)
    counters = {
        **base,
        "bytes_served": int(ep.bytes_served),
        "pull_requests_served": int(ep.pull_requests_served),
        "serve_pid": int(ep.serve_pid),
        "store_blocks_at_return": int(ep.store_blocks_count()),
        **_sender_counters(ep),
    }
    return {"addr": ep.address, "value": value, "hashes": hashes, "join_witness": jw, "counters": counters}


def _wait_msg(ep: Any, tag: str, prebuffered: list[Any]) -> Any:
    for i, (_sender, msg) in enumerate(prebuffered):
        if msg[0] == tag:
            prebuffered.pop(i)
            return msg[1]
    deadline = time.monotonic() + _MSG_WAIT_S
    while time.monotonic() < deadline:
        got = ep.recv(timeout=0.2)
        if got is None:
            continue
        sender, msg = got
        if msg[0] == tag:
            return msg[1]
        prebuffered.append((sender, msg))
    raise PullTimeoutError(f"peer waited for {tag!r} but it never arrived")


def _wait_done(ep: Any) -> None:
    deadline = time.monotonic() + _MSG_WAIT_S
    while time.monotonic() < deadline:
        got = ep.recv(timeout=0.2)
        if got is not None and got[1][0] in ("done", "abort"):
            return


def _gather_repartition(
    plane: _HttpBlockPlane, backend: Any, assign: dict[int, dict[str, list[Any]]], pull_timeout_s: float
) -> tuple[dict[int, Any], dict[int, str], dict[str, int]]:
    """Coalesce ALL of this peer's cross-fragment pulls to ONE request per holder (the <= k*k incast
    bound), then reassemble each owned dest in ascending-producer order — byte-identical to local."""
    by_holder: dict[str, list[str]] = {}
    for _dest, per_side in assign.items():
        for holder, digest in per_side["src"]:
            by_holder.setdefault(holder, []).append(digest)
    wires: dict[str, bytes] = {}
    for holder, digests in by_holder.items():
        uniq = list(dict.fromkeys(digests))  # dedup so evict-after-serve never re-reads an evicted digest
        got = plane.pull_blocks(holder, uniq, timeout_s=pull_timeout_s)
        wires.update(zip(uniq, got, strict=True))
    value: dict[int, Any] = {}
    hashes: dict[int, str] = {}
    for dest in sorted(assign):
        blocks = [backend.from_wire(wires[digest]) for _holder, digest in assign[dest]["src"]]
        if not blocks:
            continue
        gathered = backend.concat(blocks)
        value[dest] = gathered
        hashes[dest] = _sha256_hex(bytes(backend.to_wire(gathered)))
    return value, hashes, {}


def _gather_join(
    plane: _HttpBlockPlane,
    backend: Any,
    assign: dict[int, dict[str, list[Any]]],
    spec: dict[str, Any],
    pull_timeout_s: float,
) -> tuple[dict[int, Any], dict[int, str], dict[str, int]]:
    on = tuple(spec["on"])
    how = spec["how"]
    mem_budget = spec.get("mem_budget_bytes")
    left_carrier = spec.get("left_carrier")
    right_carrier = spec.get("right_carrier")
    value: dict[int, Any] = {}
    hashes: dict[int, str] = {}
    peak = spilled = rows = read = 0
    next_key = spec["parts"]  # local placeholder keys; the driver re-keys overflow into ONE global
    #                           sequence at assemble (raw per-peer keys would collide across peers)
    for dest in sorted(assign):
        per_side = assign[dest]
        out = gather_join_body(
            plane,
            backend,
            dest,
            on,
            how,
            mem_budget,
            left_carrier,
            right_carrier,
            per_side.get("left", []),
            per_side.get("right", []),
            pull_timeout_s,
        )
        if out is None:
            continue
        peak = max(peak, out.peak)
        spilled += out.spilled
        rows += out.rows
        read += out.read
        if len(out.chunks) == 1:
            value[dest], hashes[dest] = out.chunks[0], out.hashes[0]
        else:
            for ch, h in zip(out.chunks, out.hashes, strict=True):
                value[next_key], hashes[next_key] = ch, h
                next_key += 1
    return (
        value,
        hashes,
        {
            "peak_join_bytes": peak,
            "join_spilled_partitions": spilled,
            "join_chunks_read": read,
            "join_output_rows": rows,
        },
    )


# ---- the driver ---------------------------------------------------------------------------------
def _build_assignment(
    manifests: dict[str, dict[str, dict[int, tuple[str, int]]]],
    worker_addrs: tuple[str, ...],
    k: int,
    sides: tuple[str, ...],
) -> dict[str, dict[int, dict[str, list[tuple[str, str]]]]]:
    """For each dest d, owner = ``worker_addrs[d % k]``; its per-side pulls are ``[(holder, digest)]``
    in ascending-producer (== ascending-src) order — the frozen deterministic merge."""
    dests: set[int] = set()
    for m in manifests.values():
        for side in sides:
            dests |= set(m.get(side, {}))
    assign: dict[str, dict[int, dict[str, list[tuple[str, str]]]]] = {a: {} for a in worker_addrs}
    for dest in dests:
        owner = worker_addrs[dest % k]
        per_side: dict[str, list[tuple[str, str]]] = {}
        for side in sides:
            pulls = [
                (a, manifests[a][side][dest][0])
                for a in worker_addrs  # ascending producer index
                if a in manifests and dest in manifests[a].get(side, {})
            ]
            if pulls:
                per_side[side] = pulls
        assign[owner][dest] = per_side
    return assign


def _collect_tagged(
    driver_ep: Any, tag: str, k: int, futures: dict[str, Any], driver_counts: dict[str, int], timeout_s: float
) -> tuple[dict[str, Any], bool]:
    """Collect k ``tag`` messages at the driver, returning ``(collected, errored)``. ``errored`` is
    True if a peer future failed mid-collection (restart-worthy — stop waiting for the dead peer)."""
    collected: dict[str, Any] = {}
    deadline = time.monotonic() + timeout_s
    while len(collected) < k and time.monotonic() < deadline:
        got = driver_ep.recv(timeout=0.1)
        if got is not None:
            _sender, msg = got
            if msg[0] == tag:
                collected[msg[1]] = msg[2] if len(msg) > 2 else True
                _bump_driver(driver_counts, f"recv_class_{tag}")
        if any(_future_errored(f) for f in futures.values()):
            return collected, True
    return collected, len(collected) < k


def _harvest(
    futures: dict[str, Any], per_worker: dict[str, dict[str, int]]
) -> tuple[dict[str, Any], dict[str, BaseException]]:
    results: dict[str, Any] = {}
    errs: dict[str, BaseException] = {}
    for addr, fut in futures.items():
        try:
            res = fut.result()
            results[addr] = res
            _merge_peer(per_worker, addr, res.get("counters", {}))
        except BaseException as exc:  # harvest every peer for the restart decision
            errs[addr] = exc
            _merge_peer(per_worker, addr, getattr(exc, "peer_counters", {}) or {})
    return results, errs


def _run_shuffle(
    pbackend: Any,
    *,
    kind: str,
    make_peer_spec: Any,
    n_producer_bound: int,
    sides: tuple[str, ...],
    workers: int | None,
    epoch_restarts_allowed: int,
    barrier_timeout_s: float | None,
    registry_rewrite: Any,
    assemble: Any,
) -> TransportShuffleResult:
    """The shared shuffle driver: seat k peers, rendezvous+probe, aggregate manifests, broadcast the
    pull assignment, collect results, and restart the whole run on a restart-worthy failure."""
    from .transport_peer import _BARRIER_DEFAULT_S  # noqa: PLC0415

    barrier_s = _BARRIER_DEFAULT_S if barrier_timeout_s is None else barrier_timeout_s
    transport = TransportWitness()
    per_worker: dict[str, dict[str, int]] = {}
    last_exc: BaseException | None = None

    for attempt in range(epoch_restarts_allowed + 1):
        nonce = uuid.uuid4().hex
        transport.epoch_nonces.append(nonce)
        k = (
            max(1, min(_resolve_k(pbackend, workers, restart=attempt > 0), n_producer_bound))
            if n_producer_bound
            else max(1, _resolve_k(pbackend, workers, restart=attempt > 0))
        )
        worker_addrs = _worker_addrs(k)
        driver_ep = _open_driver_endpoint(nonce)
        driver_counts: dict[str, int] = {}
        futures = {
            addr: pbackend.submit(
                _parsl_peer_main_ref(),
                make_peer_spec(nonce, driver_ep, worker_addrs, k, i, addr),
                key=f"graphed-parsl-shuffle-{nonce}-{i}",
            )  # ponytail: k persistent tasks gang-schedule by slot saturation (pin_to_worker=False)
            for i, addr in enumerate(worker_addrs)
        }
        try:
            manifests, results = _drive_shuffle_attempt(
                driver_ep,
                pbackend,
                futures,
                k,
                worker_addrs,
                sides,
                barrier_s=barrier_s,
                registry_rewrite=registry_rewrite,
                driver_counts=driver_counts,
                per_worker=per_worker,
            )
        except _BarrierTimeout as bt:
            _cleanup_after_barrier(driver_ep, pbackend, futures, sorted_addrs_seen(driver_ep))
            _merge_peer(per_worker, DRIVER, driver_counts)
            driver_ep.close()
            raise _barrier_stage_error(bt) from bt
        except ProbeUnreachable:
            _merge_peer(per_worker, DRIVER, driver_counts)
            driver_ep.close()
            raise
        except BaseException as exc:  # a restart-worthy shuffle failure
            _merge_peer(per_worker, DRIVER, driver_counts)
            driver_ep.close()
            last_exc = exc
            if not is_restart_worthy(exc, pbackend) or attempt >= epoch_restarts_allowed:
                transport.per_worker = per_worker
                raise _shuffle_stage_error(exc, pbackend) from exc
            continue

        _merge_peer(per_worker, DRIVER, driver_counts)
        driver_ep.close()
        transport.epoch_restarts = attempt
        transport.per_worker = per_worker
        value, hashes, witness = assemble(manifests, results, worker_addrs, k)
        return TransportShuffleResult(
            dest_block_hashes=hashes, value=value, witness=witness, transport=transport
        )

    transport.per_worker = per_worker  # pragma: no cover  (the loop returns or raises)
    raise _shuffle_stage_error(last_exc, pbackend) if last_exc is not None else RuntimeError("no epochs run")


def _shuffle_stage_error(exc: BaseException, pbackend: Any) -> Any:
    """Wrap a run-fatal shuffle failure as an attributed ``StageError`` that NAMES the actual death
    signal (``WorkerLost``/``ManagerLost``/``PullTimeoutError``/``TransportDeliveryError``) — unlike
    the shared ``build_stage_error`` (which renders a described death as ``cause_type='KilledWorker'``,
    losing the parsl signal whose ``str`` never carries its class name), the m47 driver layers the
    victim-worker context on top of the named signal (backend.describe_failure's own note)."""
    from graphed.debug import SourceFrame, StageError  # noqa: PLC0415  (error path; dask/parsl-free)

    describe = getattr(pbackend, "describe_failure", None)
    info = describe(exc) if describe is not None else None
    worker = f" (worker: {info[1]})" if info is not None else ""
    return StageError(
        op="run",
        frames=(SourceFrame(filename="transport-shuffle", lineno=0),),
        input_forms=(),
        partition="transport-shuffle",
        cause_type=type(exc).__name__,
        cause_message=f"{type(exc).__name__}: {exc}{worker}",
        opt_level=0,
    )


def _drive_shuffle_attempt(
    driver_ep: Any,
    pbackend: Any,
    futures: dict[str, Any],
    k: int,
    worker_addrs: tuple[str, ...],
    sides: tuple[str, ...],
    *,
    barrier_s: float,
    registry_rewrite: Any,
    driver_counts: dict[str, int],
    per_worker: dict[str, dict[str, int]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    seated = _barrier_and_registry(
        driver_ep,
        futures,
        k,
        barrier_timeout_s=barrier_s,
        registry_rewrite=registry_rewrite,
        driver_counts=driver_counts,
    )
    failed = _collect_probes(driver_ep, k, driver_counts=driver_counts, timeout_s=_PROBE_COLLECT_S)
    if failed:
        _release(driver_ep, seated, ("abort",))
        _harvest(futures, per_worker)
        raise ProbeUnreachable(failed)
    _release(driver_ep, seated, ("proceed",))

    manifests, errored = _collect_tagged(driver_ep, "manifest", k, futures, driver_counts, _COLLECT_S)
    if errored:
        _release(driver_ep, seated, ("done",))
        _, errs = _harvest(futures, per_worker)
        raise (
            pick_attributable(errs, pbackend)
            if errs
            else PullTimeoutError("a peer did not report its manifest")
        )
    assign = _build_assignment(manifests, worker_addrs, k, sides)
    for addr in seated:
        driver_ep.send(addr, ("assign", assign.get(addr, {})))

    _sig, errored = _collect_tagged(driver_ep, "gathered", k, futures, driver_counts, _COLLECT_S)
    _release(driver_ep, seated, ("done",))
    results, errs = _harvest(futures, per_worker)
    if errs:
        raise pick_attributable(errs, pbackend)
    return manifests, results


_PROBE_COLLECT_S = 15.0


def _parsl_peer_main_ref() -> Any:
    from .transport_peer import _parsl_peer_main  # noqa: PLC0415

    return _parsl_peer_main


# ---- public entry points ------------------------------------------------------------------------
def transport_run_repartition(
    backend: Any,
    src_blocks: Any,
    parts: int,
    *,
    pbackend: Any,
    salt: int = 0,
    workers: int | None = None,
    fetch_budget_bytes: int | None = None,
    pull_timeout_s: float | None = None,
    epoch_restarts_allowed: int = 1,
    barrier_timeout_s: float | None = None,
    registry_rewrite: Any = None,
) -> TransportShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dests over the k-peer transport. Byte-identical
    to the local ``run_repartition``. ``fetch_budget_bytes`` bounds the reader plane (driver-side
    replay); a probe failure raises ``ProbeUnreachable`` (the facade decides error vs fallback)."""
    n_src = len(src_blocks)

    def make_peer_spec(
        nonce: str, driver_ep: Any, worker_addrs: tuple[str, ...], k: int, i: int, addr: str
    ) -> dict[str, Any]:
        t = min(k, n_src) if n_src else 0
        owned = [src_blocks[s] for s in _assign(n_src, t)[i]] if (n_src and i < t) else []
        return {
            "kind": "repartition",
            "addr": addr,
            "idx": i,
            "epoch": nonce,
            "driver_host": driver_ep.host,
            "driver_port": driver_ep.port,
            "worker_addrs": worker_addrs,
            "k": k,
            "backend": backend,
            "parts": parts,
            "salt": salt,
            "owned": owned,
            "pull_timeout_s": pull_timeout_s,
        }

    def assemble(
        manifests: dict[str, Any], results: dict[str, Any], worker_addrs: tuple[str, ...], k: int
    ) -> Any:
        value: dict[int, Any] = {}
        hashes: dict[int, str] = {}
        for res in results.values():
            value.update(res["value"])
            hashes.update(res["hashes"])
        witness = _repartition_witness(manifests, worker_addrs, k, parts, n_src, fetch_budget_bytes)
        return value, hashes, witness

    return _run_shuffle(
        pbackend,
        kind="repartition",
        make_peer_spec=make_peer_spec,
        n_producer_bound=n_src,
        sides=("src",),
        workers=workers,
        epoch_restarts_allowed=epoch_restarts_allowed,
        barrier_timeout_s=barrier_timeout_s,
        registry_rewrite=registry_rewrite,
        assemble=assemble,
    )


def _repartition_witness(
    manifests: dict[str, Any],
    worker_addrs: tuple[str, ...],
    k: int,
    parts: int,
    n_src: int,
    fetch_budget: int | None,
) -> ShuffleWitness:
    """The reader-plane fetch counters via the SAME ``replay_reader_plane`` the dask job gates, over
    the collected manifests (producer i = peer i = holder i)."""
    idx_of = {a: i for i, a in enumerate(worker_addrs)}
    replay: dict[int, dict[int, tuple[str, int]]] = {}
    size_of: dict[str, int] = {}
    for addr, m in manifests.items():
        i = idx_of[addr]
        src_m = m.get("src", {})
        replay[i] = {dest: (digest, i) for dest, (digest, _sz) in src_m.items()}
        for _dest, (digest, sz) in src_m.items():
            size_of[digest] = sz
    t = min(k, n_src) if n_src else 1
    for i in range(t):
        replay.setdefault(i, {})
    return replay_reader_plane(
        replay, parts, t, k, worker_addrs, size_of, fetch_budget=fetch_budget, disk_budget=None
    )


def transport_run_join(
    backend: Any,
    left_blocks: Any,
    right_blocks: Any,
    parts: int,
    *,
    on: Any = ("__joinkey__",),
    how: str = "inner",
    pbackend: Any,
    broadcast: Any = None,
    salt: int = 0,
    mem_budget_bytes: int | None = None,
    workers: int | None = None,
    pull_timeout_s: float | None = None,
    epoch_restarts_allowed: int = 1,
    barrier_timeout_s: float | None = None,
) -> TransportShuffleResult:
    """Distributed relational hash JOIN over the k-peer transport (SHUFFLE join — the peer plane never
    head-node-routes a broadcast build side; ``broadcast`` is accepted but the shuffle path is always
    taken). Rows == the local ``run_join``; ``mem_budget_bytes`` bounds the per-dest ``_join_with_budget``."""
    n_left, n_right = len(left_blocks), len(right_blocks)
    left_carrier = left_blocks[0] if left_blocks else None
    right_carrier = right_blocks[0] if right_blocks else None

    def make_peer_spec(
        nonce: str, driver_ep: Any, worker_addrs: tuple[str, ...], k: int, i: int, addr: str
    ) -> dict[str, Any]:
        t_l = min(k, n_left) if n_left else 0
        t_r = min(k, n_right) if n_right else 0
        owned_left = [left_blocks[s] for s in _assign(n_left, t_l)[i]] if (n_left and i < t_l) else []
        owned_right = [right_blocks[s] for s in _assign(n_right, t_r)[i]] if (n_right and i < t_r) else []
        return {
            "kind": "join",
            "addr": addr,
            "idx": i,
            "epoch": nonce,
            "driver_host": driver_ep.host,
            "driver_port": driver_ep.port,
            "worker_addrs": worker_addrs,
            "k": k,
            "backend": backend,
            "parts": parts,
            "salt": salt,
            "owned_left": owned_left,
            "owned_right": owned_right,
            "on": tuple(on),
            "how": how,
            "mem_budget_bytes": mem_budget_bytes,
            "left_carrier": left_carrier,
            "right_carrier": right_carrier,
            "pull_timeout_s": pull_timeout_s,
        }

    def assemble(
        manifests: dict[str, Any], results: dict[str, Any], worker_addrs: tuple[str, ...], k: int
    ) -> Any:
        value: dict[int, Any] = {}
        hashes: dict[int, str] = {}
        overflow: list[tuple[Any, str]] = []  # spilled sub-partition (block, hash) pairs, driver-rekeyed
        peak = spilled = read = rows = 0
        for addr in worker_addrs:  # deterministic producer order; peers own disjoint base dests
            res = results.get(addr)
            if res is None:
                continue
            res_value, res_hashes = res["value"], res["hashes"]
            for key in sorted(res_value):
                if key < parts:  # a base dest key — globally unique (owner = worker_addrs[dest % k])
                    value[key], hashes[key] = res_value[key], res_hashes[key]
                else:  # a per-peer overflow (spill) key: collect, then rekey into ONE global sequence
                    overflow.append((res_value[key], res_hashes[key]))
            jw = res.get("join_witness") or {}
            peak = max(peak, jw.get("peak_join_bytes", 0))
            spilled += jw.get("join_spilled_partitions", 0)
            read += jw.get("join_chunks_read", 0)
            rows += jw.get("join_output_rows", 0)
        next_key = parts  # spilled sub-partitions get fresh keys beyond the dest range (mirrors local)
        for block, h in overflow:
            value[next_key], hashes[next_key] = block, h
            next_key += 1
        witness = ShuffleWitness(
            n_producer_tasks=max(min(k, n_left) if n_left else 0, min(k, n_right) if n_right else 0),
            broadcast_chosen=False,
            peak_join_bytes=peak,
            join_spilled_partitions=spilled,
            join_chunks_read=read,
            join_output_rows=rows,
            manifest_fetch_is_per_dest=True,
        )
        return value, hashes, witness

    return _run_shuffle(
        pbackend,
        kind="join",
        make_peer_spec=make_peer_spec,
        n_producer_bound=max(n_left, n_right),
        sides=("left", "right"),
        workers=workers,
        epoch_restarts_allowed=epoch_restarts_allowed,
        barrier_timeout_s=barrier_timeout_s,
        registry_rewrite=None,
        assemble=assemble,
    )
