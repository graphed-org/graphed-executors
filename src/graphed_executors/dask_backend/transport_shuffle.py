"""m44 (d): the M39-M41 shuffle / relational-join engine hosted on dask workers over the worker
transport (m44-transport-plan §1.4, Impl Target 5). An **O(T+P) scheduler graph** — exactly ``T``
strict-pinned ``_transport_map_task`` producers + ``P`` strict-pinned ``_transport_gather_task`` (or
``_transport_gather_join``) consumers + a CONSTANT control tail, with NO ``T*P`` pick tier: the map
tasks register their per-dest wires in the holder's plugin block store and the gather tasks PULL
their dest's fragments worker→worker over ``graphed_block_pull`` (the pull-model data plane), so the
bulk bytes never ride a task return or the driver.

The compute BODIES + the reader-plane sizing replay MOVED to
:mod:`graphed_executors.common.transport_tasks` (plan §1.6 m47) so the parsl peer-exchange engine
runs the SAME bodies. This module keeps the O(T+P) DRIVER (submit/barrier/restart) + the four
module-level **wrapper** task fns with the exact submit names the m45 dispatch spy records
(``_transport_map_task`` / ``_transport_gather_task`` / ``_transport_gather_join`` /
``_transport_broadcast_join_part``), each binding a dask block plane
(``_get_plugin``/``pull_blocks``, deferred so this module imports on the dask-free main matrix, F13).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any

from graphed.shuffle import broadcast_join_choice

from graphed_executors.common.transport_tasks import (
    _GatherOut,
    _JoinOut,
    _MapFrag,
    broadcast_join_part_body,
    gather_join_body,
    gather_task_body,
    map_task_body,
    replay_reader_plane,
)
from graphed_executors.local.shuffle import ShuffleWitness, _assign, _sha256_hex

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


def _skey(epoch: str, *parts: object) -> str:
    """A per-epoch, collision-proof task key: the ``graphed-`` prefix cannot collide with a user
    string arg (dask/dask#9969) and the fresh epoch nonce re-executes across runs / restarts."""
    return "graphed-transport-" + epoch + "-" + "-".join(str(p) for p in parts)


# ---- the dask block plane the wrapper task fns bind (deferred distributed-touching imports) -------
class _DaskBlockPlane:
    """The block plane over the dask worker plugin: ``store_block``/``record`` on the holder side,
    ``pull_blocks`` worker→worker on the puller side. Constructed IN-TASK so the distributed-touching
    imports stay deferred (F13). ``plugin`` is ``None`` for a pull-only (gather) task."""

    def __init__(self, plugin: Any, epoch: str) -> None:
        self._plugin = plugin
        self._epoch = epoch

    def store_block(self, digest: str, wire: bytes, *, to_disk: bool) -> None:
        self._plugin.store_block(self._epoch, digest, wire, to_disk=to_disk)

    def record(self, **counters: int) -> None:
        self._plugin.record(self._epoch, **counters)

    def pull_blocks(self, holder_addr: str, digests: Sequence[str], *, timeout_s: float) -> list[bytes]:
        from .transport import pull_blocks  # noqa: PLC0415  (deferred: distributed-touching)

        return list(pull_blocks(self._epoch, holder_addr, digests, timeout_s=timeout_s))


# ---- module-level spawn-safe wrapper task fns (the m45 spy pins these __name__s) ------------------
def _transport_map_task(
    spec: Any, backend: Any, owned_blocks: Sequence[Any], parts: int, salt: int, holder_budget: int | None
) -> _MapFrag:
    from .transport import _get_plugin  # noqa: PLC0415  (deferred: distributed-touching)

    plugin = _get_plugin()
    plugin.ensure_run(spec)
    return map_task_body(
        _DaskBlockPlane(plugin, spec.epoch), backend, owned_blocks, parts, salt, holder_budget
    )


def _transport_gather_task(
    spec: Any, backend: Any, dest: int, pulls: Sequence[tuple[str, str]]
) -> _GatherOut | None:
    return gather_task_body(_DaskBlockPlane(None, spec.epoch), backend, dest, pulls, spec.pull_timeout_s)


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
    return gather_join_body(
        _DaskBlockPlane(None, spec.epoch),
        backend,
        dest,
        on,
        how,
        mem_budget,
        left_carrier,
        right_carrier,
        left_pulls,
        right_pulls,
        spec.pull_timeout_s,
    )


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
    return broadcast_join_part_body(
        backend, build_wire, probe_block, on, how, mem_budget, track_unmatched, take_indices
    )


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
        witness = replay_reader_plane(
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
