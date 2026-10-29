"""m44 (d): the M39-M41 shuffle / relational-join engine hosted on dask workers.

Over the worker transport (m44-transport-plan §1.4, Impl Target 5). An **O(T+P) scheduler graph** —
exactly ``T`` strict-pinned ``_transport_map_task`` producers + ``P`` strict-pinned
``_transport_gather_task`` (or ``_transport_gather_join``) consumers + a CONSTANT control tail,
with NO ``T*P`` pick tier: the map tasks register their per-dest wires in the holder's plugin block
store and the gather tasks PULL their dest's fragments worker→worker over ``graphed_block_pull``
(the pull-model data plane), so the bulk bytes never ride a task return or the driver.

The compute BODIES + the reader-plane sizing replay MOVED to
:mod:`graphed_executors.common.transport_tasks` (plan §1.6 m47) so the parsl peer-exchange engine
runs the SAME bodies. This module keeps the O(T+P) DRIVER (submit/barrier/restart) + the four
module-level **wrapper** task fns with the exact submit names the m45 dispatch spy records
(``_transport_map_task`` / ``_transport_gather_task`` / ``_transport_gather_join`` /
``_transport_broadcast_join_part``), each binding a dask block plane
(``_get_plugin``/``pull_blocks``, deferred so this module imports on the dask-free main matrix,
F13).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from graphed_executors.common.transport_run import harvest, run_epochs
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
from graphed_executors.local._shuffle_kernels import (
    BROADCAST_PER_BLOCK_HOW,
    JoinTally,
    ShuffleWitness,
    assign,
    choose_broadcast,
    sha256_hex,
)

from ._transport_run import (
    TransportShuffleResult,
    TransportWitness,
    collect_and_purge,
    pick_attributable,
    require_pin,
    sorted_addresses,
)


def _skey(epoch: str, *parts: object) -> str:
    """Return a per-epoch, collision-proof task key.

    The ``graphed-`` prefix cannot collide with a user string arg (dask/dask#9969) and the
    fresh epoch nonce re-executes across runs / restarts.
    """
    return "graphed-transport-" + epoch + "-" + "-".join(str(p) for p in parts)


# ---- the dask block plane the wrapper task fns bind (deferred distributed-touching imports) -----
class _DaskBlockPlane:
    """The block plane over the dask worker plugin.

    ``store_block``/``record`` on the holder side, ``pull_blocks`` worker→worker on the puller
    side. Constructed IN-TASK so the distributed-touching imports stay deferred (F13).
    ``plugin`` is ``None`` for a pull-only (gather) task.
    """

    def __init__(self, plugin: Any, epoch: str) -> None:
        self._plugin = plugin
        self._epoch = epoch

    def store_block(self, digest: str, wire: bytes, *, to_disk: bool) -> None:
        self._plugin.store_block(self._epoch, digest, wire, to_disk=to_disk)

    def record(self, **counters: int) -> None:
        self._plugin.record(self._epoch, **counters)

    def pull_blocks(
        self, holder_addr: str, digests: Sequence[str], *, timeout_s: float
    ) -> list[bytes]:
        from .transport import pull_blocks

        return list(pull_blocks(self._epoch, holder_addr, digests, timeout_s=timeout_s))


# ---- module-level spawn-safe wrapper task fns (the m45 spy pins these __name__s) ----------------
def _transport_map_task(
    spec: Any,
    backend: Any,
    owned_blocks: Sequence[Any],
    parts: int,
    salt: int,
    holder_budget: int | None,
) -> _MapFrag:
    from .transport import _get_plugin

    plugin = _get_plugin()
    plugin.ensure_run(spec)
    return map_task_body(
        _DaskBlockPlane(plugin, spec.epoch), backend, owned_blocks, parts, salt, holder_budget
    )


def _transport_gather_task(
    spec: Any, backend: Any, dest: int, pulls: Sequence[tuple[str, str]]
) -> _GatherOut | None:
    return gather_task_body(
        _DaskBlockPlane(None, spec.epoch), backend, dest, pulls, spec.pull_timeout_s
    )


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


@dataclass(frozen=True)
class _Attempt:
    """One epoch's submit context: every task strict-pinned to a worker, never retried by dask."""

    dbackend: Any
    spec: Any
    backend: Any
    addresses: Sequence[str]
    k: int

    def submit(self, fn: Any, *args: Any, key: tuple[object, ...], worker: int) -> Any:
        return self.dbackend.submit(
            fn,
            *args,
            key=_skey(self.spec.epoch, *key),
            workers=[self.addresses[worker % self.k]],
            retries=0,
        )

    def maps(
        self,
        blocks: Sequence[Any],
        parts: int,
        salt: int,
        holder_budget: int | None,
        tag: str,
        t: int,
    ) -> list[Any]:
        """Submit ``t`` producers over contiguous ascending ranges of ``blocks`` (none if 0)."""
        return [
            self.submit(
                _transport_map_task,
                self.spec,
                self.backend,
                [blocks[s] for s in owned],
                parts,
                salt,
                holder_budget,
                key=(tag, i),
                worker=i,
            )
            for i, owned in enumerate(assign(len(blocks), t) if t else [])
        ]

    def pulls(
        self, manifests: dict[int, dict[int, tuple[str, int]]], dest: int
    ) -> list[tuple[str, str]]:
        """``dest``'s ``(holder address, digest)`` pulls in ascending-producer order."""
        return [
            (self.addresses[t % self.k], m[dest][0]) for t, m in manifests.items() if dest in m
        ]

    def results(self, futures: dict[int, Any]) -> dict[int, Any]:
        """Resolve every future; any failure raises the most attributable one."""
        results, errs = harvest(futures)
        if errs:
            raise pick_attributable(errs, self.dbackend)
        return results


def _collect_maps(
    map_futs: Sequence[Any], k: int
) -> tuple[dict[int, dict[int, tuple[str, int]]], dict[str, int], dict[int, int], int]:
    """Barrier.

    Harvest every producer's fragment into per-task manifests keyed to its holder node (``holder_i
    = t % k``), the digest→size table for the replay, and the map-side witness pieces. A stage-1
    producer death propagates raw from ``fut.result()`` straight to ``_run_with_restarts``, which
    classifies it (restart-worthy) exactly as it does a gather death.
    """
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
    """Hash-repartition ``src_blocks`` into ``parts`` dests as an O(T+P) dask graph.

    Over the worker transport. ``dest_block_hashes`` are byte-identical across runs AND equal to
    the local ``run_repartition`` on identical inputs. ``fetch_budget_bytes`` /
    ``disk_budget_bytes`` bound the reader plane (accounted by the driver-side replay);
    ``holder_budget_bytes`` bounds the producer retention (F12); ``pull_timeout_s`` widens the
    per-holder block-pull ceiling for large batches (R4). A worker death restarts the whole run
    under a fresh epoch up to ``epoch_restarts_allowed``, else surfaces as an attributed
    ``StageError`` (§1.5).
    """
    require_pin(dbackend)
    from .transport import ensure_engine_plugins

    ensure_engine_plugins(dbackend.client)  # the submit shim needs graphed-worker; idempotent

    def _attempt(
        spec: Any, addresses: Sequence[str], k: int, t: int
    ) -> tuple[dict[int, Any], dict[int, str], ShuffleWitness]:
        run = _Attempt(dbackend, spec, backend, addresses, k)
        map_futs = run.maps(src_blocks, parts, salt, holder_budget_bytes, "map", t)
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
            dest: run.submit(
                _transport_gather_task,
                spec,
                backend,
                dest,
                run.pulls(manifests, dest),
                key=("gather", dest),
                worker=dest,
            )
            for dest in range(parts)
        }
        outs = [out for out in run.results(gather_futs).values() if out is not None]
        return {o.dest: o.value for o in outs}, {o.dest: o.hash for o in outs}, witness

    return _run_with_restarts(
        dbackend, len(src_blocks), n_tasks, epoch_restarts_allowed, _attempt, pull_timeout_s
    )


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
    """Distributed relational hash JOIN over the worker transport.

    ``broadcast=None`` lets the pinned ``parts``-keyed cost rule choose (F6 — never the live
    worker count); ``True``/``False`` honour a plan-recorded choice. Shuffle path: two
    co-partitioned repartitions (same ``salt``) + P per-dest ``_transport_gather_join`` tasks
    reusing ``join_with_budget`` (spill counters match the local engine exactly). Broadcast
    path: the build side ships once via ``dbackend.broadcast`` and each probe block is joined
    by a pinned ``_transport_broadcast_join_part`` — the large side never shuffles.
    ``holder_budget_bytes`` bounds BOTH sides' producer retention on the shuffle path (F12,
    mirrors repartition); ``pull_timeout_s`` widens the per-holder block-pull ceiling (R4).
    Neither applies to the broadcast path (the build side ships via ``broadcast``, not the
    holder store).
    """
    require_pin(dbackend)
    from .transport import ensure_engine_plugins

    ensure_engine_plugins(dbackend.client)
    on_t = tuple(on)
    chosen = choose_broadcast(backend, left_blocks, right_blocks, parts, broadcast)

    def _attempt(
        spec: Any, addresses: Sequence[str], k: int, _t: int
    ) -> tuple[dict[int, Any], dict[int, str], ShuffleWitness]:
        if chosen:
            return _broadcast_join_attempt(
                spec,
                backend,
                dbackend,
                left_blocks,
                right_blocks,
                on_t,
                how,
                mem_budget_bytes,
                addresses,
                k,
            )
        run = _Attempt(dbackend, spec, backend, addresses, k)
        return _shuffle_join_attempt(
            run,
            left_blocks,
            right_blocks,
            parts,
            on_t,
            how,
            salt,
            mem_budget_bytes,
            holder_budget_bytes,
        )

    # join task granularity keys on the side block counts, not a caller n_tasks; the restart
    # driver's ``t`` arg is unused (``_t``) — the map fan-out is min(k, n_side) inside each
    # attempt.
    return _run_with_restarts(
        dbackend,
        max(len(left_blocks), len(right_blocks)),
        None,
        epoch_restarts_allowed,
        _attempt,
        pull_timeout_s,
    )


def _shuffle_join_attempt(
    run: _Attempt,
    left: Sequence[Any],
    right: Sequence[Any],
    parts: int,
    on: tuple[str, ...],
    how: str,
    salt: int,
    mem_budget: int | None,
    holder_budget: int | None,
) -> tuple[dict[int, Any], dict[int, str], ShuffleWitness]:
    t_l = min(run.k, len(left)) if left else 0
    t_r = min(run.k, len(right)) if right else 0
    left_maps = run.maps(left, parts, salt, holder_budget, "lmap", t_l)
    right_maps = run.maps(right, parts, salt, holder_budget, "rmap", t_r)
    left_manifests = _collect_maps(left_maps, run.k)[0]
    right_manifests = _collect_maps(right_maps, run.k)[0]
    left_carrier = left[0] if left else None
    right_carrier = right[0] if right else None
    gather_futs = {
        dest: run.submit(
            _transport_gather_join,
            run.spec,
            run.backend,
            dest,
            on,
            how,
            mem_budget,
            left_carrier,
            right_carrier,
            run.pulls(left_manifests, dest),
            run.pulls(right_manifests, dest),
            key=("gjoin", dest),
            worker=dest,
        )
        for dest in range(parts)
    }
    tally = JoinTally(next_key=parts)
    for dest, out in run.results(gather_futs).items():
        if out is not None:
            tally.add_out(dest, out)
    witness = ShuffleWitness(
        n_producer_tasks=max(t_l, t_r),
        broadcast_chosen=False,
        build_side_blocks=sum(len(m) for m in left_manifests.values()),
        large_side_blocks=sum(len(m) for m in right_manifests.values()),
        peak_join_bytes=tally.peak,
        join_spilled_partitions=tally.spilled,
        join_chunks_read=tally.read,
        join_output_rows=tally.rows,
        manifest_fetch_is_per_dest=True,
    )
    return tally.value, tally.hashes, witness


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
    if not left:
        return (
            {},
            {},
            ShuffleWitness(broadcast_chosen=True, n_producer_tasks=1, large_side_blocks=0),
        )

    run = _Attempt(dbackend, spec, backend, addresses, k)
    build_concat = backend.concat(list(left))
    build_wire = bytes(backend.to_wire(build_concat))
    handle = dbackend.broadcast(
        build_wire, token=f"bjoin-{spec.epoch}-{sha256_hex(build_wire)[:12]}"
    )
    track_unmatched = how in ("left", "outer")
    part_futs = {
        pidx: run.submit(
            _transport_broadcast_join_part,
            backend,
            handle,
            probe_block,
            on,
            BROADCAST_PER_BLOCK_HOW[how],
            mem_budget,
            track_unmatched,
            None,
            key=("bpart", pidx),
            worker=pidx,
        )
        for pidx, probe_block in enumerate(right)
    }
    tally = JoinTally(next_key=len(right))
    for pidx, out in run.results(part_futs).items():
        tally.add_out(pidx, out)

    # emit the never-matched build rows exactly once (F2); an empty probe has no schema carrier,
    # so the tail is dropped there, as the local engine does
    unmatched = tally.unmatched(len(build_concat)) if track_unmatched else ()
    if unmatched and right:
        tail = run.submit(
            _transport_broadcast_join_part,
            backend,
            handle,
            right[0],
            on,
            "left",
            mem_budget,
            False,
            unmatched,
            key=("btail", 0),
            worker=0,
        )
        tally.add_out(None, tail.result())

    witness = ShuffleWitness(
        n_producer_tasks=len(right) or 1,
        broadcast_chosen=True,
        large_side_blocks=0,
        # DISTINCT workers that resolved the build handle (== k here)
        broadcast_puts=len({addresses[pidx % k] for pidx in range(len(right))}),
        peak_join_bytes=tally.peak,
        join_spilled_partitions=tally.spilled,
        join_chunks_read=tally.read,
        join_output_rows=tally.rows,
    )
    return tally.value, tally.hashes, witness


# ---- the §1.5 whole-run restart driver ----------------------------------------------------------
def _run_with_restarts(
    dbackend: Any,
    n_src: int,
    n_tasks: int | None,
    epoch_restarts_allowed: int,
    attempt_fn: Any,
    pull_timeout_s: float | None = None,
) -> TransportShuffleResult:
    """Run ``attempt_fn`` under fresh epochs (``run_epochs``), re-reading the worker addresses.

    Each attempt pins onto the workers alive at its start, so a restart after a death lands on
    the SURVIVORS. ``pull_timeout_s`` (when given) overrides the spec's block-pull ceiling (R4).
    """
    from .transport import make_transport_spec

    transport = TransportWitness()

    def attempt(_n: int, nonce: str) -> TransportShuffleResult | BaseException:
        addresses = sorted_addresses(dbackend)
        k = max(1, len(addresses))
        t = n_tasks if n_tasks is not None else (min(k, n_src) if n_src else 1)
        spec = make_transport_spec(nonce, addresses, pull_timeout_s=pull_timeout_s)
        try:
            value, hashes, witness = attempt_fn(spec, addresses, k, t)
        except BaseException as exc:  # run_epochs classifies it: restart or attributed StageError
            return exc
        finally:
            collect_and_purge(dbackend.client, nonce, transport.per_worker)
        return TransportShuffleResult(
            dest_block_hashes=hashes, value=value, witness=witness, transport=transport
        )

    return run_epochs(
        attempt, restarts_allowed=epoch_restarts_allowed, backend=dbackend, witness=transport
    )
