"""m44 extra witness (non-gating): the driver-side reader-plane SIZING REPLAY reproduces the real
``_stage2_gather`` byte accounting EXACTLY.

The transport engine's reader/disk budget counters (``fetch_spill_count`` / ``peak_fetch_bytes`` /
``bulk_fetch_count`` / ``cross_node_fetches`` / ``bytes_transferred`` / ``disk_backpressure_events`` /
``per_node_disk_bytes``) are computed by REPLAYING the imported ``_stage2_gather`` over block SIZES
(``common.transport_tasks.replay_reader_plane`` since the m47 factoring), never re-implementing the accounting. This test pins that
the replay equals a REAL local ``run_repartition`` on the same inputs/budgets — the guarantee the
frozen ``test_transport_budget_parity`` relies on, checked here WITHOUT a dask cluster.

Non-vacuity / discrimination: the local run is an independent engine over a real backend and real
``_IpcCluster`` bytes; the replay uses only sizes. Any drift in the sizing shims (a wrong ``concat``
size, a mis-keyed spill node, a dropped fetch) diverges a counter. The ``test_replay_detects_*``
cases MUTATE the sizes and assert the replay NOTICES — proving the equality above is a real check,
not a tautology (an all-zero or size-blind replay would pass the mutated cases too, and fails here).
"""

from __future__ import annotations

import numpy as np
import pytest
from graphed.numpy import NumpyBackend

from graphed_executors.common.transport_tasks import replay_reader_plane
from graphed_executors.local.shuffle import (
    ShuffleWitness,
    _assign,
    _coalesce_task,
    _sha256_hex,
    run_repartition,
)

PARTS = 8
K = 2


def _blocks(copies: int = 4) -> list[object]:
    dt = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])
    key_lists = [
        [0, 1, 2, 3, 4, 5, 6, 7],
        [7, 6, 5, 4, 3, 2, 1, 0],
        [10, 11, 12, 13, 10, 11, 12, 13],
        [1, 1, 2, 2, 3, 3],
        [42, 43, 44, 45, 46, 47, 48, 49],
        [3, 3, 3, 3, 5, 5, 5, 5],
    ]
    out = []
    for keys in key_lists:
        allk = list(keys) * copies
        blk = np.zeros(len(allk), dtype=dt)
        blk["__joinkey__"] = np.array(allk, dtype=np.uint64)
        blk["v"] = np.arange(len(allk), dtype=np.int64)
        out.append(blk)
    return out


def _driver_manifests(
    backend: object, src: list[object], parts: int, k: int, n_tasks: int
) -> tuple[dict[int, dict[int, tuple[str, int]]], dict[str, int]]:
    """Reproduce exactly what the transport map tasks register: per producer task, coalesce its owned
    src blocks, ``to_wire`` each dest, and record ``(sha256, len)`` keyed to holder ``t % k``."""
    manifests: dict[int, dict[int, tuple[str, int]]] = {}
    size_of: dict[str, int] = {}
    for t, owned in enumerate(_assign(len(src), n_tasks)):
        per_dest, _peak = _coalesce_task(backend, [src[s] for s in owned], parts, 0)  # type: ignore[arg-type]
        holder = t % k
        manifests[t] = {}
        for dest in sorted(per_dest):
            wire = bytes(backend.to_wire(per_dest[dest]))  # type: ignore[attr-defined]
            digest = _sha256_hex(wire)
            manifests[t][dest] = (digest, holder)
            size_of[digest] = len(wire)
    return manifests, size_of


def _reader_counters(w: ShuffleWitness) -> dict[str, object]:
    return {
        "fetch_spill_count": w.fetch_spill_count,
        "peak_fetch_bytes": w.peak_fetch_bytes,
        "bulk_fetch_count": w.bulk_fetch_count,
        "cross_node_fetches": w.cross_node_fetches,
        "bytes_transferred": w.bytes_transferred,
        "fetch_spilled_bytes": w.fetch_spilled_bytes,
        "disk_budget_bytes": w.disk_budget_bytes,
        "disk_backpressure_events": w.disk_backpressure_events,
        "per_node_disk_bytes_total": sum(w.per_node_disk_bytes.values()),
        "per_node_disk_bytes_nodes": len(w.per_node_disk_bytes),
    }


@pytest.mark.parametrize(
    ("fetch_budget", "disk_budget"),
    [(None, None), (400, None), (200, None), (1, 4000)],
)
def test_replay_matches_local_stage2_gather(fetch_budget: int | None, disk_budget: int | None) -> None:
    be = NumpyBackend()
    src = _blocks()
    n_tasks = min(K, len(src))
    local = run_repartition(
        be, src, PARTS, workers=K, fetch_budget_bytes=fetch_budget, disk_budget_bytes=disk_budget
    )

    manifests, size_of = _driver_manifests(be, src, PARTS, K, n_tasks)
    addrs = tuple(f"tcp://w{i}" for i in range(K))
    replay = replay_reader_plane(
        manifests, PARTS, n_tasks, K, addrs, size_of, fetch_budget=fetch_budget, disk_budget=disk_budget
    )
    assert _reader_counters(replay) == _reader_counters(local.witness), (
        f"replay diverged from the real _stage2_gather at fetch={fetch_budget}, disk={disk_budget}"
    )
    # the per-node disk map must be keyed by the passed (worker) addresses, never the local node-i tags
    assert set(replay.per_node_disk_bytes) <= set(addrs)


def test_replay_detects_wrong_sizes() -> None:
    """Discrimination guard: halving every fetched block's size MUST change the reader counters — a
    size-blind replay (the failure mode) would not, and would silently pass the parity test above."""
    be = NumpyBackend()
    src = _blocks()
    n_tasks = min(K, len(src))
    manifests, size_of = _driver_manifests(be, src, PARTS, K, n_tasks)
    addrs = tuple(f"tcp://w{i}" for i in range(K))

    truth = replay_reader_plane(
        manifests, PARTS, n_tasks, K, addrs, size_of, fetch_budget=200, disk_budget=None
    )
    halved = {d: max(1, s // 2) for d, s in size_of.items()}
    mutated = replay_reader_plane(
        manifests, PARTS, n_tasks, K, addrs, halved, fetch_budget=200, disk_budget=None
    )
    assert (
        mutated.peak_fetch_bytes != truth.peak_fetch_bytes
        or mutated.bytes_transferred != truth.bytes_transferred
    ), "the replay ignored block sizes — it is not actually accounting bytes"
