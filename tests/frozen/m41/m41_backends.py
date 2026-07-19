"""Shared harness for the M41 exec-local cluster-runtime-hardening suite (plan §8 M41, §4.3, §6.1.3).

M41 hardens the *data plane* (fetch/gather RAM budget + spill, incast coalescing, per-node disk
budgeting) and adds the Phase-2 ``ClusterExecutor`` launch seam. Those themes are **backend-agnostic**
(they live in the exchange/repartition ENGINE, not in a backend primitive), so — unlike the M39/M40
backend-independence themes — one real backend suffices. We use ``NumpyBackend``: deterministic, fast,
and its flat structured block has an exactly-predictable byte size (u64 key + i64 value = 16 B/row),
which lets the budget/spill/disk scenarios size themselves precisely.

Pinned execution contract this milestone ADDS (test-author decision — the implementer builds
``graphed_executors.local.shuffle`` to this; see the m41 README for the full field list + unit pins):

    run_repartition(..., *, fetch_budget_bytes: int | None = None,
                         disk_budget_bytes:  int | None = None) -> ShuffleResult

    ShuffleWitness gains: peak_fetch_bytes, fetch_spilled_bytes, fetch_spill_count, bulk_fetch_count,
    bytes_per_fetch (float mean), bytes_transferred, per_node_disk_bytes (addr->int), disk_budget_bytes,
    disk_backpressure_events, served_pid, served_host.

Every counter is cross-checked against an INDEPENDENT measurement (``tracemalloc`` peak; real
``objects/`` dir byte totals via :func:`store_bytes_on_disk`; the M39 routing oracle) so a stub that
merely adds the field and lies still fails.
"""

from __future__ import annotations

import socket
from collections.abc import Sequence
from pathlib import Path

import numpy as np
from graphed.numpy import NumpyBackend

from graphed_executors.local._transport import is_routable_host

BACKEND = NumpyBackend()
_DTYPE = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])
BYTES_PER_ROW = _DTYPE.itemsize  # 16 — exact, so scenarios size their budgets precisely


def make_block(keys: Sequence[int], values: Sequence[int] | None = None) -> np.ndarray:
    """One source partition: a flat structured block carrying a packed-u64 ``__joinkey__`` (the routed
    field) + an ``int64`` payload ``v`` (defaults to a per-row running index so rows are distinguishable
    and the gathered content is checkable)."""
    keys = list(keys)
    block = np.zeros(len(keys), dtype=_DTYPE)
    block["__joinkey__"] = np.array(keys, dtype=np.uint64)
    block["v"] = np.arange(len(keys), dtype=np.int64) if values is None else np.array(values, dtype=np.int64)
    return block


def keys_of(block: np.ndarray) -> list[int]:
    return [int(k) for k in block["__joinkey__"]]


# ---- M39 routing oracle (completeness / no-drop / no-reorder cross-check) ------------------------
def expected_dest_keys(src_blocks: Sequence[np.ndarray], parts: int) -> dict[int, list[int]]:
    """Test-authored oracle, independent of the gather plane: for each dest, the ``__joinkey__`` values
    routed there in the deterministic ascending-src_pid (then within-src) order (§4.0). Built from the
    backend's own golden-pinned ``partition`` — it does NOT re-implement routing, only the assembly order
    the executor must reproduce. Any fetch-plane spill/coalescing that DROPS or REORDERS rows fails it."""
    per_dest: dict[int, list[int]] = {}
    for src_pid in range(len(src_blocks)):  # ascending src_pid
        for dest, sub in enumerate(BACKEND.partition(src_blocks[src_pid], "__joinkey__", parts)):
            ks = keys_of(sub)
            if ks:
                per_dest.setdefault(dest, []).extend(ks)
    return per_dest


def observed_dest_keys(value: dict[int, np.ndarray]) -> dict[int, list[int]]:
    return {dest: keys_of(block) for dest, block in value.items()}


def hot_dest(hot_key: int, parts: int) -> int:
    """The single dest a hot key routes to under the pinned P-way route (identifies the over-receiving
    node for the T3 skew test — asserting the HOT node specifically, not an aggregate)."""
    subs = BACKEND.partition(make_block([hot_key]), "__joinkey__", parts)
    (dest,) = [d for d, s in enumerate(subs) if len(s)]
    return dest


# ---- independent disk measurement (ties the counters to REAL bytes on disk) ---------------------
def store_bytes_on_disk(node_dir: str | Path) -> int:
    """Sum of real file sizes under a node's Store dir tree (its ``objects/`` blobs). The independent
    ``du`` the disk counters are cross-checked against — a counter that claims a spill/cap while nothing
    (or too much) is on disk disagrees with this measurement and the assertion fails."""
    root = Path(node_dir)
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


# ---- routable address for the cross-process cluster sim (env-gated forms) ------------------------
def routable_ip() -> str | None:
    """A concrete non-loopback IP for this host, or ``None`` (then the cross-process forms skip WITH a
    reason — mirroring the M39 ``test_cluster_sim`` gate; the never-skipping contract lives in the
    ``[core]`` seam test, so the discrimination is never silently lost)."""
    try:
        candidate = socket.gethostbyname(socket.gethostname())
        if is_routable_host(candidate):
            return candidate
    except OSError:
        pass
    try:  # last resort: the source address toward a public IP (no packet is sent)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            candidate = s.getsockname()[0]
        finally:
            s.close()
        if is_routable_host(candidate):
            return candidate
    except OSError:
        pass
    return None
