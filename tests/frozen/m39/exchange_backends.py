"""Shared harness for the M39 exec-local shuffle suite: BOTH real backends + the pinned run contract.

The M2 backend-independence discipline, extended to execution (B-r5.3): the same exchange/shuffle
suite runs against the REAL ``AwkwardBackend`` AND ``NumpyBackend``, so M39 witnesses the generic
engine's backend-agnosticism by EXECUTION, not merely the §A.4 import lint.

Pinned execution contract (test-author decision — the implementer builds ``graphed_exec_local.shuffle``
to this; see the m39 README):

    run_repartition(backend, src_blocks, parts, *, workers=1, comms="ipc", store_root=None,
                    steal=False, faults=ShuffleFaults()) -> ShuffleResult
    reference_repartition(backend, src_blocks, parts) -> dict[int, str]   # dest_pid -> expected block hash

``src_blocks`` is one backend-native block per source-partition (``src_pid`` = its list index), each
carrying a packed-u64 ``__joinkey__`` column. The result's ``dest_block_hashes`` (dest_pid -> sha256 of
the gathered block's wire bytes) is the BIT-FOR-BIT key: content-addressing IS the determinism gate
(§7.1), so two runs agree iff every gathered block is byte-identical. ``ShuffleWitness`` exposes the
mechanism counters (blocks-per-task, holders, manifest owners/bytes, steals, announcement faults).
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import numpy as np
import pytest


@dataclass
class BackendAdapter:
    name: str
    backend: object
    make_block: Callable[[Sequence[int]], object]
    keys_of: Callable[[object], list[int]]


def _awkward_adapter() -> BackendAdapter:
    ak = pytest.importorskip("awkward")
    from graphed.awkward import AwkwardBackend  # noqa: PLC0415

    def make(keys: Sequence[int]) -> object:
        return ak.Array(
            {
                "__joinkey__": np.array(list(keys), dtype=np.uint64),
                "v": np.arange(len(keys), dtype=np.int64),
            }
        )

    def keys_of(block: object) -> list[int]:
        return [int(k) for k in block["__joinkey__"].to_list()]

    return BackendAdapter("awkward", AwkwardBackend(), make, keys_of)


def _numpy_adapter() -> BackendAdapter:
    from graphed.numpy import NumpyBackend  # noqa: PLC0415

    dtype = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])

    def make(keys: Sequence[int]) -> object:
        block = np.zeros(len(keys), dtype=dtype)
        block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
        block["v"] = np.arange(len(keys), dtype=np.int64)
        return block

    def keys_of(block: object) -> list[int]:
        return [int(k) for k in block["__joinkey__"]]

    return BackendAdapter("numpy", NumpyBackend(), make, keys_of)


def adapters() -> list[BackendAdapter]:
    out: list[BackendAdapter] = []
    for factory in (_awkward_adapter, _numpy_adapter):
        with contextlib.suppress(Exception):  # a backend not installed simply drops from the parametrization
            out.append(factory())
    return out


# A representative multi-src_pid scenario reused across themes: 6 source partitions with overlapping
# key populations, so under a P-way hash every producer-task routes to several dests (exercises
# coalescing) and several src_pids feed each dest (exercises ascending-src_pid merge order).
def scenario_src_blocks(adapter: BackendAdapter) -> list[object]:
    key_lists = [
        [0, 1, 2, 3, 4, 5, 6, 7],
        [7, 6, 5, 4, 3, 2, 1, 0],
        [10, 11, 12, 13, 10, 11, 12, 13],
        [100, 200, 300, 400],
        [1, 1, 2, 2, 3, 3],
        [42, 43, 44, 45, 46, 47, 48, 49],
    ]
    return [adapter.make_block(keys) for keys in key_lists]


def expected_dest_keys(adapter: BackendAdapter, src_blocks: list[object], parts: int) -> dict[int, list[int]]:
    """A TEST-AUTHORED oracle (independent of the executor): for each dest, the ``__joinkey__`` values
    routed there in the deterministic ascending-src_pid, then within-src, order (§4.0). Built from the
    backend's own ``partition`` — so it does NOT re-implement routing (that is the golden-vector pin),
    only the assembly order the executor must reproduce despite fuzzed arrival / stealing."""
    per_dest: dict[int, list[int]] = {}
    for src_pid in range(len(src_blocks)):  # ascending src_pid
        subs = adapter.backend.partition(src_blocks[src_pid], "__joinkey__", parts)
        for dest, sub in enumerate(subs):
            keys = adapter.keys_of(sub)
            if keys:
                per_dest.setdefault(dest, []).extend(keys)
    return per_dest


def observed_dest_keys(adapter: BackendAdapter, value: dict[int, object]) -> dict[int, list[int]]:
    """The keys the executor actually assembled per dest partition (for content bit-for-bit)."""
    return {dest: adapter.keys_of(block) for dest, block in value.items()}
