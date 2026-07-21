"""m43 complexity gates — the future graph has EXACTLY the §1.3.4 shape: T producer futures,
T·P pick futures, P gather futures; linear in P, independent of row count (blueprint §3
`test_dask_shuffle_structure`; the M39 complexity-gate discipline carried to dask).

Futures are counted at the ONLY pinned seam — `runner.backend.submit` — through the harness
SpySubmitBackend, classified by the §2 m43 Implementation-Target task-fn names
(`_dask_map_write`/`_dask_pick`/`_dask_gather`). EXACT Counter equality kills extra hidden stages;
counting picks at T·P (all of them, empty dests included) kills any driver-side "skip the empty
dest" shortcut, which would need a manifest mechanism the design retires; P gathers likewise.
Counts, never clocks (R0.10a)."""

from __future__ import annotations

from collections import Counter

import pytest
from dask_shuffle_backends import (
    ShuffleJoinAdapter,
    SpySubmitBackend,
    build_dask_backend,
    dask_repartition,
    install_worker_plugin,
    make_submit_runner,
    repartition_blocks,
    shuffle_adapters,
    shuffle_cluster,
)

pytest.importorskip("distributed")

ADAPTERS = shuffle_adapters()


@pytest.fixture(scope="module")
def client():
    with shuffle_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> ShuffleJoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _counts(adapter: ShuffleJoinAdapter, client, parts: int, copies: int = 1) -> tuple[Counter[str], int]:  # type: ignore[no-untyped-def]
    install_worker_plugin(client)
    spy = SpySubmitBackend(build_dask_backend(client))
    res = dask_repartition(
        adapter.backend, repartition_blocks(adapter, copies), parts, runner=make_submit_runner(spy)
    )
    return spy.submitted_fn_names(), int(res.witness.n_producer_tasks)


def test_future_counts_are_exactly_T_producers_TP_picks_P_gathers(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    for parts in (4, 8, 16):  # the {P, 2P, 4P} linearity ladder
        names, t = _counts(adapter, client, parts)
        assert t == 2, "T must be min(n_workers=2, n_src=6) producer tasks"
        expected = Counter(
            {"_dask_map_write": t, "_dask_pick": t * parts, "_dask_gather": parts}
        )
        assert names == expected, (
            f"parts={parts}: future graph shape diverged from §1.3.4 "
            f"(T + T·P + P): got {dict(names)}, want {dict(expected)} — an MxR fan-out, per-row "
            "tasks, a whole-dict gather (missing picks), or a hidden extra stage"
        )


def test_future_counts_are_row_count_independent(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    parts = 6
    lean, t_lean = _counts(adapter, client, parts, copies=1)
    fat, t_fat = _counts(adapter, client, parts, copies=8)  # 8x the rows, same block count
    assert t_lean == t_fat
    assert lean == fat, (
        f"8x rows changed the future counts ({dict(lean)} -> {dict(fat)}) — per-row/per-fragment "
        "task creation (the M39 anti-pattern the complexity gate exists to kill)"
    )
