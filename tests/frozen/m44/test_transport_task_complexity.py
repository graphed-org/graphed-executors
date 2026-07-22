"""m44 O(T+P) structural gate — the scheduler graph is EXACTLY T map tasks + P gather tasks + a
CONSTANT control tail, with zero pick-shaped futures (plan §1.4 "Graph shape — the whole point",
F11 exact-slope tightening; blueprint §3 `test_transport_task_complexity`).

Counted at the ONLY pinned seam — ``dbackend.submit`` — via the harness SpyDaskBackend, classified
by the pinned module-level task-fn names. Per-class EXACT equality across the {P,2P,4P} x {T,2T}
ladder is stronger than a fitted slope: ``_transport_map_task == T`` and
``_transport_gather_task == P`` in every cell, everything else CONSTANT across all cells (the
O(k) control tail, k fixed). The m43 engine's shape (T·P ``_dask_pick`` futures) fails the gather
count and the zero-pick gate; per-row task creation fails row-count independence. Strict pinning
is witnessed at the seam: every map submit carries a single-address ``workers=`` pin following the
F8 ``sorted_addrs[t % k]`` round-robin, every gather submit a single-address pin, both reproduced
identically on a second run (determinism). Counts, never clocks (R0.10a)."""

from __future__ import annotations

from collections import Counter

import pytest
from transport_harness import (
    SpyDaskBackend,
    TransportAdapter,
    build_dask_backend,
    install_transport_plugin,
    repartition_blocks,
    sorted_worker_addresses,
    transport_adapters,
    transport_cluster,
    transport_repartition,
)

pytest.importorskip("distributed")

ADAPTERS = transport_adapters()
LADDER_T = (2, 4)
LADDER_P = (4, 8, 16)


@pytest.fixture(scope="module")
def client():
    with transport_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> TransportAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _spy_run(adapter: TransportAdapter, client, parts: int, n_tasks: int, copies: int = 1) -> SpyDaskBackend:  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    spy = SpyDaskBackend(build_dask_backend(client))
    transport_repartition(
        adapter.backend, repartition_blocks(adapter, copies), parts, dbackend=spy, n_tasks=n_tasks
    )
    return spy


def test_graph_is_T_maps_P_gathers_and_a_constant_control_tail(adapter: TransportAdapter, client) -> None:  # type: ignore[no-untyped-def]
    control_tails: set[int] = set()
    for n_tasks in LADDER_T:
        for parts in LADDER_P:
            names = _spy_run(adapter, client, parts, n_tasks).submitted_fn_names()
            assert names["_transport_map_task"] == n_tasks, (
                f"T slope broken at (T={n_tasks}, P={parts}): {dict(names)}"
            )
            assert names["_transport_gather_task"] == parts, (
                f"P slope broken at (T={n_tasks}, P={parts}): {dict(names)} — a T·P tier or a "
                "hidden extra stage"
            )
            assert names["_dask_pick"] == 0 and not any("pick" in n for n in names), (
                f"pick-shaped futures detected: {dict(names)} — the m43 T·P fan-out must not exist "
                "in the transport engine"
            )
            control_tails.add(sum(names.values()) - n_tasks - parts)
    assert len(control_tails) == 1, (
        f"control-task tail varies across the ladder ({sorted(control_tails)}) — control must be "
        "O(k) with k fixed, independent of T and P"
    )


def test_counts_are_row_count_independent(adapter: TransportAdapter, client) -> None:  # type: ignore[no-untyped-def]
    lean = _spy_run(adapter, client, 6, 2, copies=1).submitted_fn_names()
    fat = _spy_run(adapter, client, 6, 2, copies=8).submitted_fn_names()
    assert lean == fat, (
        f"8x rows changed the submit counts ({dict(lean)} -> {dict(fat)}) — per-row/per-fragment "
        "task creation (the M39 anti-pattern the complexity gate exists to kill)"
    )


def test_map_and_gather_submits_are_strict_pinned_and_deterministic(adapter: TransportAdapter, client) -> None:  # type: ignore[no-untyped-def]
    addrs = sorted_worker_addresses(client)
    spy = _spy_run(adapter, client, 8, 4)

    map_pins = spy.pins_of("_transport_map_task")
    assert all(pin is not None and len(pin) == 1 for pin in map_pins), (
        "every map submit must carry a single-address strict pin (workers=[owner])"
    )
    assert Counter(pin[0] for pin in map_pins if pin) == Counter(addrs[t % 2] for t in range(4)), (
        "map-task pins must follow the F8 sorted-address t % k round-robin"
    )
    gather_pins = spy.pins_of("_transport_gather_task")
    assert all(pin is not None and len(pin) == 1 and pin[0] in addrs for pin in gather_pins), (
        "every gather submit must carry a single-address strict pin to a frozen-set worker"
    )

    spy2 = _spy_run(adapter, client, 8, 4)
    assert spy2.pins_of("_transport_map_task") == map_pins, "map pin sequence not deterministic"
    assert spy2.pins_of("_transport_gather_task") == gather_pins, "gather pin sequence not deterministic"
