"""m46 relay engine — repartition: the honest head-node workflow shape on HTEX (plan §1.3;
§3 m46 `test_parsl_relay_repartition`; the m39/m43 golden-gate lineage).

HTEX fixture DELIBERATELY (plan tests-minor r2): on TPE every task pid == the driver pid, so site
provenance is vacuous there — only HTEX discriminates driver-side compute.

Witnesses: `dest_block_hashes` byte-identical across two relay runs AND equal to the LOCAL
two-phase engine on identical inputs (cross-engine content addressing); routed content == the
`partition`-routed oracle; row conservation; the submit-seam Counter is EXACTLY
``{_dask_map_write: T, _dask_gather: P}`` with ZERO `_dask_pick` submits (T+P — the pick tier runs
as a plain driver-local dict lookup, plan §1.3: the m43 T·P shape would amplify driver traffic
~Px); counts are row-count-independent; map/gather sites are worker pids; and the frozen
head-node-routing witness: ``head_node_routed is True`` and ``driver_relay_bytes`` equal to the
independently-computed Σ of map wire sizes (design-M2 — engagement of the head-node route is
per-run observable, never docs-only).

Discriminates: the m43 T·P shape (nonzero picks), per-row/per-fragment task creation (row-count
ladder), a driver-side map or gather (site pids), an unlabeled relay (missing witness fields), a
driver_relay_bytes stub (wrong Σ), and any routing/merge/wire drift (hash equality vs local)."""

from __future__ import annotations

import os
import sys
from collections import Counter

import pytest
from parsl_harness import (
    BACKEND,
    SpyParslBackend,
    expected_relay_map_bytes,
    htex_pool,
    make_parsl_backend,
    make_runner,
    relay_repartition,
    repartition_blocks,
    result_dest_keys,
    route_oracle_dest_keys,
    run_bounded,
    total_result_rows,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (parsl ships no Windows support; Windows keeps the base "
    "package only — plan §4 G8)",
)

PARTS = 5


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with htex_pool(workers=2) as ex:
        yield ex


def _relay(htex, src, parts: int, **kwargs):  # type: ignore[no-untyped-def]
    spy = SpyParslBackend(make_parsl_backend(htex))
    res = run_bounded(lambda: relay_repartition(BACKEND, src, parts, runner=make_runner(spy), **kwargs))
    return spy, res


def test_two_relay_runs_and_the_local_engine_agree_bit_for_bit(htex) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks()
    _spy1, r1 = _relay(htex, src, PARTS)
    _spy2, r2 = _relay(htex, src, PARTS)
    local = run_repartition(BACKEND, src, PARTS)

    assert r1.dest_block_hashes, "no dest blocks produced — scenario degenerate"
    assert r1.dest_block_hashes == r2.dest_block_hashes, "two relay runs diverged (nondeterminism)"
    assert r1.dest_block_hashes == local.dest_block_hashes, (
        "relay and the local engine disagree on content-addressed dest blocks — routing, merge "
        "order, or wire serialization drifted from the frozen m39 contract"
    )
    assert result_dest_keys(r1.value) == route_oracle_dest_keys(src, PARTS)
    assert total_result_rows(r1.value) == sum(len(b) for b in src)  # row conservation


def test_relay_shape_is_T_maps_P_gathers_zero_picks(htex) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks()
    spy, res = _relay(htex, src, PARTS)
    t = int(res.witness.n_producer_tasks)
    assert t == 2, "T must be min(n_workers=2, n_src=6) producer tasks"
    names = spy.fn_names()
    assert names == Counter({"_dask_map_write": t, "_dask_gather": PARTS}), (
        f"relay submit shape diverged from §1.3 (T maps + P gathers, ZERO picks): got "
        f"{dict(names)} — nonzero _dask_pick is the m43 T·P shape (the ~Px driver-traffic "
        "amplification the relay exists to avoid); anything else is a hidden extra stage"
    )
    assert names["_dask_pick"] == 0

    # row-count independence: 8x the rows, same block count -> identical submit Counter
    fat_spy, fat_res = _relay(htex, repartition_blocks(copies=8), PARTS)
    assert int(fat_res.witness.n_producer_tasks) == t
    assert fat_spy.fn_names() == names, (
        f"8x rows changed the submit counts ({dict(names)} -> {dict(fat_spy.fn_names())}) — "
        "per-row/per-fragment task creation (the M39 anti-pattern)"
    )


def test_relay_witnesses_head_node_routing_with_worker_side_kernels(htex) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks()
    _spy, res = _relay(htex, src, PARTS)
    w = res.witness
    driver_pid = os.getpid()

    # the frozen §1.3/design-M2 relay witness fields — head-node routing is per-run observable
    assert w.head_node_routed is True, "the relay result must be labeled head_node_routed=True"
    expected_bytes = expected_relay_map_bytes(src, PARTS, int(w.n_producer_tasks))
    assert expected_bytes > 0
    assert w.driver_relay_bytes == expected_bytes, (
        f"driver_relay_bytes must equal the Σ of map wire sizes resolved at the driver barrier "
        f"(independent oracle: {expected_bytes}), got {w.driver_relay_bytes} — a stubbed or "
        "partially-counted relay tally"
    )

    # kernels run on workers; only the RELAY of bytes goes through the driver
    assert len(w.producer_sites) == int(w.n_producer_tasks)
    assert w.gather_sites, "no gather sites recorded"
    for site_map in (w.producer_sites, w.gather_sites):
        assert all(pid != driver_pid for pid, _addr in site_map.values()), (
            "a map/gather kernel executed in the DRIVER process — the relay moves BYTES through "
            "the submit host, never the compute (plan §1.3)"
        )


def test_salt_reroutes_deterministically(htex) -> None:  # type: ignore[no-untyped-def]
    """`salt` must reach the pinned route: two salted runs agree with each other and differ from
    the unsalted run (content-neutral re-placement — the m40 skew-salt discipline)."""
    src = repartition_blocks()
    _s, plain = _relay(htex, src, PARTS)
    _s1, s1 = _relay(htex, src, PARTS, salt=3)
    _s2, s2 = _relay(htex, src, PARTS, salt=3)

    assert s1.dest_block_hashes == s2.dest_block_hashes, "salted runs diverged (nondeterminism)"
    assert s1.dest_block_hashes != plain.dest_block_hashes, (
        "salt=3 produced the identical placement — salt never reached the route"
    )
    assert total_result_rows(s1.value) == sum(len(b) for b in src)
