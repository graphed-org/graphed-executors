"""m46 relay engine — relational join on HTEX (plan §1.3; §3 m46 `test_parsl_relay_join`; the
m40/m43 golden lineage). HTEX fixture deliberately — site provenance is vacuous on TPE.

Witnesses: inner == the TEST-AUTHORED duplicating oracle (routes via the golden-pinned
`partition`, never the join kernel) AND `dest_block_hashes` byte-identical to the LOCAL `run_join`
for EVERY `how`; the non-inner arms compared null-aware (masked rows read as None) with the
left-only key (13) / right-only key (17) rows explicitly present exactly the right number of
times (the F1 one-sided-dest carriers, m40 discrimination re-armed); `mem_budget_bytes` engages
the reused `_join_with_budget` spill (`join_spilled_partitions > 0`, `peak_join_bytes <= budget`,
duplicated rows intact); every gather-join site is a worker pid; the RelayShuffleWitness carries
`head_node_routed=True` + `driver_relay_bytes > 0` on every arm.

Discriminates: a grouped/dedup join (multiset differs), dropped one-sided dests (13/17 rows
vanish), a per-dest re-emission of unmatched rows (13/17 counts inflate), a full-RAM concat under
budget (peak exceeds), driver-side gather-join (site pids), and an unlabeled relay (witness
fields)."""

from __future__ import annotations

import os
import sys

import pytest
from parsl_harness import (
    BACKEND,
    SpyParslBackend,
    dup_join_all,
    htex_pool,
    join_hot_key_sides,
    join_sides,
    make_parsl_backend,
    make_runner,
    nullable_join_multiset,
    observed_join_all,
    relay_join,
    run_bounded,
)

from graphed_executors.local.shuffle import run_join

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (parsl ships no Windows support; Windows keeps the base "
    "package only — plan §4 G8)",
)

PARTS = 4


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with htex_pool(workers=2) as ex:
        yield ex


def _relay_join(htex, left, right, **kwargs):  # type: ignore[no-untyped-def]
    spy = SpyParslBackend(make_parsl_backend(htex))
    res = run_bounded(lambda: relay_join(BACKEND, left, right, PARTS, runner=make_runner(spy), **kwargs))
    return spy, res


def _assert_relay_witness(res) -> None:  # type: ignore[no-untyped-def]
    assert res.witness.head_node_routed is True
    assert res.witness.driver_relay_bytes > 0, (
        "a shuffle join moved zero bytes through the driver barrier — driver_relay_bytes untallied"
    )


def test_inner_join_matches_the_duplicating_oracle_and_local(htex) -> None:  # type: ignore[no-untyped-def]
    left, right = join_sides()
    oracle = dup_join_all(left, right, PARTS)
    assert oracle, "scenario degenerate — the inner join must produce rows"

    _spy, res = _relay_join(htex, left, right, how="inner")
    local = run_join(BACKEND, left, right, PARTS, how="inner")
    assert observed_join_all(res.value) == oracle, (
        "relay inner join diverged from the duplicating relational oracle"
    )
    assert res.dest_block_hashes == local.dest_block_hashes, (
        "relay and the local engine disagree on content-addressed join blocks"
    )
    _assert_relay_witness(res)

    driver_pid = os.getpid()
    assert res.witness.gather_sites, "no gather-join sites recorded"
    assert all(pid != driver_pid for pid, _a in res.witness.gather_sites.values()), (
        "a gather-join kernel executed in the DRIVER process"
    )


@pytest.mark.parametrize("how", ["left", "right", "outer"])
def test_noninner_arms_match_local_nullable_with_one_sided_rows(how: str, htex) -> None:  # type: ignore[no-untyped-def]
    # MEASURED: numpy MASKED blocks (every non-inner output) serialize over Arrow — graphed.numpy
    # to_wire raises "install graphed[parquet]" without it. The test-parsl CI job must install
    # pyarrow exactly as the test-dask job does (pinned in test_parsl_packaging_pins).
    pytest.importorskip("pyarrow", reason="non-inner numpy joins wire masked blocks over Arrow")
    left, right = join_sides()
    _spy, res = _relay_join(htex, left, right, how=how)
    local = run_join(BACKEND, left, right, PARTS, how=how)

    got = nullable_join_multiset(res.value)
    assert got == nullable_join_multiset(local.value), (
        f"how={how}: relay diverged from the local engine's null-aware relation"
    )
    assert res.dest_block_hashes == local.dest_block_hashes, f"how={how}: cross-engine hashes diverged"
    _assert_relay_witness(res)

    # the unmatched witnesses, explicit (never trust only the shared-kernel comparison):
    # left-only key 13 appears twice in the left side; right-only key 17 twice in the right side.
    n13 = sum(c for (k, _lv, rv), c in got.items() if k == 13 and rv is None)
    n17 = sum(c for (k, lv, _rv), c in got.items() if k == 17 and lv is None)
    if how in ("left", "outer"):
        assert n13 == 2, f"how={how}: the two unmatched build rows (key 13) must appear EXACTLY once each"
    else:
        assert n13 == 0
    if how in ("right", "outer"):
        assert n17 == 2, f"how={how}: the two unmatched probe rows (key 17) must appear EXACTLY once each"
    else:
        assert n17 == 0


def test_budgeted_join_spills_and_streams_within_the_budget(htex) -> None:  # type: ignore[no-untyped-def]
    n_left = n_right = 64  # -> 4096 duplicated output rows in a single hot dest
    left, right = join_hot_key_sides(n_left, n_right)
    oracle = dup_join_all(left, right, PARTS)
    assert sum(oracle.values()) == n_left * n_right, "scenario must duplicate to n_left*n_right rows"

    # bound the duplicated output from the input row widths (the m40 sizing idiom), then squeeze
    row_bytes = (
        int(BACKEND.estimated_bytes(left[0])) // n_left + int(BACKEND.estimated_bytes(right[0])) // n_right
    )
    budget = (n_left * n_right * row_bytes) // 8

    _spy, res = _relay_join(htex, left, right, how="inner", broadcast=False, mem_budget_bytes=budget)
    w = res.witness
    assert w.peak_join_bytes <= budget, (
        "the gather-join working set exceeded the budget — a full-RAM concat of the duplicated "
        "dest instead of the reused _join_with_budget spill+stream kernel"
    )
    assert w.join_spilled_partitions > 0, "spill never engaged — the budget was never exercised"
    assert w.join_output_rows == n_left * n_right, "relational duplication must emit n_left*n_right rows"
    assert observed_join_all(res.value) == oracle, "spilled/streamed join content diverged from the oracle"
    _assert_relay_witness(res)
