"""m43 parity fixup (post-review, G1) — the join edge families the original frozen suite left to
tests/extra: broadcast non-inner unmatched tails, pure one-sided shuffle dests, and
partitionless/empty sides. Every test pins DASK == LOCAL on identical inputs (content hashes and,
where the relation is defined, the null-aware pandas oracle), on BOTH real backends.

Scenario keys are chosen from the measured sha256 route at parts=4 (backend-independent golden
pin; re-verified in-test via the backend's own ``partition`` as the non-vacuity guard):
dest0 <- {3,7,9,18}, dest1 <- {2,6,8}, dest2 <- {0,1,5}, dest3 <- {4,12,14}.

The empty-probe broadcast case (e) pins EQUALITY TO LOCAL only: the local engine documents
(ponytail ceiling in ``_run_broadcast_join``) that a forced-broadcast run over an entirely empty
probe emits nothing — the pin here is "no crash + identical to local", which is exactly the
IndexError regression the review found."""

from __future__ import annotations

from collections import Counter

import pytest
from dask_shuffle_backends import (
    ShuffleJoinAdapter,
    build_dask_backend,
    dask_join,
    install_worker_plugin,
    make_submit_runner,
    nullable_join_multiset,
    pandas_join_oracle,
    shuffle_adapters,
    shuffle_cluster,
)

from graphed_executors.local.shuffle import run_join

pytest.importorskip("distributed")
pytest.importorskip("pandas")

ADAPTERS = shuffle_adapters()
PARTS = 4

# (a) broadcast: the ENTIRE build side matches nothing on the probe side
_A_LK, _A_LV = [3, 7], [30, 70]
_A_PROBE = [([2, 6], [200, 600]), ([8, 11], [800, 110]), ([0, 1], [10, 20])]  # 3 blocks, disjoint keys

# (b) shuffle: left-only keys route to dests {0,1}, right-only keys to {2,3}; key 18 (dest 0) is
# shared with build-side duplication, so dest 0 is two-sided while dests 1/2/3 are pure one-sided
_B_LEFT = [([3, 7, 9, 18, 18], [130, 170, 190, 118, 119]), ([2, 6, 8], [102, 106, 108])]
_B_RIGHT = [([0, 1, 5, 18], [900, 901, 905, 918]), ([4, 12, 14], [904, 912, 914])]


@pytest.fixture(scope="module")
def client():
    with shuffle_cluster(2, processes=False) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> ShuffleJoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def _runner(client):  # type: ignore[no-untyped-def]
    install_worker_plugin(client)
    return make_submit_runner(build_dask_backend(client))


def _sides(adapter: ShuffleJoinAdapter, spec_l, spec_r):  # type: ignore[no-untyped-def]
    left = [adapter.make_side(ks, "lval", vs) for ks, vs in spec_l]
    right = [adapter.make_side(ks, "rval", vs) for ks, vs in spec_r]
    return left, right


def _flat(spec):  # type: ignore[no-untyped-def]
    return [k for ks, _ in spec for k in ks], [v for _, vs in spec for v in vs]


@pytest.mark.parametrize("how", ["left", "right", "outer"])
def test_broadcast_never_matched_build_tail_appears_exactly_once(adapter: ShuffleJoinAdapter, how: str, client) -> None:  # type: ignore[no-untyped-def]
    left, right = _sides(adapter, [(_A_LK, _A_LV)], _A_PROBE)
    rk, rv = _flat(_A_PROBE)
    assert not set(_A_LK) & set(rk), "scenario: the build side must match NOTHING"
    oracle = pandas_join_oracle(_A_LK, _A_LV, rk, rv, how)

    res = dask_join(adapter.backend, left, right, PARTS, how=how, runner=_runner(client), broadcast=True)
    observed = nullable_join_multiset(res.value)
    assert observed == oracle, f"how={how}: missing={dict(oracle - observed)} extra={dict(observed - oracle)}"
    if how in ("left", "outer"):
        for k, lv in zip(_A_LK, _A_LV, strict=True):
            assert observed[(k, lv, None)] == 1, (
                f"how={how}: never-matched build row ({k}, {lv}) must appear EXACTLY once across "
                f"ALL probe blocks, got {observed[(k, lv, None)]} (the per-block N-fold re-emission bug)"
            )
    local = run_join(adapter.backend, left, right, PARTS, how=how, broadcast=True)
    assert res.dest_block_hashes == local.dest_block_hashes, f"how={how}: dask != local under broadcast"


@pytest.mark.parametrize("how", ["left", "right", "outer"])
def test_shuffle_one_sided_dests_null_fill_and_match_local(adapter: ShuffleJoinAdapter, how: str, client) -> None:  # type: ignore[no-untyped-def]
    left, right = _sides(adapter, _B_LEFT, _B_RIGHT)
    be = adapter.backend
    # non-vacuity via the golden-pinned route: build-only AND probe-only dests really exist
    ldests = {d for b in left for d, s in enumerate(be.partition(b, "__joinkey__", PARTS)) if len(s)}
    rdests = {d for b in right for d, s in enumerate(be.partition(b, "__joinkey__", PARTS)) if len(s)}
    assert ldests - rdests and rdests - ldests, "scenario must produce pure one-sided dests"
    assert ldests & rdests, "scenario must also keep a two-sided (matched) dest"

    lk, lv = _flat(_B_LEFT)
    rk, rv = _flat(_B_RIGHT)
    oracle = pandas_join_oracle(lk, lv, rk, rv, how)
    assert any(None in row for row in oracle), "one-sided rows must survive as null-filled rows"

    res = dask_join(adapter.backend, left, right, PARTS, how=how, runner=_runner(client), broadcast=False)
    observed = nullable_join_multiset(res.value)
    assert all(key is not None for (key, _, _) in observed), "join keys must be coalesced, never null"
    assert observed == oracle, f"how={how}: missing={dict(oracle - observed)} extra={dict(observed - oracle)}"
    local = run_join(adapter.backend, left, right, PARTS, how=how, broadcast=False)
    assert res.dest_block_hashes == local.dest_block_hashes, f"how={how}: dask != local one-sided dests"


@pytest.mark.parametrize("how", ["left", "outer"])
def test_shuffle_partitionless_probe_side_keeps_build_rows(adapter: ShuffleJoinAdapter, how: str, client) -> None:  # type: ignore[no-untyped-def]
    """right=[] on the shuffle path: the local contract emits the build rows (schema-carrierless —
    as-is, no null columns to add), never a crash and never a silent drop; dask must match
    bit-for-bit. Read back via side_rows (the blocks carry only the build columns)."""
    left, _ = _sides(adapter, _B_LEFT, [])
    res = dask_join(adapter.backend, left, [], PARTS, how=how, runner=_runner(client), broadcast=False)
    local = run_join(adapter.backend, left, [], PARTS, how=how, broadcast=False)
    assert res.dest_block_hashes == local.dest_block_hashes, f"how={how}: dask != local for right=[]"
    lk, lv = _flat(_B_LEFT)
    observed_rows = Counter(r for b in res.value.values() for r in adapter.side_rows(b, "lval"))
    assert observed_rows == Counter(zip(lk, lv, strict=True)), (
        f"how={how}: build rows must survive a partitionless probe side exactly once each"
    )


@pytest.mark.parametrize("how", ["inner", "outer"])
def test_broadcast_empty_build_side_returns_gracefully(adapter: ShuffleJoinAdapter, how: str, client) -> None:  # type: ignore[no-untyped-def]
    _, right = _sides(adapter, [], _A_PROBE)
    res = dask_join(adapter.backend, [], right, PARTS, how=how, runner=_runner(client), broadcast=True)
    local = run_join(adapter.backend, [], right, PARTS, how=how, broadcast=True)
    assert res.dest_block_hashes == local.dest_block_hashes == {}, (
        f"how={how}: an empty build side under broadcast must return the local engine's early-empty"
    )
    assert len(res.value) == len(local.value) == 0


@pytest.mark.parametrize("how", ["left", "outer"])
def test_broadcast_empty_probe_side_no_crash_equals_local(adapter: ShuffleJoinAdapter, how: str, client) -> None:  # type: ignore[no-untyped-def]
    """broadcast=True with right=[] and a NON-empty build: must not crash (the reviewed IndexError
    regression) and must equal local — which documents (ponytail ceiling) that this
    cost-model-unreachable forced combination emits nothing; the pin is equality, not content."""
    left, _ = _sides(adapter, [(_A_LK, _A_LV)], [])
    res = dask_join(adapter.backend, left, [], PARTS, how=how, runner=_runner(client), broadcast=True)
    local = run_join(adapter.backend, left, [], PARTS, how=how, broadcast=True)
    assert res.dest_block_hashes == local.dest_block_hashes, f"how={how}: dask != local for probe=[]"
    assert sorted(res.value) == sorted(local.value)
