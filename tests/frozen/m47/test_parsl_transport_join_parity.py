"""m47 behavioral golden (m40 carried) — ``transport_run_join`` reproduces the local engine's
relational semantics on both real backends over the peer-exchange plane (plan §1.4/§2 m47;
blueprint §3 `test_parsl_transport_join_parity`).

Witnesses: inner rows == the duplicating multiset oracle (routes via the golden-pinned
``partition``, never the join kernel) == the local ``run_join``; inner ``dest_block_hashes``
byte-identical to the local engine; left/right/outer null-aware multisets == the local engine
with the UNMATCHED rows present exactly as measured against the frozen local engine (left-only
key 13 x2 in left/outer, right-only key 17 x2 in right/outer, both ABSENT from inner) and nulls
OPTION-TYPED — the m40 sentinel trap re-armed on the p2p plane.

pyarrow: MEASURED m46 fact — graphed.numpy wires MASKED blocks (every non-inner join output) over
Arrow; the m46 packaging pins already hold pyarrow in the test-parsl job, and this module
importorskips it loudly so it can never silently skip in CI."""

from __future__ import annotations

import sys

import pytest
from parsl_transport_harness import (
    P2PAdapter,
    dup_join_all,
    join_sides,
    key_row_count,
    make_parsl_backend,
    nullable_join_multiset,
    observed_join_all,
    p2p_adapters,
    p2p_htex_pool,
    run_bounded,
    transport_join,
)

from graphed_executors.local.shuffle import run_join

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)
pytest.importorskip(
    "pyarrow",
    reason="pyarrow is a measured runtime need of the non-inner numpy arms (masked blocks wire "
    "over Arrow) — pinned into the test-parsl CI job by the frozen m46 packaging pins",
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

ADAPTERS = p2p_adapters()
PARTS = 8

# measured against the frozen LOCAL engine at parts=8 (never invented): rows per how + the
# unmatched-key row counts the F1 one-sided-dest carriers must preserve
EXPECTED_UNMATCHED = {
    "inner": (0, 0),
    "left": (2, 0),
    "right": (0, 2),
    "outer": (2, 2),
}


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=2) as ex:
        yield ex


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> P2PAdapter:  # type: ignore[no-untyped-def]
    return request.param


@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
def test_join_rows_match_local_engine_with_option_typed_unmatched_rows(
    adapter: P2PAdapter, htex, how: str
) -> None:  # type: ignore[no-untyped-def]
    left, right = join_sides(adapter)
    local = run_join(adapter.backend, left, right, PARTS, how=how, broadcast=False)
    r = run_bounded(
        lambda: transport_join(
            adapter.backend,
            left,
            right,
            PARTS,
            how=how,
            pbackend=make_parsl_backend(htex),
            broadcast=False,
        )
    )

    observed = nullable_join_multiset(r.value)
    assert observed == nullable_join_multiset(local.value), (
        f"how={how}: p2p join rows diverged from the local engine — a dropped/duplicated "
        "unmatched carrier or a sentinel-null implementation lands here"
    )
    n13, n17 = EXPECTED_UNMATCHED[how]
    assert key_row_count(observed, 13) == n13, f"how={how}: left-only key 13 rows != {n13}"
    assert key_row_count(observed, 17) == n17, f"how={how}: right-only key 17 rows != {n17}"

    if how == "inner":
        assert r.dest_block_hashes == local.dest_block_hashes, (
            "inner join bytes diverged from the local engine"
        )
        assert observed_join_all(adapter, r.value) == dup_join_all(adapter, left, right, PARTS), (
            "inner rows diverged from the independent duplicating oracle (m40 §3.3)"
        )
