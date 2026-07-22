"""m46 — the reuse claim made real + the honest refusal (plan §1.1 consequences, §1.6 move
table; §3 m46 `test_parsl_m43_engine_on_tpe`; Counter pins copied from the frozen m43
`test_dask_shuffle_structure` / `test_dask_join_plan_choice.py:78-113` shapes).

The MOVED m43 engine (`graphed_executors.common.tasks_engine` — names verbatim, m46 IT1) runs
UNCHANGED over `ParslBackend(TPE)`: its peer gate passes (peer_data_movement=True, the
ThreadBackend shape) and it drives the full m43 submit shape through the backend — repartition
EXACTLY ``{_dask_map_write: T, _dask_pick: T·P, _dask_gather: P}``; the shuffle join EXACTLY
``map: t_l+t_r``, **NONZERO pick: t_l·P + t_r·P** (shuffle.py:414-423 — the structural signature
separating the m43 engine from the zero-pick relay), ``gather_join: P``. Caveat stated plainly
(plan tests-(b)): on TPE this witnesses "the m43 engine drove the m43 submit shape through the
backend", NOT separate-process execution — site provenance is vacuous there (task pid == driver
pid by construction), which is exactly why the relay suite pins sites on HTEX instead.

`ParslBackend(HTEX)` is REFUSED by `_require_peer` before any work: NotImplementedError naming
peer data movement, ZERO submits and ZERO broadcasts reaching the backend, on BOTH entry points —
the §1.1.4 correction frozen (a peer-flag lie on HTEX would silently open head-node routing).

Discriminates: a relay-everywhere implementation (zero picks fails the join Counter), a
compute-on-driver stub that never submits (zero counts), a moved engine that renamed its task fns
(the Counter keys), and an HTEX capability lie (the refusal arm executes work instead of raising)."""

from __future__ import annotations

import sys
from collections import Counter

import pytest
from parsl_harness import (
    BACKEND,
    SpyParslBackend,
    dup_join_all,
    htex_pool,
    join_equal_sides,
    m43_join,
    m43_repartition,
    make_parsl_backend,
    make_runner,
    observed_join_all,
    repartition_blocks,
    result_dest_keys,
    route_oracle_dest_keys,
    run_bounded,
    total_result_rows,
    tpe_pool,
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


@pytest.fixture(scope="module")
def tpe():  # type: ignore[no-untyped-def]
    with tpe_pool(max_threads=2) as ex:
        yield ex


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with htex_pool(workers=2) as ex:
        yield ex


def test_m43_repartition_runs_on_tpe_with_the_full_m43_shape(tpe) -> None:  # type: ignore[no-untyped-def]
    parts = 4
    src = repartition_blocks()
    spy = SpyParslBackend(make_parsl_backend(tpe))
    res = run_bounded(lambda: m43_repartition(BACKEND, src, parts, runner=make_runner(spy)))

    t = int(res.witness.n_producer_tasks)
    assert t == 2, "T must be min(n_workers=2, n_src=6)"
    assert spy.fn_names() == Counter(
        {"_dask_map_write": t, "_dask_pick": t * parts, "_dask_gather": parts}
    ), (
        f"the moved m43 engine must drive the m43 T + T·P + P shape through ParslBackend(TPE), "
        f"got {dict(spy.fn_names())} — zero picks means the relay engine answered instead "
        "(caveat: on TPE this pins the submit shape, not separate-process execution)"
    )
    # parity: the moved engine still equals the local engine bit-for-bit
    local = run_repartition(BACKEND, src, parts)
    assert res.dest_block_hashes == local.dest_block_hashes
    assert result_dest_keys(res.value) == route_oracle_dest_keys(src, parts)
    assert total_result_rows(res.value) == sum(len(b) for b in src)


def test_m43_join_runs_on_tpe_with_nonzero_picks(tpe) -> None:  # type: ignore[no-untyped-def]
    parts = 8
    left, right = join_equal_sides()  # parts-keyed rule says SHUFFLE at 8 (frozen-verified gap)
    spy = SpyParslBackend(make_parsl_backend(tpe))
    res = run_bounded(lambda: m43_join(BACKEND, left, right, parts, runner=make_runner(spy)))

    t_l = t_r = 2  # min(n_workers=2, 2 blocks per side)
    names = spy.fn_names()
    expected = Counter(
        {
            "_dask_map_write": t_l + t_r,
            "_dask_pick": t_l * parts + t_r * parts,
            "_dask_gather_join": parts,
        }
    )
    assert names == expected, (
        f"the m43 shuffle-join shape (map t_l+t_r, NONZERO pick t_l·P + t_r·P, gather_join P — "
        f"shuffle.py:414-423) diverged: got {dict(names)}, want {dict(expected)} — zero picks is "
        "the relay engine, extra names are a hidden stage (this arm also keeps "
        "tasks_engine.dask_run_join exercised on the parsl job, plan tests-B2)"
    )
    assert names["_dask_broadcast_join_part"] == 0
    assert res.witness.broadcast_chosen is False
    assert observed_join_all(res.value) == dup_join_all(left, right, parts), (
        "m43-engine-on-TPE join diverged from the duplicating relational oracle"
    )


def test_htex_is_refused_by_require_peer_before_any_work(htex) -> None:  # type: ignore[no-untyped-def]
    """The §1.1.4 crux frozen: HTEX reports peer_data_movement=False, so the m43 engine's own
    `_require_peer` gate refuses it BEFORE any submit — head-node routing must never silently
    enter the peer-shuffle engine (that route is the relay engine's, honestly labeled)."""
    spy = SpyParslBackend(make_parsl_backend(htex))
    runner = make_runner(spy)
    src = repartition_blocks()
    left, right = join_equal_sides()

    with pytest.raises(NotImplementedError, match="peer data movement"):
        m43_repartition(BACKEND, src, 4, runner=runner)
    with pytest.raises(NotImplementedError, match="peer data movement"):
        m43_join(BACKEND, left, right, 4, runner=runner)

    assert spy.submitted == [], (
        f"the gate must refuse BEFORE submitting any work; {len(spy.submitted)} task(s) reached the backend"
    )
    assert spy.broadcast_tokens == [], "the gate must refuse BEFORE broadcasting any payload"
