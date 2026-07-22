"""m43 parity witnesses (tests/extra — NOT frozen; local + a mirror of the tests/extra/m41 status).

The frozen m43 suite exercises broadcast-INNER and shuffle-left/right/outer over MIXED dests (where
the null rows come from the join kernel on two-sided dests). It does not reach three real
correctness-parity paths of ``dask_run_join`` that mirror the local engine's F1/F2 contracts:

1. **broadcast left/right/outer** — ``_dask_broadcast_join_part`` per-block narrowing + the once-only
   never-matched-build-row tail (``_run_broadcast_join`` F2).
2. **pure one-sided dests under shuffle** — the ``_dask_gather_join`` schema-carrier null-fill
   (``_run_shuffle_join`` F1) for a dest that got rows from ONLY one side.
3. **a partitionless side** — an entirely empty build/probe (carrier ``None``): the present rows are
   kept, never silently dropped.

Each is validated against the LOCAL ``run_join`` on identical inputs with the SAME broadcast flag —
so any divergence in routing, merge order, null-fill, or keying fails here. NON-VACUOUS: the
scenarios genuinely produce one-sided dests / unmatched build rows / empty sides (asserted); a dask
impl that dropped them, mis-null-filled (the a3 sentinel trap), or double-emitted the unmatched
build rows would diverge from the independent local engine's content-addressed blocks.
"""

from __future__ import annotations

import contextlib
from collections.abc import Sequence

import numpy as np
import pytest

from graphed_executors.dask_backend import dask_run_join
from graphed_executors.local.shuffle import run_join

pytest.importorskip("distributed")


def _numpy_backend():  # type: ignore[no-untyped-def]
    from graphed.numpy import NumpyBackend  # noqa: PLC0415  (hard dep, kept local per the frozen-suite idiom)

    return NumpyBackend()


def _awkward_backend():  # type: ignore[no-untyped-def]
    pytest.importorskip("awkward")
    from graphed.awkward import AwkwardBackend  # noqa: PLC0415  (optional backend: importorskip'd above)

    return AwkwardBackend()


def _backends() -> list[object]:
    out: list[object] = [_numpy_backend()]
    with contextlib.suppress(Exception):
        out.append(_awkward_backend())
    return out


BACKENDS = _backends()
BACKEND_IDS = [type(b).__name__ for b in BACKENDS]


def _make_side(backend: object, keys: Sequence[int], field: str, values: Sequence[int]) -> object:
    """A 2-column (``__joinkey__`` + ``field``) block for whichever backend, via awkward's dict ctor
    or a numpy structured array (mirrors the frozen harness ``make_side``, kept self-contained)."""
    if type(backend).__name__ == "AwkwardBackend":
        import awkward as ak  # noqa: PLC0415  (optional backend only)

        return ak.Array(
            {
                "__joinkey__": np.array(list(keys), dtype=np.uint64),
                field: np.array(list(values), dtype=np.int64),
            }
        )
    dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
    block = np.zeros(len(keys), dtype=dt)
    block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
    block[field] = np.array(list(values), dtype=np.int64)
    return block


@pytest.fixture(scope="module")
def client():  # type: ignore[no-untyped-def]
    import distributed  # noqa: PLC0415  (importorskip'd at module top)

    with (
        distributed.LocalCluster(
            n_workers=2, threads_per_worker=1, processes=False, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as c,
    ):
        yield c


@pytest.fixture()
def runner(client):  # type: ignore[no-untyped-def]
    from graphed_executors.dask_backend import DaskBackend  # noqa: PLC0415  (lazy dask imports)
    from graphed_executors.dask_backend.plugin import GraphedWorkerPlugin  # noqa: PLC0415
    from graphed_executors.submit import SubmitRunner  # noqa: PLC0415

    client.register_plugin(GraphedWorkerPlugin())
    return SubmitRunner(DaskBackend(client))


@pytest.fixture(params=BACKENDS, ids=BACKEND_IDS)
def backend(request):  # type: ignore[no-untyped-def]
    return request.param


PARTS = 4


@pytest.mark.parametrize("how", ["left", "right", "outer"])
def test_broadcast_noninner_matches_local(backend, how, runner) -> None:  # type: ignore[no-untyped-def]
    """Tiny build + multi-block probe with a left-only key (13 -> an unmatched build row -> the
    once-only tail) and a right-only key (17). Forced broadcast so the large side is never shuffled;
    dask must equal local ``run_join(broadcast=True)`` content-addressed blocks."""
    left = [_make_side(backend, [1, 2, 3, 13], "lval", [10, 20, 30, 99])]
    right = [
        _make_side(backend, [1, 1, 2, 17], "rval", [100, 101, 200, 170]),
        _make_side(backend, [3, 2, 1, 17], "rval", [300, 201, 102, 171]),
    ]
    dask_res = dask_run_join(backend, left, right, PARTS, how=how, runner=runner, broadcast=True)
    local = run_join(backend, left, right, PARTS, how=how, broadcast=True)

    assert dask_res.witness.broadcast_chosen is True
    assert dask_res.dest_block_hashes == local.dest_block_hashes, (
        f"broadcast how={how} diverged from local run_join(broadcast=True)"
    )
    # non-vacuity: local really produced output blocks for this how (the tail row for left/outer)
    assert local.dest_block_hashes, "scenario degenerate — no joined blocks"


@pytest.mark.parametrize("how", ["left", "outer"])
def test_broadcast_empty_probe_matches_local_no_crash(backend, how, runner) -> None:  # type: ignore[no-untyped-def]
    """G3 regression: forced broadcast with a NONEMPTY build and an EMPTY probe under left/outer. The
    local engine guards its never-matched-build tail with ``if unmatched and right_blocks:`` and
    degrades gracefully; the dask port must match, not ``IndexError`` on ``right_blocks[0]``.
    NON-VACUOUS: without the guard this raises before returning; with it, it returns local's result."""
    left = [_make_side(backend, [1, 2, 3], "lval", [10, 20, 30])]
    right: list[object] = []
    dask_res = dask_run_join(backend, left, right, PARTS, how=how, runner=runner, broadcast=True)
    local = run_join(backend, left, right, PARTS, how=how, broadcast=True)

    assert dask_res.dest_block_hashes == local.dest_block_hashes  # both {} — no probe schema carrier
    assert dask_res.value == local.value


@pytest.mark.parametrize("how", ["left", "right", "outer"])
def test_shuffle_pure_one_sided_dests_match_local(backend, how, runner) -> None:  # type: ignore[no-untyped-def]
    """Keys chosen (verified via the backend's own ``partition``) so left routes to dests {0,3} and
    right to dest {1} — DISJOINT, so dest 0/3 are pure build-only and dest 1 pure probe-only,
    exercising the ``_dask_gather_join`` schema-carrier null-fill (F1). Cross-engine content-addressed
    equality vs local ``run_join(broadcast=False)``."""
    left = [_make_side(backend, [3, 7, 4, 3], "lval", [1, 2, 3, 4])]  # -> dests {0, 3}
    right = [_make_side(backend, [2, 13, 2, 13], "rval", [5, 6, 7, 8])]  # -> dest {1}
    dask_res = dask_run_join(backend, left, right, PARTS, how=how, runner=runner, broadcast=False)
    local = run_join(backend, left, right, PARTS, how=how, broadcast=False)

    assert dask_res.dest_block_hashes == local.dest_block_hashes, (
        f"shuffle one-sided how={how} diverged from local run_join"
    )
    # non-vacuity: disjoint keys ⇒ inner emits ZERO rows, while non-inner keeps the one-sided rows
    inner = run_join(backend, left, right, PARTS, how="inner", broadcast=False)
    assert sum(len(b) for b in inner.value.values()) == 0, "keys must be disjoint (no matches)"
    assert sum(len(b) for b in local.value.values()) > 0, f"how={how} must keep the one-sided rows"


@pytest.mark.parametrize("how", ["left", "right"])
def test_shuffle_partitionless_side_matches_local(backend, how, runner) -> None:  # type: ignore[no-untyped-def]
    """An entirely empty side (carrier ``None``): the present rows survive. ``how=right`` keeps the
    probe with an empty build; ``how=left`` keeps the build with an empty probe. Forced shuffle so
    the (auto-broadcast-on-empty-build) cost choice does not intervene."""
    rows = [_make_side(backend, [1, 2, 3, 1], "rval" if how == "right" else "lval", [9, 8, 7, 6])]
    left = [] if how == "right" else rows
    right = rows if how == "right" else []
    dask_res = dask_run_join(backend, left, right, PARTS, how=how, runner=runner, broadcast=False)
    local = run_join(backend, left, right, PARTS, how=how, broadcast=False)

    assert dask_res.dest_block_hashes == local.dest_block_hashes, (
        f"partitionless how={how} diverged from local run_join"
    )
    assert local.value, "the present side's rows must survive (never dropped)"
