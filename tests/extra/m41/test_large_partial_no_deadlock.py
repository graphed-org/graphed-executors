"""Peer reduction must not deadlock when a partial exceeds the OS pipe buffer (tests/extra — NOT
frozen).

The ``process_and_reduce`` actor sends before it polls its inbox, so a partial larger than the
~64 KB pipe parks it mid-write until the peer reads — and work-stealing supplies the upward/sideways
edge that closes a wait cycle, so nobody reads:

* **2-cycle** — the fast owner steals the slow owner's last leaf and sends the computed ``leaf``
  partial UP while the slow owner sends its ``node`` partial DOWN. Both block in the write.
* **lock convoy** — an idle worker's tiny ``steal_req`` blocks on the victim inbox's write lock held
  by a third worker mid-partial; the idle worker then stops reading, the victim routes a ``node`` to
  it and blocks too. The request traffic alone closes the cycle — no leaf need be stolen.

Both hang forever — the driver's own recovery broadcast goes through the same blocking put — so each
scenario runs in a spawn child under a hard timeout (see ``_big_partial``).
"""

from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
from _big_partial import BIG, SMALL, plan, run_in_child
from graphed.core.execution import SequentialRunner

from graphed_executors.local import ProcessPoolExecutor

# (n_workers, per-leaf weight in ms). `make_bounds` gives contiguous ranges, so the shapes are:
#   2-cycle: w0 = [fast, fast], w1 = [slow, slow, slow] -> w0 drains, steals w1's last leaf, and both
#            finish a slow leaf at the same moment (w0's steal lands as w1 starts its second).
#   convoy:  w0 = [.3, .3], w1 = [.01, 1, 1], w2 = [.2, .5, .5] -> w0 idles into steal requests while
#            w2's partials are in flight to the busy w1.
TWO_CYCLE = (2, (10, 10, 500, 500, 500))
CONVOY = (3, (300, 300, 10, 1000, 1000, 200, 500, 500))


def _child(n_workers: int, n_floats: int, weights: tuple[int, ...], out_path: str) -> None:
    """Spawn-child entry point: run the scenario and pickle (result, witness) for the parent."""
    executor = ProcessPoolExecutor(max_workers=n_workers, steal=True, monitor=None)
    value = executor.run(plan(n_floats, weights)).value
    Path(out_path).write_bytes(pickle.dumps((value, executor._last_peer_witness)))


def _run(
    scenario: str, n_workers: int, n_floats: int, weights: tuple[int, ...], tmp_path: Path
) -> tuple[np.ndarray, list[dict[str, int]]]:
    out = tmp_path / f"{scenario}-{n_floats}.pkl"
    run_in_child(scenario, _child, (n_workers, n_floats, weights, str(out)))
    result: tuple[np.ndarray, list[dict[str, int]]] = pickle.loads(out.read_bytes())
    return result


def _assert_sane(value: np.ndarray, witness: list[dict[str, int]], weights: tuple[int, ...]) -> None:
    assert sum(w["processed"] for w in witness) == len(weights)  # every leaf ran exactly once
    assert sum(w["peer_sends"] for w in witness) > 0  # partials really crossed the wire
    assert np.array_equal(value, np.full(value.size, float(len(weights))))


def test_two_cycle_steal_return_vs_node_send(tmp_path: Path) -> None:
    n_workers, weights = TWO_CYCLE
    baseline = SequentialRunner().run(plan(BIG, weights)).value

    small, small_wit = _run("2-cycle control", n_workers, SMALL, weights, tmp_path)
    _assert_sane(small, small_wit, weights)

    big, wit = _run("2-cycle", n_workers, BIG, weights, tmp_path)
    _assert_sane(big, wit, weights)
    assert np.array_equal(big, baseline)
    # the cycle needs the upward `leaf` return, so the steal must actually have engaged. Sized for
    # reliability the way tests/frozen/m38/test_steal.py explains: the owner's 0.5 s slow leaf is a
    # wide window for the idle peer's handshake, and the grant is independent of payload size.
    assert sum(w["steals"] for w in wit) > 0
    assert sum(w["steals"] for w in small_wit) > 0


def test_lock_convoy_steal_request_behind_a_big_write(tmp_path: Path) -> None:
    n_workers, weights = CONVOY
    baseline = SequentialRunner().run(plan(BIG, weights)).value

    small, small_wit = _run("convoy control", n_workers, SMALL, weights, tmp_path)
    _assert_sane(small, small_wit, weights)

    big, wit = _run("convoy", n_workers, BIG, weights, tmp_path)
    _assert_sane(big, wit, weights)
    assert np.array_equal(big, baseline)
    # the convoy is steal-REQUEST traffic colliding with a big write, so the witness that the scenario
    # engaged is that an idle worker asked at all (`asked`), not that a leaf changed hands (`steals`).
    assert sum(w["asked"] for w in wit) > 0
    assert sum(w["asked"] for w in small_wit) > 0
