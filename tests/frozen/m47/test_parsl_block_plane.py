"""m47 block plane — shuffle bytes travel peer-to-peer over the REAL ``/pull`` route, never
through the driver, coalesced within the ``<= k*k`` incast bound, and evicted after serve (plan
§1.4 block plane, tests-M2/tests-M-c; blueprint §3 `test_parsl_block_plane` — this module carries
the IN-TASK half of the reshaped anti-super-linear guard).

Witnesses: Σ holder ``bytes_served`` == the INDEPENDENT cross-fragment byte oracle (the pinned
ownership rules — producer t = contiguous ascending split, dest owner = d % k over sorted
addresses — routed with the golden-pinned ``partition`` and ``to_wire``'d test-side: fragments a
peer owns itself never ride HTTP, so a self-loopback or driver-relay implementation breaks the
equality); every ``serve_pid`` == that peer's ``endpoint_pid`` != the driver pid (serving
provenance); **``pull_requests_served`` summed <= k*k = 4 at parts=8 > k=2** — the counter is a
BUILT m47 Implementation Target (m44's frozen block-plane test has NO request-count assertion to
inherit, verified), and the in-test scenario guard asserts the per-fragment regression bound
(cross fragments > k*k, measured 7 > 4) so the ceiling is provably discriminating;
``store_blocks_at_return == 0`` on every peer (evict-after-serve + teardown — peer state must die
with the task); result hashes byte-identical to the local engine.

Discriminates: a driver-relay plane (bytes_served == 0 / oracle mismatch / driver-pid
provenance); a per-fragment (uncoalesced) puller (7 requests > 4 at this scenario); a plane
piggybacking blocks on /msg (the /pull-route counters stay 0); a store that never evicts."""

from __future__ import annotations

import os
import sys

import pytest
from parsl_transport_harness import (
    cross_fragment_bytes,
    make_parsl_backend,
    numpy_p2p_adapter,
    p2p_htex_pool,
    peer_rows,
    repartition_blocks,
    run_bounded,
    transport_repartition,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

PARTS = 8  # parts > k is LOAD-BEARING: at parts <= k a per-fragment puller could stay under k*k
K = 2


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with p2p_htex_pool(workers=K) as ex:
        yield ex


def test_bytes_move_peer_to_peer_coalesced_and_evicted(htex) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_p2p_adapter()
    src = repartition_blocks(adapter, copies=2)
    local = run_repartition(adapter.backend, src, PARTS)

    # in-test scenario guard: the ladder arithmetic that makes the <= k*k ceiling discriminating
    oracle_bytes, n_cross = cross_fragment_bytes(adapter, src, PARTS, K)
    assert oracle_bytes > 0 and n_cross > K * K, (
        f"scenario guard misfired: {n_cross} cross fragments must exceed k*k={K * K} so a "
        "per-fragment puller provably trips the bound (measured 7 > 4 on this scenario)"
    )

    r = run_bounded(
        lambda: transport_repartition(
            adapter.backend, src, PARTS, pbackend=make_parsl_backend(htex), workers=K
        )
    )
    assert r.dest_block_hashes == local.dest_block_hashes, "block-plane bytes drifted from local"

    rows = peer_rows(r)
    assert sorted(rows) == ["w0", "w1"]
    driver_pid = os.getpid()

    served_total = 0
    for addr, c in rows.items():
        served = int(c.get("bytes_served", 0))
        assert served > 0, (
            f"peer {addr} served no block bytes — with interleaved keys every producer holds "
            "fragments the REMOTE gather owner must pull; zero means the bytes rode task returns "
            "or the driver (the head-node route the p2p engine exists to end)"
        )
        served_total += served
        serve_pid = int(c.get("serve_pid", 0))
        assert serve_pid == int(c.get("endpoint_pid", -1)), (
            f"peer {addr}: blocks served from pid {serve_pid}, not the peer's own endpoint "
            f"process {c.get('endpoint_pid')}"
        )
        assert serve_pid != driver_pid, "block bytes were served from the DRIVER process"
        assert int(c.get("store_blocks_at_return", -1)) == 0, (
            f"peer {addr} returned with {c.get('store_blocks_at_return')} blocks still in its "
            "store — evict-after-serve/teardown missing (peer state must die with the task)"
        )

    assert served_total == oracle_bytes, (
        f"Σ bytes_served={served_total} != the independent cross-fragment oracle {oracle_bytes} — "
        "either self-owned fragments loop back through HTTP, a fragment was re-pulled after "
        "evict (F5), or bytes moved off the /pull route"
    )

    pulls = sum(int(c.get("pull_requests_served", 0)) for c in rows.values())
    assert 0 < pulls <= K * K, (
        f"pull_requests_served={pulls}: pulls must coalesce to one request per (puller, holder) "
        f"batch — at most k*k={K * K}, while a per-fragment regression needs ~{n_cross} (> k*k) "
        "on this scenario (the T2 incast bound, tests-M-c)"
    )
