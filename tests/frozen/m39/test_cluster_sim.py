"""M39 (f, B4 integration) — cross-process cluster-sim over a routable transport (plan §6).

Workers bound and announcing genuinely NON-LOOPBACK addresses, each with its OWN node-local Store dir,
exercise real cross-"node" push-announce / pull-fetch: a consumer's input blocks come from ANOTHER
node's Store. This is the environment-gated form (ADV1): it provisions a routable address and
SKIPS-WITH-EXPLICIT-REASON only where the runner exposes solely loopback — the never-skipping B4
discrimination lives in ``test_advertise_host.py`` so it is never silently lost.
"""

from __future__ import annotations

import socket

import pytest
from exchange_backends import (
    BackendAdapter,
    adapters,
    expected_dest_keys,
    observed_dest_keys,
    scenario_src_blocks,
)
from graphed_exec_local.shuffle import run_repartition

from graphed_exec_local._transport import is_routable_host

ADAPTERS = adapters()


def _routable_ip() -> str | None:
    """A concrete non-loopback IP for this host, or None (then the integration form skips-with-reason)."""
    try:
        candidate = socket.gethostbyname(socket.gethostname())
        if is_routable_host(candidate):
            return candidate
    except OSError:
        pass
    # last resort: the address a UDP socket would source from toward a public IP (no packet is sent)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            candidate = s.getsockname()[0]
        finally:
            s.close()
        if is_routable_host(candidate):
            return candidate
    except OSError:
        pass
    return None


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> BackendAdapter:
    return request.param


def test_blocks_move_between_distinct_node_stores(adapter: BackendAdapter, tmp_path) -> None:  # type: ignore[no-untyped-def]
    ip = _routable_ip()
    if ip is None:
        pytest.skip("no routable non-loopback address on this runner (B4 pure-logic form covers the rule)")
    src = scenario_src_blocks(adapter)
    res = run_repartition(
        adapter.backend, src, parts=4, workers=3, comms="http", store_root=tmp_path, advertise_host=ip
    )
    w = res.witness
    # each simulated node has its OWN Store dir (§6.2), and there are genuinely several of them.
    assert len(set(w.node_store_dirs.values())) >= 2, "each node needs its own Store root"
    # (f) witness: at least one gather fetched a block from ANOTHER node's Store.
    assert w.cross_node_fetches > 0, "a consumer's inputs must have come from another node's Store"
    # (B4) every announced/dialed address is routable — NOT loopback, NOT 0.0.0.0.
    assert w.node_hosts, "the run must record the announced node hosts"
    assert all(is_routable_host(h) for h in w.node_hosts), f"loopback announced: {w.node_hosts}"
    # and the shuffle is still correct across nodes.
    assert observed_dest_keys(adapter, res.value) == expected_dest_keys(adapter, src, 4)
