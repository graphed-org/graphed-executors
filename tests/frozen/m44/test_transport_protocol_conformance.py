"""m44 protocol conformance — the dask-backed endpoints ARE graphed ``WorkerTransport``s
(plan §1.1; blueprint §3 `test_transport_protocol_conformance`). Tier B (processes=True): the spec
crosses a real process boundary and the round-trip rides real sockets.

Witnesses: ``runtime_checkable`` isinstance INSIDE a worker task; send->True then a recv round-trip
carrying (src, message) with the true sender address; ``recv(timeout)`` -> None on empty (the
non-blocking contract); an ``inbox_maxsize=1`` flood makes some ``send()`` return False AND bumps
the receiver-side ``sends_dropped`` counter (drop-on-full is real, not a stub's constant True);
the asymmetric driver endpoint (address ``"driver"``) satisfies the same Protocol, receives the
worker->driver channel, and its ``broadcast`` lands in every worker inbox.

Discriminates: a stub whose ``send`` always returns True (the flood probes the drop signal), one
that blocks forever on a full inbox (the flood is bounded by the pinned-task timeout), and a
driver endpoint that fakes ``peers()``/``address``."""

from __future__ import annotations

import uuid

import pytest
from graphed.core.execution import WorkerTransport
from transport_harness import (
    build_dask_backend,
    conf_flood,
    conf_probe,
    conf_recv,
    conf_send,
    install_transport_plugin,
    make_transport_spec,
    open_driver_endpoint,
    pinned_call,
    probe_epoch_counters,
    sorted_worker_addresses,
    transport_cluster,
)

pytest.importorskip("distributed")


@pytest.fixture(scope="module")
def client():
    with transport_cluster(2, processes=True) as c:
        yield c


def test_worker_endpoint_conforms_and_round_trips(client) -> None:  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    a0, a1 = sorted_worker_addresses(client)
    spec = make_transport_spec(f"m44-conf-{uuid.uuid4().hex}", (a0, a1))

    obs = pinned_call(client, conf_probe, spec, worker=a0)
    assert obs["is_transport"] is True, "the worker endpoint must satisfy the runtime_checkable Protocol"
    assert obs["address"] == a0, "the endpoint address must be the dask worker address"
    assert set(obs["peers"]) == {a1, "driver"}, "default overlay: the other worker + the driver"
    assert obs["empty_recv"] is None, "recv(timeout) on an empty inbox must return None, not block"
    assert obs["empty_poll"] == [], "poll() on an empty inbox must return an empty drain"

    assert pinned_call(client, conf_send, spec, a0, ("hello", 1), worker=a1) is True
    got = pinned_call(client, conf_recv, spec, 60.0, worker=a0)
    assert tuple(got) == (a1, ("hello", 1)), f"round-trip lost src or payload: {got!r}"


def test_driver_endpoint_conforms_and_carries_both_channels(client) -> None:  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    a0, a1 = sorted_worker_addresses(client)
    spec = make_transport_spec(f"m44-conf-{uuid.uuid4().hex}", (a0, a1))
    driver_ep = open_driver_endpoint(build_dask_backend(client), spec)
    try:
        assert isinstance(driver_ep, WorkerTransport)
        assert driver_ep.address == "driver", "'driver' is the reserved endpoint address"
        assert set(driver_ep.peers()) == {a0, a1}

        # open worker-side state on BOTH workers before any traffic
        pinned_call(client, conf_probe, spec, worker=a0)
        pinned_call(client, conf_probe, spec, worker=a1)

        # worker -> driver (the log_event channel behind send("driver", ...))
        assert pinned_call(client, conf_send, spec, "driver", ("root", 42), worker=a0) is True
        got = driver_ep.recv(timeout=90.0)
        assert got is not None and tuple(got) == (a0, ("root", 42)), f"driver channel lost the root: {got!r}"

        # driver -> all workers (the reliable done/teardown path)
        driver_ep.broadcast(("done",))
        got0 = pinned_call(client, conf_recv, spec, 60.0, worker=a0)
        got1 = pinned_call(client, conf_recv, spec, 60.0, worker=a1)
        assert tuple(got0) == ("driver", ("done",)), f"broadcast missed {a0}: {got0!r}"
        assert tuple(got1) == ("driver", ("done",)), f"broadcast missed {a1}: {got1!r}"
    finally:
        driver_ep.close()


def test_full_inbox_drops_are_signalled_and_counted(client) -> None:  # type: ignore[no-untyped-def]
    install_transport_plugin(client)
    a0, a1 = sorted_worker_addresses(client)
    epoch = f"m44-conf-{uuid.uuid4().hex}"
    spec = make_transport_spec(epoch, (a0, a1), inbox_maxsize=1)

    pinned_call(client, conf_probe, spec, worker=a0)  # create the size-1 inbox; nothing drains it
    results = pinned_call(client, conf_flood, spec, a0, 3, worker=a1, timeout_s=240.0)
    assert results[0] is True, "a bounded inbox must still accept the first message"
    assert False in results, (
        "flooding a maxsize=1 inbox with no consumer must surface send()==False (the contract's "
        "drop signal) — a transport that never drops has unbounded buffers"
    )
    counters = client.run(probe_epoch_counters, epoch)
    assert counters[a0].get("sends_dropped", 0) >= 1, (
        "the receiver must COUNT its inbox-full rejections (the sends_dropped witness)"
    )
