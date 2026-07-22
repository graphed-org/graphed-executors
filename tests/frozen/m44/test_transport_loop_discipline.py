"""m44 threading bridge — handlers run ON the worker's IO loop and do nothing but an epoch check
and a queue put; receivers drain OFF the loop from task threads (plan §1.1 handler discipline;
blueprint §3 `test_transport_loop_discipline`). Tier A (processes=False) deliberately: the
handler-branch execution happens in the driver process where the coverage run can see it (FU3).

Witnesses: after real wire traffic, ``recv_on_loop == recv_invocations >= 1`` (every handler
invocation executed on the loop thread — impl-recorded thread ids, not clocks); the receiving
``recv()`` call ran on a task thread distinct from the worker's loop thread; a bounded
(``inbox_maxsize=1``) flood completes and is REJECTED without wedging the loop — proven by a
fresh epoch round-tripping immediately afterwards on the same workers.

Discriminates: an impl that bridges sync work into the handler (deadlocks/hangs the bounded
calls); one that runs handlers off-loop (``recv_on_loop`` diverges from ``recv_invocations``);
one whose blocking ``recv`` occupies the IO loop (the off-loop witness fails)."""

from __future__ import annotations

import uuid

import pytest
from transport_harness import (
    conf_flood,
    conf_probe,
    conf_send,
    conf_thread_probe,
    install_transport_plugin,
    make_transport_spec,
    pinned_call,
    probe_epoch_counters,
    sorted_worker_addresses,
    transport_cluster,
)

pytest.importorskip("distributed")


def test_handlers_stay_on_the_loop_and_receivers_drain_off_it() -> None:
    with transport_cluster(2, processes=False) as client:
        install_transport_plugin(client)
        a0, a1 = sorted_worker_addresses(client)
        epoch = f"m44-loop-{uuid.uuid4().hex}"
        spec = make_transport_spec(epoch, (a0, a1))

        pinned_call(client, conf_probe, spec, worker=a0)  # open the receiving state
        assert pinned_call(client, conf_send, spec, a0, ("m", 0), worker=a1) is True

        obs = pinned_call(client, conf_thread_probe, spec, 60.0, worker=a0)
        assert obs["got"] is not None and tuple(obs["got"]) == (a1, ("m", 0))
        assert obs["off_loop"] is True, (
            "recv() executed on the worker's IO-loop thread — blocking drains must live on task "
            "threads, never the loop"
        )

        counters = client.run(probe_epoch_counters, epoch)[a0]
        assert counters.get("recv_invocations", 0) >= 1
        assert counters.get("recv_on_loop", 0) == counters["recv_invocations"], (
            f"{counters}: every graphed_transport_recv invocation must execute on the worker's "
            "event-loop thread (the dispatch path P2P relies on) — an off-loop handler means the "
            "seam is not worker.handlers dispatch"
        )


def test_bounded_flood_is_rejected_without_wedging_the_loop() -> None:
    with transport_cluster(2, processes=False) as client:
        install_transport_plugin(client)
        a0, a1 = sorted_worker_addresses(client)
        flood_epoch = f"m44-loop-{uuid.uuid4().hex}"
        flood_spec = make_transport_spec(flood_epoch, (a0, a1), inbox_maxsize=1)

        pinned_call(client, conf_probe, flood_spec, worker=a0)  # size-1 inbox, no consumer
        results = pinned_call(client, conf_flood, flood_spec, a0, 3, worker=a1, timeout_s=240.0)
        assert results[0] is True and False in results, f"flood outcome {results!r}"
        counters = client.run(probe_epoch_counters, flood_epoch)[a0]
        assert counters.get("sends_dropped", 0) >= 1, "inbox-full rejections must be counted"

        # the loop survived: a FRESH epoch round-trips immediately on the same pair of workers
        after_epoch = f"m44-loop-{uuid.uuid4().hex}"
        after_spec = make_transport_spec(after_epoch, (a0, a1))
        pinned_call(client, conf_probe, after_spec, worker=a0)
        assert pinned_call(client, conf_send, after_spec, a0, ("alive", 1), worker=a1) is True
        got = pinned_call(client, conf_thread_probe, after_spec, 60.0, worker=a0)
        assert got["got"] is not None and tuple(got["got"]) == (a1, ("alive", 1)), (
            "the loop never served traffic again after the flood — a blocking handler wedged it"
        )
