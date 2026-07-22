"""m44 stale-epoch rejection — the P2P run_id guard verbatim: a message carrying an unknown OR
completed-and-purged epoch is refused with the stale marker, counted, and can never corrupt a run
(plan §1.5 run epoch; blueprint §3 `test_transport_epoch_guard`).

Mechanism: a pinned worker task fires RAW ``graphed_transport_recv`` RPCs (the real wire path,
via ``worker.rpc``) at a live plugin — first with a bogus nonce, then with the nonce of a run
that already COMPLETED (whose state the teardown purged). Witnesses: both replies are
``{"accepted": False, "stale": True}``; the plugin's ``stale_epoch_rejects`` counter advanced by
both; runs before/after the stale traffic stay byte-identical to the local oracle.

Discriminates: an engine without the guard accepts the stale payload (reply ``accepted: True``,
counter still 0) and risks combining dead-epoch traffic into a live run; an engine that never
purges keeps the completed epoch alive and fails the second stale reply."""

from __future__ import annotations

import pickle
import uuid

import pytest
from transport_harness import (
    build_dask_backend,
    fire_raw_recv,
    install_transport_plugin,
    numpy_adapter,
    pinned_call,
    probe_plugin,
    repartition_blocks,
    sorted_worker_addresses,
    transport_cluster,
    transport_repartition,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip("distributed")

PARTS = 8


def test_stale_epochs_are_rejected_counted_and_never_corrupt_a_run() -> None:
    adapter = numpy_adapter()
    src = repartition_blocks(adapter)
    local = run_repartition(adapter.backend, src, PARTS)
    payload = pickle.dumps(("node", 1, 0, 999_999))  # a well-formed reduction-class message

    with transport_cluster(2, processes=False) as client:
        install_transport_plugin(client)
        a0, a1 = sorted_worker_addresses(client)
        dbackend = build_dask_backend(client)

        # (1) an unknown epoch is refused with the stale marker, over the REAL rpc wire path
        reply = pinned_call(client, fire_raw_recv, a0, f"m44-bogus-{uuid.uuid4().hex}", payload, worker=a1)
        assert reply.get("accepted") is False and reply.get("stale") is True, (
            f"unknown-epoch message was not stale-refused: {reply!r}"
        )

        # (2) a clean run under a live epoch is byte-identical to the local engine
        r1 = transport_repartition(adapter.backend, src, PARTS, dbackend=dbackend)
        assert r1.dest_block_hashes == local.dest_block_hashes
        completed_nonce = next(iter(r1.transport.epoch_nonces))

        # (3) the completed epoch is DEAD: late traffic from it is refused (purge closes the epoch)
        reply2 = pinned_call(client, fire_raw_recv, a0, completed_nonce, payload, worker=a1)
        assert reply2.get("accepted") is False and reply2.get("stale") is True, (
            f"a message from the completed (purged) epoch was accepted: {reply2!r} — exactly the "
            "stale-shard corruption the run_id guard exists to reject"
        )

        # (4) both rejections were counted on the receiving plugin
        probes = client.run(probe_plugin)
        assert probes[a0]["stale_epoch_rejects"] >= 2, (
            f"stale_epoch_rejects={probes[a0]['stale_epoch_rejects']}: the guard must COUNT its "
            "rejections (the frozen witness)"
        )

        # (5) a subsequent run remains byte-identical — the stale traffic corrupted nothing
        r2 = transport_repartition(adapter.backend, src, PARTS, dbackend=dbackend)
        assert r2.dest_block_hashes == local.dest_block_hashes
