"""M41 theme (e) — bulk transfer throughput MEASURED through the ``fetch()`` endpoint (plan §8 M41;
decomposition §3(e)).

A large blob (2 MB) is moved through the bulk ``fetch()`` endpoint of the cross-process cluster sim; we
record ``bytes_transferred`` + elapsed and derive a throughput. This proves the bulk data plane actually
MOVES the whole blob (the counter is tied to the real bytes in the consumer Store) and does not double-
count an idempotent re-fetch.

SCOPE HONESTY (required, decomposition §3(e)): throughput is measured on the SINGLE-MACHINE cross-process
sim — a sane, complete number — NOT a cluster wall-clock SLA and NOT gated on any absolute MB/s (real
multi-host launch is deferred; there is no real network to benchmark). So we assert throughput is finite
and positive, never that it clears a bandwidth bar.

Non-vacuity on HEAD: ``launch_routable_cluster`` (+ the core seam) and ``witness.bytes_transferred`` are
absent -> the in-test import ``ImportError``s (right reason: the bulk endpoint does not exist). ENV-GATED
like (c.2); the never-skip contract is the ``[core]`` seam test.
"""

from __future__ import annotations

import math
import time

import numpy as np
import pytest
from m41_backends import routable_ip, store_bytes_on_disk

_BLOB_BYTES = 2_000_000


def test_bulk_fetch_moves_the_whole_blob_and_re_fetch_does_not_double_count(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from graphed.core.execution import NodeStore  # noqa: F401, PLC0415  (seam absent on HEAD -> ImportError)

    from graphed_executors.local.shuffle import launch_routable_cluster  # noqa: PLC0415

    ip = routable_ip()
    if ip is None:
        pytest.skip("no routable non-loopback address on this runner (the [core] seam CONTRACT never skips)")

    blob = (np.arange(_BLOB_BYTES, dtype=np.uint64) % 251).astype(np.uint8).tobytes()  # deterministic 2 MB
    sim = launch_routable_cluster(n_nodes=2, store_root=str(tmp_path), advertise_host=ip)
    try:
        target = sim.node_ids[1]
        digest = sim.put(target, blob)

        t0 = time.perf_counter()
        data = sim.fetch(target, digest)  # the bulk transfer through the fetch() endpoint
        elapsed = time.perf_counter() - t0

        assert data == blob, "the bulk fetch did not return the stored blob"
        w = sim.witness

        # (moved the full blob, cross-checked vs the consumer Store contents on disk).
        assert w.bytes_transferred == _BLOB_BYTES, (
            f"bytes_transferred {w.bytes_transferred} != blob size {_BLOB_BYTES}"
        )
        assert store_bytes_on_disk(sim.consumer_store_dir) >= _BLOB_BYTES, (
            "the blob was not streamed into the consumer node-local Store (counter unbacked by real bytes)"
        )

        # (throughput is a real, finite, positive number — NOT gated on absolute MB/s, see scope honesty).
        assert elapsed > 0.0
        throughput = w.bytes_transferred / elapsed
        assert math.isfinite(throughput) and throughput > 0.0, "throughput must be finite and positive"

        # (idempotent re-fetch of an already-present digest adds 0 to bytes_transferred).
        before = w.bytes_transferred
        again = sim.fetch(target, digest)
        assert again == blob
        assert sim.witness.bytes_transferred == before, "an idempotent re-fetch double-counted bytes_transferred"
    finally:
        sim.close()
