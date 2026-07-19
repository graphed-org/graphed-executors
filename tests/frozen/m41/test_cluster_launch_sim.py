"""M41 theme (c.2) / Target T4 — the ``ClusterExecutor`` launch seam exercised by a MOCK LAUNCHER that
registers CROSS-PROCESS routable addresses, and a fetch driven through the core ``NodeStore.fetch`` seam
(plan §6.1.3, §4.3; decomposition §2 T4, §3(c.2)).

The mock launcher (``launch_routable_cluster``, an exec deliverable) spawns K CHILD processes; each
child stands up a node-local Store served on a genuinely ROUTABLE (non-loopback) address and REGISTERS
its ``(host, port)`` — tagged with its own pid as ``registered_by`` — into the core ``AddressTable``.
The driver then ``put``s a blob to a child's Store (``PUT /blob``) and ``fetch``es it back THROUGH the
core ``NodeStore.fetch`` seam (``GET /blob``), streaming it into a consumer node-local Store.

Non-vacuity on HEAD: the seam is absent, so the in-test imports of ``ClusterExecutor``/``NodeStore``/
``AddressTable`` (core) and ``launch_routable_cluster`` (exec) raise ``ImportError`` — the whole test
ERRORS for the right reason (the §6.1.3 seam does not exist). Even if one reached the private M39
``_HttpCluster``, it serves from a driver-PID daemon THREAD, so ``served_pid == driver_pid`` -> (a) fails.

Discrimination (rejects the traps in decomposition §3(c.2)):
- in-process ``_HttpCluster`` (one PID) -> ``served_pid == driver_pid`` fails;
- a hand-built driver-side table -> ``registered_by`` is the driver, not a child, and fails;
- a loopback bind -> ``is_routable_host(served_host)`` fails;
- a fetch that bypasses the seam -> nothing lands in the consumer Store / the seam counter stays 0.

DETERMINISM: byte-identical comparisons key on the blob CONTENT only; PID / port / ephemeral host are
membership/routability-checked, never byte-compared (decomposition §5). This is the ENV-GATED form
(skips with a reason where the runner exposes only loopback); the never-skip CONTRACT is the ``[core]``
``test_cluster_seam_contract.py``.
"""

from __future__ import annotations

import os

import pytest
from m41_backends import routable_ip, store_bytes_on_disk


def test_mock_launcher_registers_cross_process_routable_addresses_and_fetch_hits_a_child(tmp_path) -> None:  # type: ignore[no-untyped-def]
    # imports INSIDE the test: on HEAD they ImportError (the seam is absent) -> ERROR for the right reason,
    # while the module still COLLECTS cleanly.
    from graphed.core.execution import AddressTable, ClusterExecutor, NodeStore  # noqa: F401, PLC0415

    from graphed_executors.local._transport import is_routable_host  # noqa: PLC0415
    from graphed_executors.local.shuffle import launch_routable_cluster  # noqa: PLC0415

    ip = routable_ip()
    if ip is None:
        pytest.skip("no routable non-loopback address on this runner (the [core] seam CONTRACT never skips)")

    driver_pid = os.getpid()
    k = 3
    sim = launch_routable_cluster(n_nodes=k, store_root=str(tmp_path), advertise_host=ip)
    try:
        assert sim.driver_pid == driver_pid, "sim.driver_pid must be the driver process"
        assert len(sim.child_pids) == k and driver_pid not in sim.child_pids, (
            "the launcher must spawn K genuine CHILD processes distinct from the driver"
        )
        # the address table is the CORE data-only type, populated by the launcher/children.
        assert isinstance(sim.address_table, AddressTable), "sim.address_table must be a core AddressTable"

        payload = b"m41-cluster-seam-" + bytes(range(256)) * 32  # ~8 KiB, distinctive content
        target = sim.node_ids[1]
        digest = sim.put(target, payload)  # PUT /blob to the CHILD hosting `target`
        data = sim.fetch(target, digest)  # GET /blob through the core NodeStore.fetch seam
        w = sim.witness

        # (a) served by a GENUINE CHILD process — not the driver's in-process thread (the HEAD trap).
        assert w.served_pid != driver_pid, "served in the driver PID: this is not a cross-process fetch"
        assert w.served_pid in sim.child_pids, "served_pid is not one of the announced child processes"

        # (b) over a ROUTABLE (non-loopback) address.
        assert is_routable_host(w.served_host), f"served over a non-routable host {w.served_host!r}"
        host, _port = sim.address_table.lookup(target)
        assert is_routable_host(host), f"the registered address is not routable: {host!r}"

        # (c) the AddressTable entry carries registered_by provenance = the registering CHILD, not the driver.
        reg = sim.address_table.registered_by(target)
        assert str(reg) in {str(p) for p in sim.child_pids}, (
            f"registered_by {reg!r} is not a child pid — a driver-side table would fail this"
        )

        # (d) idempotent — a second fetch returns byte-identical CONTENT (keyed on the content hash only).
        data2 = sim.fetch(target, digest)
        assert data == payload and data2 == payload, "the fetched bytes are not the stored blob"

        # (e) the blob was streamed to the consumer node-local Store through the seam (counter + on disk).
        assert w.bulk_fetch_count >= 1, "the fetch-through-seam counter did not advance"
        assert store_bytes_on_disk(sim.consumer_store_dir) >= len(payload), (
            "the fetched blob was not streamed into the consumer node-local Store"
        )
    finally:
        sim.close()
