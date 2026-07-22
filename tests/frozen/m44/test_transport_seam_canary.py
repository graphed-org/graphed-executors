"""m44 seam canary — the ``worker.handlers`` mutation is an undocumented-but-stable seam; drift
must fail LOUDLY at ``register_plugin`` time, never corrupt a run (plan §1.7, FU4-softened;
blueprint §3 `test_transport_seam_canary`).

Arm 1: on a healthy cluster, registration leaves ``canary_ok`` True on EVERY worker (the setup
self-RPC ping ran) and the three ``graphed_*`` ops registered in ``worker.handlers``.
Arm 2: with ``worker.handlers`` frozen read-only BEFORE registration (the drift model — a future
distributed making the dict unwritable), ``register_plugin`` raises a ``RuntimeError`` naming the
graphed-transport seam AT REGISTER TIME — before any run exists.

The two arms together kill a fake canary: hard-coding ``canary_ok = True`` without exercising the
seam passes arm 1 but cannot raise in arm 2 (only a canary that really writes+pings the seam
notices the frozen dict). Per FU4 no built-in dask op name is asserted — only what m44 itself
depends on."""

from __future__ import annotations

import pytest
from transport_harness import (
    freeze_worker_handlers,
    install_transport_plugin,
    probe_plugin,
    transport_cluster,
)

pytest.importorskip("distributed")

REQUIRED_OPS = {"graphed_transport_recv", "graphed_block_pull", "graphed_transport_ping"}


def test_canary_is_green_on_every_worker_after_registration() -> None:
    with transport_cluster(2, processes=False) as client:
        install_transport_plugin(client)
        probes = client.run(probe_plugin)
        assert len(probes) == 2
        for addr, probe in probes.items():
            assert probe["present"], f"{addr}: plugin missing from worker.extensions['graphed-transport']"
            assert probe["canary_ok"], (
                f"{addr}: canary_ok is not True after registration — the setup self-RPC ping never "
                "ran or never succeeded (§1.7)"
            )
            assert set(probe["ops"]) >= REQUIRED_OPS, (
                f"{addr}: registered graphed_* ops {probe['ops']} are missing one of {sorted(REQUIRED_OPS)}"
            )


def test_seam_drift_fails_loudly_at_register_time() -> None:
    with transport_cluster(2, processes=False) as client:
        assert all(client.run(freeze_worker_handlers).values())
        with pytest.raises(RuntimeError) as excinfo:
            install_transport_plugin(client)
        message = str(excinfo.value)
        assert "graphed-transport" in message and "seam" in message, (
            f"the drift failure must NAME the graphed-transport seam so an operator can act on it; "
            f"got: {message}"
        )
