"""m44 worker-death semantics — losing a participating worker mid-run triggers ONE whole-run
restart under a fresh epoch (the P2P model, plan §1.5); with the restart budget exhausted it
surfaces as an attributed ``StageError``, never a raw ``KilledWorker`` and NEVER a hang (plan
§8 F2/F9; blueprint §3 `test_transport_worker_death`).

Deterministic mid-run kill, no timing races (the m43 gated-kill mechanism, F9): the
``GatedTransportBackend`` marks every stage-1 ``partition`` call (``pmark``: pid+address) and
BLOCKS every gather's ``from_wire`` on a gate file (``gstart`` mark). The driver waits until
stage 1 fully ran AND a block-HOLDING worker is provably blocked inside a gather, hard-kills
exactly that worker, opens the gate, and joins the run thread under a HARD timeout — the
F2-mandated hang guard, because (measured on distributed 2026.7.1, recorded in the harness) a
strict-pinned task on a dead worker raises ``distributed.scheduler.KilledWorker`` promptly ONLY
under ``allowed-failures=0``; under the default config it hangs forever, so this module pins
``allowed_failures=0`` on its dedicated clusters.

Witnesses: restart arm — result bit-for-bit == the plain-backend local oracle (F9: hashes are
address/k-relabel invariant), ``epoch_restarts == 1``, two nonces, stage-1 re-ran under the fresh
epoch (pmark count STRICTLY exceeds n_src), and no epoch of the run survives on any live worker
(old-epoch purge). Budget-0 arm — a ``StageError`` (type-pinned, not KilledWorker) naming the
victim worker address.

Discriminates: a silent-wrong-answer engine that reuses the dead holder's stale blocks (byte
divergence), one that never restarts (pmark count stays n_src / hang), and raw-KilledWorker
leakage to the user."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest
from graphed.debug import StageError
from transport_harness import (
    GatedTransportBackend,
    build_dask_backend,
    install_transport_plugin,
    kill_worker,
    live_worker_pids,
    mark_addresses,
    mark_files,
    numpy_adapter,
    poll_until,
    probe_active_epochs,
    repartition_blocks,
    transport_cluster,
    transport_repartition,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip("distributed")

PARTS = 8


def _run_death_scenario(client: Any, tmp_path: Path, *, epoch_restarts_allowed: int) -> tuple[dict[str, Any], Any, str, int, str]:
    """Drive a gated repartition, kill one block-holding worker mid-gather, open the gate, join
    under the F2 hard timeout. Returns (outcome, oracle, mark_dir, n_src, victim_address)."""
    adapter = numpy_adapter()
    src = repartition_blocks(adapter)
    n_src = len(src)
    mark_dir = tmp_path / "marks"
    mark_dir.mkdir()
    gate = tmp_path / "gate"
    gated = GatedTransportBackend(adapter.backend, str(mark_dir), str(gate))
    # the oracle uses the PLAIN backend (no gate): the wrapper is data-transparent by construction
    oracle = run_repartition(adapter.backend, src, PARTS)

    install_transport_plugin(client)
    dbackend = build_dask_backend(client)
    outcome: dict[str, Any] = {}

    def drive() -> None:
        try:
            outcome["result"] = transport_repartition(
                gated, src, PARTS, dbackend=dbackend, epoch_restarts_allowed=epoch_restarts_allowed
            )
        except BaseException as exc:  # surfaced after join — the driver thread must never hide it
            outcome["error"] = exc

    thread = threading.Thread(target=drive, daemon=True)
    thread.start()
    try:
        # (1) stage 1 fully ran once AND (2) a block-holding worker is blocked inside a gather
        def victim_ready() -> bool:
            if len(mark_files(str(mark_dir), "pmark")) < n_src:
                return False
            return bool(
                set(mark_addresses(str(mark_dir), "pmark")) & set(mark_addresses(str(mark_dir), "gstart"))
            )

        poll_until(lambda: victim_ready() or not thread.is_alive(), timeout_s=120.0)
        assert "error" not in outcome, f"run raised before the kill: {outcome.get('error')!r}"
        assert victim_ready(), "scenario misfired: stage 1 or a blocked gather never materialised"

        victims = set(mark_addresses(str(mark_dir), "pmark")) & set(mark_addresses(str(mark_dir), "gstart"))
        victim = sorted(victims)[0]
        pids_before = live_worker_pids(client)
        assert victim in pids_before, "victim address is not a live worker"
        victim_pid = pids_before[victim]

        kill_worker(client, victim)
        poll_until(lambda: victim_pid not in live_worker_pids(client).values(), timeout_s=60.0)
        assert victim_pid not in live_worker_pids(client).values(), "the victim never died"
    finally:
        gate.touch()  # open every gather — blocked, rescheduled, and restarted-epoch alike

    thread.join(timeout=300.0)
    assert not thread.is_alive(), (
        "HARD TIMEOUT (F2): the run neither completed nor failed after the worker death — the "
        "measured default-config no-worker hang must be impossible under m44 semantics"
    )
    return outcome, oracle, str(mark_dir), n_src, victim


def test_one_epoch_restart_completes_bit_for_bit(tmp_path: Path) -> None:
    with transport_cluster(2, processes=True, allowed_failures=0) as client:
        outcome, oracle, mark_dir, n_src, _victim = _run_death_scenario(
            client, tmp_path, epoch_restarts_allowed=1
        )
        assert "error" not in outcome, f"restart-allowed run raised: {outcome.get('error')!r}"
        result = outcome["result"]

        assert result.dest_block_hashes == oracle.dest_block_hashes, (
            "result diverged from the clean local oracle after the restart — stale blocks from the "
            "dead epoch were reused, or the rerun changed the bytes"
        )
        assert result.transport.epoch_restarts == 1, (
            f"epoch_restarts={result.transport.epoch_restarts}: a participating-worker death must "
            "cost exactly one whole-run restart here"
        )
        nonces = list(result.transport.epoch_nonces)
        assert len(nonces) == 2 and nonces[0] != nonces[1]

        n_pmarks = len(mark_files(mark_dir, "pmark"))
        assert n_pmarks > n_src, (
            f"only {n_pmarks} partition calls for {n_src} src blocks — stage 1 never re-ran, so the "
            "dead worker's blocks survived OUTSIDE the epoch (a stale store, not a restart)"
        )

        active = client.run(probe_active_epochs)
        for nonce in nonces:
            for addr, epochs in active.items():
                assert nonce not in epochs, f"epoch {nonce} still active on {addr} — purge missing"


def test_exhausted_restart_budget_is_an_attributed_stage_error(tmp_path: Path) -> None:
    with transport_cluster(2, processes=True, allowed_failures=0) as client:
        outcome, _oracle, _mark_dir, _n_src, victim = _run_death_scenario(
            client, tmp_path, epoch_restarts_allowed=0
        )
        assert "result" not in outcome, (
            "with epoch_restarts_allowed=0 a participating-worker death must FAIL the run"
        )
        err = outcome["error"]
        assert type(err) is StageError, (
            f"expected an attributed StageError, got {type(err).__name__}: a raw "
            "KilledWorker/comm error must never leak to the user (§1.5, describe_failure path)"
        )
        assert victim in str(err), (
            f"the StageError must name the last worker ({victim}) — measured: "
            f"KilledWorker.last_worker carries it; got: {err}"
        )
