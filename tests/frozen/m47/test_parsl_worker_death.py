"""m47 worker-death semantics (contract obligation 17) — losing a participating peer mid-shuffle
costs ONE whole-run restart under a fresh epoch onto the surviving set; an exhausted restart
budget surfaces as an attributed ``StageError``, never a raw parsl exception and NEVER a hang
(plan §1.4 epoch/restart + §0.3 measured death latency; blueprint §3 `test_parsl_worker_death`).

Deterministic mid-run kill, no timing races (the m43/m44 gated-kill mechanism): the harness
``GatedP2PShuffleBackend`` marks every map-phase ``partition`` call (``pmark``: pid) and BLOCKS
every gather's ``from_wire`` on a gate file (``gstart`` mark). The driver waits until the map
phase fully ran AND a block-holding peer is provably blocked inside a gather, SIGKILLs exactly
that worker process (same machine — direct ``os.kill``), opens the gate, and joins under a HARD
timeout. Death-latency compression: the fixture passes ``heartbeat_period=2`` through the m47
``start_htex`` extension — MEASURED for this suite (scratchpad hb_probe.py): SIGKILL ->
``WorkerLost`` in **1.65 s** (vs ~29.7 s at the default 30 s watchdog period) and the pool
survives via in-place respawn. ``pull_timeout_s=20`` keeps the survivor's dead-holder pull leg
fast too; the timeout budget covers both legs + one restart cycle.

Witnesses: restart arm — result bit-for-bit == the plain-backend LOCAL oracle (stale blocks from
the dead epoch would diverge), ``epoch_restarts == 1``, two distinct nonces, and the map phase
RE-RAN under the fresh epoch (pmark count STRICTLY exceeds n_src). Budget-0 arm — ``StageError``
(type-pinned), LENIENT on which death signal it names (``PullTimeoutError`` OR ``WorkerLost`` /
``ManagerLost`` OR barrier OR ``TransportDeliveryError`` — the fast pull/send legs race the
watchdog, so pinning one specific signal would be a clock race; plan r3 tests-(d), set widened by
the driver-release leg and flagged in the README).

Discriminates: a silent-wrong-answer engine reusing the dead holder's stale blocks (byte
divergence); one that never restarts (pmark count stays n_src, or a hang); raw
WorkerLost/ManagerLost leakage to the user."""

from __future__ import annotations

import os
import signal
import sys
import threading
from pathlib import Path
from typing import Any

import pytest
from graphed.debug import StageError
from parsl_transport_harness import (
    GatedP2PShuffleBackend,
    make_parsl_backend,
    mark_pids,
    numpy_p2p_adapter,
    p2p_htex_pool,
    poll_until,
    repartition_blocks,
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

PARTS = 8
DEATH_SIGNALS = ("PullTimeoutError", "WorkerLost", "ManagerLost", "barrier", "TransportDeliveryError")


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _run_death_scenario(
    htex: Any, tmp_path: Path, *, epoch_restarts_allowed: int
) -> tuple[dict[str, Any], Any, str, int]:
    """Drive a gated p2p repartition, SIGKILL one block-holding peer mid-gather, open the gate,
    join under the hard timeout. Returns (outcome, oracle, mark_dir, n_src)."""
    adapter = numpy_p2p_adapter()
    src = repartition_blocks(adapter)
    n_src = len(src)
    mark_dir = tmp_path / f"marks-{epoch_restarts_allowed}"
    mark_dir.mkdir()
    gate = tmp_path / f"gate-{epoch_restarts_allowed}"
    gated = GatedP2PShuffleBackend(adapter.backend, str(mark_dir), str(gate))
    # the oracle uses the PLAIN backend (no gate): the wrapper is data-transparent by construction
    oracle = run_repartition(adapter.backend, src, PARTS)

    backend = make_parsl_backend(htex)
    driver_pid = os.getpid()
    outcome: dict[str, Any] = {}

    def drive() -> None:
        try:
            outcome["result"] = transport_repartition(
                gated,
                src,
                PARTS,
                pbackend=backend,
                pull_timeout_s=20.0,
                epoch_restarts_allowed=epoch_restarts_allowed,
            )
        except BaseException as exc:  # surfaced after join — the driver thread must never hide it
            outcome["error"] = exc

    thread = threading.Thread(target=drive, daemon=True)
    thread.start()
    try:
        # (1) the map phase fully ran AND (2) a block-holding peer is blocked inside a gather
        def victim_ready() -> bool:
            if len(mark_pids(str(mark_dir), "pmark")) < n_src:
                return False
            return bool(set(mark_pids(str(mark_dir), "pmark")) & set(mark_pids(str(mark_dir), "gstart")))

        poll_until(lambda: victim_ready() or not thread.is_alive(), timeout_s=120.0)
        assert "error" not in outcome, f"run raised before the kill: {outcome.get('error')!r}"
        assert victim_ready(), "scenario misfired: the map phase or a blocked gather never materialised"

        victims = set(mark_pids(str(mark_dir), "pmark")) & set(mark_pids(str(mark_dir), "gstart"))
        victim_pid = sorted(victims)[0]
        assert victim_pid != driver_pid and _pid_alive(victim_pid), (
            f"victim pid {victim_pid} is not a live worker process"
        )
        os.kill(victim_pid, signal.SIGKILL)
        poll_until(lambda: not _pid_alive(victim_pid), timeout_s=60.0)
        assert not _pid_alive(victim_pid), "the victim never died"
    finally:
        gate.touch()  # open every gather — blocked, restarted-epoch, and oracle-parity alike

    thread.join(timeout=240.0)
    assert not thread.is_alive(), (
        "HARD TIMEOUT: the run neither completed nor failed after the peer death — a lost done/"
        "pull must surface as a restart or an attributed error, never a hang (design-A)"
    )
    return outcome, oracle, str(mark_dir), n_src


def test_one_epoch_restart_completes_bit_for_bit(tmp_path: Path) -> None:
    with p2p_htex_pool(workers=2, heartbeat_period=2) as htex:
        outcome, oracle, mark_dir, n_src = _run_death_scenario(htex, tmp_path, epoch_restarts_allowed=1)
        assert "error" not in outcome, f"restart-allowed run raised: {outcome.get('error')!r}"
        result = outcome["result"]

        assert result.dest_block_hashes == oracle.dest_block_hashes, (
            "result diverged from the clean local oracle after the restart — stale blocks from "
            "the dead epoch were reused, or the rerun changed the bytes"
        )
        assert result.transport.epoch_restarts == 1, (
            f"epoch_restarts={result.transport.epoch_restarts}: a participating-peer death must "
            "cost exactly one whole-run restart here"
        )
        nonces = list(result.transport.epoch_nonces)
        assert len(nonces) == 2 and nonces[0] != nonces[1]

        n_pmarks = len(mark_pids(mark_dir, "pmark"))
        assert n_pmarks > n_src, (
            f"only {n_pmarks} partition calls for {n_src} src blocks — the map phase never re-ran, "
            "so the dead peer's blocks survived OUTSIDE the epoch (a stale store, not a restart)"
        )


def test_exhausted_restart_budget_is_an_attributed_stage_error(tmp_path: Path) -> None:
    with p2p_htex_pool(workers=2, heartbeat_period=2) as htex:
        outcome, _oracle, _mark_dir, _n_src = _run_death_scenario(htex, tmp_path, epoch_restarts_allowed=0)
        assert "result" not in outcome, (
            "with epoch_restarts_allowed=0 a participating-peer death must FAIL the run — a green "
            "result means the loss was silently absorbed (or the restart budget was ignored)"
        )
        err = outcome["error"]
        assert type(err) is StageError, (
            f"expected an attributed StageError, got {type(err).__name__}: a raw parsl "
            "WorkerLost/ManagerLost or transport error must never leak to the user (§1.4)"
        )
        rendered = f"{getattr(err, 'cause_type', '')}: {err}"
        assert any(sig in rendered for sig in DEATH_SIGNALS), (
            f"the StageError must name a death-class signal (any of {DEATH_SIGNALS} — lenient: "
            f"the fast pull/send legs race the ~2 s compressed watchdog); got: {rendered}"
        )
