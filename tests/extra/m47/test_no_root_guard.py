"""m47 mutation re-verification follow-up: the ``has_captured`` guard at the epoch-loop return
(``transport_peer.py`` — ``if not errs and has_captured:``) is DEFENSE-IN-DEPTH for a state no
black-box injection can construct (measured, fixup seam finding 2: retry semantics close the gap —
a driver-side root drop with budget < SEND_RETRIES re-lands the root; budget >= SEND_RETRIES errs
the sender's task, so ``errs`` fills). The mutation review's MUT3 (``if not errs:``) therefore
survives every frozen scenario. This white-box unit test forces the exact guarded state by
stubbing ``_drive_reduce`` and pins the loss-safety contract at the guard itself: empty ``errs``
with NO captured root must RAISE the attributed no-root ``StageError`` — never return a result
(whose value slot would be ``None``/identity, §1.4 "No silent None anywhere").

Discriminates: MUT3 returns ``TransportExecResult(None, …)`` here — ``pytest.raises`` reports
DID NOT RAISE (kill measured against the applied mutant in the re-verification log); an
implementation that raises the wrong species or an unattributed error fails the cause pins.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("parsl", reason="graphed-executors[parsl] extra not installed")

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m47"))

from graphed.debug import StageError
from parsl_transport_harness import make_p2p_plan, p2p_tpe_pool, run_bounded


def test_empty_errs_without_a_captured_root_still_raises(monkeypatch: Any) -> None:
    import graphed_executors.parsl_backend.transport_peer as tp  # noqa: PLC0415
    from graphed_executors.common.http_plane import TransportDeliveryError  # noqa: PLC0415
    from graphed_executors.parsl_backend import ParslBackend  # noqa: PLC0415

    # the unconstructible state, forced: no peer errored AND no root was ever captured
    monkeypatch.setattr(tp, "_drive_reduce", lambda *a, **k: ({}, None, False))

    with p2p_tpe_pool(max_threads=2) as tpe, pytest.raises(StageError) as excinfo:
        run_bounded(
            lambda: tp.parsl_run_plan(
                make_p2p_plan(6, "mut3guard"),
                ParslBackend(tpe),
                workers=2,
                epoch_restarts_allowed=0,
                # small bounds: the stubbed driver never answers the rendezvous, so the
                # stranded peer threads must self-release before pool teardown joins them
                barrier_timeout_s=3.0,
                root_timeout_s=3.0,
            )
        )

    cause = excinfo.value.__cause__
    assert isinstance(cause, TransportDeliveryError), (
        f"the no-root state must be attributed as a transport loss, got {type(cause).__name__}"
    )
    assert "no captured root" in str(cause), (
        f"the cause must name the no-captured-root contract, got: {cause}"
    )
