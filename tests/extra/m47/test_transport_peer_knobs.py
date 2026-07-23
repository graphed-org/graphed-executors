"""m47 review remediation (findings 2/3/4, non-gating): the reduce-seam knobs that were accepted
but never reached the peers.

- **Finding 2** — ``parsl_run_plan(inbox_maxsize=…)`` was accepted but never forwarded into the peer
  spec, so a caller got a silently unbounded inbox. Proven wired by spying the peer endpoint
  constructor over an in-process TPE (peers run as driver threads, so the driver-side spy sees their
  construction). The endpoint-level 503/``sends_rejected`` MECHANISM is already frozen-tested
  (test_parsl_transport_conformance); this pins the run_plan-level WIRING that was the gap.
- **Finding 4** — the reduce-seam bounds were hard-coded (``root_timeout_s`` had no knob; the peer
  ``idle_deadline_s`` was a bare 90.0). Now ``root_timeout_s`` is a parameter and the idle deadline
  is DERIVED (``root_timeout_s + slack``), so it stays ≥ the root wait for any root_timeout_s.
- **Finding 3** — the epoch restart recomputes k from a FRESH worker count
  (``min(requested_k, n_workers())``) instead of re-asking for the original explicit k a shrunken
  cluster can no longer seat.

All deterministic and clock-free.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("parsl", reason="graphed-executors[parsl] extra not installed")

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m47"))

from parsl_transport_harness import (
    make_p2p_plan,
    p2p_seq_value,
    p2p_tpe_pool,
    run_bounded,
)

from graphed_executors.parsl_backend.transport_peer import _resolve_k


def _spy_endpoint_ctor(monkeypatch: Any) -> dict[str, dict[str, Any]]:
    """Record ``(inbox_maxsize, idle_deadline_s)`` per address the peer/driver endpoints are built
    with — patching the method on the class so the ``from … import EscalatingHttpTransport`` name in
    transport_peer (the SAME class object) is covered too."""
    import graphed_executors.common.http_plane as hp  # noqa: PLC0415

    recorded: dict[str, dict[str, Any]] = {}
    orig = hp.EscalatingHttpTransport.__init__

    def spy(
        self: Any,
        address: str,
        *,
        epoch: str,
        host: str = "127.0.0.1",
        inbox_maxsize: int | None = None,
        idle_deadline_s: float | None = None,
        recv_failures: dict[str, str] | None = None,
    ) -> None:
        recorded[address] = {"inbox_maxsize": inbox_maxsize, "idle_deadline_s": idle_deadline_s}
        orig(
            self,
            address,
            epoch=epoch,
            host=host,
            inbox_maxsize=inbox_maxsize,
            idle_deadline_s=idle_deadline_s,
            recv_failures=recv_failures,
        )

    monkeypatch.setattr(hp.EscalatingHttpTransport, "__init__", spy)
    return recorded


def test_inbox_maxsize_and_root_timeout_reach_the_peers(monkeypatch: Any) -> None:
    recorded = _spy_endpoint_ctor(monkeypatch)
    root_timeout_s = 45.0
    expected_idle = root_timeout_s + 60.0  # _IDLE_SLACK_S — derived, never double-hardcoded

    with p2p_tpe_pool(max_threads=2) as tpe:
        from graphed_executors.parsl_backend import ParslBackend  # noqa: PLC0415
        from graphed_executors.parsl_backend.transport_peer import parsl_run_plan  # noqa: PLC0415

        res = run_bounded(
            lambda: parsl_run_plan(
                make_p2p_plan(6, "knobs"),
                ParslBackend(tpe),
                workers=2,
                inbox_maxsize=7,
                root_timeout_s=root_timeout_s,
            )
        )

    # loss-safety sanity: a generous inbox bound does not break the reduction
    assert res.value == p2p_seq_value(6, "knobs")

    peers = {a: v for a, v in recorded.items() if a != "driver"}
    assert peers, "no peer endpoints were constructed"
    for addr, seen in peers.items():
        # finding 2: the inbox bound must reach every peer (pre-fix it stayed None — unbounded)
        assert seen["inbox_maxsize"] == 7, f"{addr}: inbox_maxsize not forwarded ({seen['inbox_maxsize']})"
        # finding 4: the peer idle deadline is derived from root_timeout_s (pre-fix it was a bare 90.0
        # regardless of root_timeout_s, which did not even exist as a parameter)
        assert seen["idle_deadline_s"] == expected_idle, (
            f"{addr}: idle_deadline_s={seen['idle_deadline_s']} not derived as root_timeout_s+slack="
            f"{expected_idle}"
        )


class _FakeBackend:
    def __init__(self, n: int) -> None:
        self._n = n

    def n_workers(self) -> int:
        return self._n


def test_resolve_k_first_attempt_honors_the_explicit_ask() -> None:
    # first attempt: an explicit k stands as-given even if more workers exist
    assert _resolve_k(_FakeBackend(8), 5, restart=False) == 5


def test_resolve_k_restart_recomputes_over_a_shrunken_cluster() -> None:
    # finding 3: a restart after a worker death must NOT re-ask for the original 5 (a doomed barrier);
    # it degrades to min(5, fresh n_workers()=2) == 2. Pre-fix this returned 5.
    assert _resolve_k(_FakeBackend(2), 5, restart=True) == 2


def test_resolve_k_restart_keeps_k_when_cluster_is_intact() -> None:
    # a restart with the full cluster still present keeps the explicit ask
    assert _resolve_k(_FakeBackend(5), 5, restart=True) == 5


def test_resolve_k_none_reads_fresh_workers_each_attempt() -> None:
    # workers=None always tracks the live count, first attempt and restart alike
    assert _resolve_k(_FakeBackend(3), None, restart=False) == 3
    assert _resolve_k(_FakeBackend(3), None, restart=True) == 3
