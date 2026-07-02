"""M39 (B4, pure-logic companion) — the advertised address must be routable (plan §6.1).

The transport conflates two concerns by defaulting ``host="127.0.0.1"`` and then announcing it: the
BIND interface may be ``0.0.0.0`` (all NICs), but the ANNOUNCED/dialed address MUST be a concrete
routable node IP — never loopback, never ``0.0.0.0`` (which peers cannot dial). This companion test is
PURE LOGIC and NEVER SKIPS (unlike the environment-gated cluster-sim integration form), so the B4
discrimination is never silently lost to a loopback-only CI runner: the ``advertise_host`` selector
must reject any loopback / ``0.0.0.0`` result.
"""

from __future__ import annotations

import pytest

from graphed_exec_local._transport import is_routable_host, select_advertise_host


def test_loopback_and_wildcard_are_not_routable() -> None:
    assert is_routable_host("127.0.0.1") is False
    assert is_routable_host("127.5.6.7") is False  # all of 127.0.0.0/8, not just .0.0.1
    assert is_routable_host("0.0.0.0") is False  # the bind-all wildcard is unroutable as an address


def test_concrete_ips_are_routable() -> None:
    assert is_routable_host("10.1.2.3") is True
    assert is_routable_host("192.168.0.5") is True
    assert is_routable_host("8.8.8.8") is True


def test_selector_accepts_a_concrete_routable_candidate() -> None:
    assert select_advertise_host("10.1.2.3") == "10.1.2.3"


def test_selector_rejects_loopback_and_wildcard_candidates() -> None:
    # THE discrimination: a selector that passed loopback/0.0.0.0 through (today's default) must fail.
    with pytest.raises(ValueError):
        select_advertise_host("127.0.0.1")
    with pytest.raises(ValueError):
        select_advertise_host("0.0.0.0")
