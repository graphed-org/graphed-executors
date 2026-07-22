"""m44 remediation witnesses (non-gating): the R2 silent-empty-root fix and the R4 pull-timeout
classification. Both are discriminating against the exact pre-remediation behaviour.

R2 — a peer reduction that completes with NO captured root must RAISE (restart-worthy
``TransportDeliveryError`` → §1.5), never silently return ``plan.empty()`` (the identity value). The
``_select_root`` helper is the pure, clock-free seam that decision routes through; a genuinely-``None``
reduction root must still resolve to ``None`` (not raise, not empty) — the ``_NO_ROOT`` sentinel is what
disambiguates it. The integration arm proves the new ``root_timeout_s`` kwarg is actually forwarded to
``collect_peer_root`` (a hardcoded 30.0 would fail it), via a recording monkeypatch seam — no clock.

R4 — a block-plane ``PullTimeoutError`` is classified restart-worthy so a timed-out holder restarts the
whole run onto the survivors instead of leaking a bare timeout as an opaque ``StageError``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from graphed.core.execution import SequentialRunner

pytest.importorskip("distributed")

from graphed_executors.dask_backend._transport_run import is_restart_worthy
from graphed_executors.dask_backend.transport import PullTimeoutError, TransportDeliveryError
from graphed_executors.dask_backend.transport_peer import _MISSING, _select_root

# import the frozen harness by path (importing, not editing — the integrity rule forbids only edits)
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "frozen" / "m44"))


# ---- R2: _select_root — the silent-empty-root fix, unit-level and clock-free --------------------
def test_driver_root_is_authoritative_even_when_none() -> None:
    # a driver-delivered root wins outright; a genuinely-None driver root is a value, not "missing"
    assert _select_root("R", {}, _MISSING) == "R"
    assert _select_root(None, {"a": {"has_root": True, "root": "peer"}}, _MISSING) is None


def test_peer_root_fallback_when_driver_missing() -> None:
    results = {"a": {"has_root": False, "root": None}, "b": {"has_root": True, "root": 42}}
    assert _select_root(_MISSING, results, _MISSING) == 42


def test_none_peer_root_is_kept_not_treated_as_missing() -> None:
    # the pre-r2 bug: `r.get("root") is not None` skipped a captured None root and defaulted to empty.
    # has_root=True with root=None must resolve to None (the real reduction value), never raise/empty.
    results = {"a": {"has_root": True, "root": None}}
    assert _select_root(_MISSING, results, _MISSING) is None


def test_no_captured_root_raises_not_empty() -> None:
    # the core R2 discrimination: driver lost the root AND no peer captured one ⇒ RAISE, never empty.
    results = {"a": {"has_root": False, "root": None}, "b": {"has_root": False, "root": None}}
    with pytest.raises(TransportDeliveryError, match="no captured root"):
        _select_root(_MISSING, results, _MISSING)


# ---- R4: PullTimeoutError classification — restart-worthy, discriminating -----------------------
class _NoDescribeBackend:
    """A backend without ``describe_failure`` — isolates the type-name classification arm."""


def test_pull_timeout_is_restart_worthy() -> None:
    be = _NoDescribeBackend()
    assert is_restart_worthy(PullTimeoutError("holder tcp://w1 timed out"), be) is True
    # chained the way pull_blocks raises it (from asyncio.TimeoutError) still classifies
    try:
        raise PullTimeoutError("x") from TimeoutError()
    except PullTimeoutError as exc:
        assert is_restart_worthy(exc, be) is True
    # discrimination: a non-timeout, non-comm error must NOT be restart-worthy (else the classifier
    # would restart on everything and the assertion above would be vacuous)
    assert is_restart_worthy(RuntimeError("a logic bug in a kernel"), be) is False


# ---- R2 integration: root_timeout_s reaches collect_peer_root (recording seam, no clock) --------
def test_root_timeout_kwarg_is_forwarded_to_collect_peer_root(monkeypatch: pytest.MonkeyPatch) -> None:
    from transport_harness import (  # noqa: PLC0415  (frozen harness, imported by path above)
        build_dask_backend,
        install_transport_plugin,
        make_peer_plan,
        transport_cluster,
        transport_plan_run,
    )

    import graphed_executors.dask_backend.transport_peer as tp  # noqa: PLC0415

    seen: dict[str, float] = {}
    real = tp.collect_peer_root

    def _recording(driver_ep: object, empty: object, n: int, *, timeout_s: float) -> object:
        seen["timeout_s"] = timeout_s  # record the forwarded ceiling, then run the real collector
        return real(driver_ep, empty, n, timeout_s=timeout_s)

    monkeypatch.setattr(tp, "collect_peer_root", _recording)

    plan = make_peer_plan(6, "m44tmo")
    with transport_cluster(1, processes=False) as client:
        install_transport_plugin(client)
        res = transport_plan_run(plan, build_dask_backend(client), root_timeout_s=7.5)

    assert res.value == SequentialRunner().run(make_peer_plan(6, "m44tmo")).value, "the run diverged"
    assert seen.get("timeout_s") == 7.5, (
        f"root_timeout_s was not forwarded to collect_peer_root (saw {seen.get('timeout_s')!r}); a "
        "hardcoded _ROOT_TIMEOUT_S=30.0 fails this"
    )
