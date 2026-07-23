"""m47 (a): the parsl shuffle/join facade + the runtime reachability probe (plan §1.5, IT4).

``run_repartition`` / ``run_join`` resolve the engine through the SHARED ``common.facade`` resolver
(``parsl_backend.api.resolve_shuffle_method IS dask_backend.api.… IS common.facade.…`` — a copy
could drift the m45-frozen truth table), then dispatch honestly per instance:

* ``"auto"``/``"tasks"`` -> the RELAY engine (``common.relay_engine``; head-node-routed, no p2p) — no
  parsl vector carries pin AND peer, so ``"auto"`` always lands on tasks;
* explicit ``"transport"`` -> the §1.4 peer engine, gated on ``pbackend.peer_transport`` (TPE ->
  ``NotImplementedError`` naming ``"tasks"``, zero submits) and fronted by the rendezvous-time
  reachability probe. A probe failure routes through ``on_unreachable``: ``"error"`` (the default) ->
  an attributed ``StageError`` naming the unreachable pair; ``"fallback"`` -> the relay engine on the
  same inputs with ``witness.fallback_reason`` set (observable, never silent).

A transport-only knob explicitly set while resolution lands on ``"tasks"`` is a loud ``ValueError``
BEFORE any submit. Imports only ``common.facade`` (the shared resolver) + the parsl-free probe
primitives at load; the relay + transport engines are deferred (G9: never couple to ``dask_backend``,
never import the heavy engine bodies until dispatch)."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from graphed.debug import SourceFrame, StageError

# THE shared resolver objects (identity-pinned across the dask + parsl facades).
from graphed_executors.common.facade import (
    _VALID_METHODS,
    _reject_transport_only_knobs,
    resolve_shuffle_method,
)

from .transport_peer import (
    _BARRIER_DEFAULT_S,
    _ROOT_TIMEOUT_S,
    ProbeUnreachable,
    _barrier_and_registry,
    _collect_probes,
    _open_driver_endpoint,
    _parsl_peer_main,
    _release,
    _worker_addrs,
)

__all__ = [
    "_VALID_METHODS",
    "ProbeReport",
    "_reject_transport_only_knobs",
    "probe_peer_reachability",
    "resolve_shuffle_method",
    "run_join",
    "run_repartition",
]


@dataclass
class ProbeReport:
    """The standalone ``probe_peer_reachability`` verdict: ``ok`` is True iff every overlay pair
    dialed; ``failed_pairs`` are the ``(src_addr, dst_addr)`` that could not connect (empty iff ok)."""

    ok: bool
    failed_pairs: tuple[tuple[str, str], ...]


def run_repartition(
    backend: Any,
    src_blocks: Sequence[Any],
    parts: int,
    *,
    pbackend: Any,
    shuffle_method: str = "auto",
    salt: int = 0,
    fetch_budget_bytes: int | None = None,
    pull_timeout_s: float | None = None,
    epoch_restarts_allowed: int | None = None,
    workers: int | None = None,
    on_unreachable: str | None = None,
    registry_rewrite: Any = None,
) -> Any:
    """Hash-repartition via the ``shuffle_method``-selected engine. ``fetch_budget_bytes`` /
    ``pull_timeout_s`` / ``epoch_restarts_allowed`` / ``workers`` / ``on_unreachable`` /
    ``registry_rewrite`` are transport-only (set one while resolution lands on ``"tasks"`` and it
    raises); ``salt`` is common. ``on_unreachable=None`` means ``"error"``."""
    resolved = resolve_shuffle_method(shuffle_method, pbackend.capabilities)
    _reject_transport_only_knobs(
        resolved,
        {
            "fetch_budget_bytes": fetch_budget_bytes,
            "pull_timeout_s": pull_timeout_s,
            "epoch_restarts_allowed": epoch_restarts_allowed,
            "workers": workers,
            "on_unreachable": on_unreachable,
            "registry_rewrite": registry_rewrite,
        },
    )
    if resolved == "transport":
        _require_peer_transport(pbackend)
        from .transport_shuffle import transport_run_repartition  # noqa: PLC0415  (G9: deferred)

        try:
            return transport_run_repartition(
                backend,
                src_blocks,
                parts,
                pbackend=pbackend,
                salt=salt,
                workers=workers,
                fetch_budget_bytes=fetch_budget_bytes,
                pull_timeout_s=pull_timeout_s,
                epoch_restarts_allowed=1 if epoch_restarts_allowed is None else epoch_restarts_allowed,
                registry_rewrite=registry_rewrite,
            )
        except ProbeUnreachable as pu:
            return _on_probe_failure(
                pu,
                on_unreachable,
                lambda: _relay_repartition(backend, src_blocks, parts, _runner(pbackend), salt),
            )
    return _relay_repartition(backend, src_blocks, parts, _runner(pbackend), salt)


def run_join(
    backend: Any,
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    *,
    on: Sequence[str] = ("__joinkey__",),
    how: str = "inner",
    pbackend: Any,
    shuffle_method: str = "auto",
    broadcast: bool | None = None,
    salt: int = 0,
    mem_budget_bytes: int | None = None,
    pull_timeout_s: float | None = None,
    epoch_restarts_allowed: int | None = None,
    workers: int | None = None,
    on_unreachable: str | None = None,
) -> Any:
    """Distributed relational JOIN via the ``shuffle_method``-selected engine. ``on`` / ``how`` /
    ``broadcast`` / ``salt`` / ``mem_budget_bytes`` are common; ``pull_timeout_s`` /
    ``epoch_restarts_allowed`` / ``workers`` / ``on_unreachable`` are transport-only."""
    resolved = resolve_shuffle_method(shuffle_method, pbackend.capabilities)
    _reject_transport_only_knobs(
        resolved,
        {
            "pull_timeout_s": pull_timeout_s,
            "epoch_restarts_allowed": epoch_restarts_allowed,
            "workers": workers,
            "on_unreachable": on_unreachable,
        },
    )
    if resolved == "transport":
        _require_peer_transport(pbackend)
        from .transport_shuffle import transport_run_join  # noqa: PLC0415  (G9: deferred)

        try:
            return transport_run_join(
                backend,
                left_blocks,
                right_blocks,
                parts,
                on=on,
                how=how,
                pbackend=pbackend,
                broadcast=broadcast,
                salt=salt,
                mem_budget_bytes=mem_budget_bytes,
                workers=workers,
                pull_timeout_s=pull_timeout_s,
                epoch_restarts_allowed=1 if epoch_restarts_allowed is None else epoch_restarts_allowed,
            )
        except ProbeUnreachable as pu:
            return _on_probe_failure(
                pu,
                on_unreachable,
                lambda: _relay_join(
                    backend,
                    left_blocks,
                    right_blocks,
                    parts,
                    on,
                    how,
                    _runner(pbackend),
                    broadcast,
                    salt,
                    mem_budget_bytes,
                ),
            )
    return _relay_join(
        backend,
        left_blocks,
        right_blocks,
        parts,
        on,
        how,
        _runner(pbackend),
        broadcast,
        salt,
        mem_budget_bytes,
    )


def probe_peer_reachability(
    pbackend: Any, k: int, *, timeout_s: float = 5.0, registry_rewrite: Any = None
) -> ProbeReport:
    """User-facing pre-flight (directive #2): seat k peers, run the overlay probe at rendezvous time,
    RELEASE the peers (the pool is usable afterwards), and report. ``registry_rewrite`` is the test
    poisoner seam (production never passes it)."""
    nonce = uuid.uuid4().hex
    worker_addrs = _worker_addrs(k)
    driver_ep = _open_driver_endpoint(nonce)
    driver_counts: dict[str, int] = {}
    futures = {
        addr: pbackend.submit(
            _parsl_peer_main,
            {
                "kind": "probe",
                "addr": addr,
                "epoch": nonce,
                "driver_host": driver_ep.host,
                "driver_port": driver_ep.port,
                "worker_addrs": worker_addrs,
                "probe_timeout_s": timeout_s,
            },
            key=f"graphed-parsl-probe-{nonce}-{i}",
        )
        for i, addr in enumerate(worker_addrs)
    }
    try:
        seated = _barrier_and_registry(
            driver_ep,
            futures,
            k,
            barrier_timeout_s=_BARRIER_DEFAULT_S,
            registry_rewrite=registry_rewrite,
            driver_counts=driver_counts,
        )
        failed = _collect_probes(driver_ep, k, driver_counts=driver_counts, timeout_s=timeout_s + 10.0)
        _release(driver_ep, seated, ("abort",))  # release the probe peers — no shuffle follows
    finally:
        for fut in futures.values():
            with contextlib.suppress(BaseException):  # drain the released markers; a probe never raises
                fut.result(timeout=_ROOT_TIMEOUT_S)
        driver_ep.close()
    return ProbeReport(ok=(not failed), failed_pairs=tuple(failed))


# ---- helpers -------------------------------------------------------------------------------------
def _require_peer_transport(pbackend: Any) -> None:
    if not getattr(pbackend, "peer_transport", False):
        raise NotImplementedError(
            "shuffle_method='transport' needs a peer_transport backend (HTEX worker processes); this "
            "instance is not one — use shuffle_method='tasks' (a loopback re-enactment of head-node "
            "routing on TPE threads adds mechanism and removes honesty)"
        )


def _on_probe_failure(pu: ProbeUnreachable, on_unreachable: str | None, run_relay: Any) -> Any:
    """Route a rendezvous-time probe failure: ``"fallback"`` -> the relay engine (labeled), else an
    attributed ``StageError`` naming the unreachable pair (the default ``"error"`` posture)."""
    if on_unreachable == "fallback":
        result = run_relay()
        result.witness.fallback_reason = (
            f"peer overlay unreachable, fell back to tasks: {list(pu.failed_pairs)}"
        )
        return result
    raise StageError(
        op="run",
        frames=(SourceFrame(filename="reachability-probe", lineno=0),),
        input_forms=(),
        partition="reachability-probe",
        cause_type="ProbeUnreachable",
        cause_message=f"peer overlay unreachable before any data plane traffic: {list(pu.failed_pairs)}",
        opt_level=0,
    ) from pu


def _runner(pbackend: Any) -> Any:
    from graphed_executors.submit import SubmitRunner  # noqa: PLC0415  (defer: keep api load lean)

    return SubmitRunner(pbackend)


def _relay_repartition(backend: Any, src_blocks: Any, parts: int, runner: Any, salt: int) -> Any:
    from graphed_executors.common.relay_engine import relay_run_repartition  # noqa: PLC0415  (G9: deferred)

    return relay_run_repartition(backend, src_blocks, parts, runner=runner, salt=salt)


def _relay_join(
    backend: Any,
    left_blocks: Any,
    right_blocks: Any,
    parts: int,
    on: Sequence[str],
    how: str,
    runner: Any,
    broadcast: bool | None,
    salt: int,
    mem_budget_bytes: int | None,
) -> Any:
    from graphed_executors.common.relay_engine import relay_run_join  # noqa: PLC0415  (G9: deferred)

    return relay_run_join(
        backend,
        left_blocks,
        right_blocks,
        parts,
        on=on,
        how=how,
        runner=runner,
        broadcast=broadcast,
        salt=salt,
        mem_budget_bytes=mem_budget_bytes,
    )
