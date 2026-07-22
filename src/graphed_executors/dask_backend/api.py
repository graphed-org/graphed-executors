"""The unified shuffle facade — THE front door to the two dask shuffle engines (m45-facade-plan, r2).

``run_repartition`` / ``run_join`` take a single ``shuffle_method="auto"|"transport"|"tasks"`` knob and
dispatch over the frozen m43 as-tasks engine (``shuffle.py``) and m44 worker-transport engine
(``transport_shuffle.py``), so users pick an engine with ONE word instead of juggling both entry points
(the "too many knobs" directive). ``"auto"`` is capability-static: transport iff the backend supports
BOTH strict worker pinning and peer data movement, else the as-tasks engine — it does NOT inspect cluster
elasticity, so adaptive-cluster users pass ``shuffle_method="tasks"`` explicitly.

This is a THIN dispatcher — zero engine logic. It (1) resolves the method from capabilities, (2) rejects a
transport-only knob that was explicitly set while the resolution landed on ``"tasks"`` (a loud ``ValueError``
naming the knob, never a silent drop), and (3) forwards to the engine, returning its own result object
unchanged. ``mem_budget_bytes`` on ``run_join`` is COMMON (both joins bound their working set with it).

Return type is the union ``ShuffleResult | TransportShuffleResult``: the portable contract is the COMMON
triple ``dest_block_hashes`` / ``value`` / ``witness``; ``.partitions`` (tasks) and ``.transport``
(transport) are engine-specific extras. The extra doubles as resolution observability — a result carrying
``.transport`` proves the transport engine ran (useful for "why was auto slow on my adaptive cluster").

Callers needing ``monitor=`` / ``retries=`` use the still-public ``dask_run_*`` / ``transport_run_*`` entry
points directly; the facade always builds ``SubmitRunner(dbackend)`` internally (a caller-supplied runner
would open a ``runner.backend is not dbackend`` consistency hole between resolution and dispatch).

Dask-import-free at module load (F13): the engine-run functions and ``SubmitRunner`` are deferred into the
function bodies; only the dask-free result dataclasses are imported at top for the union annotation.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from graphed_executors.local.shuffle import ShuffleResult
from graphed_executors.submit.protocol import SubmitCapabilities

from ._transport_run import TransportShuffleResult

_VALID_METHODS = ("auto", "transport", "tasks")


def resolve_shuffle_method(method: str, caps: SubmitCapabilities) -> str:
    """Pure, deterministic engine choice (no cluster, no size heuristics). Explicit ``"transport"`` /
    ``"tasks"`` resolve to themselves — the engines' own gates then apply. ``"auto"`` picks transport iff
    ``caps.pin_to_worker AND caps.peer_data_movement``, else tasks. Anything else raises."""
    if method in ("transport", "tasks"):
        return method
    if method == "auto":
        return "transport" if (caps.pin_to_worker and caps.peer_data_movement) else "tasks"
    raise ValueError(f"shuffle_method must be one of {_VALID_METHODS}; got {method!r}")


def _reject_transport_only_knobs(resolved: str, knob_values: dict[str, Any]) -> None:
    """When resolution landed on ``"tasks"``, a transport-only knob that was explicitly set (not None) is
    a caller error — raise naming it, in declared order, BEFORE any dispatch (§1.2). No-op for transport."""
    if resolved != "tasks":
        return
    for name, value in knob_values.items():
        if value is not None:
            raise ValueError(f"{name} applies only to shuffle_method='transport' (resolved: 'tasks')")


def run_repartition(
    backend: Any,
    src_blocks: Sequence[Any],
    parts: int,
    *,
    dbackend: Any,
    shuffle_method: str = "auto",
    salt: int = 0,
    n_tasks: int | None = None,
    fetch_budget_bytes: int | None = None,
    disk_budget_bytes: int | None = None,
    holder_budget_bytes: int | None = None,
    pull_timeout_s: float | None = None,
    epoch_restarts_allowed: int | None = None,
) -> ShuffleResult | TransportShuffleResult:
    """Hash-repartition ``src_blocks`` into ``parts`` dests via the ``shuffle_method``-selected engine.
    ``n_tasks`` / ``fetch_budget_bytes`` / ``disk_budget_bytes`` / ``holder_budget_bytes`` /
    ``pull_timeout_s`` / ``epoch_restarts_allowed`` are transport-only (setting one while resolution lands
    on ``"tasks"`` raises); ``salt`` is common. ``epoch_restarts_allowed=None`` maps to the engine default (1)."""
    resolved = resolve_shuffle_method(shuffle_method, dbackend.capabilities)
    _reject_transport_only_knobs(
        resolved,
        {
            "n_tasks": n_tasks,
            "fetch_budget_bytes": fetch_budget_bytes,
            "disk_budget_bytes": disk_budget_bytes,
            "holder_budget_bytes": holder_budget_bytes,
            "pull_timeout_s": pull_timeout_s,
            "epoch_restarts_allowed": epoch_restarts_allowed,
        },
    )
    if resolved == "transport":
        from .transport_shuffle import transport_run_repartition  # noqa: PLC0415  (F13: deferred)

        return transport_run_repartition(
            backend,
            src_blocks,
            parts,
            dbackend=dbackend,
            salt=salt,
            n_tasks=n_tasks,
            fetch_budget_bytes=fetch_budget_bytes,
            disk_budget_bytes=disk_budget_bytes,
            holder_budget_bytes=holder_budget_bytes,
            pull_timeout_s=pull_timeout_s,
            epoch_restarts_allowed=1 if epoch_restarts_allowed is None else epoch_restarts_allowed,
        )
    from graphed_executors.submit import SubmitRunner  # noqa: PLC0415  (dask-free, but keep the top clean)

    from .shuffle import dask_run_repartition  # noqa: PLC0415  (F13: deferred)

    return dask_run_repartition(backend, src_blocks, parts, runner=SubmitRunner(dbackend), salt=salt)


def run_join(
    backend: Any,
    left_blocks: Sequence[Any],
    right_blocks: Sequence[Any],
    parts: int,
    *,
    on: Sequence[str] = ("__joinkey__",),
    how: str = "inner",
    dbackend: Any,
    shuffle_method: str = "auto",
    broadcast: bool | None = None,
    salt: int = 0,
    mem_budget_bytes: int | None = None,
    holder_budget_bytes: int | None = None,
    pull_timeout_s: float | None = None,
    epoch_restarts_allowed: int | None = None,
) -> ShuffleResult | TransportShuffleResult:
    """Distributed relational hash JOIN via the ``shuffle_method``-selected engine. ``on`` / ``how`` /
    ``broadcast`` / ``salt`` / ``mem_budget_bytes`` are COMMON (both engines bound their working set with
    ``mem_budget_bytes``); ``holder_budget_bytes`` / ``pull_timeout_s`` / ``epoch_restarts_allowed`` are
    transport-only (setting one while resolution lands on ``"tasks"`` raises)."""
    resolved = resolve_shuffle_method(shuffle_method, dbackend.capabilities)
    _reject_transport_only_knobs(
        resolved,
        {
            "holder_budget_bytes": holder_budget_bytes,
            "pull_timeout_s": pull_timeout_s,
            "epoch_restarts_allowed": epoch_restarts_allowed,
        },
    )
    if resolved == "transport":
        from .transport_shuffle import transport_run_join  # noqa: PLC0415  (F13: deferred)

        return transport_run_join(
            backend,
            left_blocks,
            right_blocks,
            parts,
            on=on,
            how=how,
            dbackend=dbackend,
            broadcast=broadcast,
            salt=salt,
            mem_budget_bytes=mem_budget_bytes,
            holder_budget_bytes=holder_budget_bytes,
            pull_timeout_s=pull_timeout_s,
            epoch_restarts_allowed=1 if epoch_restarts_allowed is None else epoch_restarts_allowed,
        )
    from graphed_executors.submit import SubmitRunner  # noqa: PLC0415

    from .shuffle import dask_run_join  # noqa: PLC0415  (F13: deferred)

    return dask_run_join(
        backend,
        left_blocks,
        right_blocks,
        parts,
        on=on,
        how=how,
        runner=SubmitRunner(dbackend),
        broadcast=broadcast,
        salt=salt,
        mem_budget_bytes=mem_budget_bytes,
    )
