"""The shared shuffle-method resolver (plan §1.6 m47 move).

``resolve_shuffle_method`` + ``_reject_transport_only_knobs`` moved here VERBATIM from
``dask_backend/api.py`` so the dask facade AND the m47 parsl facade re-export the SAME function
objects (``dask_backend.api.resolve_shuffle_method IS parsl_backend.api.resolve_shuffle_method IS
common.facade.resolve_shuffle_method`` — the m45 truth-table suite + the frozen m47 identity witness
both gate the sharing; a copy could drift). Pure and dependency-light: it reads only a
``SubmitCapabilities`` vector, so it imports NOTHING from dask or parsl (the ``common/`` import rule).
"""

from __future__ import annotations

from typing import Any

from graphed_executors.submit.protocol import SubmitCapabilities

_VALID_METHODS = ("auto", "transport", "tasks")


def resolve_shuffle_method(method: str, caps: SubmitCapabilities) -> str:
    """Pure, deterministic engine choice (no cluster, no size heuristics). Explicit ``"transport"`` /
    ``"tasks"`` resolve to themselves — the engines' own gates then apply. ``"auto"`` picks transport
    iff ``caps.pin_to_worker AND caps.peer_data_movement``, else tasks. Anything else raises."""
    if method in ("transport", "tasks"):
        return method
    if method == "auto":
        return "transport" if (caps.pin_to_worker and caps.peer_data_movement) else "tasks"
    raise ValueError(f"shuffle_method must be one of {_VALID_METHODS}; got {method!r}")


def _reject_transport_only_knobs(resolved: str, knob_values: dict[str, Any]) -> None:
    """When resolution landed on ``"tasks"``, a transport-only knob that was explicitly set (not None)
    is a caller error — raise naming it, in declared order, BEFORE any dispatch. No-op for transport."""
    if resolved != "tasks":
        return
    for name, value in knob_values.items():
        if value is not None:
            raise ValueError(f"{name} applies only to shuffle_method='transport' (resolved: 'tasks')")
