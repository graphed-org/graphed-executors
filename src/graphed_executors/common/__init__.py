"""Backend-generic engines shared by the dask and parsl adapters (plan §1.6, bootstrapped in m46).

``common/`` imports stdlib, ``graphed.*``, ``graphed_executors.local`` and
``graphed_executors.submit`` ONLY — never parsl, dask, or distributed (importable with none of the
three installed; frozen no-cross-import test). It hosts:

- :mod:`~graphed_executors.common.tasks_engine` — the m43 as-tasks engine moved verbatim; the
  ``dask_backend.shuffle`` re-export shim keeps every frozen m43/m45 dotted path alive.
- :mod:`~graphed_executors.common.relay_engine` — the m46 relay (as-tasks/workflow) engine: T maps
  + driver-local regroup + P gathers, the honest head-node shape for a broker without
  worker<->worker reachability (e.g. parsl HTEX).

It also holds :func:`refuse_staged`, the one refusal of a join or repartition plan by the runners
that take a single-stage ``Plan``, and :func:`renamed`, the one deprecation of a setting's name.
"""

from __future__ import annotations

import warnings
from typing import Any, TypeVar

from graphed.core import DurablePlanV2

T = TypeVar("T")


def renamed(workers: int | None, old: int | None, old_name: str, default: T) -> int | T:
    """Return ``workers``, its deprecated ``old_name`` spelling, or ``default``; never both."""
    if old is None:
        return default if workers is None else workers
    if workers is not None:
        raise TypeError(f"pass workers= or {old_name}=, not both")
    warnings.warn(f"{old_name}= is deprecated; use workers=", DeprecationWarning, stacklevel=3)
    return old


def warn_few_tasks(plan: Any, workers: int) -> None:
    """Warn that ``plan`` cannot use all of ``workers``; a pull stream's count is not known yet."""
    n = len(getattr(plan, "tasks", ()))
    if 0 < n < workers and getattr(plan, "next_tasks", None) is None:
        warnings.warn(
            f"{n} task(s) for {workers} workers; "
            "pass events_per_task= or steps_per_file= to the plan",
            stacklevel=3,
        )


def refuse_staged(plan: object, runner: str, backend: str) -> None:
    """Refuse a ``DurablePlanV2`` in ``runner``.

    Naming the ``SubmitRunner`` over ``backend`` that runs it.
    """
    if isinstance(plan, DurablePlanV2):
        raise TypeError(
            f"{runner} runs a single-stage Plan; a join or repartition plan (DurablePlanV2) runs "
            f"through SubmitRunner({backend})"
        )
