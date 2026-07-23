"""Lazy ``parsl`` accessor (plan §1.2, m46 IT3) — the repo's optional-dep idiom (no ``HAS_X`` flag),
mirroring ``dask_backend/_lazy.py``.

``graphed_executors.parsl_backend`` imports NOTHING from parsl at module import time; only
constructing :class:`ParslBackend` (which needs a real parsl executor anyway) touches this accessor,
so ``test_parsl_no_cross_import`` (imports the package, asserts ``"parsl" not in sys.modules``) stays
satisfiable.
"""

from __future__ import annotations

from typing import Any


def _parsl_executor_types() -> tuple[Any, Any]:
    """Return ``(HighThroughputExecutor, ThreadPoolExecutor)`` — the two verified executor classes
    the capability vector is derived from (plan §1.1). Deferred so the module imports parsl-free."""
    try:
        from parsl.executors import (  # noqa: PLC0415  (lazy: parsl is the optional extra)
            HighThroughputExecutor,
            ThreadPoolExecutor,
        )
    except ImportError as exc:  # pragma: no cover - a ParslBackend needs a parsl executor to exist
        raise ImportError(
            "the parsl backend needs parsl — install the optional extra: "
            "pip install 'graphed-executors[parsl]'"
        ) from exc
    return HighThroughputExecutor, ThreadPoolExecutor
