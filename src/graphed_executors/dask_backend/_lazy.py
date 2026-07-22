"""Lazy ``distributed`` accessor (plan §1.3.1) — the repo's optional-dep idiom (no ``HAS_X`` flag).

``graphed_executors.dask_backend`` imports NOTHING from dask/distributed at module import time; only
constructing :class:`DaskBackend` touches this accessor, so ``test_submit_no_dask_import`` (imports
the package, asserts ``"distributed" not in sys.modules``) stays satisfiable while the backend is
still implementable against the pinned signatures.
"""

from __future__ import annotations

from typing import Any


def _distributed() -> Any:
    try:
        import distributed  # noqa: PLC0415  (lazy: dask is the optional extra)
    except ImportError as exc:  # pragma: no cover - exercised via the no-dask frozen test (subprocess)
        raise ImportError(
            "the dask backend needs dask.distributed — install the optional extra: "
            "pip install 'graphed-executors[dask]'"
        ) from exc
    return distributed
