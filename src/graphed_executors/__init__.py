"""graphed-executors: pluggable executors for a :class:`graphed.core.Plan`.

The reference single-machine executor is :mod:`graphed_executors.local` — thread/process pools,
deterministic straggler-tolerant tree reduction, ``open_once`` file-locality, peer reduction,
work-stealing, and the repartition/shuffle substrate. Additional executors (e.g. distributed
backends) are added as sibling subpackages under their own optional-dependency extras; nothing here
imports a specific backend, so installing the base package pulls only the local one.

The pools, ``SequentialRunner``, the ``*_runner`` functions and ``start_htex``/``stop_htex`` import
from here, as in
``from graphed_executors import ProcessPoolExecutor``.
"""

from __future__ import annotations

from importlib import import_module

TYPE_CHECKING = False  # the same flag as typing's, without typing's import time
if TYPE_CHECKING:
    from graphed.core import SequentialRunner

    from .dask_backend import dask_runner
    from .htcondor_backend import htcondor_runner
    from .local import PinnedPoolExecutor, ProcessPoolExecutor, ThreadExecutor
    from .parsl_backend import parsl_runner, start_htex, stop_htex

__version__ = "0.0.7"

__all__ = [
    "ParslRunner",
    "PinnedPoolExecutor",
    "ProcessPoolExecutor",
    "SequentialRunner",
    "ThreadExecutor",
    "__version__",
    "dask_runner",
    "htcondor_runner",
    "parsl_runner",
    "start_htex",
    "stop_htex",
]

# Resolved on first use, so importing this package loads neither graphed nor a backend.
_HOMES = {
    "ParslRunner": "graphed_executors.parsl_backend",
    "PinnedPoolExecutor": "graphed_executors.local",
    "ProcessPoolExecutor": "graphed_executors.local",
    "SequentialRunner": "graphed.core",
    "ThreadExecutor": "graphed_executors.local",
    "dask_runner": "graphed_executors.dask_backend",
    "htcondor_runner": "graphed_executors.htcondor_backend",
    "parsl_runner": "graphed_executors.parsl_backend",
    "start_htex": "graphed_executors.parsl_backend",
    "stop_htex": "graphed_executors.parsl_backend",
}


# Hidden from type checkers, which would otherwise accept a misspelled name as ``object``.
if not TYPE_CHECKING:

    def __getattr__(name: str) -> object:
        if name not in _HOMES:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        return getattr(import_module(_HOMES[name]), name)


def __dir__() -> list[str]:
    return __all__
