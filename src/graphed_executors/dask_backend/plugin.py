"""The per-worker init plugin (plan §1.3.3): a real :class:`distributed.WorkerPlugin` subclass
(``register_plugin`` raises ``TypeError`` on duck types) holding this worker's ``open_once``
resources. Imports distributed at module top — imported ONLY lazily (when a ``GraphedWorkerPlugin``
is constructed, distributed present) so the package's ``__init__`` stays dask-free.
"""

from __future__ import annotations

from typing import Any

import distributed
from graphed.core.execution import LocalResources


class GraphedWorkerPlugin(distributed.WorkerPlugin):  # type: ignore[misc]
    """Holds a per-worker :class:`LocalResources` so ``open_once`` gives file-locality per dask
    worker exactly as per local pool worker. ``idempotent`` makes re-registration a no-op and
    ``setup`` runs on existing AND late-joining (elastic/jobqueue) workers."""

    name = "graphed-worker"
    idempotent = True

    def setup(self, worker: Any) -> None:
        self.resources = LocalResources()

    def teardown(self, worker: Any) -> None:
        resources = getattr(self, "resources", None)
        if resources is not None:
            resources.close()
