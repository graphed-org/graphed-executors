"""The canonical direct-HTEX recipe (plan §1.2 ``launch.py``, m46 IT3): ``start_htex`` /
``stop_htex``.

``start_htex`` encodes the three measured direct-use moves a DFK normally performs (§0.3, verified
live on parsl 2026.7.20):

1. set ``executor.run_dir`` (the DFK sets it per-executor);
2. set + ``mkdir`` ``provider.script_dir`` (the DFK move at dflow.py:1107-1109);
3. call ``executor.scale_out_facade(provider.init_blocks)`` after ``start()`` (executors "will not
   initialize the blocks requested by any init_blocks parameter" — status_handling.py:45-48; the DFK
   strategy does it at jobs/strategy.py:159).

It also pins the fixed-blocks posture (``init_blocks == min_blocks == max_blocks`` — no strategy, no
scale-in) that the peer-transport milestone relies on, and is the drift canary: parsl's weekly CalVer
changing any of the three moves fails the first fixture loudly.

This is the ONLY module that constructs parsl objects; parsl is imported inside the functions so the
package stays parsl-free at load (``test_parsl_no_cross_import``).
"""

from __future__ import annotations

import os
from typing import Any


def start_htex(*, workers: int, run_dir: str, address: str | None = None, encrypted: bool = False) -> Any:
    """Start a direct-use ``HighThroughputExecutor`` with ``workers`` worker processes in one fixed
    LocalProvider block. Returns the STARTED executor (feed it to :class:`ParslBackend`). The caller
    owns teardown via :func:`stop_htex`.

    The caller's environment is deliberately NOT scrubbed — HTEX workers launch via the
    ``process_worker_pool`` console script and do NOT inherit the driver's ``sys.path``, so a test
    harness exports ``PYTHONPATH`` before calling this (measured load-bearing); clearing it here would
    break worker-side imports of caller-defined task fns.
    """
    from parsl.executors import HighThroughputExecutor  # noqa: PLC0415  (lazy: parsl is the optional extra)
    from parsl.providers import LocalProvider  # noqa: PLC0415

    kwargs: dict[str, Any] = {
        "label": "graphed-htex",
        "max_workers_per_node": workers,
        "encrypted": encrypted,
        "provider": LocalProvider(init_blocks=1, min_blocks=1, max_blocks=1),
    }
    if address is not None:
        kwargs["address"] = address
    executor = HighThroughputExecutor(**kwargs)

    executor.run_dir = run_dir  # move 1
    os.makedirs(run_dir, exist_ok=True)
    executor.provider.script_dir = os.path.join(run_dir, "submit_scripts")  # move 2 (dflow.py:1107-1109)
    os.makedirs(executor.provider.script_dir, exist_ok=True)

    executor.start()
    executor.scale_out_facade(executor.provider.init_blocks)  # move 3 (status_handling.py:45-48)
    return executor


def stop_htex(executor: Any) -> None:
    """Shut the executor down, reaping the interchange, manager, and worker processes (a shutdown
    that signals without joining leaks processes/ports across the fixture bill)."""
    executor.shutdown()
