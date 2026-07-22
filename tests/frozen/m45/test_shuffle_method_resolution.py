"""m45 resolution pins — ``resolve_shuffle_method`` is a pure capability truth table, an invalid
method fails loudly before any work, and the api module imports dask-free (m45-facade-plan §1.1,
§2 items 6/10/11). Runs in the MAIN CI matrix — no distributed needed.

The r2-corrected truth table: explicit methods resolve to themselves under EVERY capability
combination (the engines' own gates handle unsupported backends); "auto" needs BOTH
pin_to_worker AND peer_data_movement for transport (peer-only, pin-only, and neither all degrade
to tasks); an unknown method raises ValueError naming all three valid values regardless of caps,
with ZERO submits/broadcasts reaching the backend."""

from __future__ import annotations

import subprocess
import sys

import pytest
from shuffle_method_harness import (
    BACKEND,
    RefusingBackend,
    api,
    facade_join,
    facade_repartition,
    join_sides,
    make_caps,
    repartition_blocks,
)

ALL_CAPS = {
    "both": make_caps(peer=True, pin=True),
    "peer-only": make_caps(peer=True, pin=False),
    "pin-only": make_caps(peer=False, pin=True),
    "neither": make_caps(peer=False, pin=False),
}

PROBE = """
import sys
import graphed_executors.dask_backend
import graphed_executors.dask_backend.api as api
assert callable(api.run_repartition) and callable(api.run_join)
assert callable(api.resolve_shuffle_method)
assert "distributed" not in sys.modules, "importing the facade pulled in distributed"
assert "dask" not in sys.modules, "importing the facade pulled in dask"
print("CLEAN")
"""


def test_resolution_truth_table() -> None:
    resolve = api().resolve_shuffle_method
    for name, caps in ALL_CAPS.items():
        assert resolve("transport", caps) == "transport", f"explicit transport must stand ({name})"
        assert resolve("tasks", caps) == "tasks", f"explicit tasks must stand ({name})"
    assert resolve("auto", ALL_CAPS["both"]) == "transport"
    for name in ("peer-only", "pin-only", "neither"):
        assert resolve("auto", ALL_CAPS[name]) == "tasks", (
            f"auto must degrade to tasks on {name} — transport needs BOTH pin_to_worker and "
            "peer_data_movement"
        )
    for name, caps in ALL_CAPS.items():
        with pytest.raises(ValueError, match="auto") as excinfo:
            resolve("p2p", caps)
        msg = str(excinfo.value)
        assert "transport" in msg and "tasks" in msg, (
            f"the error must list all three valid values ({name}): {msg}"
        )


def test_invalid_method_raises_before_any_work() -> None:
    stub = RefusingBackend(make_caps(peer=True, pin=True))
    blocks = repartition_blocks()[:2]
    left, right = join_sides()

    with pytest.raises(ValueError, match="auto"):
        facade_repartition(BACKEND, blocks, 4, dbackend=stub, shuffle_method="p2p")
    with pytest.raises(ValueError, match="auto"):
        facade_join(BACKEND, left, right, 4, dbackend=stub, shuffle_method="p2p")
    assert stub.submit_attempts == 0 and stub.broadcast_attempts == 0, (
        "an invalid shuffle_method must be rejected before ANY work reaches the backend"
    )


def test_api_module_imports_without_touching_distributed() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", PROBE], capture_output=True, text=True, timeout=120, check=False
    )
    assert proc.returncode == 0, f"stderr:\n{proc.stderr}"
    assert "CLEAN" in proc.stdout
