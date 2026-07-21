"""m43 honest scoping — shuffle on a backend WITHOUT peer data movement refuses loudly, and the
shuffle module itself imports dask-free (plan §1.3.4 capability gate; blueprint §3
`test_dask_shuffle_capability_gate`). Runs in the MAIN CI matrix — no distributed needed.

A head-node-routed backend (parsl-HTEX class) would funnel every block through the driver — the
exact pathology §1.3.4 rejects — so both entry points must check
`runner.backend.capabilities.peer_data_movement` and raise `NotImplementedError` naming BOTH the
missing capability and the Phase-2 store-handle data plane, BEFORE any work is submitted (the stub
counts and refuses submits/broadcasts). Discriminates: a silent wrong answer on such backends, a
gate that fires only after stage 1 was already submitted, and a module-import-time `import
distributed` in the shuffle module (subprocess probe, the m42 packaging idiom)."""

from __future__ import annotations

import subprocess
import sys

import pytest
from dask_shuffle_backends import (
    NoPeerBackend,
    dask_join,
    dask_repartition,
    make_submit_runner,
    repartition_blocks,
    shuffle_adapters,
)

ADAPTERS = shuffle_adapters()

PROBE = """
import sys
import graphed_executors.dask_backend.shuffle as shuf
assert callable(shuf.dask_run_repartition) and callable(shuf.dask_run_join)
assert "distributed" not in sys.modules, "importing the shuffle module pulled in distributed"
assert "dask" not in sys.modules, "importing the shuffle module pulled in dask"
print("CLEAN")
"""


def test_shuffle_module_imports_without_touching_distributed() -> None:
    """A fresh interpreter imports the shuffle module and both pinned entry points while
    distributed/dask stay unimported (works whether or not the extra is installed)."""
    proc = subprocess.run(
        [sys.executable, "-c", PROBE], capture_output=True, text=True, timeout=120, check=False
    )
    assert proc.returncode == 0, f"stderr:\n{proc.stderr}"
    assert "CLEAN" in proc.stdout


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request):  # type: ignore[no-untyped-def]
    return request.param


def test_no_peer_data_movement_refuses_before_any_work(adapter) -> None:  # type: ignore[no-untyped-def]
    stub = NoPeerBackend()
    runner = make_submit_runner(stub)
    blocks = repartition_blocks(adapter)[:2]

    with pytest.raises(NotImplementedError, match="peer data movement") as rep_exc:
        dask_repartition(adapter.backend, blocks, 3, runner=runner)
    assert "Phase 2" in str(rep_exc.value), "the refusal must point at the Phase-2 store data plane"

    with pytest.raises(NotImplementedError, match="peer data movement") as join_exc:
        dask_join(adapter.backend, blocks, blocks, 3, runner=runner)
    assert "Phase 2" in str(join_exc.value)

    assert stub.submit_attempts == 0 and stub.broadcast_attempts == 0, (
        "the gate must fire BEFORE any task or payload reaches the backend — a late gate has "
        "already routed blocks through the head node"
    )
