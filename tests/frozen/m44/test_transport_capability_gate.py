"""m44 honest degradation — the transport engine refuses loudly on a backend without strict
worker pinning, BEFORE any work is submitted, and its entry-point modules import dask-free (plan
§1.6, §8 F13/FU2; blueprint §3 `test_transport_capability_gate`). Runs in the MAIN CI matrix —
no distributed needed.

FU2 discrimination: the ``PinlessBackend`` stub carries the frozen-m42 ThreadBackend flag shape
(``peer_data_movement=True, pin_to_worker=False``) so the m43-era peer-movement gate PASSES and
only the NEW strict-pin requirement can fire — a gate that checks the wrong flag lets it through.
The all-False ``NoPeerTransportBackend`` covers the head-node-routed class. All three entry
points (repartition, join, plan) must raise ``NotImplementedError`` naming the pinning
requirement and the "Phase 2" store-plane alternative with ZERO submits/broadcasts reaching the
stub. The subprocess probe pins F13: importing the DEFERRING entry-point modules
(``transport_peer``, ``transport_shuffle``) leaves ``distributed``/``dask`` out of
``sys.modules`` (``transport.py`` itself is import-dirty by design and is NOT probed)."""

from __future__ import annotations

import subprocess
import sys

import pytest
from transport_harness import (
    NoPeerTransportBackend,
    PinlessBackend,
    make_peer_plan,
    numpy_adapter,
    repartition_blocks,
    transport_join,
    transport_plan_run,
    transport_repartition,
)

PROBE = """
import sys
import graphed_executors.dask_backend.transport_peer as tp
import graphed_executors.dask_backend.transport_shuffle as ts
assert callable(tp.transport_run_plan)
assert callable(ts.transport_run_repartition) and callable(ts.transport_run_join)
assert "distributed" not in sys.modules, "importing the m44 entry-point modules pulled in distributed"
assert "dask" not in sys.modules, "importing the m44 entry-point modules pulled in dask"
print("CLEAN")
"""


def test_entry_point_modules_import_without_touching_distributed() -> None:
    """A fresh interpreter imports both deferring entry-point modules and the three pinned entry
    points while distributed/dask stay unimported (works whether or not the extra is installed)."""
    proc = subprocess.run(
        [sys.executable, "-c", PROBE], capture_output=True, text=True, timeout=120, check=False
    )
    assert proc.returncode == 0, f"stderr:\n{proc.stderr}"
    assert "CLEAN" in proc.stdout


@pytest.mark.parametrize("stub_cls", [PinlessBackend, NoPeerTransportBackend])
def test_missing_pin_capability_refuses_before_any_work(stub_cls) -> None:  # type: ignore[no-untyped-def]
    adapter = numpy_adapter()
    blocks = repartition_blocks(adapter)[:2]
    stub = stub_cls()

    with pytest.raises(NotImplementedError, match="pin") as rep_exc:
        transport_repartition(adapter.backend, blocks, 3, dbackend=stub)
    assert "Phase 2" in str(rep_exc.value), "the refusal must point at the Phase-2 store data plane"

    with pytest.raises(NotImplementedError, match="pin") as join_exc:
        transport_join(adapter.backend, blocks, blocks, 3, dbackend=stub)
    assert "Phase 2" in str(join_exc.value)

    with pytest.raises(NotImplementedError, match="pin") as plan_exc:
        transport_plan_run(make_peer_plan(4, "m44gate"), stub)
    assert "Phase 2" in str(plan_exc.value)

    assert stub.submit_attempts == 0 and stub.broadcast_attempts == 0, (
        "the gate must fire BEFORE any task or payload reaches the backend — a late gate has "
        "already routed work through a backend that cannot host the transport"
    )
