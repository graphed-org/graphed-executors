"""m47 import hygiene for the NEW modules (MAIN matrix — no parsl/dask needed): the frozen §1.6
import rule applied to every module m47 creates, plus the shim identities the §1.6 move must
preserve (plan §0.4, §1.6 import rule, G9; the m46 `test_parsl_no_cross_import` precedent extended
to modules that did not exist at the m46 freeze).

Subprocess probes (each import in a FRESH interpreter so sys.modules is a clean witness):
- ``common.http_plane`` / ``common.transport_run`` / ``common.facade`` / ``common.transport_tasks``
  import with parsl AND dask AND distributed absent from sys.modules (importable with none
  installed — the frozen `common/` rule);
- ``parsl_backend.api`` / ``.transport_peer`` / ``.transport_shuffle`` import with parsl, dask,
  distributed AND ``graphed_executors.dask_backend`` absent (G9: the parsl facade never couples to
  the dask package);
- in-process identity pins: ``dask_backend.api.resolve_shuffle_method IS
  common.facade.resolve_shuffle_method`` (the m47 move leaves the dask facade a re-export shim —
  the m45 truth-table suite keeps gating the shared object) and
  ``dask_backend._transport_run.TransportWitness IS common.transport_run.TransportWitness`` (the
  witness classes are MOVED objects, not copies — a copy would fork the result types the m44
  suite pins).

Discriminates: an eager ``import parsl``/``import dask`` anywhere under the new modules; a parsl
facade importing ``dask_backend`` for the resolver instead of ``common/facade``; copied (drift-
prone) resolver/witness definitions left behind by an incomplete move."""

from __future__ import annotations

import subprocess
import sys

COMMON_PROBE = """
import sys
import graphed_executors.common.http_plane
import graphed_executors.common.transport_run
import graphed_executors.common.facade
import graphed_executors.common.transport_tasks
for banned in ("parsl", "dask", "distributed"):
    assert banned not in sys.modules, f"importing common/* pulled in {banned}"
print("CLEAN")
"""

PARSL_BACKEND_PROBE = """
import sys
import graphed_executors.parsl_backend.api
import graphed_executors.parsl_backend.transport_peer
import graphed_executors.parsl_backend.transport_shuffle
for banned in ("parsl", "dask", "distributed", "graphed_executors.dask_backend"):
    assert banned not in sys.modules, f"importing parsl_backend/* pulled in {banned}"
print("CLEAN")
"""


def _run_probe(code: str) -> None:
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=180, check=False
    )
    assert proc.returncode == 0, f"stderr:\n{proc.stderr}"
    assert "CLEAN" in proc.stdout


def test_common_m47_modules_import_free_of_parsl_dask_distributed() -> None:
    _run_probe(COMMON_PROBE)


def test_parsl_backend_m47_modules_never_pull_dask_backend_or_engines() -> None:
    _run_probe(PARSL_BACKEND_PROBE)


def test_dask_facade_and_transport_run_are_shims_over_common() -> None:
    import graphed_executors.common.facade as cfacade  # noqa: PLC0415  (module under test)
    import graphed_executors.common.transport_run as ctr  # noqa: PLC0415  (module under test)
    import graphed_executors.dask_backend._transport_run as dtr  # noqa: PLC0415  (frozen anchor)
    import graphed_executors.dask_backend.api as dapi  # noqa: PLC0415  (frozen anchor)

    assert dapi.resolve_shuffle_method is cfacade.resolve_shuffle_method, (
        "dask_backend.api must re-export THE resolver object from common/facade — a copy forks "
        "the m45-frozen truth table"
    )
    assert dtr.TransportWitness is ctr.TransportWitness, (
        "dask_backend._transport_run.TransportWitness must be the MOVED common/transport_run "
        "class, not a copy — the m44 result types and the parsl results must share it"
    )
    assert dtr.TransportShuffleResult is ctr.TransportShuffleResult
    assert dtr.TransportExecResult is ctr.TransportExecResult
