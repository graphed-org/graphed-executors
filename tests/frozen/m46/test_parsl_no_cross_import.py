"""m46 packaging hygiene — the symmetric no-cross-import matrix (plan §0.4, §1.6 import rule,
§3 m46 `test_parsl_no_cross_import`). Runs in the MAIN CI matrix: no parsl, no dask needed — every
probe is a fresh subprocess asserting on ``sys.modules``, not on the environment.

The frozen import rules: ``graphed_executors.parsl_backend`` imports neither parsl (lazy-import
discipline, the `dask_backend/_lazy.py` pattern) nor dask/distributed (G9: parsl never imports
`dask_backend`); ``graphed_executors.common`` imports neither parsl nor dask nor distributed
(importable with none installed); ``local``/``submit`` stay parsl-free; and symmetrically
``dask_backend`` — which m46 reduces to a re-export shim over ``common/tasks_engine`` — stays
parsl-free too.

Discriminates: a module-import-time ``import parsl`` anywhere on the ``parsl_backend`` import
path, an eager engine import in ``common/``, a shim that drags parsl into the dask (or dask into
the parsl) package, and a missing public lazy export (``ParslBackend``/``parsl_runner``)."""

from __future__ import annotations

import subprocess
import sys

PARSL_BACKEND_PROBE = """
import sys
import graphed_executors.parsl_backend
from graphed_executors.parsl_backend import ParslBackend, parsl_runner
assert "parsl" not in sys.modules, "importing parsl_backend pulled in parsl (lazy-import broken)"
assert "dask" not in sys.modules, "importing parsl_backend pulled in dask (G9 violation)"
assert "distributed" not in sys.modules, "importing parsl_backend pulled in distributed (G9 violation)"
print("CLEAN")
"""

COMMON_PROBE = """
import sys
import graphed_executors.common
import graphed_executors.common.tasks_engine
import graphed_executors.common.relay_engine
for banned in ("parsl", "dask", "distributed"):
    assert banned not in sys.modules, f"importing graphed_executors.common pulled in {banned}"
print("CLEAN")
"""

LOCAL_SUBMIT_PROBE = """
import sys
import graphed_executors.local
import graphed_executors.local.shuffle
import graphed_executors.submit
assert "parsl" not in sys.modules, "local/submit pulled in parsl"
print("CLEAN")
"""

DASK_BACKEND_PROBE = """
import sys
import graphed_executors.dask_backend
import graphed_executors.dask_backend.shuffle
assert "parsl" not in sys.modules, "dask_backend (the m46 shim included) pulled in parsl"
print("CLEAN")
"""


def _run_probe(probe: str) -> None:
    proc = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=120, check=False
    )
    assert proc.returncode == 0, f"stderr:\n{proc.stderr}"
    assert "CLEAN" in proc.stdout


def test_parsl_backend_imports_without_parsl_dask_or_distributed() -> None:
    """A fresh interpreter imports the package and both pinned public names while parsl, dask,
    and distributed all stay unimported (works whether or not the extras are installed)."""
    _run_probe(PARSL_BACKEND_PROBE)


def test_common_imports_without_parsl_dask_or_distributed() -> None:
    """``common/`` (tasks_engine + relay_engine — the m46 bootstrap) is importable with none of
    the three optional stacks touched (plan §0.4 hard rule; frozen-tested per §1.6)."""
    _run_probe(COMMON_PROBE)


def test_local_and_submit_leave_parsl_out() -> None:
    """``local/`` and ``submit/`` are untouched by m46 and must never name parsl (plan §0.4).
    NOTE: this probe passes pre-implementation by design — it is the regression guard those
    edit-frozen packages carry through the m46 factoring."""
    _run_probe(LOCAL_SUBMIT_PROBE)


def test_dask_backend_leaves_parsl_out() -> None:
    """The symmetric arm (plan r2 tests-minor): ``dask_backend`` — whose ``shuffle.py`` m46
    reduces to the ``common/tasks_engine`` re-export shim — must not pull parsl in. Passes
    pre-implementation by design; guards the shim edit itself."""
    _run_probe(DASK_BACKEND_PROBE)
