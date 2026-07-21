"""m42 packaging: the submit protocol/engine and the dask_backend package import WITHOUT dask
installed, and a dask-less environment gets the pinned actionable ImportError — this module runs in
the MAIN CI matrix, no distributed needed (plan §1.1 "no dask import anywhere in submit/", §1.3.1;
blueprint §3 `test_submit_no_dask_import`).

Discriminates: a module-import-time `import distributed` anywhere on the import path of
`graphed_executors.submit` or `graphed_executors.dask_backend`, a silent None backend instead of
the hinted ImportError, and a missing public re-export."""

from __future__ import annotations

import subprocess
import sys

import pytest
from submit_backends import make_dask_backend

PROBE = """
import sys
import graphed_executors.submit
import graphed_executors.dask_backend
from graphed_executors.submit import (
    RunContext,
    SubmitBackend,
    SubmitCapabilities,
    SubmitFuture,
    SubmitRunner,
    ThreadBackend,
    WorkerEnv,
    current_env,
    set_worker_env,
)
from graphed_executors.dask_backend import DaskBackend, dask_runner
assert "distributed" not in sys.modules, "importing the packages pulled in distributed"
assert "dask" not in sys.modules, "importing the packages pulled in dask"
print("CLEAN")
"""


def test_import_and_public_surface_without_touching_distributed() -> None:
    """A fresh interpreter imports both packages and every pinned public name while distributed
    stays unimported (works whether or not the extra is installed — the check is on sys.modules,
    not the environment)."""
    proc = subprocess.run(
        [sys.executable, "-c", PROBE], capture_output=True, text=True, timeout=120, check=False
    )
    assert proc.returncode == 0, f"stderr:\n{proc.stderr}"
    assert "CLEAN" in proc.stdout


def test_missing_distributed_raises_the_hinted_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Constructing a DaskBackend with distributed unimportable raises an ImportError that tells
    the user the fix — the exact extra name (plan §1.3.1 accessor)."""
    monkeypatch.setitem(sys.modules, "distributed", None)  # import distributed -> ImportError
    with pytest.raises(ImportError, match=r"graphed-executors\[dask\]"):
        make_dask_backend(object())
