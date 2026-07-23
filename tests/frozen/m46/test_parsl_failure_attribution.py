"""m46 failure attribution — §A.3 #8 on the parsl seam (plan §1.2 `describe_failure`; §3 m46
`test_parsl_failure_attribution`; the m42 `test_dask_stageerror_intact` shape + the r3 synthetic
classifier freeze, tests-lens (d)).

Two obligations:

1. **StageError intact across the parsl result path** (the M6 pickle-across-process pin): a
   `process` raising a fully-populated StageError surfaces at the driver AS a StageError with the
   user frame intact — on HTEX (a real process boundary + parsl's serialized exception path) and
   on TPE. Never a string, a RuntimeError, or a parsl wrapper.

2. **The name-string classifier, frozen SYNTHETICALLY**: `describe_failure` maps exceptions whose
   chain carries a class NAMED "WorkerLost"/"ManagerLost" — classes synthesized locally, NOT
   parsl's — to a (failing_key, last_worker)-style attribution 2-tuple, and unrelated exceptions
   to None. Pinning on synthesized names freezes the plan's is_restart_worthy string-match
   contract (`_transport_run.py` idiom) while staying parsl-version-independent: the test never
   depends on parsl actually raising, and a classifier that `isinstance`s against parsl's real
   classes fails it (and would also break the module's parsl-import-free load, frozen in
   `test_parsl_no_cross_import`).

Discriminates: an exception path that re-wraps or stringifies StageError, a classifier keyed on
parsl class identity instead of chain names, one that ignores `__cause__` chains, and one that
attributes everything (the unrelated-exception None arm)."""

from __future__ import annotations

import sys

import pytest
from graphed.debug import StageError
from parsl_harness import (
    USER_FRAME,
    htex_pool,
    make_parsl_backend,
    make_runner,
    run_bounded,
    stage_error_plan,
    tpe_pool,
)

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (parsl ships no Windows support; Windows keeps the base "
    "package only — plan §4 G8)",
)


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with htex_pool(workers=2) as ex:
        yield ex


@pytest.fixture(scope="module")
def tpe():  # type: ignore[no-untyped-def]
    with tpe_pool(max_threads=2) as ex:
        yield ex


@pytest.fixture(params=["tpe", "htex"])
def backend(request, tpe, htex):  # type: ignore[no-untyped-def]
    return make_parsl_backend(tpe if request.param == "tpe" else htex)


def test_worker_error_surfaces_as_intact_stage_error(backend) -> None:  # type: ignore[no-untyped-def]
    """A StageError raised in `process` reaches the driver as a StageError pointing at the USER'S
    line — over the real HTEX process boundary this is the M6 picklability obligation executed."""
    runner = make_runner(backend)
    with pytest.raises(StageError) as excinfo:
        run_bounded(lambda: runner.run(stage_error_plan(4, "m46err")))
    assert excinfo.value.op == "mul"
    assert excinfo.value.user_frame.filename == USER_FRAME.filename == "user_analysis.py"
    assert excinfo.value.user_frame.lineno == USER_FRAME.lineno == 42


def _synth(name: str, *args: object) -> BaseException:
    """An exception class synthesized BY NAME — deliberately not parsl's (the name-string
    contract: the classifier must match on chain class names, never on parsl identity)."""
    return type(name, (Exception,), {})(*args)


def test_describe_failure_maps_synthetic_worker_lost_by_name(tpe) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(tpe)

    direct = backend.describe_failure(_synth("WorkerLost", "Task failure due to loss of worker 0"))
    assert direct is not None, "a WorkerLost-named exception must map to an attribution tuple"
    assert isinstance(direct, tuple) and len(direct) == 2
    assert all(isinstance(part, str) for part in direct), (
        f"the attribution must be the engine's (failing_key, last_worker) 2-tuple of strings, got {direct!r}"
    )

    manager = backend.describe_failure(_synth("ManagerLost", "manager gone"))
    assert manager is not None and isinstance(manager, tuple) and len(manager) == 2


def test_describe_failure_walks_the_exception_chain(tpe) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(tpe)
    outer = RuntimeError("wrapped by an intermediate layer")
    outer.__cause__ = _synth("WorkerLost", "loss of worker 1 on host x")
    assert backend.describe_failure(outer) is not None, (
        "a WorkerLost buried under __cause__ must still attribute — the classifier walks the "
        "chain (the _in_chain idiom), not just the top exception"
    )


def test_describe_failure_returns_none_for_unrelated_exceptions(tpe) -> None:  # type: ignore[no-untyped-def]
    backend = make_parsl_backend(tpe)
    assert backend.describe_failure(ValueError("negative pt")) is None, (
        "an ordinary exception must NOT be attributed as a worker death — the engine re-raises "
        "it intact (over-attribution would blame workers for user bugs)"
    )
    assert backend.describe_failure(_synth("WorkerBusy", "not a death signal")) is None
