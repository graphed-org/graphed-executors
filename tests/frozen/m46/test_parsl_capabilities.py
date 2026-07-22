"""m46 per-instance capability honesty — the research critique's one correction, frozen
(plan §1.1 table; §3 m46 `test_parsl_capabilities`).

The frozen 7-field ``SubmitCapabilities`` (m42 pin — NO new fields, ever) carries per-INSTANCE
vectors: ``ParslBackend(HTEX)`` is all-seven-False (literally the protocol docstring's "parsl
floor"), ``ParslBackend(ThreadPoolExecutor)`` is the ThreadBackend shape (``peer_data_movement``
alone True — same-process shared memory). Pinned as DATACLASS EQUALITY against locally-built
expected instances, so a wrong flag anywhere fails without string games. Unknown executor types
are refused with a loud TypeError naming both supported classes — per-instance honesty means not
vouching for executors the plan has not verified (TaskVine/WorkQueue are the Phase-2 seam).

Discriminates: a per-library (non-instance) capability stub (fails the TPE row), a
``peer_data_movement=True`` HTEX lie (the §1.1.4 `_require_peer` silent-pathology blast radius —
also frozen at the engine gate in `test_parsl_m43_engine_on_tpe`), a capability subclass smuggling
an eighth field (dataclass equality is class-exact), and a name-string type check (the stdlib
``ThreadPoolExecutor`` trap: same class name, wrong library, must still be refused)."""

from __future__ import annotations

import concurrent.futures
import dataclasses
import sys

import pytest
from parsl_harness import (
    expected_htex_caps,
    expected_tpe_caps,
    htex_pool,
    make_parsl_backend,
    tpe_pool,
)

from graphed_executors.submit.protocol import SubmitCapabilities

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (parsl ships no Windows support; Windows keeps the base "
    "package only — plan §4 G8)",
)

FLAG_NAMES = (
    "peer_data_movement",
    "scatter_broadcast",
    "pin_to_worker",
    "per_task_retries",
    "per_task_resources",
    "cancel_running",
    "worker_file_cache",
)


@pytest.fixture(scope="module")
def htex():  # type: ignore[no-untyped-def]
    with htex_pool(workers=2) as ex:
        yield ex


@pytest.fixture(scope="module")
def tpe():  # type: ignore[no-untyped-def]
    with tpe_pool(max_threads=2) as ex:
        yield ex


def test_htex_instance_reports_the_all_false_parsl_floor(htex) -> None:  # type: ignore[no-untyped-def]
    caps = make_parsl_backend(htex).capabilities
    assert isinstance(caps, SubmitCapabilities)
    assert caps == expected_htex_caps(), (
        "ParslBackend(HTEX) must report all seven flags False — future args resolve on the submit "
        "host (peer_data_movement), broadcast reships per task, no pinning/per-task retries/"
        "resources/running-cancel/file-cache exist (plan §1.1 table). In particular "
        "peer_data_movement=True would falsely open the m43 _require_peer gate to head-node routing."
    )


def test_tpe_instance_reports_the_threadbackend_shape(tpe) -> None:  # type: ignore[no-untyped-def]
    caps = make_parsl_backend(tpe).capabilities
    assert isinstance(caps, SubmitCapabilities)
    assert caps == expected_tpe_caps(), (
        "ParslBackend(TPE) must report peer_data_movement=True (same-process shared memory — the "
        "ThreadBackend precedent) and everything else False. A per-LIBRARY implementation that "
        "gives every parsl instance one vector fails exactly here (the critique correction)."
    )


def test_capability_fields_are_exactly_the_frozen_seven(htex, tpe) -> None:  # type: ignore[no-untyped-def]
    """No new capability field, ever (m42 pin restated by m45): both instances carry the exact
    7-name frozen dataclass — not a subclass with extras (a subclass is a shadow capability
    channel; m47's transport dispatch goes through a backend attribute, never capability fields)."""
    for backend in (make_parsl_backend(htex), make_parsl_backend(tpe)):
        caps = backend.capabilities
        assert tuple(f.name for f in dataclasses.fields(type(caps))) == FLAG_NAMES
        assert type(caps) is SubmitCapabilities


def test_unknown_executor_types_are_refused_loudly() -> None:
    """A vector is never guessed: non-parsl objects — including the stdlib ThreadPoolExecutor,
    whose class NAME matches parsl's — get a TypeError naming both supported executor classes."""
    with pytest.raises(TypeError) as excinfo:
        make_parsl_backend(object())
    message = str(excinfo.value)
    assert "HighThroughputExecutor" in message and "ThreadPoolExecutor" in message, (
        f"the refusal must name the two verified executor classes, got: {message!r}"
    )

    stdlib_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        with pytest.raises(TypeError):
            make_parsl_backend(stdlib_pool)  # same __name__ as parsl's TPE — a name check passes it
    finally:
        stdlib_pool.shutdown()
