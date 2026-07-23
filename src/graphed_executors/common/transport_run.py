"""Backend-agnostic worker-transport result types + failure classifier (plan §1.6 m47 move).

The m44 witness/result dataclasses and the failure-classification helpers, moved here VERBATIM from
``dask_backend/_transport_run.py`` so BOTH the dask worker-transport engine and the m47 parsl
peer-exchange engine share the SAME objects (``dask_backend._transport_run.TransportWitness IS
common.transport_run.TransportWitness`` — a frozen m47 identity witness). This module imports NOTHING
from dask, distributed, or parsl: the classifier matches exception classes by NAME string, so a
parsl-side ``TransportDeliveryError``/``PullTimeoutError`` is recognized without importing either
backend (the §1.6 ``common/`` import rule; ``test_parsl_transport_imports``).

``dask_backend/_transport_run.py`` keeps the dask-model-specific pieces (``require_pin`` reads
``backend.capabilities``; ``sorted_addresses``/``_counters_probe``/``_purge_probe``/
``collect_and_purge`` touch a dask ``Client``) and re-exports the moved names from here.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TransportWitness:
    """The transport-plane witness (``result.transport``): the run epochs, restarts performed, and
    the per-worker sender/holder/reduction counters (aggregated driver-side before purge)."""

    epoch_nonces: list[str] = field(default_factory=list)
    epoch_restarts: int = 0
    per_worker: dict[str, dict[str, int]] = field(default_factory=dict)


@dataclass
class TransportShuffleResult:
    """Carries the local engine's shape (``.dest_block_hashes``/``.value``/``.witness`` — the imported
    ``ShuffleWitness`` counter names) plus the transport-plane witness."""

    dest_block_hashes: dict[int, str]
    value: dict[int, Any]
    witness: Any  # a graphed_executors.local.shuffle.ShuffleWitness
    transport: TransportWitness


@dataclass
class TransportExecResult:
    """``ExecResult``-shaped (``.value``/``.n_partitions``/``.n_combines``) + the transport witness."""

    value: Any
    n_partitions: int
    n_combines: int
    transport: TransportWitness


def merge_counters(per_worker: dict[str, dict[str, int]], counters: Mapping[str, Mapping[str, int]]) -> None:
    for addr, c in counters.items():
        acc = per_worker.setdefault(addr, {})
        for key, val in c.items():
            if key == "peak_holder_bytes":
                acc[key] = max(acc.get(key, 0), int(val))
            elif key == "serve_pid":
                if val:
                    acc[key] = int(val)
            else:
                acc[key] = acc.get(key, 0) + int(val)


# ---- failure classification / attribution (matched by exception NAME, so dask/parsl-agnostic) ----
def _in_chain(exc: BaseException | None, name: str) -> bool:
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if type(exc).__name__ == name:
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def is_restart_worthy(exc: BaseException | None, backend: Any) -> bool:
    """A run restarts under a fresh epoch on ANY of: an exhausted-send ``TransportDeliveryError``, a
    block-plane ``PullTimeoutError`` (a timed-out holder is classified LOST — the whole-run restart
    re-runs the producers onto the survivors), a ``describe_failure``-recognized worker death
    (``KilledWorker``/``WorkerLost``/``ManagerLost``), or a bare comm loss to a dead holder."""
    if exc is None:
        return False
    if _in_chain(exc, "TransportDeliveryError") or _in_chain(exc, "PullTimeoutError"):
        return True
    describe = getattr(backend, "describe_failure", None)
    if describe is not None and describe(exc) is not None:
        return True
    return _in_chain(exc, "CommClosedError") or _in_chain(exc, "KilledWorker")


def pick_attributable(errs: Mapping[Any, BaseException], backend: Any) -> BaseException:
    """Prefer a ``describe_failure``-attributable death (names the victim worker) over a generic
    comm/delivery failure, so the surfaced ``StageError`` names the true cause."""
    describe = getattr(backend, "describe_failure", None)
    if describe is not None:
        for exc in errs.values():
            if describe(exc) is not None:
                return exc
    return next(iter(errs.values()))


def _with_frames(exc: BaseException) -> str:
    """``str(exc)`` plus the exception's own traceback frames when it carries one (a worker task's
    exception ships its remote traceback through ``fut.result()``). Best-effort: never let formatting
    mask the real error."""
    base = str(exc)
    tb = getattr(exc, "__traceback__", None)
    if tb is None:
        return base
    import traceback  # noqa: PLC0415  (error path only)

    with contextlib.suppress(Exception):
        frames = "".join(traceback.format_tb(tb)).rstrip()
        if frames:
            return f"{base}\n--- wrapped traceback ---\n{frames}"
    return base


def build_stage_error(exc: BaseException, backend: Any) -> Any:
    """Wrap a run-fatal failure as an attributed ``StageError`` (never a raw ``KilledWorker`` /
    ``WorkerLost`` / ``TransportDeliveryError`` to the user, §1.5 / m42 precedent). A
    ``describe_failure``-recognized death names the victim; anything else keeps its own type +
    message (with the wrapped traceback frames appended, so a CI-only crash names its site)."""
    from graphed.debug import SourceFrame, StageError  # noqa: PLC0415  (dask/parsl-free; error path only)

    describe = getattr(backend, "describe_failure", None)
    info = describe(exc) if describe is not None else None
    if info is not None:
        key, last_worker = info
        return StageError(
            op="run",
            frames=(SourceFrame(filename=str(key), lineno=0),),
            input_forms=(),
            partition=str(key),
            cause_type="KilledWorker",
            cause_message=(
                f"worker {last_worker} died (segfault/OOM/preemption suspected; note: blame can be "
                "unfair under co-located tasks)"
            ),
            opt_level=0,
        )
    return StageError(
        op="run",
        frames=(SourceFrame(filename="transport-run", lineno=0),),
        input_forms=(),
        partition="transport-run",
        cause_type=type(exc).__name__,
        cause_message=_with_frames(exc),
        opt_level=0,
    )
