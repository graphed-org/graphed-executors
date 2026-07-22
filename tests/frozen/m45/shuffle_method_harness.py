"""Shared harness for the m45 frozen suite: the unified shuffle facade
``graphed_executors.dask_backend.api`` with ``shuffle_method="auto"|"transport"|"tasks"`` over the
frozen m43 (as-tasks) and m44 (transport) engines (m45-facade-plan §1/§2, final r2).

Pinned facade contract under test (plan §1, test-author restatement):

    api.run_repartition(backend, src_blocks, parts, *, dbackend, shuffle_method="auto", salt=0,
        n_tasks=None, fetch_budget_bytes=None, disk_budget_bytes=None, holder_budget_bytes=None,
        pull_timeout_s=None, epoch_restarts_allowed=None)
    api.run_join(backend, left_blocks, right_blocks, parts, *, on=("__joinkey__",), how="inner",
        dbackend, shuffle_method="auto", broadcast=None, salt=0, mem_budget_bytes=None,
        holder_budget_bytes=None, pull_timeout_s=None, epoch_restarts_allowed=None)
    api.resolve_shuffle_method(method, caps) -> "transport" | "tasks"   (pure, no cluster)

- resolution: "auto" -> transport iff caps.pin_to_worker AND caps.peer_data_movement, else tasks;
  explicit methods always resolve to themselves (the ENGINES' own gates then apply); anything
  else -> ValueError listing the three valid values, before any work.
- knob honesty (§1.2): transport-only knobs ({n_tasks, fetch_budget_bytes, disk_budget_bytes,
  holder_budget_bytes, pull_timeout_s, epoch_restarts_allowed} on repartition;
  {holder_budget_bytes, pull_timeout_s, epoch_restarts_allowed} on join) EXPLICITLY set while
  resolution lands on tasks -> ValueError naming the knob, AFTER resolution, BEFORE any dispatch.
  ``mem_budget_bytes`` on join is COMMON (r2 correction) and must run on both paths.
- return: the engine's own result unchanged — common triple ``dest_block_hashes``/``value``/
  ``witness`` on both; ``.transport`` only on the transport result, ``.partitions`` only on the
  tasks result (the resolution-observability pin).

Repo convention (plan §2 r2): this harness REPLICATES its fixtures — zero cross-suite imports
(pyproject ``pythonpath`` lists only src + m7/m10/m31/m34/m37; importing another frozen suite's
harness is collection-order-fragile). The two engine modules ARE imported directly: they are the
frozen-verified substrate the parity oracles call. All witnesses are counters/hashes/fn-name
shapes, never clocks; every cluster call runs under a hard-timeout thread join. Pre-implementation
every test fails in-body with ``ModuleNotFoundError: ...dask_backend.api`` via ``api()``."""

from __future__ import annotations

import functools
import threading
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

import numpy as np
from graphed.numpy import NumpyBackend

from graphed_executors.submit import SubmitRunner
from graphed_executors.submit.protocol import SubmitCapabilities

# ---- deferred accessor: the module under test ----------------------------------------------------


def api() -> Any:
    """``graphed_executors.dask_backend.api`` — absent pre-implementation: every test's
    right-reason ``ModuleNotFoundError`` lands here."""
    import graphed_executors.dask_backend.api as mod  # noqa: PLC0415  (deferred: module under test)

    return mod


def facade_repartition(*args: Any, **kwargs: Any) -> Any:
    return api().run_repartition(*args, **kwargs)


def facade_join(*args: Any, **kwargs: Any) -> Any:
    return api().run_join(*args, **kwargs)


# ---- capability sets (the frozen 7-flag SubmitCapabilities; no new fields) -----------------------


def make_caps(*, peer: bool, pin: bool) -> SubmitCapabilities:
    return SubmitCapabilities(
        peer_data_movement=peer,
        scatter_broadcast=False,
        pin_to_worker=pin,
        per_task_retries=False,
        per_task_resources=False,
        cancel_running=False,
        worker_file_cache=False,
    )


FULL_CAPS = make_caps(peer=True, pin=True)
PINLESS_CAPS = make_caps(peer=True, pin=False)  # the frozen-m42 ThreadBackend flag shape
ALLFALSE_CAPS = make_caps(peer=False, pin=False)


class RefusingBackend:
    """Protocol-shaped stub with the given capabilities: any facade path that raises BEFORE work
    must leave it untouched — ``submit``/``broadcast`` count and refuse (the m43/m44 gate idiom)."""

    def __init__(self, capabilities: SubmitCapabilities) -> None:
        self.capabilities = capabilities
        self.submit_attempts = 0
        self.broadcast_attempts = 0

    def n_workers(self) -> int:
        return 1

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        self.submit_attempts += 1
        raise AssertionError("the facade must refuse BEFORE submitting any work")

    def broadcast(self, payload: bytes, *, token: str) -> object:
        self.broadcast_attempts += 1
        raise AssertionError("the facade must refuse BEFORE broadcasting any payload")

    def subscribe_events(self, topic: str, handler: Any) -> Callable[[], None]:
        return lambda: None

    def cancel(self, futures: Sequence[Any]) -> None:
        return None

    def close(self) -> None:
        return None

    def describe_failure(self, exc: BaseException) -> tuple[str, str] | None:
        return None


class PinlessView:
    """A REAL backend seen through pin_to_worker=False capabilities: ``auto`` must degrade to the
    tasks engine and the run must genuinely execute (blueprint test 3 — degrade, not refuse)."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner

    @property
    def capabilities(self) -> SubmitCapabilities:
        return PINLESS_CAPS

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


# ---- SpyDaskBackend: the submit-seam tap (dispatch witnesses are fn-name shapes) ----------------


def _fn_name(fn: Any) -> str:
    while isinstance(fn, functools.partial):
        fn = fn.func
    return str(getattr(fn, "__name__", type(fn).__name__))


class SpyDaskBackend:
    """Delegating wrapper recording every submit's fn name (and forwarding ``workers=`` pins);
    everything else delegates via ``__getattr__`` so both engines run unchanged. The m43 shape is
    ``_dask_map_write``/``_dask_pick``/``_dask_gather``; the m44 shape is
    ``_transport_map_task``/``_transport_gather_task`` — the engines stay structurally
    distinguishable at this seam."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.submitted: list[str] = []
        self.broadcast_tokens: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def submit(self, fn: Any, /, *args: Any, key: str, workers: Sequence[str] | None = None, **kwargs: Any) -> Any:
        self.submitted.append(_fn_name(fn))
        extra: dict[str, Any] = {} if workers is None else {"workers": workers}
        return self.inner.submit(fn, *args, key=key, **extra, **kwargs)

    def broadcast(self, payload: bytes, *, token: str) -> Any:
        self.broadcast_tokens.append(token)
        return self.inner.broadcast(payload, token=token)

    def fn_names(self) -> Counter[str]:
        return Counter(self.submitted)


# ---- numpy scenario builders (adapter economy: numpy suffices for every m45 theme) ---------------

BACKEND = NumpyBackend()
_BLOCK_DT = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])


def make_block(keys: Sequence[int]) -> Any:
    block = np.zeros(len(keys), dtype=_BLOCK_DT)
    block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
    block["v"] = np.arange(len(keys), dtype=np.int64)
    return block


def make_side(keys: Sequence[int], field: str, values: Sequence[int]) -> Any:
    dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
    block = np.zeros(len(keys), dtype=dt)
    block["__joinkey__"] = np.array(list(keys), dtype=np.uint64)
    block[field] = np.array(list(values), dtype=np.int64)
    return block


def repartition_blocks() -> list[Any]:
    """The m39-lineage multi-src overlapping-key scenario (several dests per task, several tasks
    per dest)."""
    key_lists = [
        [0, 1, 2, 3, 4, 5, 6, 7],
        [7, 6, 5, 4, 3, 2, 1, 0],
        [10, 11, 12, 13, 10, 11, 12, 13],
        [100, 200, 300, 400],
        [1, 1, 2, 2, 3, 3],
        [42, 43, 44, 45, 46, 47, 48, 49],
    ]
    return [make_block(keys) for keys in key_lists]


def join_sides() -> tuple[list[Any], list[Any]]:
    """Overlapping keys plus a left-only (13) and right-only (17) key: the outer arm has real
    null-filled rows on both sides."""
    left = [
        make_side([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
        make_side([1, 1, 2, 2, 13, 13], "lval", [22, 23, 24, 25, 32, 33]),
    ]
    right = [
        make_side([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
        make_side([3, 3, 2, 17, 17, 4], "rval", [106, 107, 108, 116, 117, 111]),
    ]
    return left, right


def hot_key_sides(n: int) -> tuple[list[Any], list[Any]]:
    """All rows key 7 -> n*n duplicated output rows in one dest (the m43 bounded-memory sizing)."""
    left = [make_side([7] * n, "lval", list(range(n)))]
    right = [make_side([7] * n, "rval", list(range(100, 100 + n)))]
    return left, right


def total_result_rows(value: Mapping[int, Any]) -> int:
    return sum(len(block) for block in value.values())


# ---- cluster + hang guards (replicated m44 discipline) -------------------------------------------


@contextmanager
def facade_cluster(n_workers: int = 2) -> Iterator[Any]:
    import distributed  # noqa: PLC0415  (cluster modules call this after their importorskip)

    with (
        distributed.LocalCluster(
            n_workers=n_workers, threads_per_worker=1, processes=False, dashboard_address=":0"
        ) as cluster,
        distributed.Client(cluster) as client,
    ):
        yield client


def build_dask_backend(client: Any) -> Any:
    from graphed_executors.dask_backend import DaskBackend  # noqa: PLC0415  (deferred: dask extra)

    return DaskBackend(client)


def install_transport_plugin(client: Any) -> None:
    """The m44 engine's established precondition (idempotent), replicated verbatim."""
    from graphed_executors.dask_backend.transport import GraphedTransportPlugin  # noqa: PLC0415

    client.register_plugin(GraphedTransportPlugin())


def make_runner(backend: Any) -> SubmitRunner:
    return SubmitRunner(backend)


def run_bounded(fn: Callable[[], Any], timeout_s: float = 240.0) -> dict[str, Any]:
    """Hard-timeout driver: a hang surfaces as failure, never a hung test."""
    out: dict[str, Any] = {}

    def _drive() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    thread = threading.Thread(target=_drive, daemon=True)
    thread.start()
    thread.join(timeout_s)
    assert not thread.is_alive(), f"HARD TIMEOUT: facade call did not finish within {timeout_s}s"
    return out
