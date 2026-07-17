"""Persistent worker pools (opt-in): one spawn amortized over many run() calls.

Real analyses execute MANY plans (the ADL notebook runs eight; the benchmark sweep, hundreds) —
spawning a fresh import-heavy pool per plan dwarfs small-plan work. ``persistent=True`` keeps
the pool across ``run()`` calls (witnessed by worker-side state SURVIVING between runs), changes
no result (bit-identical to the default), and releases workers on ``close()`` / context exit;
the DEFAULT remains a fresh pool per run (the existing suites pin that behavior unchanged).
"""

from __future__ import annotations

import numpy as np
from graphed.core import Partition, Plan, Task

from graphed_exec_local import ProcessExecutor, ThreadExecutor

_calls = 0


def _count_calls(partition: Partition, resources: object) -> np.ndarray:
    """Returns THIS WORKER's cumulative call count (worker-global: survives between runs only
    if the pool itself survives)."""
    global _calls
    _calls += 1
    return np.asarray([_calls], dtype=np.int64)


def _concat(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return np.concatenate([a, b])


def _empty() -> np.ndarray:
    return np.asarray([], dtype=np.int64)


def _plan(n: int = 4) -> Plan[np.ndarray]:
    parts = tuple(Partition(f"p{i}", "", 0, 1) for i in range(n))
    return Plan(
        process=_count_calls,
        combine=_concat,
        empty=_empty,
        tasks=tuple(Task(i, p) for i, p in enumerate(parts)),
    )


def test_persistent_pool_survives_between_runs() -> None:
    ex = ProcessExecutor(max_workers=1, persistent=True)
    try:
        first = ex.run(_plan()).value
        second = ex.run(_plan()).value
        assert first.tolist() == [1, 2, 3, 4]  # one worker, fresh state
        assert second.tolist() == [5, 6, 7, 8]  # SAME worker: state survived -> pool was reused
    finally:
        ex.close()


def test_default_still_spawns_a_fresh_pool_per_run() -> None:
    ex = ProcessExecutor(max_workers=1)
    first = ex.run(_plan()).value
    second = ex.run(_plan()).value
    assert first.tolist() == [1, 2, 3, 4]
    assert second.tolist() == [1, 2, 3, 4]  # fresh worker each run (the pinned default)


def test_close_releases_and_a_later_run_respawns() -> None:
    ex = ProcessExecutor(max_workers=1, persistent=True)
    assert ex.run(_plan()).value.tolist() == [1, 2, 3, 4]
    ex.close()
    assert ex.run(_plan()).value.tolist() == [1, 2, 3, 4]  # fresh state after close
    ex.close()
    ex.close()  # idempotent


def test_context_manager_and_result_equality() -> None:
    with ProcessExecutor(max_workers=2, persistent=True) as ex:
        a = ex.run(_plan(6))
        b = ProcessExecutor(max_workers=2).run(_plan(6))
        assert a.n_partitions == b.n_partitions == 6
        assert a.n_combines == b.n_combines  # the fixed tree is unchanged
    # after exit the pool is gone; a fresh run still works (lazy respawn)
    assert ex.run(_plan(2)).value.shape == (2,)
    ex.close()


def test_thread_executor_supports_persistence_too() -> None:
    with ThreadExecutor(max_workers=2, persistent=True) as ex:
        assert ex.run(_plan(3)).n_partitions == 3
        assert ex.run(_plan(3)).n_partitions == 3
