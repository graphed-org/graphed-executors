"""The hub broadcast waits for a pool worker that boots long after its sibling, up to a deadline."""

from __future__ import annotations

import functools
import os
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import m65a_probe as mp
import pytest
from graphed.core import StopReason

from graphed_executors.local import executors


def _second_boots_late(marker: str, late_s: float, init: Callable[..., None] | None, *initargs: Any) -> None:
    try:
        os.close(os.open(marker, os.O_CREAT | os.O_EXCL))
    except FileExistsError:
        time.sleep(late_s)
    if init is not None:
        init(*initargs)


class _LateSiblingPool(ProcessPoolExecutor):
    marker = ""
    late_s = 0.0

    def __init__(
        self, *args: Any, initializer: Any = None, initargs: tuple[Any, ...] = (), **kw: Any
    ) -> None:
        late = functools.partial(_second_boots_late, self.marker, self.late_s, initializer)
        super().__init__(*args, initializer=late, initargs=initargs, **kw)


def _late_sibling(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, late_s: float) -> None:
    monkeypatch.setattr(_LateSiblingPool, "marker", str(tmp_path / "first-booted"))
    monkeypatch.setattr(_LateSiblingPool, "late_s", late_s)
    monkeypatch.setattr(executors, "_StdProcessPool", _LateSiblingPool)


def test_a_worker_booting_after_its_sibling_has_answered_is_still_primed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    late_s = 8.0
    _late_sibling(monkeypatch, tmp_path, late_s)
    t0 = time.monotonic()
    res = executors.ProcessPoolExecutor(max_workers=2, comms=None).run(mp.make_plan(mp.Probe()))
    assert (res.value, res.n_partitions, res.stopped) == ((1,) * mp.N, mp.N, StopReason.EXHAUSTED)
    assert time.monotonic() - t0 >= late_s  # the late worker was waited for, not skipped


def test_a_worker_still_booting_at_the_deadline_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _late_sibling(monkeypatch, tmp_path, 3.0)
    monkeypatch.setattr(executors, "_WORKER_START_TIMEOUT_S", 0.5)
    with pytest.raises(RuntimeError, match=r"broadcast reached only 1/2 workers in \d+\.\ds"):
        executors.ProcessPoolExecutor(max_workers=2, comms=None).run(mp.make_plan(mp.Probe()))
