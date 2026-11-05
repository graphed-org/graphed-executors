"""A runner closes its backend even when finishing its plans is interrupted."""

from __future__ import annotations

import pytest

from graphed_executors.submit import SubmitRunner, ThreadBackend


class _Interrupted:
    def close(self) -> None:
        raise KeyboardInterrupt


def test_an_interrupted_close_still_closes_the_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = ThreadBackend(1)
    closed: list[bool] = []
    monkeypatch.setattr(backend, "close", lambda: closed.append(True))
    runner = SubmitRunner(backend)
    runner._plans.close()
    monkeypatch.setattr(runner, "_plans", _Interrupted())
    with pytest.raises(KeyboardInterrupt):
        runner.close()
    assert closed == [True]
    ThreadBackend.close(backend)
