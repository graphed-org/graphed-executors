"""Closing the driver's HTTP transport must not drop the release it just queued (tests/extra — NOT
frozen).

``_peer_http`` releases the workers and closes the driver transport in the very next breath. Sends are
buffered for a background POST, so a ``close`` that stops the sender before it drains throws the
``done`` away — and ``pool.shutdown()`` then waits forever on workers that were never released.

POSITIVE CONTROL: the same release with a pause before ``close`` (nothing left queued) must deliver
both messages, so a zero in the no-pause leg is a real drop and not a dead instrument.
"""

from __future__ import annotations

import time

from graphed_executors.local._peer import release_workers
from graphed_executors.local._transport import build_http_transports

WORKERS = ("w0", "w1")


def _released(pause: float) -> int:
    transports = build_http_transports(("driver", *WORKERS))
    try:
        release_workers(transports["driver"])
        time.sleep(pause)
        transports["driver"].close()
        time.sleep(0.5)  # generous: any POST still in flight would have landed
        return sum(len(transports[a].poll()) for a in WORKERS)
    finally:
        for a in WORKERS:
            transports[a].close()


def test_close_delivers_the_release_it_queued() -> None:
    assert _released(0.3) == len(WORKERS)  # control: the release lands when close is delayed
    assert _released(0.0) == len(WORKERS)
