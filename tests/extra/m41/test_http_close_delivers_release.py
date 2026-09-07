"""Closing the driver's HTTP transport must not drop the release it just queued (tests/extra — NOT
frozen).

``_peer_http`` releases the workers and closes the driver transport in the very next breath. Sends are
buffered for a background POST, so a ``close`` that stops the sender before it drains throws the
``done`` away — and ``pool.shutdown()`` then waits forever on workers that were never released.

POSITIVE CONTROL: the same release with a pause before ``close`` (nothing left queued) must deliver
both messages, so a zero in the no-pause leg is a real drop and not a dead instrument.

The second test is the shape CI's slow runners hit: the crashed worker's server is mid-shutdown, so it
accepts the connection and never answers. One shared sender would spend the whole drain bound retrying
that POST and the live worker's ``done`` behind it would never be sent.
"""

from __future__ import annotations

import socket
import time

from graphed_executors.local._peer import release_workers
from graphed_executors.local._transport import CLOSE_DRAIN_S, HttpTransport, build_http_transports

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


def test_a_destination_that_never_answers_does_not_starve_the_live_one() -> None:
    hole = socket.socket()
    hole.bind(("127.0.0.1", 0))
    hole.listen(1)  # accepts at the kernel level, never reads
    driver, live = HttpTransport("driver"), HttpTransport("w1")
    try:
        driver.set_registry(
            {"driver": (driver.host, driver.port), "w0": hole.getsockname(), "w1": (live.host, live.port)}
        )
        release_workers(driver)  # w0 (the hole) is queued first
        t0 = time.monotonic()
        driver.close()
        assert time.monotonic() - t0 < CLOSE_DRAIN_S + 1.0
        time.sleep(0.5)
        assert [m for _sender, m in live.poll()] == [("done",)]
    finally:
        live.close()
        hole.close()
