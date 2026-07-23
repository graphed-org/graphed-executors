"""m47 stale-epoch rejection — the run-nonce guard at the /msg route: an envelope carrying a
WRONG epoch is never delivered and is counted, over the REAL wire path (a raw POST speaking the
pinned envelope), while the live epoch's traffic flows intact (plan §1.4 epoch/restart/attribution,
obligation 9; blueprint §3 `test_parsl_epoch_guard`).

This module needs NO parsl (``common/http_plane.py`` is stdlib-only by the frozen import rule), so
it also runs on the POSIX main matrix — operational proof of the plane's parsl-freeness. Fresh
per-epoch ports make cross-epoch delivery doubly improbable in a real run (the plan's own words);
the guard is defense-in-depth against late/duplicate wire traffic, and it LIVES at the transport
layer — which is exactly where this test freezes it.

Witnesses: a wrong-nonce POST of a well-formed reduction-class message -> ``recv()`` never yields
it + ``stale_epoch_rejects`` advanced; the same message under the LIVE nonce -> delivered with the
true (sender, message); the counter did not advance on the live delivery.

Discriminates: a guard-less inbox delivers the stale node (poisoning a reduction with dead-epoch
traffic); an epoch-blind handler that counts nothing; an envelope drift (the injector speaks the
pinned ``pickle.dumps((sender, epoch, message))`` form — a changed wire shape fails delivery of
the live-epoch arm)."""

from __future__ import annotations

import sys
import uuid

import pytest
from parsl_transport_harness import make_endpoint, post_msg, wire_endpoints

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="the parsl HTTP transport plane is POSIX-only (Windows keeps the base package — plan §4 G8)",
)

NODE_MSG = ("node", 1, 0, 999_999)  # a well-formed reduction-class message (the m44 probe shape)


def test_stale_epoch_is_rejected_and_counted_live_epoch_delivers() -> None:
    live_epoch = f"m47-live-{uuid.uuid4().hex}"
    ep = make_endpoint("w0", epoch=live_epoch)
    try:
        wire_endpoints(ep)

        # (1) a WRONG-nonce message over the real wire: never delivered, counted
        status = post_msg(ep.host, ep.port, "w9", f"m47-stale-{uuid.uuid4().hex}", NODE_MSG)
        assert status != -1, "the stale POST never reached the endpoint (injector misfired)"
        assert ep.recv(timeout=0.3) is None, (
            "a stale-epoch message was DELIVERED — exactly the dead-epoch corruption the run-nonce "
            "guard exists to reject (obligation 9)"
        )
        assert int(ep.stale_epoch_rejects) == 1, (
            f"stale_epoch_rejects={ep.stale_epoch_rejects}: the guard must COUNT its rejections "
            "(the frozen witness)"
        )

        # (2) the SAME message under the live nonce: delivered intact, not counted as stale
        status = post_msg(ep.host, ep.port, "w9", live_epoch, NODE_MSG)
        assert status != -1, "the live-epoch POST never reached the endpoint"
        got = ep.recv(timeout=10.0)
        assert got is not None and tuple(got) == ("w9", NODE_MSG), (
            f"the live-epoch message must deliver with the true (sender, message): {got!r} — a "
            "drifted /msg envelope (not pickle((sender, epoch, message))) also lands here"
        )
        assert int(ep.stale_epoch_rejects) == 1, "a live-epoch delivery must not count as stale"
    finally:
        ep.close()
