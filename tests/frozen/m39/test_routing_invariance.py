"""M39 (B2) — cross-process routing invariance under different PYTHONHASHSEED (plan §4).

The single hardest determinism obligation: output-partition assignment must be a pure, process-
independent function of the key. Python's built-in ``hash()`` is per-process salted (PYTHONHASHSEED)
for bytes/str, so if it leaked into routing, two producer processes would route the SAME key to
DIFFERENT dests and the shuffle would silently lose matches. This test routes a fixed key set in TWO
separate OS processes with DIFFERENT seeds and asserts the dests are identical AND equal to the pinned
golden vectors (so it catches both a leaked ``hash()`` and any non-conforming hash).
"""

from __future__ import annotations

import subprocess
import sys

from golden_route import GOLDEN

# the numpy backend is the lighter one to spin up in a child; the routing rule is shared with awkward.
_CHILD = r"""
import sys
import numpy as np
from graphed.numpy import NumpyBackend
be = NumpyBackend()
dt = np.dtype([("__joinkey__", np.uint64)])
parts = 8
out = []
for tok in sys.argv[1:]:
    k = int(tok)
    b = np.zeros(3, dtype=dt)
    b["__joinkey__"] = np.uint64(k)
    subs = be.partition(b, "__joinkey__", parts)
    dest = next(i for i, s in enumerate(subs) if len(s) > 0)
    out.append(str(dest))
print(" ".join(out))
"""

_KEYS = [key for key, parts, _ in GOLDEN if parts == 8]
_EXPECTED = [str(dest) for key, parts, dest in GOLDEN if parts == 8]


def _route_in_child(seed: str) -> list[str]:
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, *[str(k) for k in _KEYS]],
        env={"PYTHONHASHSEED": seed, "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout.split()


def test_same_key_routes_identically_across_processes_and_seeds() -> None:
    a = _route_in_child("0")
    b = _route_in_child("1")
    assert a == b, "routing must be independent of PYTHONHASHSEED (no Python hash() in the route path)"
    assert a == _EXPECTED, "and it must match the pinned §4 golden dests, not merely be self-consistent"
