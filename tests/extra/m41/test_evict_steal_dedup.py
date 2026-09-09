"""M41 F1 regression (review-mandated, tests/extra — NOT frozen).

The M41 reader-plane gather evicts each fetched block from its producer Store once consumed. When
work-stealing CO-LOCATES byte-identical hot-key blocks on one holder they share a single
content-address, so a per-(dest,t) evict pops that shared digest on the first ref and the next
``cluster.get(holder, digest)`` KeyErrors — a crash on the T3 skew workload crossed with M39 steal.

The fix defers eviction to once-per-unique-digest after all refs are pulled. This guards it:

- NON-VACUITY: against the pre-fix code ``run_repartition`` raises ``KeyError`` before returning, so the
  test ERRORS (never reaches an assert). Post-fix it returns and every assert holds.
- DISCRIMINATION: (1) the steal path is actually engaged (``steals > 0`` — else the co-location that
  triggers the bug never happens); (2) every row is conserved AND uncorrupted (a gather that dropped the
  second, same-digest ref would lose rows); (3) bytes flowed (``bytes_transferred > 0``).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from graphed.numpy import NumpyBackend

from graphed_executors.local.shuffle import ShuffleFaults, run_repartition

_DTYPE = np.dtype([("__joinkey__", np.uint64), ("v", np.int64)])
_HOT = 7
_N = 6
_ROWS = 4


def _identical_block() -> np.ndarray:
    b: np.ndarray = np.zeros(_ROWS, dtype=_DTYPE)
    b["__joinkey__"] = np.full(_ROWS, _HOT, dtype=np.uint64)
    b["v"] = np.ones(
        _ROWS, dtype=np.int64
    )  # SAME values -> byte-identical wire -> one shared content-address
    return b


def test_steal_colocated_byte_identical_blocks_do_not_crash_evict(tmp_path: Path) -> None:
    be = NumpyBackend()
    src = [_identical_block() for _ in range(_N)]  # all rows -> one hot dest, all blocks byte-identical

    res = run_repartition(
        be,
        src,
        parts=4,
        workers=3,
        comms="ipc",
        store_root=str(tmp_path),
        steal=True,
        faults=ShuffleFaults(force_steal=True),  # co-locate stolen (identical) blocks on a thief holder
    )

    w = res.witness
    assert w.steals > 0, "force_steal did not steal — the co-location that triggers the evict bug never ran"

    rows = [
        (int(k), int(v)) for b in res.value.values() for k, v in zip(b["__joinkey__"], b["v"], strict=True)
    ]
    assert len(rows) == _N * _ROWS, f"rows dropped under steal + co-located identical blocks: {len(rows)}"
    assert rows == [(_HOT, 1)] * (_N * _ROWS), "the gather corrupted or reordered the hot-key rows"

    assert w.bytes_transferred > 0, "nothing transferred — the gather never pulled the co-located blocks"
