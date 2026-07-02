"""M39 (a-BI) — the SAME exchange suite runs against BOTH real backends (plan §8, B-r5.3).

The M2 backend-independence discipline (``tests/frozen/m2/backends.py`` +
``test_backend_independence.py`` ship two backends in the seam-introducing milestone) extended to the
shuffle seam: ONE test body, parametrized over ``AwkwardBackend`` AND ``NumpyBackend``. A suite green
on both witnesses the ``ShuffleBackend`` seam is real (not an awkward-shaped hole) BY EXECUTION — a
primitive that leaked one backend's semantics fails on the other.
"""

from __future__ import annotations

import pytest
from exchange_backends import BackendAdapter, adapters
from golden_route import GOLDEN

ADAPTERS = adapters()
_IDS = [a.name for a in ADAPTERS]


@pytest.fixture(params=ADAPTERS, ids=_IDS)
def adapter(request) -> BackendAdapter:
    return request.param


def test_at_least_two_backends_are_exercised() -> None:
    # the whole point of (a-BI): the suite MUST run on >= 2 real backends, not silently one.
    assert len(ADAPTERS) >= 2, f"expected awkward AND numpy exchange backends, got {_IDS}"


def test_golden_routing_holds_on_this_backend(adapter: BackendAdapter) -> None:
    be = adapter.backend
    for key, parts, dest in GOLDEN:
        subs = be.partition(adapter.make_block([key, key, key]), "__joinkey__", parts)
        assert len(subs) == parts
        assert len(subs[dest]) == 3
        assert sum(len(s) for i, s in enumerate(subs) if i != dest) == 0


def test_partition_conserves_rows_on_this_backend(adapter: BackendAdapter) -> None:
    be = adapter.backend
    block = adapter.make_block(list(range(40)))
    subs = be.partition(block, "__joinkey__", 8)
    assert sum(len(s) for s in subs) == 40


def test_concat_preserves_order_on_this_backend(adapter: BackendAdapter) -> None:
    be = adapter.backend
    merged = be.concat([adapter.make_block([1, 1]), adapter.make_block([2, 2, 2])])
    assert adapter.keys_of(merged) == [1, 1, 2, 2, 2]


def test_slice_rows_on_this_backend(adapter: BackendAdapter) -> None:
    be = adapter.backend
    block = adapter.make_block(list(range(10)))
    assert adapter.keys_of(be.slice_rows(block, 3, 7)) == [3, 4, 5, 6]


def test_wire_roundtrip_on_this_backend(adapter: BackendAdapter) -> None:
    be = adapter.backend
    block = adapter.make_block([5, 6, 7, 8])
    back = be.from_wire(be.to_wire(block))
    assert adapter.keys_of(back) == [5, 6, 7, 8]


def test_estimated_bytes_positive_on_this_backend(adapter: BackendAdapter) -> None:
    be = adapter.backend
    assert be.estimated_bytes(adapter.make_block(list(range(20)))) > 0
