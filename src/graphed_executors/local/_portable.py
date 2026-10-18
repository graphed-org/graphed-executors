"""Carry a worker-produced value across a process boundary: stdlib pickle, or cloudpickle when stdlib
pickle refuses it (a lambda or local class the user's value holds, e.g. in an awkward ``behavior``)."""

from __future__ import annotations

import pickle
from typing import Any

import cloudpickle


def from_pickle(data: bytes) -> Any:
    return pickle.loads(data)


def from_cloudpickle(data: bytes) -> Any:
    """Load a cloudpickled value; a separate name from :func:`from_pickle` so the fallback is visible."""
    return pickle.loads(data)


class Portable:
    """Pickles to its value's own serialized bytes, so whatever unpickles it receives the value itself."""

    __slots__ = ("cloud", "data")

    def __init__(self, value: object) -> None:
        try:
            self.data, self.cloud = pickle.dumps(value, pickle.HIGHEST_PROTOCOL), False
        except (pickle.PicklingError, AttributeError, TypeError):
            self.data, self.cloud = cloudpickle.dumps(value, pickle.HIGHEST_PROTOCOL), True

    def __reduce__(self) -> tuple[Any, tuple[bytes]]:
        return (from_cloudpickle if self.cloud else from_pickle, (self.data,))
