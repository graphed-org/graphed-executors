"""A behavior registered on the BACKEND only, importable by a spawned worker through the
``backend="module:factory"`` import reference of ``aggregate_plan``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import awkward as ak
import numpy as np
from graphed.awkward import AwkwardBackend
from graphed.core import Partition
from graphed.core.execution import WorkerResources


class JetArray(ak.Array):  # type: ignore[misc]
    """Derives from plain ``ak.Array`` so nothing in global ``ak.behavior`` can resolve it."""

    def scaled(self, k: float, *, offset: float = 0.0) -> Any:
        return self.pt * k + offset


BEHAVIOR: dict[Any, Any] = {("*", "Jet"): JetArray}


def events(n: int = 400, seed: int = 7) -> ak.Array:
    rng = np.random.default_rng(seed)
    counts = rng.integers(1, 4, n)
    total = int(counts.sum())
    # integer-valued pt: every partial sum is exact, so partition order cannot move a bit
    jets = ak.unflatten(
        ak.Array({"pt": rng.integers(20, 100, total).astype(float), "eta": rng.uniform(-2.4, 2.4, total)}),
        counts,
    )
    return ak.Array({"Jet": jets})


EVENTS = events()


@dataclass
class CorpusEvents:
    data: ak.Array

    def __call__(self) -> ak.Array:
        return self.data

    def partitions(self, steps_per_file: int = 1) -> tuple[Partition, ...]:
        return tuple(Partition.blind("corpus://events", "", s, steps_per_file) for s in range(steps_per_file))

    def read_partition(self, partition: Partition, columns: Any, resources: WorkerResources) -> ak.Array:
        part = partition.resolve(len(self.data))
        return self.data[part.entry_start : part.entry_stop]


def make_backend() -> AwkwardBackend:
    return AwkwardBackend(behavior=BEHAVIOR)


def sum_reduce(vals: Any) -> list[float]:
    return [float(ak.sum(ak.Array(vals[0])))]


def add2(a: list[float], b: list[float]) -> list[float]:
    return [a[0] + b[0]]


def zero() -> list[float]:
    return [0.0]
