"""Run the H->gg translation on one NanoAOD file on this machine and print the summed counters.

python examples/hgg/run_local.py FILE --dataset MC --year 2024 --parts 4 --workers 4 --out output_inclusive
"""

from __future__ import annotations

import argparse
import itertools
import json
from typing import Any

from coffea.nanoevents import NanoEventsFactory
from graphed.core import SequentialRunner

import analysis
from graphed_executors.submit import SubmitRunner, ThreadBackend


def num_entries(uri: str) -> int:
    """The file's entry count, from coffea's lazy (virtual) NanoEvents, which reads no branch for it."""
    return len(NanoEventsFactory.from_root({uri: "Events"}, mode="virtual").events())


def split(stop: int, parts: int) -> list[tuple[int, int]]:
    """``[0, stop)`` in ``parts`` contiguous ranges whose sizes differ by at most one."""
    edges = [stop * i // parts for i in range(parts + 1)]
    return list(itertools.pairwise(edges))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("uri")
    parser.add_argument("--dataset", default="MC")
    parser.add_argument("--year", default="2024")
    parser.add_argument("--parts", type=int, default=1)
    parser.add_argument("--workers", type=int, default=1, help="1 runs in-process; more use a thread pool")
    parser.add_argument("--out", default="output_inclusive")
    args = parser.parse_args(argv)

    ranges = split(num_entries(args.uri), args.parts)
    plan = analysis.plan(args.uri, ranges=ranges, dataset=args.dataset, year=args.year, out=args.out)
    runner: Any = SequentialRunner() if args.workers == 1 else SubmitRunner(ThreadBackend(args.workers))
    try:
        value = runner.run(plan).value
    finally:
        getattr(runner, "close", lambda: None)()
    print(json.dumps(analysis.totals(value), indent=1))


if __name__ == "__main__":
    main()
