"""Compare the graphed translation with the original, part for part, on real 2024 NanoAOD.

For the first file of ``GluGluHto2G_M-125_amcatnlo_2024`` and of ``DataC_2024`` (the HiggsDNA 2024
sample manifests) it prints the file's entry count, splits ``[0, --entry-stop)`` into ``--parts``
ranges, runs the original processor (the m69a oracle) and the graphed plan on those ranges, and prints
each part's ``compare_part`` result and both counters. Exits 1 on any difference.

    python examples/hgg/validate_real.py --parts 2 [--entry-stop N] [--out DIR]
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
from graphed.core import SequentialRunner

import analysis
from run_local import num_entries, split

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests" / "frozen" / "m69a"))
import hgg_harness as h  # the frozen harness: the original as oracle, and the comparator

STORE = "root://cms-xrd-global.cern.ch//store"
MC = (
    f"{STORE}/mc/RunIII2024Summer24NanoAODv15/GluGluH-Hto2G_Par-M-125_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/"
    "NANOAODSIM/150X_mcRun3_2024_realistic_v2-v2/120000/acebfb52-a25b-48bc-b9f6-80fe54a98d56.root"
)
DATA = f"{STORE}/data/Run2024C/EGamma0/NANOAOD/MINIv6NANOv15-v1/2540000/3151ab41-a5bd-48f1-be40-449b760a60e6.root"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--parts", type=int, default=2)
    parser.add_argument("--entry-stop", type=int, default=None, help="default: the whole file")
    parser.add_argument("--out", default=None, help="default: a temporary directory")
    parser.add_argument("--mc", default=MC, help="the MC file (default: the manifest's first)")
    parser.add_argument("--data", default=DATA, help="the data file (default: the manifest's first)")
    args = parser.parse_args(argv)

    h.place_higgs_dna_data()
    out = Path(args.out or tempfile.mkdtemp(prefix="hgg-validate-"))
    differences = 0
    for dataset, uri in {"GluGluHto2G_M-125_amcatnlo_2024": args.mc, "DataC_2024": args.data}.items():
        n = num_entries(uri)
        stop = n if args.entry_stop is None else min(args.entry_stop, n)
        ranges = split(stop, args.parts)
        print(f"FILE {dataset} {uri} num_entries={n} ranges={ranges}", flush=True)
        oracle = h.oracle_parts(uri, dataset, h.YEAR, ranges, out / "oracle" / dataset)
        plan = analysis.plan(uri, ranges=ranges, dataset=dataset, year=h.YEAR, out=str(out / "graphed"))
        value = SequentialRunner().run(plan).value
        for (start, stop_), (counters, table) in oracle.items():
            name = f"{Path(uri).stem}_Events_{start}-{stop_}.parquet"
            (path,) = (out / "graphed").rglob(name)
            diffs = h.compare_part((counters, table), (value[name], pq.read_table(path)))
            differences += len(diffs)
            print(f"PART {dataset} {start}-{stop_} rows={table.num_rows} cols={table.num_columns}")
            print(f"  original {counters}")
            print(f"  graphed  {value[name]}")
            print(f"  compare_part {diffs or 'IDENTICAL'}", flush=True)
    print(f"DIFFERENCES {differences}")
    return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
