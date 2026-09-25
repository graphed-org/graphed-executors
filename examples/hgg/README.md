# H→γγ inclusive processor on graphed

`analysis.py` is a translation of `inclusive_processor.py`, a standalone distillation of HiggsDNA's
H→γγ inclusive base processor, to graphed. It reads coffea NanoEvents in `mode="graphed"` with
`NanoAODSchema`, applies the same selections, and writes the same flat diphoton parquet, one part
per chunk, with the original's columns and key-value metadata. It also returns the original's
per-chunk counters `{dataset: {nTot, nPos, nNeg, nEff, genWeightSum}}`.

| File | What it is |
|---|---|
| `analysis.py` | the processor, `plan(uri, *, ranges, dataset, year, out)`, `totals(value)`, `lumi_mask(run, lumi, year)` |
| `run_local.py` | run one file on this machine, in-process or on a thread pool, and print the summed counters |
| `validate_real.py` | run the original and the translation on the same ranges of a real 2024 MC file and a real 2024 data file, and compare every part |

## Running it

You need the coffea fork with graphed mode (`graphed-org/coffea-graphed-mvp`), `correctionlib`,
`pyarrow`, and `higgs_dna` installed with `--no-deps`. The processor reads HiggsDNA's data files
where the original reads them, inside the installed `higgs_dna` package. HiggsDNA's
`pull_files.py --target GoldenJSON` and `--target JetMET` put them there, or the m69a test harness
copies the 2024 golden JSON and jet-ID set in.

```bash
python examples/hgg/run_local.py FILE.root --dataset MC --year 2024 --parts 4 --workers 4 --out output_inclusive
```

```python
from graphed.core import SequentialRunner
import analysis

plan = analysis.plan(uri, ranges=[(0, 50_000), (50_000, 100_000)], dataset="MC", year="2024", out="out")
value = SequentialRunner().run(plan).value   # {"<file>_Events_0-50000.parquet": {"MC": {...}}, ...}
analysis.totals(value)                       # the counters summed, as coffea's Runner would
```

Each part goes to `out/<dataset>/nominal/<file stem>_Events_<start>-<stop>.parquet`. That is the
original's name, except that the original begins it with the file's UUID and this begins it with
the file name.

## How it is checked

The frozen suite `tests/frozen/m69a` imports the original script, byte-identical, and runs it as
the oracle on the same ranges. It then compares every graphed part with the original's part: the
counters and their Python types, the arrow schema, each column's validity bitmap and valid values
bit for bit, and the key-value metadata. The inputs are two 200-event NanoAOD v15 fixtures, one MC
and one data.

`validate_real.py --parts 2` does the same on the first file of `GluGluHto2G_M-125_amcatnlo_2024`
and of `DataC_2024` over xrootd. It exits 1 if any part differs.

`docs/hgg.rst` lists the eleven places the translation departs from the original's spelling.
