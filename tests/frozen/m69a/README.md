# m69a frozen suite — the H→γγ inclusive processor on graphed

This suite freezes the acceptance tests for `examples/hgg/`, the graphed translation of the owner's
coffea processor `inclusive_processor.py` (a distillation of HiggsDNA's H→γγ inclusive processor).
The oracle is the original script itself, byte-identical, run on coffea NanoEvents the way its
`__main__` runs it. The source of truth is `lanes/htcondor/plan-services.md` §4 (the m69a unit) and
constraint C1 of `reviews/plan-services-r5.md`. **Frozen — read-only after the m69a freeze tag.**

Run it with `GRAPHED_HGG_REQUIRED=1 pytest tests/frozen/m69a` in an environment with the coffea fork
(`graphed-org/coffea-graphed-mvp`), `vector`, `correctionlib` and `higgs_dna` installed `--no-deps`;
the `test-hgg` CI job provides them. Without `GRAPHED_HGG_REQUIRED=1` every file `importorskip`s
coffea and higgs_dna, so the main matrix collects the directory and skips it.

## Files

| File | Plan item | What it shows | A wrong implementation it fails |
|---|---|---|---|
| `data/make_hgg_fixture.py` | §4 fixtures | Builds `nano_hgg_v15.root` (MC) from `nano_tt_v15.root` with two photons and the mainAnalysis triggers in events 0–59 (seed 20260925); `--data` drops `GenPart_*`/`GenVtx_*`/`LHE*`/`genWeight` and sets run/lumi from the 2024 golden JSON (events 30–59 uncertified); fixed clock, file UUID and no compression, so a rebuild is byte-identical | — |
| `data/nano_tt_v15.root` | §4 fixtures | The builder's input, coffea's `tests/samples/nano_tt_v15.root` (sha256 pinned) | — |
| `data/nano_hgg_v15.root`, `data/nano_hgg_v15_data.root` | §4 fixtures | The two fixtures the oracle and the translation read | — |
| `data/inclusive_processor.py` | §4 oracle | The owner's original, byte-identical (sha256 `791229c2…d1dd`), excluded from ruff and mypy | — |
| `data/Cert_Collisions2024_378981_386951_Golden.json`, `data/jetid.json.gz` | §4 oracle | The 2024 golden JSON and the 2024 jet-ID set, sha256-equal to what `pull_files.py` places at LPC | — |
| `hgg_harness.py` | §4 harness | `place_higgs_dna_data()`, `oracle_parts(uri, dataset, year, ranges, out)`, `compare_part(expected, actual)`; puts `examples/hgg` on `sys.path` | — |
| `conftest.py` | §4 harness | The session fixture that places the two JSONs in the installed `higgs_dna` | — |
| `test_hgg_oracle_fixture.py` | §4 row 1 | The original's sha256; its `infer_nano_version` is the installed `higgs_dna`'s; the placed files equal `data/`'s and are the paths the original resolves; placing over a different file fails naming it; the oracle on the MC fixture in one chunk gives `run_oracle.txt`'s counters, 52 × 145 part, sorted columns, KV metadata and `<file UUID>_Events_0-200.parquet`; on the data fixture the only rows are the MC rows from events 0–29 (23 rows); each fixture's (100, 200) chunk is an empty selection; a rebuilt fixture is byte-identical | the wrong original, a fixture drifted from its builder |
| `test_hgg_compare.py` | §4 row 2, C1 | `compare_part` is `[]` for an oracle part against itself re-read from disk, and each mutation fires its own leg, asserted by leg set and entry: one ulp in `mass` → `values`; a dropped row → `validity` and `values` in every column; two columns swapped → `schema`; `NJ` int64→int32 with equal values → `schema` (and `values`); one `NJ` value null → `validity` (and `values`); one KV value → `metadata`; `nTot` off by one, or `nTot` as a float → `counters` | a comparator missing any leg (each leg's deletion fails exactly its tests) |
| `test_hgg_conversion.py` | §4 row 3 | Per fixture, on `SequentialRunner` and `SubmitRunner(ThreadBackend(2))`: the plan value's keys are exactly `nano_hgg_v15[_data]_Events_<s>-<e>.parquet` for (0, 100) and (100, 200); each part `compare_part`s `[]` against the oracle's part for the same range (the second is empty); `totals(value)` equals coffea's `accumulate` of the oracle's counters. Every events object the analysis gets from `NanoEventsFactory` is a `GraphedNanoArray` whose `Photon` has `metric_table`/`delta_r` (an absent name is `False`) and whose `metadata["dataset"]` is the dataset; its source node is `_GraphedTTreeSource`, as a raw `uproot.graphed` array's is, which has no `Photon`; no `examples/hgg` Python file matches `import uproot`/`uproot.`, while `data/make_hgg_fixture.py` does (control) | wrong-row-space counters, a float changed by a rewrite, a part named without its range, whole-file metadata, a raise on the empty chunk, events outside NanoEvents, hand-read uproot arrays |
| `test_hgg_lumimask_plugin.py` | §4 row 4 | `lumi_mask` on (run, lumi) pairs at the edges of certified ranges of three runs, an absent run, and the data fixture's pairs equals coffea's `LumiMask` on `data/`'s golden JSON; its External node's descriptor carries `sha256_bytes` of that JSON | the wrong golden JSON, a mask that differs from coffea's |

## The contract these tests pin

Beyond the names in plan §4, the tests rely on these readings of it:

- `analysis.plan(uri, *, ranges, dataset, year, out)` returns a plan a runner's `run()` executes;
  `dataset` and `year` are what the oracle gets (`metadata={"dataset": …}`, `year={dataset: [year]}`),
  and `out` is the directory the parts are written under (found by file name, at any depth).
- The plan value maps each part's file name to the original's return for that chunk,
  `{dataset: {nTot, nPos, nNeg, nEff, genWeightSum}}`, with the original's Python types (`int`, `float`).
- `analysis.totals(value)` returns what `coffea.processor.accumulate` returns on the oracle's counters.
- `analysis.lumi_mask(run, lumi, year)` takes graphed arrays in one session and returns the deferred
  boolean mask; its single External node's `content_hash` is
  `graphed.preserve.externals.sha256_bytes` of the golden JSON's bytes.
- The events witness records every events object `NanoEventsFactory.events()` returns while
  `analysis.plan(...)` runs, so the analysis builds its events there.
- The search for `import uproot`/`uproot.` covers `examples/hgg/**/*.py`.
