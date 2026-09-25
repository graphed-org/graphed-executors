# m72 — implementer iterations (graphed-executors, m69a re-spec)

Multiout lane (plans in `graphed-workdir/lanes/multiout/`: `plan.md`, `plan-C.md`). Frozen
`freeze-m72` = `3a3ea2e` (`tests/frozen/m69a/test_hgg_conversion.py` refrozen for
`plan(fileset, *, year, out)`); graphed side at `graphed-org/graphed` branch
`feat/multi-output-plans`.

## C1 — CI installs graphed from the multi-output-plans commit

`GRAPHED` and `docs/requirements.txt` pin `graphed[awkward,numpy] @ git+…@4a3ae76b…`; `test-hgg`
gains `dtolnay/rust-toolchain@stable` (the other six jobs installing `GRAPHED` already had it).
Re-pin when the graphed PR's head moves or it merges.
