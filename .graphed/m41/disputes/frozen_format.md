# Dispute / config note — frozen suites exempt from `ruff format` (extends the M39 I001 precedent)

**Filed by:** team-lead, post-M41-push (CI `ci` red on the `ruff format --check` prek step).
**Severity:** cosmetic (auto-formatting), NOT a behavioural defect. **No gate weakened for maintained code.**

## The conflict

CI runs `ruff format --check --force-exclude` as a prek hook. Several **immutable frozen** test files
(`tests/frozen/m40/*`, `tests/frozen/m41/*`) were authored not-`ruff format`-clean (long trailing comments,
call wrapping). They are read-only under the §A.7 frozen-test integrity rule, so they cannot be reformatted
in place — yet the formatter demands it, so the gate stays red.

This is the same class of conflict the M39 dispute resolved for the linter's `I001` import-sort rule
(`.graphed/M39/disputes/frozen_m39_import_sort.md`), now for the **formatter**.

## Resolution (consistent with M39)

`[tool.ruff.format] exclude = ["tests/frozen/**"]` in `pyproject.toml`. Rationale:
- `ruff format` is a **pure auto-formatting** pass — it makes no correctness check on the code under test,
  so exempting the immutable frozen tree loses no defect-detection.
- **`ruff check` (real lint: E/F/B/SIM/RUF/…) still covers the frozen files** — only the formatter skips them
  (verified: `ruff check` passes on `tests/frozen/**` unchanged; the exclude is `format`-only, not `lint`).
- **Every `src/**` file and every non-frozen test stays fully format-enforced.** To prove the gate was not
  relaxed for maintained code, the non-frozen format debt this masked was fixed in the same commit:
  `src/graphed_executors/local/shuffle.py` (M41 counter comments moved above their fields; two long trailing
  comments shortened — **comment/whitespace only, `git diff -w` shows no code change**) and
  `tests/extra/m41/test_evict_steal_dedup.py` (`ruff format`).

## Follow-up (process)

Test-authors should run `ruff format` before freezing so future frozen suites need no exemption. Remove this
exclude once the existing frozen files are re-tagged format-clean.
