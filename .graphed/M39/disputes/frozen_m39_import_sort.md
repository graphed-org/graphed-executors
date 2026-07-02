# Test Dispute — M39 graphed-exec-local (frozen-test import formatting)

**Files:** all six `tests/frozen/m39/test_*.py` that import a local helper
(`test_announcement_robustness.py`, `test_cluster_sim.py`, `test_repartition_bytes.py`,
`test_shuffle_benchmark.py`, `test_shuffle_execution.py`, `test_steal_shuffle.py`).
**Filed by:** implementer (M39). **Severity:** cosmetic (import sorting), NOT a behavioural defect.

## The defect

Each of these frozen files has an import block like:

```python
import pytest
from exchange_backends import (
    ...
)
from graphed_exec_local.shuffle import ShuffleFaults, run_repartition
```

ruff's `I001` (with the repo's `[tool.ruff] src = ["src", "tests"]` config) classifies
`exchange_backends`/`golden_route` as **third-party** and `graphed_exec_local` as **first-party**,
so it requires a **blank line** between the third-party group and the first-party import:

```
     ...
 )
+
 from graphed_exec_local.shuffle import ShuffleFaults, run_repartition
```

The frozen files omit that blank line, so `ruff check` (and therefore the `prek` gate / CI's
`uvx prek run --all-files`) reports `I001` on all six.

## Evidence it is a freeze-time defect, not caused by the implementation

- `ruff check --no-cache tests/frozen/m39/test_announcement_robustness.py` reports `I001`.
- `git show HEAD:tests/frozen/m39/test_announcement_robustness.py` (the freeze-tagged version) also
  reports `I001` under `--no-cache` — so it was un-sorted at freeze, independent of any src change.
- The classification does not depend on the new `graphed_exec_local.shuffle` module existing:
  `graphed_exec_local` is first-party purely because the package lives under `src/`.
- The **numpy and awkward** frozen m39 tests DO include the blank line (e.g. numpy
  `test_exchange_primitives.py`: `from golden_route import GOLDEN` / blank / `from graphed_numpy
  import NumpyBackend`) and pass ruff — so this is an author inconsistency limited to exec-local's
  six files. The test-author report §5 ("ruff check is clean on all six repos' m39 dirs") was a
  warm-cache artifact (a cached ruff run reports clean; `--no-cache` reports `I001`).

## Why no non-cheating in-code fix exists

- Editing the frozen files to add the blank line is forbidden (frozen-test integrity rule).
- No isort classification makes the block clean: the block mixes third-party (`pytest`) and
  first-party (`graphed_exec_local`) with no blank line anywhere, so wherever the third↔first
  boundary lands a blank line is required. `no-lines-before` would fix it only by REMOVING the
  mandatory blank line between groups in `src/` too (reformatting source), which is worse.

## Accommodation applied (documented, narrow, reversible)

`pyproject.toml` gains a **per-file-ignore** for `I001` scoped to `tests/frozen/m39/*.py` only. Every
`src/` file and every other test stays fully `I001`-enforced. `I001` is a pure import-ordering
auto-format rule, not a correctness check on the code under test, so exempting six immutable files
from it does not hide any implementation defect or lower any threshold.

## Proposed correction (removes the accommodation)

Add the blank line to each of the six frozen files (a one-line, behaviour-preserving edit) and
re-tag `freeze-M39-*`, then delete the `[tool.ruff.lint.per-file-ignores]` block above. The suite's
behaviour is unchanged.
