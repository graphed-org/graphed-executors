# Test dispute: tests/frozen/m43/test_dask_join_parity_edges.py::test_broadcast_empty_build_side_returns_gracefully[outer]

## The test code
```python
@pytest.mark.parametrize("how", ["inner", "outer"])
def test_broadcast_empty_build_side_returns_gracefully(adapter, how, client):
    _, right = _sides(adapter, [], _A_PROBE)
    res = dask_join(adapter.backend, [], right, PARTS, how=how, runner=_runner(client), broadcast=True)
    local = run_join(adapter.backend, [], right, PARTS, how=how, broadcast=True)
    assert res.dest_block_hashes == local.dest_block_hashes == {}
    assert len(res.value) == len(local.value) == 0
```
`README-fixup.md` lists "value non-empty while local's is `{}`" as the defect it guards against.

## The clause it contradicts
The relational contract of the m40 join suite (an outer join keeps every unmatched probe row) and the
engines' own shuffle path: the same inputs with `broadcast=False` return every probe row (frozen
m44 `test_one_side_empty_matches_the_local_oracle` pins `("outer", "left"): 7` and `("right", "left"): 7`).
For `how="outer"` the `== {}` pin freezes graphed-executors#91: an empty build side makes the cost
model choose broadcast, and every broadcast engine then dropped all probe rows. The `inner` case is correct
and unaffected.

## Proposed correction
Keep the dask-equals-local equality and the `inner` pin. For `outer`, replace `== {}` with "every probe row
kept exactly once", for example `Counter(rows of res.value) == Counter(probe rows)`, still asserting
`res.dest_block_hashes == local.dest_block_hashes`.

## Ruling
owner ruling 2026-10-29: approved
