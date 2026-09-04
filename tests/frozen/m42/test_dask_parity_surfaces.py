"""m42 frontend parity on dask (plan §1.5 rows 1, 3, 4, 5, 11; blueprint §3
`test_dask_parity_surfaces`): the executor is domain-blind — every frontend surface reaches it as
a picklable `Plan`, so each surface is exercised end-to-end against `SequentialRunner`:

1.  `aggregate_plan` multi-output (shared-subgraph IR) over a REAL parquet dataset — surfaces 1+5
    in one run: outs bit-for-bit vs sequential AND >=2 workers ran leaves, driver ran none.
3.  External payloads resolved worker-side by content hash; a MISSING evaluator fails loudly from
    the worker with the pinned `needs an evaluator` text (never a silent skip or an opaque wrap).
4.  `to_parquet(..., executor=dask_runner(...))`: part basenames == sequential's, bytes identical.
11. `open_once` file-locality via the worker plugin's resources: opens-per-worker == 1 while
    tasks-per-worker > 1 (plus the §1.3.3 plugin name/idempotent pins).

Requires pyarrow at runtime (`ak.to_parquet`/`ak.from_parquet`) — the test-dask CI job must
install it alongside the dask extra."""

from __future__ import annotations

import dataclasses
import hashlib
import pathlib

import awkward as ak
import graphed.core
import pytest
from graphed import GraphedError, Session, aggregate_plan, compile_ir
from graphed.awkward import AwkwardBackend
from graphed.awkward import gak as gak_fns
from graphed.awkward.io import from_parquet, to_parquet
from graphed.core.execution import SequentialRunner
from submit_backends import (
    ToyListBackend,
    ToyListSource,
    agg_combine,
    agg_empty,
    agg_reduce,
    cluster_client,
    make_dask_runner,
    open_once_plan,
    times10,
    worker_pids,
)

pytest.importorskip("distributed")

ROWS_PER_FILE = 12
N_FILES = 2


@pytest.fixture(scope="module")
def client():
    with cluster_client(2, processes=True) as c:
        yield c


@pytest.fixture(scope="module")
def parquet_paths(tmp_path_factory: pytest.TempPathFactory) -> list[str]:
    """A deterministic INT-valued 2-file dataset (exact float sums — grouping-safe bit-for-bit)."""
    d = tmp_path_factory.mktemp("m42-parquet")
    paths = []
    for i in range(N_FILES):
        arr = ak.Array(
            {
                "x": [3 * k + i for k in range(ROWS_PER_FILE)],
                "y": [k % 5 for k in range(ROWS_PER_FILE)],
            }
        )
        path = str(d / f"events-{i}.parquet")
        ak.to_parquet(arr, path)
        paths.append(path)
    return paths


def _truth() -> tuple[float, float]:
    xs = [3 * k + i for i in range(N_FILES) for k in range(ROWS_PER_FILE)]
    ys = [k % 5 for _ in range(N_FILES) for k in range(ROWS_PER_FILE)]
    total = float(sum(xs) + sum(ys))
    return 2.0 * total, total


def test_parquet_aggregate_multi_output_matches_sequential_and_spreads(client, parquet_paths) -> None:
    session = Session(AwkwardBackend())
    events = from_parquet(session, "events", parquet_paths, open_files=False, steps_per_file=4)
    shared = events.x + events.y  # the shared subgraph, compiled ONCE into the plan's IR
    out1 = gak_fns.sum(shared * 2)
    out2 = gak_fns.sum(shared)
    plan = aggregate_plan(
        out1, out2, reduce=agg_reduce, combine=agg_combine, empty=agg_empty, steps_per_file=4
    )
    assert len(plan.tasks) == N_FILES * 4

    seq = SequentialRunner().run(plan)
    with make_dask_runner(client) as runner:
        got = runner.run(plan)

    assert got.value.outs == seq.value.outs == _truth()  # bit-for-bit (exact int-valued sums)
    assert got.n_partitions == seq.n_partitions == 8
    pids = {pid for pid, _ in got.value.sites}
    wpids = set(worker_pids(client).values())
    assert pids <= wpids, "aggregate leaves/combines ran outside the worker processes"
    assert len({w for _, w in got.value.sites}) >= 2, "all aggregate work landed on one worker"


def _toy_external_plan(*, bind_external: bool):
    """A toy-backend aggregate whose graph holds ONE External (`map`) node; ``bind_external``
    resolves it by content hash exactly as a deployment would (the m10 idiom)."""
    session = Session(ToyListBackend())
    src = ToyListSource(tuple(range(1, 13)), n_parts=6)
    x = session.source("x", form="num", data=src)
    out = x.map(times10).reduce("sum")
    externals = None
    if bind_external:
        compiled = compile_ir(session, out)
        store = graphed.core.GraphStore.deserialize(compiled.ir)
        (chash,) = [n["descriptor"]["content_hash"] for n in store.nodes() if n["kind"] == "external"]
        externals = {chash: times10}
    plan = aggregate_plan(
        out, reduce=agg_reduce, combine=agg_combine, empty=agg_empty, externals=externals
    )
    if not bind_external:
        # aggregate_plan auto-wires evaluators from the recording session, so externals=None is not
        # an unbound plan; strip them to exercise a genuinely unbound External.
        plan = dataclasses.replace(plan, process=dataclasses.replace(plan.process, externals=()))
    return plan


def test_external_payload_resolves_worker_side_by_content_hash(client) -> None:
    plan = _toy_external_plan(bind_external=True)
    seq = SequentialRunner().run(plan)
    with make_dask_runner(client) as runner:
        got = runner.run(plan)
    assert got.value.outs == seq.value.outs == (float(10 * sum(range(1, 13))),)
    assert {pid for pid, _ in got.value.sites} <= set(worker_pids(client).values())


def test_missing_external_fails_loudly_from_the_worker(client) -> None:
    plan = _toy_external_plan(bind_external=False)
    with make_dask_runner(client, retries=0) as runner, pytest.raises(GraphedError) as excinfo:
        runner.run(plan)
    assert "needs an evaluator" in str(excinfo.value)  # the pinned loud text, not a silent skip
    assert "externals" in str(excinfo.value)  # the message tells the user the fix


def test_to_parquet_on_dask_matches_sequential(client, parquet_paths, tmp_path: pathlib.Path) -> None:
    def write(dest: pathlib.Path, executor) -> list[pathlib.Path]:
        session = Session(AwkwardBackend())
        events = from_parquet(session, "events", parquet_paths, open_files=False, steps_per_file=2)
        return [pathlib.Path(p) for p in to_parquet(events.x + events.y, str(dest), steps_per_file=2, executor=executor)]

    seq_parts = write(tmp_path / "seq", SequentialRunner())
    with make_dask_runner(client) as runner:
        dask_parts = write(tmp_path / "dask", runner)

    assert [p.name for p in dask_parts] == [p.name for p in seq_parts]  # key-ordered path reduce
    assert len(dask_parts) == N_FILES * 2
    for dp, sp in zip(dask_parts, seq_parts, strict=True):
        assert hashlib.sha256(dp.read_bytes()).hexdigest() == hashlib.sha256(sp.read_bytes()).hexdigest()


def test_open_once_opens_each_uri_once_per_worker(client) -> None:
    shared_uri = "shared://m42-parity-open-once"
    with make_dask_runner(client) as runner:
        records = runner.run(open_once_plan(8, "oo", shared_uri)).value

    assert len(records) == 8  # every task reported
    by_pid: dict[int, tuple[set[str], set[str]]] = {}
    for uri, pid, _worker, handle in records:
        uris, handles = by_pid.setdefault(pid, (set(), set()))
        uris.add(uri)
        handles.add(handle)
    assert by_pid.keys() <= set(worker_pids(client).values())
    assert any(len(uris) > 1 for uris, _ in by_pid.values())  # some worker ran MANY tasks...
    for pid, (_uris, handles) in by_pid.items():
        assert len(handles) == 1, f"worker {pid} opened the shared uri {len(handles)} times"
        (handle,) = handles
        assert handle.startswith(f"{pid}:"), "the open happened in a different process (not local)"


def test_worker_plugin_pins(client) -> None:
    """§1.3.3 pins: the plugin is a named, idempotent WorkerPlugin holding per-worker resources."""
    from graphed_executors.dask_backend.plugin import GraphedWorkerPlugin  # noqa: PLC0415  (deferred)

    assert GraphedWorkerPlugin.name == "graphed-worker"
    assert GraphedWorkerPlugin.idempotent is True
