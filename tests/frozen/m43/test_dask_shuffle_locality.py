"""m43 peer data movement — blocks cross worker↔worker via future dependencies, never through the
driver (plan §1.3.4 decision 1; blueprint §3 `test_dask_shuffle_locality`).

THE test that discriminates the REJECTED design (§1.3.4 option (i), the duck-typed cluster): that
design runs every `partition`/`concat` kernel in a driver-side loop and only farms out STORAGE, so
every producer/gather site would carry the driver pid. Here the pinned `DaskShuffleWitness`
`producer_sites`/`gather_sites` — (pid, worker address) recorded AT EXECUTION by the stage-1 and
gather task bodies — must (a) all carry live WORKER pids (cross-checked against `client.run`'s
address→pid map, so fabricated sites don't match), (b) never carry the driver pid, (c) span at
least two distinct workers, and (d) contain at least one (producer ≠ gather) worker pair — with
every gather depending on every producer's pick futures, that pair proves block bytes moved
worker↔worker through the scheduler's peer fetch.

All witnesses are pids/addresses/counts (R0.10a) on a tier-B (processes=True) cluster where worker
pids genuinely differ from the driver's."""

from __future__ import annotations

import os

import pytest
from dask_shuffle_backends import (
    ShuffleJoinAdapter,
    build_dask_backend,
    dask_repartition,
    install_worker_plugin,
    live_worker_pids,
    make_submit_runner,
    repartition_blocks,
    shuffle_adapters,
    shuffle_cluster,
)

pytest.importorskip("distributed")

ADAPTERS = shuffle_adapters()
PARTS = 6


@pytest.fixture(scope="module")
def client():
    # tier B is REQUIRED here: processes=False would give every "worker" the driver's pid and
    # vacuously pass the driver-pid exclusion
    with shuffle_cluster(2, processes=True) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> ShuffleJoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_stage1_and_gather_run_on_workers_and_blocks_cross_between_them(adapter: ShuffleJoinAdapter, client) -> None:  # type: ignore[no-untyped-def]
    src = repartition_blocks(adapter)
    install_worker_plugin(client)
    addr_to_pid = live_worker_pids(client)
    assert len(addr_to_pid) == 2 and os.getpid() not in addr_to_pid.values(), "fixture sanity"

    res = dask_repartition(adapter.backend, src, PARTS, runner=make_submit_runner(build_dask_backend(client)))
    w = res.witness
    psites: dict[int, tuple[int, str]] = dict(w.producer_sites)
    gsites: dict[int, tuple[int, str]] = dict(w.gather_sites)

    assert len(psites) == w.n_producer_tasks and psites, "every producer task records its site"
    assert set(gsites) == set(res.value), "every produced dest block records its gather site"

    driver_pid = os.getpid()
    for pid, addr in [*psites.values(), *gsites.values()]:
        assert pid != driver_pid, (
            f"a stage site ran in the DRIVER process (pid {pid}) — the §1.3.4 rejected "
            "duck-typed-cluster design (kernels in a driver-side loop) leaks exactly this way"
        )
        assert addr_to_pid.get(addr) == pid, (
            f"site ({pid}, {addr}) does not match a live worker — sites must be recorded by the "
            "executing worker, not synthesized"
        )

    all_addrs = {a for _, a in psites.values()} | {a for _, a in gsites.values()}
    assert len(all_addrs) >= 2, "all shuffle work landed on ONE worker — no distribution happened"
    assert any(pa != ga for _, pa in psites.values() for _, ga in gsites.values()), (
        "no gather consumed a block produced on a DIFFERENT worker — peer data movement never "
        "engaged (a driver-funnel or single-worker-pinned implementation)"
    )
