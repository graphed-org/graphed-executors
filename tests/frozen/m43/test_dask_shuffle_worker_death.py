"""m43 recompute-on-loss — a worker holding stage-1 blocks dies MID-SHUFFLE and the result is
still bit-for-bit: dask recomputes the lost producer from the future graph (plan §1.3.4 point 2 —
completeness IS the graph; blueprint §3 `test_dask_shuffle_worker_death`).

Deterministic mid-run kill, no timing races (R0.10a — every step gates on observed FILE marks):
the GatedShuffleBackend (1) drops a `pmark` file per stage-1 `partition` call and (2) makes every
gather's `from_wire` drop a `gstart` mark then BLOCK until a gate file exists. The driver waits
until stage 1 has fully run once AND some producer-holding worker is blocked inside a gather, then
kills exactly that worker (validated `client.run(os._exit)` idiom + nanny restart), then opens the
gate. The killed worker's producer output AND its blocked gather are lost, so the scheduler must
re-run the producer task elsewhere — witnessed by `pmark` count STRICTLY exceeding n_src, while
the final `dest_block_hashes` equal a plain-backend LOCAL oracle run bit-for-bit.

Discriminates: an engine that memoizes stage-1 output OUTSIDE dask's graph (loses the recompute →
error or divergent bytes), and one whose merge order depends on which worker recomputed."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
from dask_shuffle_backends import (
    GatedShuffleBackend,
    ShuffleJoinAdapter,
    build_dask_backend,
    dask_repartition,
    install_worker_plugin,
    kill_worker,
    live_worker_pids,
    make_submit_runner,
    mark_addresses,
    mark_files,
    poll_until,
    repartition_blocks,
    shuffle_adapters,
    shuffle_cluster,
)

from graphed_executors.local.shuffle import run_repartition

pytest.importorskip("distributed")

ADAPTERS = shuffle_adapters()
PARTS = 8


@pytest.fixture()
def client():
    # dedicated per-test cluster: deaths must not bleed into other modules; a generous
    # allowed-failures budget so the innocent recomputed tasks are never blamed
    with shuffle_cluster(2, processes=True, allowed_failures=100) as c:
        yield c


@pytest.fixture(params=ADAPTERS, ids=[a.name for a in ADAPTERS])
def adapter(request) -> ShuffleJoinAdapter:  # type: ignore[no-untyped-def]
    return request.param


def test_lost_stage1_blocks_are_recomputed_and_the_result_is_bit_for_bit(adapter: ShuffleJoinAdapter, client, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    from dask_shuffle_backends import shuffle_api  # noqa: PLC0415  (deferred: module under test)

    shuffle_api()  # right-reason gate: fail HERE pre-implementation, before the scenario spins up
    src = repartition_blocks(adapter)
    n_src = len(src)
    mark_dir = tmp_path / "marks"
    mark_dir.mkdir()
    gate = tmp_path / "gate"
    gated = GatedShuffleBackend(adapter.backend, str(mark_dir), str(gate))
    # the oracle uses the PLAIN backend (no gate): the wrapper is data-transparent by construction
    oracle = run_repartition(adapter.backend, src, PARTS)

    install_worker_plugin(client)
    runner = make_submit_runner(build_dask_backend(client))
    outcome: dict[str, object] = {}

    def drive() -> None:
        try:
            outcome["result"] = dask_repartition(gated, src, PARTS, runner=runner)
        except BaseException as exc:  # surfaced after join — the driver thread must never hide it
            outcome["error"] = exc

    thread = threading.Thread(target=drive, daemon=True)
    thread.start()

    # (1) stage 1 fully ran at least once AND (2) a producer-holding worker is blocked in a gather
    def victim_ready() -> bool:
        if len(mark_files(str(mark_dir), "pmark")) < n_src:
            return False
        return bool(set(mark_addresses(str(mark_dir), "pmark")) & set(mark_addresses(str(mark_dir), "gstart")))

    poll_until(lambda: victim_ready() or not thread.is_alive(), timeout_s=120.0)
    assert "error" not in outcome, f"shuffle raised before the kill: {outcome.get('error')!r}"
    assert victim_ready(), "scenario misfired: stage 1 or a blocked gather never materialised"

    victims = set(mark_addresses(str(mark_dir), "pmark")) & set(mark_addresses(str(mark_dir), "gstart"))
    victim = sorted(victims)[0]
    pids_before = live_worker_pids(client)
    assert victim in pids_before, "victim address is not a live worker"
    victim_pid = pids_before[victim]

    kill_worker(client, victim)
    poll_until(lambda: victim_pid not in live_worker_pids(client).values(), timeout_s=60.0)
    assert victim_pid not in live_worker_pids(client).values(), "the victim never died"
    gate.touch()  # open every gather (blocked and rescheduled alike)

    thread.join(timeout=240.0)
    assert not thread.is_alive(), "shuffle never completed after the worker death (deadlock/stall)"
    assert "error" not in outcome, f"shuffle raised after the death: {outcome.get('error')!r}"

    result = outcome["result"]
    assert result.dest_block_hashes == oracle.dest_block_hashes, (  # type: ignore[attr-defined]
        "result diverged from the clean local oracle — recompute changed the bytes"
    )
    n_pmarks = len(mark_files(str(mark_dir), "pmark"))
    assert n_pmarks > n_src, (
        f"only {n_pmarks} partition calls for {n_src} src blocks — the lost producer was never "
        "re-executed, so its output survived OUTSIDE the dask graph (a retired-store memoization)"
    )
