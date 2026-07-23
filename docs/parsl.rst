Using the parsl backend
=======================

This page is the **how-to** for running graphed work on a `parsl <https://parsl-project.org>`_
executor: installing the extra, starting a direct-use ``HighThroughputExecutor``, running a
``Plan``, and repartitioning/joining blocks with the **relay** shuffle engine. For **how it
works** — direct executor submit (no DFK), the per-instance capability floor, and why HTEX shuffles
route through the submit host — read :ref:`design-parsl-backend` in :doc:`design`; this page links
into it rather than repeating it.

Every runnable snippet below was executed against a live 2-worker ``HighThroughputExecutor`` before
being committed; the printed values in the comments are real outputs.


Installing
----------

The parsl backend lives behind the ``[parsl]`` optional extra::

    pip install "graphed-executors[parsl]"    # pulls parsl>=2026.7.20

The base package stays parsl-free: the local executors (:mod:`graphed_executors.local`), the submit
seam (:mod:`graphed_executors.submit`), and even *importing*
:mod:`graphed_executors.parsl_backend` leave ``parsl`` out of ``sys.modules`` — the import is
deferred until you actually construct a backend (which needs a parsl executor anyway).

**Platform + Python.** parsl HTEX needs POSIX (no Windows support) and spawns worker processes via
the ``process_worker_pool`` / ``interchange.py`` console scripts, so run inside an **activated
virtualenv** (those scripts must be on ``PATH``). CI gates on CPython 3.12 (parsl's classifier
ceiling), observes 3.13 non-blocking, and does not install the extra on 3.14/3.14t (free-threaded
parsl is upstream-unverified). The extra itself carries no ``python_version`` marker — the CI axis,
not the metadata, is the tested-support fence.


Quickstart: start an executor and run a Plan
--------------------------------------------

:func:`~graphed_executors.parsl_backend.launch.start_htex` starts a direct-use
``HighThroughputExecutor`` — no ``parsl.load`` / DFK — with a fixed LocalProvider block
(``init_blocks == min_blocks == max_blocks``, no strategy, no scale-in). You own the executor;
hand it to :func:`~graphed_executors.parsl_backend.backend.parsl_runner`, which returns a
:class:`~graphed_executors.submit.engine.SubmitRunner`::

    import tempfile

    import numpy as np
    from graphed.core import Partition, Plan, Task
    from graphed_executors.parsl_backend import parsl_runner, start_htex, stop_htex

    from my_tasks import count, add, zero      # module-level: importable on the workers

    if __name__ == "__main__":
        parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
        plan = Plan(process=count, combine=add, empty=zero,
                    tasks=tuple(Task(i, p) for i, p in enumerate(parts)))
        executor = start_htex(workers=2, run_dir=tempfile.mkdtemp())
        try:
            with parsl_runner(executor) as runner:
                result = runner.run(plan)
            print(result.value, result.n_partitions, result.n_combines)   # [700] 7 6
        finally:
            stop_htex(executor)

Notes that save debugging time:

* ``process``/``combine``/``empty`` and any task fn **must be importable by reference on the
  workers** — put them in an installed module (``my_tasks`` above), not in your driver's
  ``__main__``. HTEX workers are fresh Python processes launched by a console script; they do **not**
  inherit your driver's ``sys.path``, so a function defined in ``__main__`` fails to unpickle with
  ``AttributeError: Can't get attribute 'count' on <module '__mp_main__'>``. (In a test/dev setup,
  export the module's directory on ``PYTHONPATH`` before ``start_htex``.)
* ``start_htex`` performs the three integration moves the DFK normally does (set ``run_dir``, create
  ``provider.script_dir``, ``scale_out_facade(init_blocks)``); it is also the drift canary — if a
  future parsl release changes that seam, the first run fails loudly here.
* ``runner.close()`` (or the ``with`` exit) does **not** shut your executor down — you own it, and
  ``stop_htex`` reaps the interchange, manager, and worker processes.
* The result is a ``graphed.core.ExecResult``: ``.value`` (bit-for-bit equal to a sequential run,
  invariant to worker count and completion order), ``.n_partitions``, ``.n_combines``.

Worker exceptions — including a picklable ``graphed.debug.StageError`` pointing at the user's
analysis line — re-raise in the driver intact (see `Failure semantics`_).


The capability floor
--------------------

Capabilities are **per instance**, derived from the executor type
(:ref:`design-parsl-backend`)::

    from graphed_executors.parsl_backend import ParslBackend

    ParslBackend(htex).capabilities
    # SubmitCapabilities(peer_data_movement=False, scatter_broadcast=False, pin_to_worker=False,
    #                    per_task_retries=False, per_task_resources=False, cancel_running=False,
    #                    worker_file_cache=False)                     # the all-False "parsl floor"

    ParslBackend(thread_pool_executor).capabilities
    # SubmitCapabilities(peer_data_movement=True, ...rest False)      # the ThreadBackend shape

``ParslBackend(HighThroughputExecutor)`` reports **all seven flags False** — future args resolve on
the submit host, broadcast reships per task, and there is no pinning / per-task retries / per-task
resources / running-cancel / worker file cache. This is not pessimism: it is the honest floor an
engine must be correct on. ``ParslBackend(ThreadPoolExecutor)`` reports ``peer_data_movement=True``
alone (same-process shared memory — the stdlib :class:`~graphed_executors.submit.threadpool.ThreadBackend`
shape). Any other executor type is refused with a ``TypeError`` naming both verified classes.


Repartition and joins: the relay engine
----------------------------------------

Because HTEX resolves future args on the submit host (``peer_data_movement=False``), the m43
as-tasks shuffle engine's peer gate refuses it — routing every block worker-to-worker is not
something HTEX can do. graphed instead ships the **relay engine**
(:func:`~graphed_executors.common.relay_engine.relay_run_repartition` /
:func:`~graphed_executors.common.relay_engine.relay_run_join`): the honest head-node workflow shape
— ``T`` producer maps, a driver-side barrier that regroups each destination locally, then ``P``
gathers. Bulk data crosses the submit host exactly twice; the scheduler sees ``T + P`` tasks and
zero pick tasks. Executed::

    import numpy as np
    from graphed.numpy import NumpyBackend
    from graphed_executors.common.relay_engine import relay_run_repartition, relay_run_join
    from graphed_executors.parsl_backend import ParslBackend
    from graphed_executors.submit import SubmitRunner

    from my_tasks import make_block            # module-level helper (importable on workers)

    src = [make_block([0, 1, 2, 3, 4, 5, 6, 7]),
           make_block([7, 6, 5, 4, 3, 2, 1, 0]),
           make_block([100, 200, 300, 400])]

    shuffled = relay_run_repartition(NumpyBackend(), src, 4,
                                     runner=SubmitRunner(ParslBackend(executor)))
    print({d: len(b) for d, b in sorted(shuffled.value.items())})     # {0: 5, 1: 7, 2: 6, 3: 2}
    print(shuffled.witness.head_node_routed, shuffled.witness.driver_relay_bytes)   # True 1088

The relay engine's ``dest_block_hashes`` are **byte-identical** to the local single-machine engine
and to the dask as-tasks engine on identical inputs — the same headline property the dask facade
pins, so the engine you run can never change an analysis result. Its result carries a
:class:`~graphed_executors.common.relay_engine.RelayShuffleWitness`: the usual shuffle counters
**plus** ``head_node_routed`` (always ``True``) and ``driver_relay_bytes`` (the total bytes
resolved at the driver barrier) — so the head-node routing is per-run observable, not docs-only.

A join goes through the same door; ``on`` / ``how`` / ``broadcast`` / ``salt`` /
``mem_budget_bytes`` mirror the dask engine (``mem_budget_bytes`` spills duplicated output
partitions instead of accumulating them)::

    left = [make_block([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
            make_block([1, 1, 2, 2, 13, 13], "lval", [22, 23, 24, 25, 32, 33])]
    right = [make_block([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
             make_block([3, 3, 2, 17, 17, 4], "rval", [106, 107, 108, 116, 117, 111])]
    joined = relay_run_join(NumpyBackend(), left, right, 2, on=("__joinkey__",), how="inner",
                            runner=SubmitRunner(ParslBackend(executor)))
    print(sum(len(block) for block in joined.value.values()))         # 16

``broadcast=None`` (the default) lets the pinned cost rule choose broadcast-vs-shuffle, keyed on
``parts`` — never the live worker count — so the same logical join makes the same choice on any
pool size. Non-inner joins produce masked (null-filled) blocks that ``graphed.numpy`` wires over
Arrow, so install ``graphed[parquet]`` (pyarrow) for ``how`` other than ``inner``.

.. note::

   **The relay engine holds the whole shuffle in the driver at the barrier** (≈ total shuffle
   bytes) — that *is* head-node routing, the defining cost of a broker without worker-to-worker
   reachability. It remains the ``"auto"`` / ``"tasks"`` engine (no parsl vector carries both
   ``pin_to_worker`` and ``peer_data_movement``, so ``"auto"`` always resolves to tasks). The
   **peer-exchange transport engine** below (opt-in via ``shuffle_method="transport"``) is the
   head-node-free alternative.


The peer-exchange transport engine (``shuffle_method="transport"``)
-------------------------------------------------------------------

On an HTEX instance (:attr:`~graphed_executors.parsl_backend.backend.ParslBackend.peer_transport`
is ``True``), ``shuffle_method="transport"`` runs the M39–M41 shuffle/join and the M38 reduction on
**k persistent peer tasks** (one per worker slot) that move blocks worker-to-worker — never through
the driver::

    from graphed_executors.parsl_backend.api import run_repartition, probe_peer_reachability

    report = probe_peer_reachability(ParslBackend(htex), k=2)   # optional pre-flight
    if report.ok:
        res = run_repartition(NumpyBackend(), src, 8, pbackend=ParslBackend(htex),
                              shuffle_method="transport")

Each peer mints an ``EscalatingHttpTransport`` endpoint **in-task**, announces a ``hello`` to a
driver-hosted rendezvous endpoint, and blocks on a barrier until all k hellos arrive — so no peer
holds the address book (and no send can race a missing inbox) before the driver broadcasts the
assembled registry. Blocks then travel peer↔peer over a ``/pull`` route (coalesced to one request
per holder — the ``≤ k·k`` incast bound — and evicted after serve); the driver's inbox sees only
control traffic. ``dest_block_hashes`` are byte-identical to the local engine.

Because worker-to-worker dialability is a cluster property parsl never guarantees, a **reachability
probe** runs at rendezvous time, before any data moves. ``on_unreachable`` routes the verdict:
``"error"`` (the default) raises an attributed ``StageError`` naming the unreachable pair;
``"fallback"`` transparently re-runs the relay engine on the same inputs and sets
``witness.fallback_reason`` (observable, never silent). A transport-only knob
(``fetch_budget_bytes`` / ``pull_timeout_s`` / ``epoch_restarts_allowed`` / ``workers`` /
``on_unreachable`` / ``registry_rewrite``) set while resolution lands on ``"tasks"`` is a loud
``ValueError`` before any submit; on a ``ThreadPoolExecutor`` (``peer_transport`` is ``False``)
``"transport"`` raises ``NotImplementedError`` naming ``"tasks"`` (a loopback re-enactment of
head-node routing on driver threads adds mechanism and removes honesty).


The m43 engine over a ThreadPoolExecutor
----------------------------------------

A parsl ``ThreadPoolExecutor`` reports ``peer_data_movement=True`` (its task threads share the
driver's memory), so the m43 as-tasks engine — moved verbatim to
:mod:`graphed_executors.common.tasks_engine` — runs over it unchanged, with its full ``T`` maps /
``T·P`` picks / ``P`` gathers submit shape::

    from graphed_executors.common.tasks_engine import dask_run_repartition

    dask_run_repartition(NumpyBackend(), src, 4, runner=SubmitRunner(ParslBackend(tpe)))

Because a TPE's "workers" are threads in the driver process, this witnesses the engine's submit
*shape* through the backend, not separate-process execution — the relay engine is the one to reach
for on a real HTEX cluster.


Failure semantics
-----------------

**Ordinary worker exceptions** round-trip intact: a picklable ``graphed.debug.StageError`` raised
in a ``process`` re-raises in the driver still pointing at the user's analysis line — over the real
HTEX process boundary this is the M6 picklability obligation executed, never an opaque parsl
wrapper.

**Hard worker death** (SIGKILL / OOM / preemption) surfaces after parsl's watchdog detection as a
``parsl.executors.high_throughput.errors.WorkerLost``; the pool respawns the dead worker in place
and stays usable. The detection window is parsl's ``heartbeat_period`` — default ≈ 30 s, so a
SIGKILL takes that long to surface as a ``WorkerLost`` (and, under the transport engine, to trip the
epoch restart). Pass ``start_htex(..., heartbeat_period=2)`` to compress it (measured ≈ 29.7 s → ≈
1.65 s). :meth:`~graphed_executors.parsl_backend.backend.ParslBackend.describe_failure`
recognises ``WorkerLost`` / ``ManagerLost`` **by class name anywhere in the exception chain** (never
by parsl identity, so the module stays parsl-import-free at load) and returns a
``(key, worker)`` attribution tuple the engine turns into an attributed ``StageError``. Under the
transport engine this drives **epoch-restart recovery**: a lost peer (or an exhausted
``EscalatingHttpTransport.send``) restarts the whole run under a fresh epoch onto the survivors up
to ``epoch_restarts_allowed`` (default 1); an exhausted budget surfaces as an attributed
``StageError`` naming the death signal, never a raw parsl exception and never a hang.


Monitoring
----------

Pass a passive ``graphed.core.execution.Monitor`` to ``parsl_runner`` and the run delivers the M37
``TaskEvent`` lifecycle (``submitted`` driver-side; ``started`` / ``finished`` worker-side). parsl
has no worker-to-driver event transport, so the worker shim **buffers** its events and they are
dispatched to the subscribed handler when the task's result is unwrapped — completion-granularity
delivery (documented limitation; live M37 dashboard parity over parsl is Phase 2). Emission is
passive by contract: the reduced value is byte-identical whether a monitor is attached, detached,
or actively raising.
