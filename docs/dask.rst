Using the dask backend
======================

This page is the **how-to** for running graphed work on a ``dask.distributed`` cluster: installing
the extra, running a ``Plan``, repartitioning and joining blocks through the one-knob shuffle
facade, and deploying on batch clusters. For **how it works** — the future-graph topology, the
worker-transport engine, keys, broadcast, and the determinism contract — read
:ref:`design-dask-backend` in :doc:`design`; this page links into it rather than repeating it.

Every runnable snippet below was executed against a live ``LocalCluster`` before being committed;
the printed values in the comments are real outputs. The batch-cluster sketches near the end are
the one exception and are explicitly marked *illustrative*.

.. contents::
   :local:
   :depth: 2


Installing
----------

The dask backend lives behind the ``[dask]`` optional extra::

    pip install "graphed-executors[dask]"    # pulls dask[distributed]>=2026.6

The base package stays dask-free: the local executors (:mod:`graphed_executors.local`) and the
submit seam (:mod:`graphed_executors.submit`) install and run without it, and even *importing*
:mod:`graphed_executors.dask_backend` leaves ``distributed`` out of ``sys.modules`` — the import
is deferred until you actually construct a backend, so the hinted ``ImportError`` (install the
extra) fires at construction, not at import.

Keep the same ``dask``/``distributed``/``graphed`` versions on the client and the workers
(``client.get_versions(check=True)`` verifies). **Free-threaded CPython (3.14t) is not supported
for the dask backend** — upstream ``distributed`` has no free-threaded build yet; the local
executors keep their 3.14t support.


Quickstart: run a Plan
----------------------

The public seam is a **ready** ``distributed.Client`` — you build the cluster however your site
does (``LocalCluster`` here; SLURM/HTCondor below), and hand the client to
:func:`~graphed_executors.dask_backend.backend.dask_runner`, which registers the per-worker plugin
and returns a :class:`~graphed_executors.submit.engine.SubmitRunner`::

    import numpy as np
    from distributed import Client, LocalCluster

    from graphed.core import Partition, Plan, Task
    from graphed_executors.dask_backend import dask_runner

    def count(partition, resources):          # module-level: picklable
        return np.asarray([partition.entry_stop - partition.entry_start])

    def add(a, b):
        return a + b

    def zero():
        return np.zeros(1, dtype=int)

    if __name__ == "__main__":
        parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
        plan = Plan(process=count, combine=add, empty=zero,
                    tasks=tuple(Task(i, p) for i, p in enumerate(parts)))
        with LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                          dashboard_address=":0") as cluster, Client(cluster) as client:
            with dask_runner(client) as runner:
                result = runner.run(plan)
        print(result.value)                        # [700]
        print(result.n_partitions, result.n_combines)   # 7 6

Notes that save debugging time:

* ``process``/``combine``/``empty`` must be **module-level** (or ``functools.partial`` of
  module-level, or frozen dataclasses) — they are pickled to the workers. Guard the driver code
  with ``if __name__ == "__main__":`` when workers run as separate processes.
* ``processes=False`` keeps this example fast and self-contained; on a real cluster prefer
  process workers with ``threads_per_worker=1`` for GIL-holding compiled HEP stages, and
  ``dashboard_address=":0"`` to dodge ``EADDRINUSE`` (see :ref:`design-deployment`).
* ``dask_runner(client, retries=3)`` forwards ``retries`` to dask's **native** per-task retries
  (resubmit on another worker); there is deliberately no second graphed-level retry loop.
* ``runner.close()`` (or the ``with`` exit) does **not** close your client — you own the cluster.
* The result is a ``graphed.core.ExecResult``: ``.value`` (the reduced result — bit-for-bit equal
  to a sequential run, invariant to worker count and completion order), ``.n_partitions``, and
  ``.n_combines``.

A ``Plan`` with ``next_tasks`` runs on the adaptive path exactly as it does locally; worker
exceptions — including a picklable ``graphed.debug.StageError`` pointing at the user's analysis
line — re-raise in the driver intact (see `Failure semantics`_).

The same engine without dask
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``SubmitRunner`` is one engine over *any* :class:`~graphed_executors.submit.protocol.SubmitBackend`;
the stdlib :class:`~graphed_executors.submit.threadpool.ThreadBackend` runs the identical Plan with zero extra
dependencies — useful for tests and as the conformance floor an adapter author starts from
(:ref:`design-submit-seam`)::

    from graphed.core import Partition, Plan, Task
    from graphed_executors.submit import SubmitRunner, ThreadBackend

    with SubmitRunner(ThreadBackend(max_workers=4)) as runner:
        print(runner.run(plan).value)          # same value, same combine tree


Repartition and joins: the shuffle facade
-----------------------------------------

Two distributed shuffle engines ship with the backend — the **as-tasks** future-graph engine and
the **worker-transport** engine (:ref:`design-shuffle-graph`, :ref:`design-transport-engine`).
The front door is :func:`~graphed_executors.dask_backend.api.run_repartition` /
:func:`~graphed_executors.dask_backend.api.run_join`, which select an engine with a single
``shuffle_method`` knob:

* ``"transport"`` — always the worker-transport engine (m44).
* ``"tasks"`` — always the as-tasks engine (m43).
* ``"auto"`` (default) — **capability-static**: transport iff the backend advertises *both*
  ``pin_to_worker`` and ``peer_data_movement`` (a plain ``DaskBackend`` does), else tasks.
  Resolution is a pure function of ``backend.capabilities`` — it does **not** inspect cluster
  elasticity, so adaptive-cluster users pass ``shuffle_method="tasks"`` explicitly (below).

Before any transport-engine run, register the transport plugin once per client with
:func:`~graphed_executors.dask_backend.transport.dask_transport_setup` (idempotent). Executed
end-to-end::

    import numpy as np
    from distributed import Client, LocalCluster

    from graphed.numpy import NumpyBackend
    from graphed_executors.dask_backend import DaskBackend, run_join, run_repartition
    from graphed_executors.dask_backend.transport import dask_transport_setup

    def make_block(keys, field="v", values=None):
        dt = np.dtype([("__joinkey__", np.uint64), (field, np.int64)])
        block = np.zeros(len(keys), dtype=dt)
        block["__joinkey__"] = np.asarray(keys, dtype=np.uint64)
        block[field] = np.arange(len(keys)) if values is None else np.asarray(values)
        return block

    if __name__ == "__main__":
        src = [make_block([0, 1, 2, 3, 4, 5, 6, 7]),
               make_block([7, 6, 5, 4, 3, 2, 1, 0]),
               make_block([100, 200, 300, 400])]
        with LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                          dashboard_address=":0") as cluster, Client(cluster) as client:
            dask_transport_setup(client)     # transport-engine precondition, once per client
            dbackend = DaskBackend(client)

            auto = run_repartition(NumpyBackend(), src, 4, dbackend=dbackend)
            tasks = run_repartition(NumpyBackend(), src, 4, dbackend=dbackend,
                                    shuffle_method="tasks")

            assert hasattr(auto, "transport")     # "auto" resolved to the transport engine
            assert hasattr(tasks, "partitions")   # the as-tasks engine ran
            assert auto.dest_block_hashes == tasks.dest_block_hashes  # byte-identical engines
            print({d: len(b) for d, b in sorted(auto.value.items())})
            # {0: 5, 1: 7, 2: 6, 3: 2}

The final assertion is the headline property, pinned by the frozen suites: **both engines — and
the local single-machine engine — produce byte-identical** ``dest_block_hashes`` on identical
inputs, so switching engines can never change an analysis result.

A join goes through the same door. ``on`` / ``how`` / ``broadcast`` / ``salt`` /
``mem_budget_bytes`` are common to both engines — ``mem_budget_bytes`` bounds the join's working
set on *either* path (duplicated output partitions spill instead of accumulating)::

        left = [make_block([0, 1, 2, 3, 4, 5], "lval", [10, 11, 12, 13, 14, 15]),
                make_block([1, 1, 2, 2, 13, 13], "lval", [22, 23, 24, 25, 32, 33])]
        right = [make_block([0, 0, 1, 2, 3, 5], "rval", [100, 101, 102, 103, 104, 105]),
                 make_block([3, 3, 2, 17, 17, 4], "rval", [106, 107, 108, 116, 117, 111])]
        joined = run_join(NumpyBackend(), left, right, 2,
                          on=("__joinkey__",), how="inner", dbackend=dbackend,
                          mem_budget_bytes=1 << 20)
        print(sum(len(block) for block in joined.value.values()))   # 16

``broadcast=None`` (the default) lets the pinned cost rule choose broadcast-vs-shuffle, keyed on
``parts`` — never the live worker count — so the same logical join makes the same choice on any
cluster size; ``True``/``False`` honour a plan-recorded choice.

Reading the result
~~~~~~~~~~~~~~~~~~

The facade returns the chosen engine's own result — a union of
``graphed_executors.local.shuffle.ShuffleResult`` (tasks) and
``graphed_executors.dask_backend._transport_run.TransportShuffleResult`` (transport). The
**portable contract** is the common triple every caller should code against:

* ``.dest_block_hashes`` — ``{dest_pid: sha256-of-wire-bytes}``, the content-addressed identity of
  each output block (equal across engines and runs);
* ``.value`` — ``{dest_pid: block}``, the gathered output blocks;
* ``.witness`` — the shuffle witness counters (spills, buffer peaks, broadcast choice).

The engine-specific extra doubles as **resolution observability**: a result with a ``.transport``
attribute proves the transport engine ran (its ``TransportWitness`` carries the transport-plane
counters), one with ``.partitions`` proves the as-tasks engine ran — the first thing to check when
asking "why was ``auto`` slow on my adaptive cluster?".

Knob honesty
~~~~~~~~~~~~

Setting a transport-only knob while resolution lands on ``"tasks"`` is a loud, named error —
never a silent drop. Validation runs *after* resolution, so ``"auto"`` degrading to tasks with an
explicitly-set transport knob raises too::

        run_repartition(NumpyBackend(), src, 4, dbackend=dbackend,
                        shuffle_method="tasks", holder_budget_bytes=1 << 20)
    # ValueError: holder_budget_bytes applies only to shuffle_method='transport' (resolved: 'tasks')

The facade always builds its runner internally. Callers who need ``monitor=`` or ``retries=`` on a
shuffle use the still-public direct entry points —
:func:`~graphed_executors.dask_backend.shuffle.dask_run_repartition` /
:func:`~graphed_executors.dask_backend.shuffle.dask_run_join` (tasks, taking a ``runner=``) and
:func:`~graphed_executors.dask_backend.transport_shuffle.transport_run_repartition` /
:func:`~graphed_executors.dask_backend.transport_shuffle.transport_run_join` (transport, taking
``dbackend=``).


Choosing an engine
------------------

Both engines are byte-identical to each other and to the local engine; the choice is purely about
cluster shape and scale (rationale: :ref:`design-facade`).

.. list-table::
   :header-rows: 1
   :widths: 30 22 48

   * - Your situation
     - ``shuffle_method``
     - Why
   * - Fixed-size cluster, large shuffles
     - ``"auto"`` (→ transport)
     - O(T+P) scheduler graph, bulk bytes move worker→worker over the transport, no T·P pick
       tier — minimal interpreter/scheduler touchpoints.
   * - Adaptive / elastic cluster (workers join and leave)
     - ``"tasks"`` explicitly
     - The future graph lets the dask scheduler *recompute* a lost producer from its inputs; the
       transport engine strict-pins tasks, so a departed owner forces a whole-run epoch restart.
   * - Backend without ``pin_to_worker`` + ``peer_data_movement``
     - ``"auto"`` (→ tasks)
     - ``"auto"`` degrades to the engine that can run; forcing ``"transport"`` raises the engine's
       own ``NotImplementedError`` before any work is submitted.
   * - Small shuffles, no preference
     - ``"auto"``
     - Either engine finishes quickly; the default picks for you.


Budgets and knobs
-----------------

``salt`` (skew mitigation, folded into the pinned route hash) is common to both operations and
engines. On joins, ``on`` / ``how`` / ``broadcast`` / ``mem_budget_bytes`` are common as above.
The transport-only knobs, and what each one actually bounds:

.. list-table::
   :header-rows: 1
   :widths: 26 74

   * - Knob
     - What it bounds
   * - ``n_tasks``
     - The producer-task count T (default ``min(n_workers, n_src)``).
   * - ``holder_budget_bytes``
     - Producer-plane retention: a producer's per-dest wires live in the worker plugin's block
       store under this cap; overflow **really spills** to the worker's local disk
       (``holder_spill_count`` / ``peak_holder_bytes`` in the witness).
   * - ``fetch_budget_bytes`` / ``disk_budget_bytes``
     - The **reader-plane accounting replay** — see the honesty note below.
   * - ``pull_timeout_s``
     - The per-holder block-pull ceiling. A legitimately large batch should *widen* this rather
       than burn the restart budget on spurious timeouts.
   * - ``epoch_restarts_allowed``
     - How many whole-run epoch restarts a failure may trigger before the run surfaces an
       attributed ``StageError`` (facade default: 1).

.. note::

   **Honesty about the reader budgets.** ``fetch_budget_bytes`` / ``disk_budget_bytes`` bound —
   and are witnessed by — a driver-side *replay* of the reference engine's reader accounting over
   block sizes, which is how the transport engine's witness counters stay exactly equal to the
   local engine's. They do **not** throttle a live reader on a worker: the real per-dest gather
   pulls its fragments, holds one dest resident, concatenates, and returns. The producer-plane
   ``holder_budget_bytes`` *is* a real runtime bound with real spill. Details:
   :ref:`design-budget-honesty`.


Failure semantics
-----------------

**Ordinary worker exceptions** round-trip intact on every path: a picklable
``graphed.debug.StageError`` re-raises in the driver still pointing at the user's analysis line —
never an opaque scheduler string.

**Hard worker death** (segfault, OOM, preemption) surfaces as ``distributed.KilledWorker`` after
``distributed.scheduler.allowed-failures`` deaths; the engine recognises it via the backend's
``describe_failure`` and raises an **attributed** ``StageError`` naming the partition and the last
worker (with a caveat that blame can be unfair under co-located tasks).

The two shuffle engines diverge on recovery, and this is the fixed-vs-adaptive trade above:

* **Tasks engine** — recovery is dask's: a worker dying mid-shuffle loses its producer's blocks,
  the scheduler recomputes that producer from its inputs, and the result is bit-for-bit unchanged.
  Per-task ``retries`` (via a ``SubmitRunner``) map to dask's native resubmit.
* **Transport engine** — failure means **whole-run restart under a fresh epoch**: an exhausted
  peer delivery (``TransportDeliveryError``), a timed-out block pull (``PullTimeoutError``), a
  peer reduction finishing with no captured root, or a ``KilledWorker`` each restart the run under
  a new epoch nonce, re-reading the surviving worker set so the restart pins onto survivors, up to
  ``epoch_restarts_allowed``. Exhausting that budget raises an attributed ``StageError`` naming
  the victim — never a raw ``KilledWorker``, and never a hang. Stale workers from a previous epoch
  are refused by the plugin's epoch guard.


Monitoring
----------

Pass a passive ``graphed.core.execution.Monitor`` to ``dask_runner`` and the run streams the M37
``TaskEvent`` lifecycle (``submitted`` driver-side; ``started`` / ``finished`` / ``errored``
worker-side) over dask's structured-event channel, on a per-run namespaced topic. Executed::

    class ListMonitor:
        """Passive observer: collects TaskEvents; must never raise or block."""

        def __init__(self):
            self.events = []

        def on_task(self, event):
            self.events.append((str(event.phase), event.key, event.worker))

        def on_profile(self, worker, payload):
            pass

        def on_combine(self, leaves_done):
            pass

        def worker_profiler_factory(self):
            return None

    monitor = ListMonitor()
    with dask_runner(client, monitor=monitor) as runner:
        result = runner.run(plan)             # result.value identical with or without it
    print(sorted({phase for phase, _k, _w in monitor.events}))
    # ['finished', 'started', 'submitted']

Monitoring is **passive by contract**: emission is off the data path and swallow-on-error, so the
reduced value is byte-identical whether the monitor is attached, detached, or actively raising.
Adapter authors get the same tap through ``SubmitBackend.subscribe_events(topic, handler)``.

.. note::

   **Documented limitation:** the transport engine's peer-reduction path
   (``transport_run_plan``) accepts ``monitor=`` for signature parity but does **not** emit
   peer-mode telemetry over the transport yet (``emit=False``) — a Phase-2 follow-up
   (:ref:`design-m44-limitations`).


Deploying on batch clusters
---------------------------

The backend consumes a **ready** ``Client``; cluster construction is out-of-band user code, so
any launcher that yields a ``distributed.Client`` works unchanged. The sketches below are
**illustrative — they require a batch cluster and are not executed in CI**; the tested guidance
behind them (lifetimes, ``allowed-failures``, ``local_directory``, version pinning) lives in
:ref:`design-deployment`.

SLURM via `dask-jobqueue <https://jobqueue.dask.org>`_ (illustrative)::

    from dask_jobqueue import SLURMCluster
    from distributed import Client
    from graphed_executors.dask_backend import dask_runner

    cluster = SLURMCluster(
        cores=8, memory="16GB", walltime="01:00:00", interface="ib0",
        local_directory="/scratch/$USER",          # node-local, never a network mount
        worker_extra_args=["--lifetime", "55m", "--lifetime-stagger", "4m"],
    )
    cluster.scale(jobs=20)                         # jobs, not workers
    with Client(cluster) as client, dask_runner(client) as runner:
        result = runner.run(plan)

Fermilab LPC HTCondor via `lpcjobqueue <https://github.com/CoffeaTeam/lpcjobqueue>`_
(illustrative)::

    from distributed import Client
    from lpcjobqueue import LPCCondorCluster
    from graphed_executors.dask_backend import dask_runner

    cluster = LPCCondorCluster(ship_env=True)      # ships your venv into the singularity image
    cluster.scale(50)
    with Client(cluster) as client, dask_runner(client) as runner:
        result = runner.run(plan)

``graphed-executors`` takes no dependency on either package — these are patterns, not APIs. On
preemption-prone queues: set ``--lifetime`` strictly below the walltime so workers self-drain and
migrate their keys instead of dying hard, raise ``allowed-failures`` to 5–10, and for long
shuffles keep the lifetime comfortably above a single producer task's runtime so an eviction does
not force repeated recomputation.


Known limitations
-----------------

* **Per-task resources are advisory-dropped.** ``DaskBackend.submit`` accepts ``resources=`` but
  does not forward it — dask treats it as a *hard* constraint, so an unsatisfiable request would
  pin the task in no-worker state forever. Enforcement on a resource-provisioned cluster is a
  future opt-in (an ``enforce_resources``-style knob); today, shape the cluster uniformly instead.
* **The transport engine is hostile to adaptive down-scaling.** Strict worker pins mean a departed
  owner costs a whole-run epoch restart; pass ``shuffle_method="tasks"`` on elastic clusters.
* **No free-threaded (3.14t) support** — upstream ``distributed`` has no free-threaded build; the
  local executors keep theirs.
* **Peer-mode transport telemetry is not wired** (``emit=False``), per the monitoring note above.
* **Checkpoint/resume is out of scope on dask**: ``run_resumable`` / ``run_shuffle_resumable`` are
  self-driving loops over a *local* content-addressed store, not ``Executor`` consumers — a
  distributed store data plane is Phase 2.
