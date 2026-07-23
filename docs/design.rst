How graphed-executors works
============================

``graphed-executors`` is the reference executor: it takes a ``graphed.core.Plan`` — process
each partition, combine the partials, start from empty — and runs it on one machine with a
thread pool or a process pool, producing one reduced result. "Reference" does not mean toy:
this is the executor the integration suites run real analyses through (thousands of tiny
tasks, deliberate stragglers, worker crashes), and its semantics — determinism under any
completion order, straggler tolerance, errors that survive the process boundary — are the
contract any future distributed executor must match.

.. contents::
   :local:
   :depth: 2


The Plan contract
-----------------

An executor consumes, and never interprets, four things::

    Plan(process = f(partition, resources) -> R,    # one partition's work
         combine = f(R, R) -> R,                    # associative merge
         empty   = f() -> R,                        # the identity
         tasks   = (Task(key, partition), ...))     # the fixed partition set

``process``/``combine``/``empty`` must be picklable for the process pool (module-level
functions, ``functools.partial`` of them, or frozen dataclasses — the conventions every
graphed writer/aggregator follows). ``resources.open_once(uri, opener)`` gives workers
file-handle reuse: thread-local sets for the thread pool, a per-process global installed by the
pool initializer for the process pool. An optional ``next_tasks`` hook switches the driver into
adaptive mode (below).

A minimal, runnable plan::

    import numpy as np
    from graphed.core import Partition, Plan, Task
    from graphed_executors.local import ProcessExecutor

    def count(partition, resources):          # module-level: picklable
        return np.asarray([partition.entry_stop - partition.entry_start])

    def add(a, b):  return a + b
    def zero():     return np.zeros(1, dtype=int)

    if __name__ == "__main__":                # spawned workers re-import __main__
        parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
        plan  = Plan(process=count, combine=add, empty=zero,
                     tasks=tuple(Task(i, p) for i, p in enumerate(parts)))

        ProcessExecutor(max_workers=4).run(plan).value     # -> array([700])


The fixed combine tree: deterministic *and* straggler-tolerant
--------------------------------------------------------------

The heart of the package is ``plan_tree`` + ``tree_reduce``, and the design resolves a tension
worth spelling out.

*Naively*, you either combine results in completion order (fast, but floating-point results
then depend on which worker finished first — non-deterministic), or you wait for all leaves
and reduce in index order (deterministic, but one straggler stalls everything).

The fixed tree does neither. ``plan_tree(n)`` builds a binary combine-tree **over leaf
indices** — pairing (0,1), (2,3), … level by level — *before* anything runs. ``tree_reduce``
then consumes leaf results in **whatever order they complete** and fires each combine the
moment both of its inputs exist. Consequences:

* **Determinism**: the grouping is a pure function of the leaf count, so the result is
  bit-for-bit identical regardless of completion order, worker count, or executor class. (For
  float-summing combines this is what makes "deterministic per configuration" a theorem rather
  than a hope; integer-counting combines are exact under any tree at all.)
* **Straggler tolerance**: a slow partition blocks only the ``log n`` combines on its own
  root-path; every other subtree reduces to completion meanwhile. There is no barrier. The
  frozen suite pins this with a deliberately slow leaf and a probe asserting that combines
  keep firing while it sleeps.

By default combines run on the driver thread as results arrive — fine when partials are small.
``pooled_combines=True`` schedules the combines onto the *same worker pool* as the leaves
(same fixed pairing, so results are unchanged), for workloads whose partials are heavy enough
that a serial driver-side merge becomes the bottleneck — large histograms over many
partitions, concatenated path lists, and the like.

The two pools
-------------

``ThreadExecutor`` and ``ProcessExecutor`` share one driver; they differ only in the
``concurrent.futures`` pool and the resource plumbing. The process pool uses the **spawn**
context deliberately: forked CPython processes inherit lock and allocator state that bites
exactly when you scale, and spawn is the semantics every platform shares. The cost is an
import-heavy worker startup, which leads to:

**Persistent pools.** By default each ``run()`` spawns a fresh pool — the right default for
isolation, and the pinned historical behavior. But a notebook running eight small plans, or a
benchmark sweep running hundreds, pays that import-heavy spawn per plan and can end up *slower
parallel than sequential*. ``ProcessExecutor(max_workers=4, persistent=True)`` keeps one pool
across ``run()`` calls (worker state demonstrably survives between runs — that is the test),
released by ``close()`` or context-manager exit, with lazy respawn afterwards::

    with ProcessExecutor(max_workers=4, persistent=True) as ex:
        for plan in plans:           # one spawn, amortized over every plan
            results.append(ex.run(plan).value)

Errors cross the boundary intact
--------------------------------

A worker exception propagates to the driver as the exception it was. In particular a
``graphed_debug.StageError`` — which is picklable by design — re-raises in the driver carrying
the failing op, the user's source frames, and the failing partition. The executor adds nothing
and strips nothing; "remote errors are opaque strings" is the legacy failure this stack was
built against, and the integration suite pins the round trip.

Adaptive plans
--------------

A plan with ``next_tasks`` runs as a **running fold** instead of a fixed tree: the driver
folds results as they complete and periodically consults ``next_tasks(ExecContext)`` — which
sees elapsed time, completed counts, and errors — to obtain more partitions or a
``StopReason``. This is the seam for timing-aware partitioning (grow chunk sizes as observed
throughput stabilizes) without changing the executor; the fixed tree remains the path for
known partition sets, where determinism matters most.


Live observability: the monitor seam (M37)
------------------------------------------

Every executor accepts an optional ``monitor=`` — a passive ``graphed.core.execution.Monitor`` that
*watches* a run. It is the seam a live dashboard plugs into (see ``graphed-debug``'s ``Dashboard``),
but the executor knows nothing about rendering or transport: it only emits a small, picklable
``TaskEvent`` vocabulary.

The lifecycle of one task is three events: the driver emits ``SUBMITTED`` when it hands the task to
the pool; the worker emits ``STARTED`` before running it and exactly one of ``FINISHED`` / ``ERRORED``
after. Where those worker events go differs by pool, and this is the interesting part:

* **Thread pool** — workers share the driver's address space, so they call the monitor directly.
* **Process pool** — workers cannot reach the driver's monitor object, so they push events onto a
  bounded ``multiprocessing.Manager().Queue()``; a **driver-side collector daemon thread** drains it
  and replays them into the monitor. (The driver still emits ``SUBMITTED`` locally.) A per-worker
  statistical profiler, if one is supplied via the monitor's ``worker_profiler_factory``, rides the
  same queue.

The non-negotiable property is **passivity**: emission is best-effort and *drop-on-full*. If the
monitor is slow or its queue is full, events are dropped — never buffered into back-pressure that
would change task timing (and thus the adaptive ``next_tasks`` path) or stall a worker. A monitor that
raises is swallowed. The upshot, pinned by the suite: a run's ``ExecResult.value`` and combine count
are byte-identical whether or not a monitor (even a profiling one) is attached. Observability here is
strictly a side channel, never part of the computation.


Inter-worker comms: peer reduction + work-stealing (M38)
--------------------------------------------------------

By default (``comms="ipc"``) the reduction runs **across the workers, off the driver**. The seam is
:class:`graphed.core.execution.WorkerTransport` — an addressable, non-blocking, best-effort message
channel — with two backends: **IPC** (``QueueTransport`` over ``multiprocessing.SimpleQueue`` inboxes,
one per address, no ``Manager`` server in the data path) for a single machine, and **HTTP** (loopback
``http.server`` + a discovery handshake; ``HttpTransport``) as the path a real distributed scheduler
reuses. Determinism is *not* the transport's job; it is the reduction protocol's.

The IPC path has **two worker pools, and you pick which by choosing the executor class** — there is no
silent runtime switch. :class:`~graphed_executors.local.ProcessPoolExecutor` (the default; original M7
behaviour) uses a full-registry pool: every worker *inherits the whole registry* (O(N²) fds — fine
while N is well under the per-process fd limit, and the fast common path). :class:`~graphed_executors.local.PinnedPoolExecutor`
uses a ``PinnedProcessPool`` of **identity-pinned** workers that each inherit ONLY their inbox + the
O(log N) outboxes of their *overlay* peers (reduction targets + a symmetric **hypercube lifeline**
graph + driver, ``worker_outbox_addresses``), so the registry is O(N log N), not O(N²). Both bound
work-stealing to the lifelines, and both produce **bit-for-bit identical** results — only the
communication footprint differs.

**Which to use.** Default to ``ProcessPoolExecutor``: it is the simplest and is fastest up to roughly
the fd limit. Reach for ``PinnedPoolExecutor`` on large many-core machines (>~128 cores, or any low
``RLIMIT_NOFILE``), where the full registry's O(N²) descriptors would exhaust the limit. So you are not
surprised, ``ProcessPoolExecutor`` *warns* (via the advisory predicate ``_exceeds_fd_budget``) and
points you at ``PinnedPoolExecutor`` when its worker count would strain the budget — it warns rather
than switching, so the pool in use is always the one named at the call site. ``ProcessExecutor`` remains
as a **deprecated alias** for ``ProcessPoolExecutor``. (A *dynamic* cluster — workers joining/dying —
needs a lazy-connect transport + multi-hop routing over this same overlay: the Phase-2 distributed
runtime, which reuses ``worker_outbox_addresses``.)

* **Peer reduction** (``_peer.py``). Each worker owns a contiguous **leaf range** and reduces it with
  the lazy index tree (``_reduce.LazyReducer`` — the same fixed ``plan_tree``, computed by index
  arithmetic, frontier-bounded so N can be huge with no O(N) pre-pass). Partials that straddle a range
  boundary are handed **worker→worker** by ownership (a segment-tree merge: node ``(level,pos)`` is
  owned by the worker holding its leftmost leaf; an odd node is shipped to its parent's owner). Every
  node keeps its **global** ``(level,pos)`` identity, so distributing the *combines* never changes the
  *grouping* — the result is **bit-for-bit identical to the old driver-hub path even for
  non-associative float histograms**. The driver only collects the root (a ``done`` broadcast
  terminates); a worker failure is detected promptly and re-raised intact (the M7 obligation). On the
  real ADL benchmark this is within noise of the hub — the driver is no longer the combine bottleneck.
* **Work-stealing**. An idle worker steals **one** leaf from a busy peer's far end
  (Blumofe–Leiserson/Cilk — *not* steal-half, which under many idle thieves drains a victim
  geometrically and over-concentrates work). Stealing redistributes only the ``process`` work — the
  leaf's **owner still reduces it** (the thief ships the partial back), so the tree and the result are
  unchanged. An idle delay + exponential backoff make it free on balanced loads (no spurious steals)
  while rebalancing a genuine straggler.
* **Parity with the hub.** Peer emits the full monitor lifecycle (SUBMITTED/STARTED/FINISHED/ERRORED +
  the combine count) and runs the off-thread profiler, so the live dashboard — flamegraph included —
  works under peer. ``comms=None`` selects the legacy driver-hub path (still used for
  ``pooled_combines`` and the broadcast-cache tests); peer **refuses** ``pooled_combines`` loudly
  rather than silently degrading to hub.


.. _design-dask-backend:

How the dask backend works
--------------------------

The dask backend (``graphed_executors.dask_backend``, behind the optional ``[dask]`` extra) runs the
**same** ``Plan`` on a ``dask.distributed`` cluster used as a *dumb scheduler* — no dask collections,
no ``HighLevelGraph``, no dask-awkward. It ``client.submit``\ s opaque graphed callables with explicit
keys and future-dependency edges, and inherits determinism, straggler tolerance, and intact errors
from the local design rather than re-deriving them. It sits behind a small **common protocol** so a
second library (parsl) can be adapted later without touching the engine.

.. _design-submit-seam:

The common seam: ``SubmitBackend`` + ``SubmitRunner``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``graphed_executors.submit`` defines the portable intersection every submit-style library shares:

* ``SubmitBackend`` — ``submit(fn, *args, key=…) -> future`` (future-valued args arrive **resolved**),
  ``broadcast(payload, token=…)`` (a payload placed once, referenced by many tasks),
  ``subscribe_events(topic, handler)`` (a driver-side monitor tap), ``cancel``, ``close``, and a
  ``n_workers()``. Each backend advertises a per-**instance** ``SubmitCapabilities`` (seven flags:
  peer data movement, scatter/broadcast, worker pinning, per-task retries, per-task resources,
  running-cancel, worker file cache). The engine may use a capability only behind an
  ``if backend.capabilities.X`` check, and both Plan paths are correct with **every flag false** —
  that is the parsl floor. A flag states what the underlying *library* supports, not what the MVP
  adapter wires: ``DaskBackend`` reports ``per_task_resources``/``pin_to_worker`` true (real dask
  features) but its ``submit`` does **not** auto-forward a Plan's per-task ``resources`` — dask treats
  ``resources=`` as a hard constraint, so an unsatisfiable request would stall the task forever;
  enforcement on a resource-provisioned cluster is a future deployment-time opt-in.
* ``SubmitRunner`` — one ``graphed.core.Executor`` over any ``SubmitBackend``. ``DaskBackend`` is the
  first real backend; a stdlib ``ThreadBackend`` is the conformance second one, so the seam is
  witnessed by *executing two backends against one frozen suite*, not by an import lint. Its flag set
  differs from dask's on five of seven flags, proving the engine is correct across capability
  variation.

``plan_tree`` as a future graph
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fixed path mirrors the local reduction topology **exactly**: leaves are ``process`` tasks;
combines are submitted **up front** with future dependencies following the same ``plan_tree`` shape
``(out, a, b)``, ``a < b``; the driver waits on the single root future. The grouping is fixed by
*leaf index*, never by arrival time or worker count — so the result is bit-for-bit equal to
``SequentialRunner`` and invariant to the worker count, inherited rather than re-proven. dask resolves
each combine's future args on whichever worker runs it and fetches inputs **peer-to-peer**, so the
combines run off the driver (the role ``PeerReducer`` plays locally); a slow leaf blocks only its own
path to the root. Intermediate futures are released as their parent combine consumes them — an
``O(log N)`` live frontier the scheduler enforces, the bound ``LazyReducer`` gives locally. This is
*not* coffea's arrival-batched reduction, whose grouping varies with future-submission order (the
source of its known reduce-time race); ``plan_tree`` keys every combine by index.

Broadcast-once, keys, and determinism
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pickled ``process`` (and ``combine``) ships **once** as an identity future
(``client.submit(_identity, payload)`` — the coffea pattern, chosen over ``client.scatter`` whose
worker-discovery timeout breaks on scale-to-zero clusters, and over closure-per-task which
re-serializes the payload for every task). A worker-side token cache deserializes it **once per
worker** however many tasks that worker runs. Every submit is ``pure=False`` with an explicit,
namespaced, per-run-nonced key ``graphed-<plan-fp>-<nonce>-leaf|combine-<i>``: ``pure=False`` stops
dask deduping distinct-byte-range I/O reads by content token; the ``graphed-`` prefix makes a
user-string collision with a key impossible (dask/dask#9969); the nonce makes a second ``run()`` on
one client re-execute instead of returning the first run's cached futures. A construction knob
``replicate_broadcast=True`` spreads the payload's replicas (a mitigation for the coffea#1490
single-worker-pinning suspicion; a frozen witness fails CI if leaves ever pin to one worker).

The worker seam: ``RunContext`` + ``WorkerEnv`` + the plugin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The engine's task functions are backend-neutral, yet on the worker a task needs the monitor topic,
``open_once`` resources, and an event transport. Per-run state travels as an ordinary pickled
``RunContext`` first argument; per-worker capability arrives via a ``WorkerEnv`` the **backend**
installs by wrapping every submitted function in its own module-level shim
(``dask_backend/_shim.py``). A ``GraphedWorkerPlugin`` (``name="graphed-worker"``, ``idempotent=True``,
so re-registration is a no-op and *late-joining* elastic/jobqueue workers get ``setup`` too) holds one
``LocalResources`` per worker, so a uri opens **once per dask worker** across its tasks — exactly the
local ``open_once`` locality. All dask-touching code lives under ``dask_backend/`` and is imported only
lazily (at ``DaskBackend`` construction) or by-reference (worker unpickle); ``submit/`` names dask
nowhere, so it installs and runs with the base package.

Errors and worker death
~~~~~~~~~~~~~~~~~~~~~~~~~

An ordinary worker exception round-trips intact: dask re-raises it driver-side and
``StageError.__reduce__`` reconstructs it byte-for-byte, so ``format_traceback`` still points at the
user's analysis line (the M6 obligation) with **zero** wrapping. A hard worker death — segfault, OOM,
preemption — surfaces as a ``distributed.KilledWorker`` after ``distributed.scheduler.allowed-failures``
deaths; the engine recognises it (via the backend's ``describe_failure``, staying dask-import-free)
and raises an **attributed** ``StageError`` naming the partition and the last worker, with a message
noting the blame can be unfair under co-located tasks — never an opaque scheduler string. Per-task
retries map to dask's native ``retries=`` (resubmit on another worker); there is no second
graphed-level retry loop (coffea zeroes its own under dask to avoid double-retrying — we follow).

Live observability
~~~~~~~~~~~~~~~~~~~~

Monitoring rides dask's structured-event channel: ``Worker.log_event(topic, msg)`` on the worker,
``Client.subscribe_topic(topic, handler)`` on the driver. Each run mints a namespaced
``graphed-monitor-<nonce>`` topic, subscribed for the run and released in a ``finally`` after trailing
events drain. ``SUBMITTED`` is emitted driver-side at submit; ``STARTED``/``FINISHED``/``ERRORED``
worker-side as msgpack-safe scalar dicts. Emission is off the data path and swallow-on-error, so the
reduced value is **byte-identical** whether a monitor is attached, detached, or actively raising (M37
passivity) — telemetry never inflates the payload or breaks the run.

.. _design-shuffle-graph:

Shuffle and joins on dask
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``dask_run_repartition`` / ``dask_run_join`` (``graphed_executors.dask_backend.shuffle``) express the
M39–M41 exchange/join as a **native dask future graph** rather than running the local two-phase engine
over a dask-backed store. The local engine computes every ``partition``/join kernel *in the driver* (its
cluster duck-type only farms out block *storage*), so a dask-backed store would distribute storage but
not compute — a driver CPU+NIC bottleneck. Instead the graph is::

    T = min(n_workers, n_src) producer futures   _dask_map_write   (worker-side coalesce + to_wire)
    T·P pick futures                             _dask_pick        (runs on the holder; one block each)
    P gather / gather-join futures               _dask_gather / _dask_gather_join

reusing the local per-task kernels verbatim (``_assign``, ``_coalesce_task``, ``_join_with_budget``,
``broadcast_join_choice``) — no kernel is re-implemented. Under dask the future graph **is**
completeness: a gather *depends on* its producers, the scheduler tracks who holds what, and workers
fetch deps peer-to-peer. A backend without ``peer_data_movement`` (a parsl-HTEX-class head-node router)
is refused with ``NotImplementedError`` before any work is submitted — routing every block through the
driver is the pathology this design rejects.

**Cross-engine determinism contract.** ``_assign`` gives producer tasks contiguous ascending src ranges
and gathers concatenate in ascending-task order, so a dest's rows assemble in ascending-src order
regardless of worker count. Therefore ``dest_block_hashes`` (sha256 of each gathered block's wire bytes,
computed worker-side) are **byte-identical across two dask runs AND equal to the local**
``run_repartition``/``run_join`` **on identical inputs** — content-addressing across two independent
engines pins the route, the merge order, and the wire serialization at once. The broadcast-vs-shuffle
choice is keyed on ``parts`` (a plan-stable N), never the live worker count, so the same logical join
makes the same choice on a 1- or 3-worker pool.

**Retired mechanisms.** The M38/M39 announcement/manifest/steal machinery does not exist under dask —
the scheduler already provides what it was built to construct:

.. list-table::
   :header-rows: 1
   :widths: 42 58

   * - M38/M39 mechanism
     - Why retired under dask
   * - Announcements / manifests / reliable push
     - Gather completeness is the future graph, not droppable hints.
   * - Work-stealing
     - ``distributed`` owns stealing (``work-stealing: True``); a second layer would fight it.
   * - ``WorkerTransport`` / ``QueueTransport`` / ``HttpTransport``
     - dask comms move future data peer-to-peer.
   * - Node Stores + disk budgets
     - Block bytes live in worker memory with dask's own spill-to-``local_directory``.

Their witness counters are correspondingly absent from ``DaskShuffleWitness`` (which carries only
``n_producer_tasks``, ``blocks_per_producer_task``, ``peak_writer_buffer_bytes``, ``broadcast_chosen``,
the ``_join_with_budget`` spill counters, and ``producer_sites``/``gather_sites``). A worker holding
stage-1 blocks that dies mid-shuffle is handled by the graph itself: the scheduler recomputes the lost
producer from its inputs and the result is bit-for-bit unchanged.

**Preemption interplay with long shuffles.** The jobqueue preemption guidance below applies with one
addition: a producer future recomputes from scratch if its worker dies, so on preemption-prone queues
set ``--lifetime`` comfortably **above a single producer task's runtime** (and raise
``allowed-failures``). Otherwise a worker evicted mid-shuffle forces its producer — and every gather
depending on it — to recompute, turning a long shuffle into repeated work. This is deployment guidance,
not a correctness gate: the result is bit-for-bit regardless.

.. _design-transport-engine:

Worker-transport engine (m44)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The shuffle/join graph above lets the *scheduler* move blocks (a gather depends on its producers). The
worker-transport engine (``graphed_executors.dask_backend.transport`` +
``transport_peer``/``transport_shuffle``) is the alternative that implements graphed's M38
``WorkerTransport`` **atop dask's own worker-to-worker comm layer** — the P2P-shuffle pattern: a
``distributed.WorkerPlugin`` registers custom ops in ``worker.handlers`` and workers dial each other over
``worker.rpc`` (the pooled ``ConnectionPool``). It hosts the M38 peer reduction and the M39–M41
shuffle/join engine **on the workers**, moving bulk bytes worker→worker over a ``graphed_block_pull``
handler — never through the driver — with an **O(T+P) scheduler graph** (``T`` pinned
``_transport_map_task`` producers + ``P`` pinned ``_transport_gather_task``/``_transport_gather_join``
consumers + an O(k) control tail): no ``T·P`` pick tier and no per-row task creation.

Three design points carry the parity:

- **Reader-plane budgets are a driver-side replay.** A per-*dest* gather cannot reproduce the reference
  ``_stage2_gather``'s per-*node* shared-fetch-buffer counters (``fetch_spill_count`` /
  ``peak_fetch_bytes``), yet the frozen goldens pin exact equality with the local engine *and* pin the
  ``P`` gather-task count. Both hold because the reader/disk counters are computed by **replaying the
  imported** ``_stage2_gather`` over block *sizes* at the barrier (a size-only backend + a
  worker-address-keyed size-only cluster — ``_replay_reader_plane``), while the real bytes move
  worker→worker through the ``P`` gather tasks. The kernel is reused verbatim, so the accounting tracks
  the local engine bit-for-bit (``tests/extra/m44`` witnesses the replay-vs-real equality directly).
- **Holder plane is bounded (F12).** A producer's per-dest wires live in the plugin's block store with a
  ``holder_budget_bytes`` cap — a wire that pushes producer-local RAM over the cap spills to the worker's
  local disk (``holder_spill_count``/``peak_holder_bytes``), so the producer store is never an unmanaged
  memory pause/terminate trap. The block plane never routes bytes through the client.
- **Failure = whole-run restart under a fresh epoch (§1.5).** Every run mints an epoch nonce; the plugin
  refuses recv/pull for an unknown or purged epoch (the P2P ``run_id`` guard). The restart-worthy set is:
  a lost peer send that exhausts its bounded at-least-once retry (``TransportDeliveryError``); a block-pull
  that times out against a slow/dead holder (``PullTimeoutError`` — the pull ceiling is the caller-settable
  ``pull_timeout_s``, so a legitimately large batch widens it before the restart budget burns); a peer
  reduction that completes with **no captured root** (also a ``TransportDeliveryError`` — never a silent
  ``plan.empty()``); and a worker death (``KilledWorker``). Any of these restarts the whole run under a new nonce (re-reading the surviving worker set
  so the restart pins onto survivors) up to ``epoch_restarts_allowed``, then an attributed ``StageError``
  naming the victim — never a raw ``KilledWorker`` and never a hang (a hard-timeout-guarded gate proves
  it). Because setup can run during a nanny **restart**, the plugin's handler-seam canary is a *bounded*
  self-RPC that falls back to a direct handler-dispatch check when the worker's server is not yet
  accepting — a self-RPC that blocked startup would hang every subsequent ``client.run`` against the
  restarted worker.

.. _design-budget-honesty:

Honesty about what the budgets and witnesses mean
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Three things are deliberately *not* what a first read might assume, and the frozen goldens are honest
only because of them:

- **The reader-plane budgets drive the driver-side accounting replay ONLY.** ``fetch_budget_bytes`` /
  ``disk_budget_bytes`` bound (and are witnessed by) ``_replay_reader_plane``, which re-runs the imported
  ``_stage2_gather`` accounting over block *sizes* at the barrier. They do **not** cap a live reader on a
  worker: the real per-*dest* ``_transport_gather_task`` pulls its fragments, holds *one* dest resident,
  concats, and returns — there is **no runtime reader-side fetch buffer and no runtime reader spill**. The
  producer/holder plane *is* really bounded and really spills (``holder_budget_bytes`` → ``holder_spill_count``);
  the reader budgets are an accounting model of the reference engine, not a runtime backpressure knob here.
- **``DaskWorkerTransport.send`` retries INLINE, blocking the actor thread — unlike the Protocol.** The M38
  ``WorkerTransport.send`` contract is non-blocking; this dask implementation instead does a bounded
  at-least-once retry *synchronously* inside ``send`` (``SEND_RETRIES`` × ``SEND_ATTEMPT_TIMEOUT_S`` ≈ up to
  ~25 s of wall time, plus backoff, blocking the seceded peer actor's task thread) before raising
  ``TransportDeliveryError``. This is the deliberate trade: the un-editable ``_peer.py`` consumers ignore
  ``send``'s bool, so at-least-once delivery has to be *inside* ``send`` or a dropped reduction message is
  lost silently. It is correct but not the Protocol's latency profile.
- **The bulk-fetch witnesses map to real RPCs only up to coalescing.** ``bulk_fetch_count`` /
  ``cross_node_fetches`` come from the replay, which coalesces a dest's fragments per node; the real gather
  coalesces per *holder* and issues one ``graphed_block_pull`` RPC per ``(dest, holder)``. So the witnessed
  counts can **undercount** the real per-``(dest, holder)`` RPC total. The frozen bound (``≤ P·k``) stays
  honest because ``k`` holders × ``P`` dests is the ceiling either way; the witness is a lower-bound-safe
  model of the real bulk traffic, not a per-RPC ledger.

.. _design-m44-limitations:

Known limitations (m44)
^^^^^^^^^^^^^^^^^^^^^^^^^

Recorded rather than fixed (all are phase-barriered — none can corrupt a result, and the reviewer raised
them as nits, not blockers):

- A gather that owns a fragment on its *own* worker still pulls it over a loopback ``graphed_block_pull``
  RPC instead of reading the local store directly (a small self-pull cost, never a correctness issue).
- The holder store lock is held across the spill *write* (disk IO under the lock); the serving handler
  contends for it only during a spill, and a run is phase-barriered (all producers finish before any
  gather), so it cannot stall a live pull today.
- The per-epoch counter probe reads block-store sizes on the IO loop; on a very large store this is a small
  loop-thread cost. Witness counters are per-worker dicts updated without a lock — safe only because the
  phase barrier means no two task threads touch the same epoch's counters concurrently.
- **Peer-mode M37 telemetry is not wired (``emit=False``).** ``transport_run_plan`` accepts ``monitor`` for
  signature parity but does not emit peer-reduction telemetry over the transport — a Phase-2 follow-up.
- A **zero-worker** cluster (or one where every root is withheld) no longer stalls: the R2 no-captured-root
  raise turns it into an attributed ``StageError`` / restart, never a silent identity value or a hang.

Determinism is unchanged from the shuffle graph: the same ascending-src merge makes
``dest_block_hashes`` byte-identical to the local engine and across runs, and the peer reduction's fixed
``(level, pos)`` ownership makes the reduced value bit-for-bit the ``SequentialRunner`` baseline. dask
stays an optional extra: ``transport_peer``/``transport_shuffle`` import on the dask-free matrix (the
``distributed``-touching code is deferred into function bodies).

.. _design-facade:

Choosing an engine — the shuffle facade (m45)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two dask shuffle engines is one knob too many for most callers, so ``graphed_executors.dask_backend.api``
is the **front door**: ``run_repartition`` / ``run_join`` take a single ``shuffle_method`` and dispatch
over both. It is a thin dispatcher — zero engine logic — imported dask-free at module load like the
engines it fronts.

``shuffle_method`` takes three values:

- ``"transport"`` — always the m44 worker-transport engine (the m44 pin/peer gate raises its own
  ``NotImplementedError`` if the backend can't support it; the facade adds nothing).
- ``"tasks"`` — always the m43 as-tasks future-graph engine.
- ``"auto"`` (default) — **capability-static**: the transport engine iff the backend advertises BOTH
  ``pin_to_worker`` and ``peer_data_movement``, else the as-tasks engine. Resolution is a pure function of
  ``dbackend.capabilities`` — no size heuristics, and it does **not** inspect cluster elasticity.

**Fixed vs adaptive clusters.** ``"auto"`` keys only on capabilities, not on whether workers come and go.
The transport engine strict-pins tasks to specific workers, so on an *adaptive/elastic* cluster (workers
joining and dying) prefer ``shuffle_method="tasks"`` explicitly — the as-tasks future graph lets the dask
scheduler recompute lost stage-1 blocks, whereas a pinned owner that leaves forces a whole-run epoch
restart. On a fixed cluster, ``"auto"`` (transport) minimises interpreter touchpoints.

**Knob honesty.** ``salt`` (and ``on`` / ``how`` / ``broadcast`` / ``mem_budget_bytes`` on joins) are
common and forward on both paths — ``mem_budget_bytes`` bounds the join working set on *either* engine.
The transport-only knobs (``n_tasks`` / ``fetch_budget_bytes`` / ``disk_budget_bytes`` /
``holder_budget_bytes`` / ``pull_timeout_s`` / ``epoch_restarts_allowed`` on repartition;
``holder_budget_bytes`` / ``pull_timeout_s`` / ``epoch_restarts_allowed`` on join) raise a ``ValueError``
naming the knob if you set one while resolution lands on ``"tasks"`` — never a silent drop. Validation runs
*after* resolution, so ``"auto"`` degrading to tasks with an explicit transport knob raises too. Callers
needing ``monitor=`` / ``retries=`` use the still-public ``dask_run_*`` / ``transport_run_*`` entry points.

**Result shape + observability.** The facade returns the engine's own result unchanged — a union
``ShuffleResult | TransportShuffleResult`` whose portable contract is the common triple
``dest_block_hashes`` / ``value`` / ``witness``. The engine-specific extra is also a resolution witness: a
result carrying ``.transport`` proves the transport engine ran, ``.partitions`` the as-tasks engine — handy
for "why was ``auto`` slow on my adaptive cluster?".

What is *not* distributed here (checkpoint scope)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dask backend executes ``execution.Plan``\ s and shuffle/join stages (see above).
Checkpoint/resume is **out of scope**: ``run_resumable``/``run_shuffle_resumable`` are self-driving
loops over a *local* content-addressed store, not ``Executor`` consumers, so
``run_resumable(executor=dask_runner(...))`` does not exist. Journaled, resumable execution on dask
needs a distributed content-addressed store and belongs with the Phase-2 store data plane.

.. _design-deployment:

Deployment recipes
~~~~~~~~~~~~~~~~~~~~

The public seam is a **ready** ``distributed.Client`` — cluster construction (``LocalCluster``,
``SLURMCluster``, ``LPCCondorCluster``) is out-of-band user code. Produce a client, hand it to
``dask_runner``::

    import numpy as np
    from distributed import Client, LocalCluster
    from graphed.core import Partition, Plan, Task
    from graphed_executors.dask_backend import dask_runner

    def count(partition, resources):          # module-level: picklable + spawn-safe
        return np.asarray([partition.entry_stop - partition.entry_start])
    def add(a, b):  return a + b
    def zero():     return np.zeros(1, dtype=int)

    if __name__ == "__main__":                # processes=True nannies re-import __main__
        parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
        plan  = Plan(process=count, combine=add, empty=zero,
                     tasks=tuple(Task(i, p) for i, p in enumerate(parts)))
        with LocalCluster(n_workers=2, processes=True, threads_per_worker=1,
                          dashboard_address=":0") as cluster, Client(cluster) as client:
            with dask_runner(client) as runner:
                runner.run(plan).value        # -> array([700])

``processes=True`` + one thread per worker suits GIL-holding compiled HEP stages; the random dashboard
port avoids ``EADDRINUSE``. Batch clusters follow the same "produce a Client" shape (recipes are
**site-dependent** — syntax shown, not run in CI):

* **SLURM (dask-jobqueue)** — ``SLURMCluster(cores=…, memory=…, walltime=…, interface="ib0",
  worker_extra_args=["--lifetime", "55m", "--lifetime-stagger", "4m"])`` then
  ``cluster.scale(jobs=N)`` (jobs ≠ workers — ``scale`` converts by ``worker_processes``).
  ``local_directory`` must be node-local scratch, not a network mount. Size ``scale`` to the Plan and
  use ``adapt()`` only to absorb the tail.
* **LPC HTCondor (lpcjobqueue)** — ``Client(LPCCondorCluster(ship_env=…, image=…))``; mind the CVMFS
  singularity image, the shipped venv, the worker port band, and the graceful-then-``condor_rm``
  teardown. graphed-executors takes **no** lpcjobqueue (or coffea) dependency — this is a pattern.
* **Preemption** — set ``--lifetime`` strictly below the queue walltime (workers self-drain via
  ``close_gracefully``, migrating in-memory keys to peers instead of a hard kill) with a
  ``--lifetime-stagger``, and raise ``distributed.scheduler.allowed-failures`` to 5–10 on
  preemption-prone queues so innocent tasks on evicted workers are not blamed. With draining, in-flight
  leaves reroute and the fixed tree is unaffected (grouping is by index, not worker). Keep the same
  ``dask``/``distributed``/``graphed`` versions on client and workers (``client.get_versions(check=True)``).
* **Per-task resources are not enforced yet.** Even on a resource-provisioned cluster (e.g. GPU
  workers advertising ``resources={"GPU": 1}``), a Plan's per-task ``resources`` hints are dropped —
  this adapter does not forward them to ``client.submit``, because an unsatisfiable request would
  stall the task in no-worker state. Provisioning them is a future opt-in; today, pin resource-bound
  work by shaping the cluster (all workers uniform) rather than by per-task hints.

**Free-threaded 3.14t is not supported for the dask backend** — upstream ``distributed`` has no
free-threaded build (its ``py314t`` CI is commented out, "WIP - tests don't pass yet"). The
``test-dask`` CI job pins GIL builds (py 3.12 + 3.14); the local executors keep their 3.14t story.


.. _design-parsl-backend:

How the parsl backend works
---------------------------

The parsl backend (:mod:`graphed_executors.parsl_backend`, behind the optional ``[parsl]`` extra)
runs the same ``Plan`` and shuffle contracts over a `parsl <https://parsl-project.org>`_ executor,
via the *same* :class:`~graphed_executors.submit.protocol.SubmitBackend` seam the dask backend uses.

**Direct executor submit — no DFK.** :class:`~graphed_executors.parsl_backend.backend.ParslBackend`
takes a **started** ``HighThroughputExecutor`` (or ``ThreadPoolExecutor``) and calls its public
``executor.submit(func, resource_specification, *args)`` directly. It deliberately does *not* go
through ``parsl.load`` / the DataFlowKernel: the DFK unwraps future args on the head node anyway, so
it would add a global singleton and config-global retries/caching (which fight the determinism gate)
for zero capability gain. :func:`~graphed_executors.parsl_backend.launch.start_htex` encodes the
three integration moves a DFK normally performs (set ``run_dir``; create ``provider.script_dir``;
``scale_out_facade(init_blocks)`` after ``start()``) and pins the fixed-blocks posture
(``init_blocks == min_blocks == max_blocks``) — it is both the canonical recipe and the drift canary
for parsl's weekly CalVer.

**The capability floor, per instance.** Capabilities are derived from the executor *type*, honestly:
``ParslBackend(HighThroughputExecutor)`` is the **all-seven-False "parsl floor"** — future args are
resolved on the submit host (so ``peer_data_movement`` is False *even when* a peer-transport plane
exists; reporting it True would falsely open the m43 ``_require_peer`` gate to head-node routing),
broadcast reships per task, and there is no pinning / per-task retries / per-task resources /
running-cancel / worker file cache. ``ParslBackend(ThreadPoolExecutor)`` is the
:class:`~graphed_executors.submit.threadpool.ThreadBackend` shape (``peer_data_movement=True`` alone —
same-process shared memory). Any other executor type is refused with a ``TypeError`` naming both
verified classes — per-instance honesty means not vouching for an executor the plan has not verified.

**The worker seam.** ``ParslBackend.submit`` resolves any future args driver-side (``.result()``),
then wraps the task fn in a module-level shim (:mod:`graphed_executors.parsl_backend._shim`) — after
any spy seam, so a submitted-fn-name witness records the raw fn. On the worker the shim installs a
``WorkerEnv`` whose ``resources`` is a process-global ``LocalResources`` (so ``open_once`` file
locality holds across the many tasks a reused worker runs), whose ``worker`` identity is recomputed
in-task from parsl's own ``PARSL_WORKER_POOL_ID``/``PARSL_WORKER_RANK`` env vars, and whose ``emit``
**buffers** events (parsl has no worker-to-driver event channel) to ride back with the task result
and be dispatched driver-side at completion granularity. The module imports parsl nowhere at load
(the ``_lazy`` accessor + the parsl-free shim), so importing the package leaves parsl out of
``sys.modules``.

**The relay shuffle engine.** HTEX resolves future args on the submit host, so the m43 as-tasks
engine's peer gate (:func:`~graphed_executors.common.tasks_engine._require_peer`) correctly refuses
it — a peer shuffle needs worker-to-worker data movement HTEX cannot do. The parsl backend instead
ships the **relay engine** (:mod:`graphed_executors.common.relay_engine`), the honest head-node
workflow: ``T = min(n_workers, n_src)`` producer maps submit ``_dask_map_write`` (the m43 body
unchanged); the driver resolves the map payloads at a barrier (bulk data arrives at the submit host
once), regroups each destination by calling ``_dask_pick`` **locally** (a dict lookup — zero pick
tasks), and submits ``P`` gathers with the picked wire fragments as concrete args (data leaves the
submit host once). The scheduler sees ``T + P`` tasks and zero picks — the optimal head-node shape
(the m43 ``T·P`` pick tier would re-ship each producer's whole payload per destination). The task
bodies, kernels, and result assembly are the *same objects* the dask shim gates, so
``dest_block_hashes`` are byte-identical to the local and dask engines — only the pick tier moves
driver-side. The whole-barrier driver residency (≈ total shuffle bytes) *is* head-node routing, made
per-run observable by the ``RelayShuffleWitness`` (``head_node_routed=True`` + ``driver_relay_bytes``).
A parsl ``ThreadPoolExecutor`` reports ``peer_data_movement=True``, so the m43 engine (moved verbatim
to :mod:`graphed_executors.common.tasks_engine`, keeping its ``_dask_*`` names so the frozen Counter
gates stay green) runs over *it* unchanged.

**The peer-exchange transport engine.** The relay engine is honest but head-node-bound. For
fixed-size pools where worker-to-worker dialability holds, the parsl backend also ships a **true
peer-exchange engine** (opt-in via ``shuffle_method="transport"`` on an HTEX instance) that never
routes bulk bytes through the driver. parsl exposes no worker-to-worker in-memory transfer — the
DataFlowKernel resolves futures on the head node, and TaskVine's peer transfer is file-cache-only —
so graphed builds its own overlay: ``k`` persistent peer tasks (one per worker slot; they
gang-schedule by slot saturation, so no ``pin_to_worker`` is needed), each of which **mints its own**
``EscalatingHttpTransport`` **endpoint in-task**, announces a ``hello`` to a driver-hosted rendezvous
endpoint, and **blocks on a barrier until all k hellos arrive** before the driver broadcasts the
assembled ``addr → host:port`` registry. No peer holds the address book — so no send can race a
missing inbox — before every inbox exists (the pre-created-inbox obligation, subsumed by
construction). Blocks then travel peer↔peer over the plane's ``/pull`` route, coalesced to one
request per holder (the ``≤ k·k`` incast bound, witnessed by ``pull_requests_served``) and evicted
after serve; the driver's endpoint sees only control traffic. The peer bodies are the *same* M38
reduction and M39–M41 shuffle/join actors the local engine runs, hosted unchanged (the shuffle leg
is a deferred import), so ``dest_block_hashes`` stay byte-identical to the local, relay, and dask
engines: the engine you pick can never change a result.

**"The cluster decides, not the broker."** Worker-to-worker reachability is a property parsl never
guarantees (NAT, firewalls, multi-node overlays), so a **runtime reachability probe** runs at
rendezvous time, before any data moves. ``on_unreachable="error"`` (the default) raises an attributed
``StageError`` naming the unreachable pair; ``on_unreachable="fallback"`` transparently re-runs the
relay engine on the same inputs and records ``witness.fallback_reason`` (observable, never silent).
``probe_peer_reachability`` exposes the same probe as an optional pre-flight. Recovery is **whole-run
epoch restart**: an exhausted escalating send, a timed-out pull, a lost peer, or a reduction that
captures no root restarts the run under a fresh epoch nonce, re-reading the surviving worker set
(``_resolve_k`` degrades a shrunken cluster to ``min(workers, live n_workers())`` so the restart pins
onto survivors) up to ``epoch_restarts_allowed`` (default 1); an exhausted budget surfaces an
attributed ``StageError`` naming the death — never a raw parsl exception, never a hang. The reduction
counterpart ``parsl_run_plan`` runs a whole ``Plan`` as this k-peer reduction, bounding the driver's
root wait with ``root_timeout_s`` and **deriving** the peer idle deadline as ``root_timeout_s + slack``
so it always stays ≥ the root wait. Critically, ``peer_transport`` is a parsl_backend-private
attribute (``True`` for HTEX, ``False`` for a ``ThreadPoolExecutor``), **not** a capability flag:
advertising ``peer_data_movement=True`` would falsely open the m43 ``_require_peer`` gate to head-node
routing, so the transport engine is reached explicitly and never by ``"auto"`` (no parsl vector
carries both ``pin_to_worker`` and ``peer_data_movement``).

**What stays Phase 2.** TaskVine's file-cache byte plane (``worker_file_cache``, native
worker-to-worker file transfers), live M37 dashboard parity over parsl (worker events arrive at
completion granularity today), and TLS on the graphed HTTP plane are the next milestone — see
:doc:`improvements`.


Phase 2 (deliberately not built)
--------------------------------

* **Distributed executors** (TaskVine / HTCondor / Slurm) — the ``Plan`` contract, the
  ``WorkerTransport`` seam, and the ``SubmitBackend`` protocol are built so they can be written
  against later. The dask backend is the first real distributed backend and the parsl backend
  (above) the second; both ship a peer-exchange transport shuffle engine (parsl additionally ships
  the head-node relay for elastic or non-dialable pools).
* NUMA-aware placement.
* **Adaptive chunk-size policies** shipped as library code (the ``next_tasks`` hook exists;
  policies beyond tests are user-land for now).
* **Per-query resource hints** (memory-bound combinatoric stages want fewer concurrent
  workers — observed empirically on trijet workloads; the executor currently treats all plans
  alike).

See :doc:`improvements` for the live tracked list.
