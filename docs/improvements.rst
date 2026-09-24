Current limitations
===================

What does not work yet, and what to do instead.

- **No TaskVine or WorkQueue support, and no direct HTCondor/Slurm submission.**
  ``ParslBackend`` accepts parsl's ``HighThroughputExecutor`` and ``ThreadPoolExecutor`` and
  refuses any other executor type with a ``TypeError``. To run on a batch system, keep the
  graphed side unchanged and let the pool layer do the submission: parsl's providers
  (``SlurmProvider``, ``CondorProvider``) under an HTEX pool, or
  `dask-jobqueue <https://jobqueue.dask.org/>`__ in front of the dask backend. :doc:`dask`
  carries worked recipes for the dask side; on parsl the provider goes in your own parsl
  config and you hand the started executor to ``parsl_runner`` exactly as in :doc:`parsl`.

- **Stopping on statistical convergence is not implemented.**
  ``graphed.core.execution.StopCondition`` ends a run on an event target
  (``target_events``), a wall-clock budget (``max_wall_s``), or an error budget
  (``max_errors``) — not on the precision of the result. Set an event target and check the
  uncertainty yourself between runs.

- **The runners here do not resume a killed run.** A dask or parsl run that dies starts over.
  ``graphed.checkpoint.run_resumable`` does resume — against a local directory, or against a
  store at a URL (an ``s3://`` bucket, a shared ``file://`` path) that any machine can reach —
  but it processes the partitions one at a time in a single process. When a restart would cost
  too much, split the work into smaller plans and combine their results, or save the analysis as
  a ``graphed.core.DurablePlan`` and drive it with ``run_resumable`` where surviving a crash
  matters more than wall time.

- **No TLS on graphed's own exchange plane on parsl.** ``shuffle_method="transport"`` on a
  parsl pool moves blocks over HTTP endpoints graphed mints in-task, and parsl's
  ``encrypted=True`` protects parsl's own channels, not those. Use it on a trusted cluster
  network, or stay on the default route, which relays through your submit node over parsl's
  own channels. This does not arise on dask, where blocks ride ``distributed``'s worker
  connections and inherit whatever the cluster's comms are configured with.

- **Dashboard events over parsl arrive at task completion by default.** parsl has no
  worker-to-driver event stream, so a task's ``started``/``finished`` events are buffered on the
  worker and delivered together when its result comes back. Every event still arrives and the
  result is unaffected. To watch tasks while they run, give the runner
  ``graphed.debug.NetworkMonitor(url, per_worker=True)``: each worker then sends its own events
  straight to the dashboard as they happen, provided the workers can reach the dashboard's
  address. On either backend the worker-to-worker exchange engine reports the combine count from
  the driver rather than emitting one event per combine.

- **No free-threaded CPython (3.14t) on the dask path.** ``distributed`` declares support up to
  3.14 and nothing about free-threading. The local executors run on 3.14t; on a dask cluster, use
  standard CPython.

- **Pause, resume and cancel do not reach the worker-to-worker routes on a cluster.** The local
  executors and the dask and parsl runners (``dask_runner``, ``parsl_runner``) all honour a run
  control. The cluster plan routes that merge worker to worker (``transport_run_plan``,
  ``parsl_run_plan``) and the repartition and join engines take no control and emit no task
  events, so the dashboard can neither see nor steer them; stop such a run by interrupting it.
