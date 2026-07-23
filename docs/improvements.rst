Future improvements
===================

Catalogued, not silently dropped (plan A.7 / Part F).

- **Distributed executors** (TaskVine / HTCondor / Slurm) are Phase 2; this repo is single-machine
  only. The execution contract and the **WorkerTransport seam (M38)** are built so a real distributed
  adapter — reusing the HTTP backend's socket transport — can be written against them.
- **In-worker tree combines** and **work-stealing** — **done (M38):** peer reduction runs the combines
  across the workers off the driver (the default ``comms="ipc"``); an idle worker steals one leaf from
  a busy peer (steal-one). Remaining follow-ups: (a) **HTTP + ThreadExecutor profiling under
  free-threaded CPython 3.14t** — excluded from the witness under the GIL (the transport + sampler
  threads contend and the off-thread sampler can starve; with no GIL they run in parallel, so revisit
  when 3.14t is the norm); (b) **per-combine ``on_combine`` emission** for peer (the driver reports the
  count today); (c) a **steal-half / bulk-transfer knob** for fine-grained workloads (steal-one is the
  coarse-partition default).
- **Precision-based stopping** (statistical convergence) is contracted but not yet implemented.
- **parsl peer-transport shuffle engine** — the m46 parsl backend ships the head-node **relay**
  engine (bulk data crosses the submit host); a true peer-exchange transport over HTEX worker
  processes (self-minted HTTP endpoints, a driver rendezvous barrier, and a **runtime reachability
  probe** with ``on_unreachable="error"|"fallback"`` — "the cluster decides, not the broker") plus
  the unified ``shuffle_method`` facade are the next milestone. Until then, HTEX shuffles route
  through the driver by design.
- **parsl TaskVine / WorkQueue instances** — the capability model reserves ``worker_file_cache`` for
  a file-cache peer byte plane (native worker-to-worker transfers); ``ParslBackend`` refuses those
  executor types today rather than guess a vector.
- **Live M37 dashboard parity over parsl** — the parsl worker shim delivers monitor events at
  completion granularity (buffered, dispatched on result unwrap); a live back-channel (e.g. the
  HTEX radio sender) is Phase 2. **TLS on the head-node relay/transport HTTP plane** is likewise
  deferred (parsl's own ``encrypted=True`` does not cover the graphed plane).
