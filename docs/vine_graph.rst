Running on TaskVine
===================

If your workers are `TaskVine <https://cctools.readthedocs.io/en/latest/taskvine/>`_ workers,
graphed runs your plan on them through vine_graph (``ndcctools.taskvine.vine_graph``). You create
the ``Executor``, you own it, and you hand it over. Workers read each other's partial results, and
merges run on workers. The Manager also receives each partial result as its task completes, so its
machine needs the disk for them (the ``vault-disk-limit`` setting).

This page is the how-to. For *why* the answer doesn't move when the worker count does, read
:doc:`design`.

Install
-------

vine_graph is on the ``vine-graph`` branch of
`a CCTools fork <https://github.com/JinZhou5042/cctools/tree/vine-graph>`__: no release or conda
package carries it yet, so this backend has no extra and you build CCTools from that branch. Build
it into the environment you run from; it installs ``vine_worker`` and the Python package together.
The workers need ``graphed-executors``, ``graphed`` and your analysis's dependencies as well. A
worker's library refuses to start if its Python version, cloudpickle or vine_graph code differs
from the manager's.

Run a plan
----------

.. code-block:: python

   from ndcctools.taskvine import vine_graph as vg
   from graphed_executors import vine_graph_runner

   with vg.Executor(9123, "my-analysis") as executor, vine_graph_runner(executor) as runner:
       result = runner.run(plan)

Start workers against the manager with ``vine_worker -M my-analysis``, or
``vine_worker <host> 9123``. Closing the runner leaves the ``Executor`` open, so several runs can
share it. ``process``, ``combine`` and the rest of the plan must import on the workers; a module
they lack goes to every worker through the Executor's ``library_files``.

Every plan shape runs here: fixed, adaptive, ``DurablePlanV2`` stages, a ``control``, and services.

When a task fails
-----------------

vine_graph retries what the infrastructure broke: a task whose worker went away runs again, up to
the Executor's ``max-retries`` setting (``vg.Executor(..., params={"max-retries": 5})``), and a
result lost with a worker is recomputed. **A function that raises is not retried.** The run fails on
the first attempt with the function's own exception, so a transient error such as a read timeout
needs a retry inside your ``process``. ``vine_graph_runner`` has no ``retries`` argument for that
reason; dask and parsl do.

A task whose function was killed by a signal (a segfault, or the kernel's OOM killer), whose workers
kept exiting, or that used up ``max-retries`` raises a ``StageError`` naming its partition. Its
message is vine_graph's own: vine_graph does not say which worker it was.

Monitors
--------

vine_graph has no channel for monitor events beside a task's value. So each leaf of a monitored run
gets one small extra task, which loads the leaf's result on a worker and returns its events alone.
Your session loads no partial result for them; the cost is one more task per leaf, and events
arrive when that task finishes.

A monitor with a ``worker_monitor_factory`` (the dashboard's ``per_worker=True``) gets its events the
same way, on the driver: each call is a fork that ends without running exit handlers, so a monitor
on a worker would lose what it had buffered.

What a task keeps
-----------------

vine_graph forks every call from a library process, so nothing a task leaves in its process reaches
the next one. ``open_once`` opens a file once per task rather than once per worker, and each task
deserializes the plan's ``process`` again. The bytes themselves do arrive once per worker, since
vine_graph stages a large argument as a cached file.
