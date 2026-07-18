graphed-executors
==================

Reference single-machine executors for ``graphed`` (milestone M7): a thread pool and a process pool
that run a ``graphed_core.Plan`` to one reduced result via a deterministic, straggler-tolerant tree
reduction, with ``open_once`` file-locality, stopping conditions, adaptive reshaping, and intact
remote ``StageError`` surfacing (plan A.3 #8).

Start with :doc:`design` for the engineering walkthrough.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   design
   api
   improvements

Indices
-------

* :ref:`genindex`
* :ref:`modindex`
