graphed-executors
==================

Reference single-machine executors for ``graphed`` (milestone M7): a thread pool and a process pool
that run a ``graphed_core.Plan`` to one reduced result via a deterministic, straggler-tolerant tree
reduction, with ``open_once`` file-locality, stopping conditions, adaptive reshaping, and intact
remote ``StageError`` surfacing (plan A.3 #8).

Beyond one machine, the ``[dask]`` extra adds a ``dask.distributed`` backend behind the common
:class:`~graphed_executors.submit.protocol.SubmitBackend` seam, plus distributed repartition/join engines
with a one-knob facade — see :doc:`dask` for the how-to. The ``[parsl]`` extra adds a `parsl
<https://parsl-project.org>`_ backend over direct executor submit, with the head-node **relay**
shuffle engine — see :doc:`parsl`.

Start with :doc:`design` for the engineering walkthrough.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   design
   dask
   parsl
   api
   improvements

Indices
-------

* :ref:`genindex`
* :ref:`modindex`
