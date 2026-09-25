Example: an H→γγ analysis, translated
=====================================

``examples/hgg/analysis.py`` is a coffea processor rewritten for graphed: ``inclusive_processor.py``,
a standalone distillation of HiggsDNA's H→γγ inclusive base processor. It covers the lumi mask, MET
filters and triggers, photon preselection, diphoton building, the detector-level fiducial cut, the
cleaned jet variables, and the particle-level truth and fiducial flags. For each chunk it writes one
flat parquet file of diphoton candidates and returns the chunk's event and weight counters.

The translation keeps the original's methods, names and cut values. Events are coffea NanoEvents in
graphed mode:

.. code-block:: python

   events = NanoEventsFactory.from_root(
       {uri: "Events"},
       schemaclass=NanoAODSchema,
       mode="graphed",
       metadata={"dataset": dataset, "filename": uri.split("/")[-1]},
   ).events()

The analysis reads the same files the original reads: HiggsDNA's ``infer_nano_version``, plus the
metaconditions, golden JSON and jet-ID JSONs, found through ``importlib.resources`` inside the
installed ``higgs_dna``. ``plan(uri, ranges=..., dataset=..., year=..., out=...)`` builds one task
per ``(start, stop)`` range. Any runner in this package runs it.


What changed, and why
---------------------

Nothing is read when the processor runs, so wherever the original needs a value on the spot, the
translation has to spell that step another way. These are all eleven such places:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Original
     - Translation
   * - ``int(ak.num(...))``, ``len(events)``
     - The counters (``nTot``, ``nPos``, ``nNeg``, ``genWeightSum``, and the two weight sums in the
       metadata) are plan outputs, computed per chunk. ``HggProcess`` converts each one to the
       original's Python type (``int``, ``float``, or ``str`` of the float32 sum).
   * - ``.to_numpy()``
     - The same counters. The old-NanoAOD supercluster-η projection uses the deferred PV columns
       directly.
   * - ``array["field"] = value``
     - ``gak.with_field(array, value, "field")``.
   * - ``ak.Array({...})`` for the flat output record
     - ``gak.zip`` of the flat columns.
   * - ``ak.to_arrow_table`` and ``pq.write_table`` inside ``process``
     - ``HggProcess`` wraps the plan's per-chunk ``process``. It takes the materialized record and
       runs the original's ``dump_to_parquet`` steps: ``extensionarray=False``, sorted columns,
       and the key-value metadata merged in.
   * - ``events.attrs["@events_factory"]._partition_key`` for the part name
     - ``HggProcess`` builds the name from the task's partition:
       ``<file stem>_Events_<start>-<stop>.parquet``. The original has the file's UUID where this
       has the file's stem.
   * - ``LumiMask(path)(events.run, events.luminosityBlock)``
     - ``lumi_mask(run, lumi, year)``, an External recorded through
       ``graphed.preserve.externals.record_external``. Its payload is the golden JSON's bytes and
       its content hash is their SHA-256. Its evaluator is coffea's ``LumiMask`` on each chunk's
       run and lumi values. The plan therefore carries the certification it applied.
   * - ``correctionlib.CorrectionSet.evaluate`` for the jet ID
     - ``gak.apply_correction`` with a call template. The JSON is gunzipped first, because the
       correctionlib External parses JSON bytes.
   * - ``numpy.where``, ``numpy.copy``
     - ``gak.where``. The copy is dropped, since deferred arrays are never modified in place.
   * - ``if ak.num(diphotons.pt, axis=0) > 0:`` guards around jet cleaning
     - Deleted. The cleaning ops are defined on an empty chunk and give the same empty masks.
   * - ``ak.with_name(..., "PtEtaPhiMCandidate")``, which needs ``charge``
     - Unchanged. The records are zipped with ``charge`` already, and graphed NanoEvents carry
       coffea's candidate behaviors into each task.

The plan's value is ``{part name: {dataset: counters}}``, and two parts combine as a dictionary
union. ``totals(value)`` is ``coffea.processor.accumulate`` over the parts, which is how coffea's
``Runner`` combines the original's returns.


How the translation is checked
------------------------------

The frozen suite ``tests/frozen/m69a`` uses the original script as the oracle. It imports the
file, kept byte-identical and pinned by SHA-256, and calls its ``process()`` on NanoEvents built
the way the script's ``__main__`` builds them, one chunk per range. It then compares each part the
translation writes against the original's part for the same range. ``compare_part`` covers:

* the counters, by ``==`` and by each value's Python type;
* the arrow schema: names, order, types and nullability;
* each column's validity bitmap;
* each column's valid values, bit for bit, with floats compared through their same-width
  unsigned view so NaN positions count;
* the key-value metadata.

Every mutation test of the comparator asserts that its own leg fired, not just that some
difference was found.

The inputs are two 200-event NanoAOD v15 fixtures, one MC and one data. Each is built by a script
checked in beside it. The data fixture has certified and uncertified lumi sections. The builder puts
the diphoton signal only in events 0–59, so the (100, 200) chunk selects nothing: its part has zero
rows and the original's schema. The suite runs both fixtures on ``SequentialRunner`` and on
``SubmitRunner(ThreadBackend(2))``. It also records every events object coffea hands out while
the plan is built and run, and requires each to be graphed NanoEvents.

``examples/hgg/validate_real.py --parts 2`` runs the same comparison on real 2024 NanoAOD over
xrootd: the first file of ``GluGluHto2G_M-125_amcatnlo_2024`` and the first of ``DataC_2024``. It
prints each file's entry count, every part's ``compare_part`` result and both sets of counters. It
exits 1 if any part differs.

The CI job ``test-hgg`` (ubuntu, Python 3.12) installs the coffea fork, ``uproot`` from the commit
the fork needs, and ``higgs_dna`` with ``--no-deps``. It then runs
``GRAPHED_HGG_REQUIRED=1 pytest tests/frozen/m69a``. The main matrix has no coffea, so the files
skip there.
