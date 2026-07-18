"""Back-compat alias: ``graphed-exec-local`` was renamed ``graphed-executors``.

The code now lives in :mod:`graphed_executors.local`. This shim maps ``graphed_exec_local`` and
every submodule onto the *same* module objects (not re-exports) so the frozen acceptance suites keep
passing unmodified: they do ``import graphed_exec_local._peer`` and monkeypatch
``graphed_exec_local.executors._exceeds_fd_budget`` by string path, so module *identity* is required
— a re-export module would make the monkeypatch patch the wrong object. New code should import from
:mod:`graphed_executors.local`.
"""

from __future__ import annotations

import sys

from graphed_executors import local as _local
from graphed_executors.local import _peer, _pinned_pool, _reduce, _transport, executors, shuffle

# Alias the package and each submodule to graphed_executors.local's objects, preserving identity.
for _mod in (_local, _peer, _pinned_pool, _reduce, _transport, executors, shuffle):
    sys.modules[_mod.__name__.replace("graphed_executors.local", __name__)] = _mod
