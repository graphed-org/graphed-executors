"""The direct HTCondor backend, behind the ``[htcondor]`` optional extra.

:class:`HTCondorBackend` launches pilot jobs (:class:`CondorPilots` through the ``htcondor2`` bindings, or
:class:`LocalPilots` as local subprocesses) that pull pickled tasks from a driver-side task server.
Importing this package imports no bindings: ``CondorPilots.start`` does, on first use.
"""

from __future__ import annotations

from .launch import CondorPilots, LocalPilots, PilotLauncher
from .sites import SITES, SiteProfile

__all__ = [
    "SITES",
    "CondorPilots",
    "LocalPilots",
    "PilotLauncher",
    "SiteProfile",
]
