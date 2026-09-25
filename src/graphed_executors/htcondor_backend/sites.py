"""Per-site HTCondor submit profiles and the schedd choice, as data: nothing here imports the bindings.

A :class:`SiteProfile` holds the submit keys a site needs (templates over ``{image}``, ``{uid}``,
``{user}`` and ``{home}``), whether its schedd needs spooled sandboxes, whether pilots get the driver's
venv shipped as ``env.tgz``, the directory tree its schedd can read, how to find a schedd, and the
driver-side ports its execute nodes can reach.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# The attributes schedd_weight reads, projected in the collector query.
WEIGHT_ATTRS = ("Name", "RecentDaemonCoreDutyCycle", "ShadowsRunning", "MaxJobsRunning", "TotalIdleJobs")

_SPOOLING_INPUT = 16  # HoldReasonCode of a spooled job while its input sandbox uploads: not a real hold


@dataclass(frozen=True)
class SiteProfile:
    """``schedd_query`` is ``(param naming the collectors, constraint)``; ``None`` means the user's own
    ``SCHEDD_HOST``. ``sandbox_root`` (a template over ``{user}``) is the only tree the site's schedd
    reads, so ``log_dir`` must lie under it. ``driver_ports`` is the inclusive range the task server
    binds on the submit host."""

    name: str
    submit: Mapping[str, str]
    spool: bool
    ship_env: bool
    sandbox_root: str | None
    schedd_query: tuple[str, str] | None
    driver_ports: tuple[int, int] = (10000, 10100)


SITES: Mapping[str, SiteProfile] = {
    "lpc": SiteProfile(
        name="lpc",
        submit={
            "use_x509userproxy": "true",
            # without an explicit path the LPC schedd looks in /tmp and fails with "unable to read proxy"
            "x509userproxy": "{home}/x509up_u{uid}",
            "+DesiredOS": '"EL9"',
            "MY.SingularityImage": '"{image}"',
        },
        spool=True,
        ship_env=True,
        sandbox_root="/uscmst1b_scratch/lpc1/3DayLifetime/{user}",
        schedd_query=(
            "FERMIHTC_REMOTE_POOL",
            'FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC" && MaxJobsRunning!=0',
        ),
        driver_ports=(10000, 10100),
    ),
    "lxplus": SiteProfile(
        name="lxplus",
        submit={
            "MY.SingularityImage": '"{image}"',
            "MY.SendCredential": "True",
            "+JobFlavour": '"longlunch"',
        },
        spool=True,
        ship_env=True,
        sandbox_root=None,
        schedd_query=None,
        # batch nodes are refused on 10000 at a login node; CERN opens 8786 there for a dask scheduler
        driver_ports=(8786, 8786),
    ),
    "generic": SiteProfile(
        name="generic",
        submit={},
        spool=False,
        ship_env=False,
        sandbox_root=None,
        schedd_query=None,
        driver_ports=(10000, 10100),
    ),
}


def schedd_weight(ad: Mapping[str, Any]) -> float:
    """The LPC ``condor_submit`` wrapper's load score; lower is better."""
    return float(
        0.7 * ad["RecentDaemonCoreDutyCycle"] * 100
        + 0.2 * ad["ShadowsRunning"] / ad["MaxJobsRunning"] * 100
        + 0.1 * ad["TotalIdleJobs"]
    )


def choose_schedd(ads: list[Mapping[str, Any]]) -> str:
    return str(min(ads, key=schedd_weight)["Name"])


def counts_as_alive(ad: Mapping[str, Any]) -> bool:
    """Idle, running, or held only while its spooled input uploads."""
    status = ad.get("JobStatus")
    return status in (1, 2) or (status == 5 and ad.get("HoldReasonCode") == _SPOOLING_INPUT)
