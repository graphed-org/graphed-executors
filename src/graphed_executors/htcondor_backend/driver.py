"""The driverless entry: ``python -m graphed_executors.htcondor_backend.driver <dir>``, run inside ONE job.

``<dir>`` (the job's scratch dir, its cwd) holds ``plan.pkl`` (a stdlib-pickled runtime ``Plan``) and
``run.json``. The driver runs the plan with :class:`HTCondorRunner` over :class:`LocalPilots` in its own
slot (``pilots="local"``) or over pilot jobs it submits itself (``pilots="condor"``, to the schedd named
by ``run.json["schedd_locate"]``), and on every exit writes ``result.pkl`` = ``(ok, ExecResult |
exception)`` and ``driver.log`` for the output transfer. Exit codes: 0 done; 3 the plan raised (a plan
error is deterministic, so the job's ``retry_until`` stops retrying it); 1 anything before or after the run.
"""

from __future__ import annotations

import json
import os
import pickle
import socket
import sys
import time
import traceback
from pathlib import Path
from typing import Any, TextIO

from .backend import HTCondorBackend, HTCondorRunner
from .launch import CondorPilots, LocalPilots
from .sites import SITES

PLAN_FILE = "plan.pkl"
RUN_FILE = "run.json"
RESULT_FILE = "result.pkl"
LOG_FILE = "driver.log"

EXIT_DONE, EXIT_FAILED, EXIT_PLAN_ERROR = 0, 1, 3


def machine_host() -> str:
    """``Machine`` from the ad file ``$_CONDOR_MACHINE_AD`` names: a container's own hostname is not the
    execute node's, and no bindings are needed to read it. This host's FQDN outside a job."""
    path = os.environ.get("_CONDOR_MACHINE_AD")
    if path and os.path.isfile(path):
        for line in Path(path).read_text().splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "Machine":
                return value.strip().strip('"')
    return socket.getfqdn()


def _runner(run: dict[str, Any], job: Path, log: TextIO) -> HTCondorRunner:
    profile = SITES[run["site"]]
    n = int(run["n_pilots"])
    if run["pilots"] == "condor":
        launcher: Any = CondorPilots(
            profile,
            image=run["image"],
            request_memory_mb=int(run["request_memory_mb"]),
            log_dir=run["log_dir"],
            user_modules=[job / name for name in run["user_modules"]],
            schedd_locate=tuple(run["schedd_locate"]),
        )
        host, ports = machine_host(), profile.worker_ports
    else:
        # the slot's own pilots dial loopback; any free port serves them
        launcher = LocalPilots(python=sys.executable, pythonpath=[job])
        host, ports = "127.0.0.1", profile.worker_ports or (0, 0)
    backend = HTCondorBackend(launcher, n, host=host, port_range=ports)
    pids = [p.pid for p in getattr(launcher, "_procs", ())]  # local pilots only
    print(f"{n} {run['pilots']} pilots on {backend._server.url} pids={pids}", file=log, flush=True)
    return HTCondorRunner(
        backend,
        min_pilots=int(run["min_pilots"]),
        retries=int(run["retries"]),
        max_in_flight=int(run["max_in_flight"]),
    )


def _result_blob(ok: bool, payload: object) -> bytes:
    try:
        return pickle.dumps((ok, payload))
    except Exception as exc:  # an unpicklable result or exception still reaches the submitter, as text
        return pickle.dumps(
            (False, RuntimeError(f"{type(payload).__name__} did not pickle ({exc}): {payload}"))
        )


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    job = Path(args[0] if args else ".").resolve()
    if str(job) not in sys.path:
        sys.path.insert(0, str(job))  # user_modules arrive in the scratch dir
    start = time.monotonic()
    result: object = None
    error: BaseException | None = None
    code = EXIT_FAILED
    with open(job / LOG_FILE, "a") as log:
        print(f"driver pid={os.getpid()} dir={job}", file=log, flush=True)
        try:
            run = json.loads((job / RUN_FILE).read_text())
            with open(job / PLAN_FILE, "rb") as f:
                plan = pickle.load(f)
            runner = _runner(run, job, log)
            try:
                live = runner.backend.wait_for_pilots(int(run["min_pilots"]))
                print(f"{live} pilots live after {time.monotonic() - start:.1f}s", file=log, flush=True)
                try:
                    result, code = runner.run(plan), EXIT_DONE
                except Exception as exc:
                    error, code = exc, EXIT_PLAN_ERROR
            finally:
                runner.close()
        except Exception as exc:
            error, code = exc, EXIT_FAILED
        blob = _result_blob(True, result) if error is None else _result_blob(False, error)
        if error is not None:
            traceback.print_exception(error, file=log)
        print(f"exit {code} after {time.monotonic() - start:.1f}s", file=log, flush=True)
    (job / RESULT_FILE).write_bytes(blob)
    return code


if __name__ == "__main__":
    sys.exit(main())
