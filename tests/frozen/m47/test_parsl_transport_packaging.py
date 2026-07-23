"""m47 packaging/coverage-allocation pins (MAIN matrix) — the §1.6 pre-authorized m47 deltas as
file content, so a drift is caught mechanically, not by eyeball (plan §1.6 item 2, §2 m47 IT5;
the m46 `test_parsl_packaging_pins` precedent; a coverage-config change OUTSIDE the pre-authorized
allocation is indistinguishable from gate tampering).

Pins: `.coveragerc-parsl` [run] source gains ``common.http_plane`` (the m47 parsl-only module —
its reject/timeout/evict branches gate HERE) while ``tasks_engine``/``transport_run``/``facade``/
``transport_tasks`` stay OUT (one gate per module — they gate on the dask job whose m42..m45
suites execute them through the shims); `.coveragerc-dask` [run] source gains
``common.transport_run`` + ``common.facade`` + ``common.transport_tasks`` while ``relay_engine``/
``http_plane`` stay banned (no dask-suite caller — they would sit at 0%); ``fail_under = 90`` and
``branch = true`` UNTOUCHED in both; the `test-parsl` pytest step runs ``tests/frozen/m47``, keeps
the bare ``--cov`` (activation — without it the modifiers are a silent no-op, empirically
verified), and carries NO path-valued ``--cov=`` (which would OVERRIDE the config sources)."""

from __future__ import annotations

import configparser
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def _sources(rc_name: str) -> list[str]:
    cp = configparser.ConfigParser()
    assert cp.read(REPO_ROOT / rc_name), f"{rc_name} missing"
    return [line.strip() for line in cp.get("run", "source").splitlines() if line.strip()]


def _report_gate(rc_name: str) -> tuple[str, str]:
    cp = configparser.ConfigParser()
    cp.read(REPO_ROOT / rc_name)
    return cp.get("report", "fail_under"), cp.get("run", "branch")


def test_coveragerc_parsl_gains_http_plane_and_keeps_one_gate_per_module() -> None:
    sources = _sources(".coveragerc-parsl")
    assert "graphed_executors.parsl_backend" in sources
    assert "graphed_executors.common.relay_engine" in sources
    assert "graphed_executors.common.http_plane" in sources, (
        "common.http_plane must GATE on the parsl job (the §1.6 m47 allocation) — it has no dask "
        "caller; its reject/timeout/evict branches are exercised only by this suite's injectors"
    )
    for banned in (
        "graphed_executors.common.tasks_engine",
        "graphed_executors.common.transport_run",
        "graphed_executors.common.facade",
        "graphed_executors.common.transport_tasks",
    ):
        assert banned not in sources, (
            f"{banned} must NOT be double-gated here — it gates on the dask job (one gate per "
            "module: a module gated twice can go red on the job that only partially exercises it)"
        )
    fail_under, branch = _report_gate(".coveragerc-parsl")
    assert fail_under == "90" and branch == "true", "the coverage gate must stay 90/branch (§B.3)"


def test_coveragerc_dask_gains_the_moved_transport_modules() -> None:
    sources = _sources(".coveragerc-dask")
    for required in (
        "graphed_executors.submit",
        "graphed_executors.dask_backend",
        "graphed_executors.common.tasks_engine",
        "graphed_executors.common.transport_run",
        "graphed_executors.common.facade",
        "graphed_executors.common.transport_tasks",
    ):
        assert required in sources, (
            f"{required} missing from .coveragerc-dask [run] source — after the §1.6 m47 move the "
            "m44/m45 suites execute the moved bodies in common/*, and this job is their ONE gate; "
            "without the source line the moved engine is gated by ZERO jobs while staying green"
        )
    for banned in ("graphed_executors.common.relay_engine", "graphed_executors.common.http_plane"):
        assert banned not in sources, f"{banned} has no dask caller and would sit at 0% here"
    fail_under, branch = _report_gate(".coveragerc-dask")
    assert fail_under == "90" and branch == "true", "the coverage gate must stay 90/branch (§B.3)"


def test_ci_parsl_step_runs_m47_with_a_bare_cov_and_no_path_valued_cov() -> None:
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()
    m = re.search(r"pytest tests/frozen/m46[^\n]*(?:\n\s+[^\n]+)*", ci)
    assert m, "the test-parsl pytest step is gone from ci.yml"
    step = m.group(0)
    assert "tests/frozen/m47" in step, (
        "the test-parsl pytest step must run tests/frozen/m47 alongside m46 (m47 IT5) — without "
        "it the m47 suite is frozen but never gated"
    )
    assert re.search(r"--cov(\s|\\|$)", step), (
        "the bare --cov is MANDATORY to activate coverage (modifiers alone are a silent no-op and "
        "fail_under never runs — pytest-cov 7.1.0, empirically verified)"
    )
    assert "--cov=" not in step, (
        "a path-valued --cov= OVERRIDES the .coveragerc-parsl [run] source (no union) — the "
        "per-module allocation must stay authoritative (tests-B-cov-1/2)"
    )
    assert "--cov-config=.coveragerc-parsl" in step
