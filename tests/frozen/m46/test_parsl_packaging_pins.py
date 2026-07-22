"""m46 packaging/CI content pins — the parts of §3's acceptance that are FILE CONTENT, asserted
as content (plan §1.6 items 1-3, §2 m46 IT4/IT5, §4; the r3 blockers tests-B-cov-1/2 made these
load-bearing). Runs in the MAIN matrix (no parsl needed — it reads files, not the environment).

Why these are frozen: the coverage-allocation edits are pre-authorized IN THE PLAN precisely so
no mid-iteration coverage-config change is ever needed (indistinguishable from gate tampering);
freezing the exact allocation makes any later drift mechanically visible. The two pytest-cov
facts were empirically verified twice (plan author + closure verifier, pytest-cov 7.1.0):
modifiers WITHOUT a bare ``--cov`` are a silent no-op (fail_under never runs), and a CLI
``--cov=path`` OVERRIDES the config ``[run] source`` (no union) — so the bare-`--cov` step shapes
below are the difference between gated and silently-ungated code.

Discriminates: a parsl extra with a drifted floor or a smuggled python_version marker, a main
matrix red-by-construction (missing omit) or silently unmeasured (`fail_under` lowered), a
`.coveragerc-parsl` that forgets relay_engine (or gate-shares tasks_engine with the dask job —
the one-gate-per-job allocation), a dask CI step whose path-valued ``--cov=`` flags silently
discard the moved engine from every gate, and a test-parsl job without pyarrow (measured: the
non-inner numpy join arms wire masked blocks over Arrow and would skip forever)."""

from __future__ import annotations

import configparser
import re
import tomllib

from parsl_harness import REPO_ROOT


def _pyproject() -> dict:
    with open(REPO_ROOT / "pyproject.toml", "rb") as f:
        return tomllib.load(f)


def _coveragerc(name: str) -> configparser.ConfigParser:
    path = REPO_ROOT / name
    assert path.is_file(), f"{name} is an m46 Implementation Target (plan §1.6 item 2) and is missing"
    parser = configparser.ConfigParser()
    parser.read(path)
    return parser


def _sources(parser: configparser.ConfigParser) -> set[str]:
    return {line.strip() for line in parser.get("run", "source").splitlines() if line.strip()}


def test_parsl_extra_is_the_dated_floor_with_no_marker() -> None:
    """`parsl = ["parsl>=2026.7.20"]` exactly (plan §4): the dated CalVer floor every claim was
    verified against, and deliberately NO python_version marker — the CI axis, not the metadata,
    is the tested-support fence."""
    extras = _pyproject()["project"]["optional-dependencies"]
    assert "parsl" in extras, "the [parsl] extra is missing from pyproject.toml (m46 IT4)"
    assert extras["parsl"] == ["parsl>=2026.7.20"], (
        f"the parsl extra must be exactly ['parsl>=2026.7.20'], got {extras['parsl']!r}"
    )


def test_main_coverage_omit_covers_common_and_parsl_backend() -> None:
    """The main matrix installs neither dask nor parsl, so the three optional packages would
    register ~0% and sink fail_under=90 on every OS — the established omit-because-gated-elsewhere
    pattern must now cover all three (plan §1.6 item 1; fail_under itself UNTOUCHED)."""
    data = _pyproject()
    omit = set(data["tool"]["coverage"]["run"]["omit"])
    for pattern in ("*/dask_backend/*", "*/common/*", "*/parsl_backend/*"):
        assert pattern in omit, f"[tool.coverage.run] omit must contain {pattern!r} (m46 IT4), got {omit}"
    assert data["tool"]["coverage"]["report"]["fail_under"] == 90, "fail_under=90 is untouchable"


def test_coveragerc_parsl_gates_exactly_the_parsl_only_modules() -> None:
    """`.coveragerc-parsl` sources = parsl_backend + common.relay_engine (plan §1.6 item 2 — each
    common/ module gated by exactly ONE job). tasks_engine EXECUTES on the parsl job via the TPE
    arm but GATES on the dask job — listing it here would double-gate; omitting relay_engine would
    leave the relay engine gated by zero jobs. m47 may ADD its parsl-only modules; it may not
    remove these or touch fail_under."""
    parser = _coveragerc(".coveragerc-parsl")
    sources = _sources(parser)
    assert "graphed_executors.parsl_backend" in sources, f"parsl_backend missing from {sources}"
    assert "graphed_executors.common.relay_engine" in sources, f"common.relay_engine missing from {sources}"
    assert "graphed_executors.common.tasks_engine" not in sources, (
        "tasks_engine gates on the DASK job (one gate per module — plan §1.6 item 2)"
    )
    assert "graphed_executors.dask_backend" not in sources, "the parsl job must not gate dask code"
    assert parser.getint("report", "fail_under") == 90, "fail_under=90 is untouchable"
    assert parser.getboolean("run", "branch") is True, "branch coverage is part of the frozen gate"


def test_coveragerc_dask_gains_the_moved_tasks_engine() -> None:
    """`.coveragerc-dask` sources += common.tasks_engine (m46): after the §1.6 move the m43
    frozen suite executes the engine's bodies THERE, so the dask job is the one gate that keeps
    the moved code at >=90. relay_engine/http_plane must never appear (no dask caller — they
    would sit at 0% and sink the job)."""
    parser = _coveragerc(".coveragerc-dask")
    sources = _sources(parser)
    for required in (
        "graphed_executors.submit",
        "graphed_executors.dask_backend",
        "graphed_executors.common.tasks_engine",
    ):
        assert required in sources, f"{required} missing from .coveragerc-dask sources: {sources}"
    for banned in ("graphed_executors.common.relay_engine", "graphed_executors.common.http_plane"):
        assert banned not in sources, f"{banned} has no dask-suite caller and would gate at ~0%"
    assert parser.getint("report", "fail_under") == 90, "fail_under=90 is untouchable"


def _ci_text() -> str:
    return (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()


def _job_block(text: str, job: str) -> str:
    """The yaml block of one 2-space-indented job key (text-level: no yaml dep in the main matrix)."""
    match = re.search(rf"(?ms)^  {re.escape(job)}:\n(.*?)(?=^  \w[\w-]*:$|\Z)", text)
    assert match, f"job {job!r} not found in ci.yml"
    return match.group(0)


def _pytest_commands(block: str) -> list[str]:
    """Logical pytest command lines with backslash continuations folded."""
    folded = block.replace("\\\n", " ")
    return [line.strip() for line in folded.splitlines() if "pytest " in line]


def test_ci_parsl_job_runs_m46_with_a_bare_cov_and_pyarrow() -> None:
    """The test-parsl job exists, installs pyarrow (measured runtime need of the non-inner numpy
    join arms — the same reason the dask job installs it), and its pytest step carries a BARE
    ``--cov`` + ``--cov-config=.coveragerc-parsl`` with NO path-valued ``--cov=`` (which would
    override the config's per-module sources — plan §1.6 item 3 / tests-B-cov-1)."""
    block = _job_block(_ci_text(), "test-parsl")
    assert "pyarrow" in block, (
        "the test-parsl job must install pyarrow: without it the frozen non-inner join arms "
        "importorskip out forever (measured: masked numpy blocks wire over Arrow)"
    )
    commands = [c for c in _pytest_commands(block) if "tests/frozen/m46" in c]
    assert commands, "no pytest command over tests/frozen/m46 in the test-parsl job"
    for command in commands:
        assert "--cov-config=.coveragerc-parsl" in command
        assert "--cov=" not in command, (
            f"path-valued --cov= OVERRIDES the config [run] source (verified pytest-cov 7.1.0 "
            f"behavior) — the per-module allocation would be silently discarded: {command!r}"
        )
        assert re.search(r"--cov(\s|$)", command), (
            f"a bare --cov is MANDATORY to activate coverage at all (without it the modifiers "
            f"are a silent no-op and fail_under never runs): {command!r}"
        )


def test_ci_dask_step_dropped_path_valued_cov_flags() -> None:
    """The r3 blocker tests-B-cov-2 frozen: the dask step's ``--cov=graphed_executors.submit
    --cov=graphed_executors.dask_backend`` flags must be REPLACED by a bare ``--cov`` — otherwise
    the `.coveragerc-dask` source additions are inert and the moved m43 engine is gated by ZERO
    jobs while everything stays green."""
    block = _job_block(_ci_text(), "test-dask")
    commands = [c for c in _pytest_commands(block) if "--cov-config=.coveragerc-dask" in c]
    assert commands, "the dask pytest step no longer names .coveragerc-dask"
    for command in commands:
        assert "--cov=" not in command, (
            f"path-valued --cov= still present — it overrides the config source and silently "
            f"un-gates common.tasks_engine: {command!r}"
        )
        assert re.search(r"--cov(\s|$)", command), f"bare --cov missing: {command!r}"
