"""The first example on each backend page runs as written."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[3] / "docs"
NEEDS = {"dask.rst": "distributed", "parsl.rst": "parsl", "htcondor.rst": None}


def first_example(page: str) -> str:
    text = (DOCS / page).read_text()
    block = re.search(r"\.\. code-block:: python\n\n((?:    .*\n|\n)+)", text)
    assert block is not None, page
    return "\n".join(line[4:] for line in block[1].splitlines())


@pytest.mark.parametrize(
    "page",
    [
        "dask.rst",
        "parsl.rst",
        pytest.param(
            "htcondor.rst",
            marks=pytest.mark.skipif(sys.platform == "win32", reason="the pilots need POSIX"),
        ),
    ],
)
def test_the_first_example_runs_as_written(page: str, tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    if (need := NEEDS[page]) is not None:
        pytest.importorskip(need)
    code = first_example(page)
    expected = re.findall(r"^\s*print\(.*\)\s+# (.+?)\s*$", code, re.MULTILINE)
    assert "[1 2 2 1]" in expected, code
    script = tmp_path / "example.py"
    script.write_text(code)
    out = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert out.returncode == 0, out.stderr
    assert [line for line in out.stdout.splitlines() if line in expected] == expected, (
        out.stdout + out.stderr
    )
