"""Public docstrings cite no internal ids, and each entry point's example runs."""

from __future__ import annotations

import ast
import builtins
import importlib
import inspect
import re
import textwrap
from pathlib import Path
from typing import Any

import pytest

import graphed_executors

IDS = re.compile(
    r"§|\bplan [A-F]\.\d|\(plan M\d|\bM\d{1,2}\b|\bm\d{2}\b|\bR\d{1,2}(?:\.\d)?\b|\bF\d{1,2}\b"
    r"|\bguidance \d|\b(?:IT|G|N|D|E|B|T)\d\b|design-[A-Z]|[Dd]irective #\d|hard constraint \d"
    r"|owner ruling"
)
SRC = Path(graphed_executors.__file__).parent
# entry point -> the extra its example needs to run here
RUNS_HERE = {
    "ThreadExecutor": None,
    "ProcessPoolExecutor": None,
    "dask_runner": "distributed",
    "parsl_runner": "parsl",
}
SITE_ONLY = {
    "htcondor_runner": "graphed_executors.htcondor_backend",
    "submit_driverless": "graphed_executors.htcondor_backend",
}


def _public_docstrings() -> dict[str, str]:
    """Every docstring Sphinx publishes, read from source so no optional extra is imported."""
    docs: dict[str, str] = {}
    for path in sorted(SRC.rglob("*.py")):
        parts = path.relative_to(SRC.parent).with_suffix("").parts
        parts = parts[:-1] if parts[-1] == "__init__" else parts
        if any(part.startswith("_") for part in parts):
            continue
        module = ".".join(parts)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docs[module] = ast.get_docstring(tree) or ""
        for node in tree.body:
            if isinstance(node, ast.ClassDef | ast.FunctionDef) and not node.name.startswith("_"):
                docs[f"{module}.{node.name}"] = ast.get_docstring(node) or ""
                for member in node.body if isinstance(node, ast.ClassDef) else ():
                    if isinstance(member, ast.FunctionDef) and not member.name.startswith("_"):
                        docs[f"{module}.{node.name}.{member.name}"] = (
                            ast.get_docstring(member) or ""
                        )
    return docs


def test_public_docstrings_cite_no_internal_ids() -> None:
    docs = _public_docstrings()
    assert len(IDS.findall("the m47 restart (plan §1.2), its M7 meaning, F13, R4")) == 5
    assert len(IDS.findall("IT4, G9, review N2, design-A, directive #2, hard constraint 8")) == 6
    assert len(docs) > 300, len(docs)
    assert {where: IDS.findall(doc) for where, doc in docs.items() if IDS.search(doc)} == {}
    with_examples = {where.rsplit(".", 1)[-1] for where, doc in docs.items() if "Example::" in doc}
    assert with_examples == {*RUNS_HERE, *SITE_ONLY, "start_htex"}


def test_start_htex_shows_the_parsl_runner_example() -> None:
    assert _example("start_htex") == _example("parsl_runner")


def _example(name: str, module: str = "graphed_executors") -> str:
    doc = inspect.getdoc(getattr(importlib.import_module(module), name)) or ""
    _, sep, rest = doc.partition("Example::\n\n")
    assert sep, f"{name} has no Example:: block"
    return textwrap.dedent(re.split(r"\n(?=\S)", rest, maxsplit=1)[0])


@pytest.mark.parametrize("name", sorted(SITE_ONLY))
def test_each_site_only_example_imports_what_it_names_and_calls_with_real_keywords(
    name: str,
) -> None:
    module = SITE_ONLY[name]
    tree = ast.parse(_example(name, module))
    names = [n for n in ast.walk(tree) if isinstance(n, ast.Name)]
    bound = {n.id for n in names if not isinstance(n.ctx, ast.Load)}
    bound |= {
        a.asname or a.name
        for n in ast.walk(tree)
        if isinstance(n, ast.ImportFrom)
        for a in n.names
    }
    loaded = {n.id for n in names if isinstance(n.ctx, ast.Load)}
    assert loaded - bound - set(dir(builtins)) == {"plan"}
    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == name
    ]
    assert calls
    target = getattr(importlib.import_module(module), name)
    for call in calls:
        inspect.signature(target).bind(
            *call.args, **{k.arg: k.value for k in call.keywords if k.arg is not None}
        )


@pytest.fixture
def plan(tmp_path: Path) -> Any:
    ak = pytest.importorskip("awkward")
    pytest.importorskip("pyarrow")
    import graphed  # noqa: PLC0415
    from graphed import Session  # noqa: PLC0415
    from graphed.awkward import AwkwardBackend, from_parquet  # noqa: PLC0415

    data = tmp_path / "events.parquet"
    ak.to_parquet(ak.Array({"pt": [[40.0, 25.0], [55.0], [30.0, 60.0, 20.0], [80.0]]}), data)
    events = from_parquet("events", str(data), steps_per_file=4, session=Session(AwkwardBackend()))
    return graphed.aggregate_plan(
        events.pt,
        reduce=lambda pt: float(ak.sum(pt)),
        combine=lambda a, b: a + b,
        empty=lambda: 0.0,
    )


@pytest.mark.parametrize("name", sorted(RUNS_HERE))
def test_each_runnable_example_prints_the_plans_value(
    name: str, plan: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    if (extra := RUNS_HERE[name]) is not None:
        pytest.importorskip(extra)
    block = _example(name)
    monkeypatch.chdir(tmp_path)
    exec(compile(block, f"<{name} example>", "exec"), {"plan": plan})
    assert capsys.readouterr().out.splitlines()[-1] == "310.0"
