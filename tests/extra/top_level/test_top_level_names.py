"""The runners import from ``graphed_executors`` itself, without loading a backend."""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Callable
from importlib import import_module
from typing import Any

import pytest

import graphed_executors

HOMES = {
    "PinnedPoolExecutor": "graphed_executors.local",
    "ProcessPoolExecutor": "graphed_executors.local",
    "SequentialRunner": "graphed.core",
    "ThreadExecutor": "graphed_executors.local",
    "dask_runner": "graphed_executors.dask_backend",
    "htcondor_runner": "graphed_executors.htcondor_backend",
    "parsl_runner": "graphed_executors.parsl_backend",
    "start_htex": "graphed_executors.parsl_backend",
    "stop_htex": "graphed_executors.parsl_backend",
}


def loaded_after(statement: str, *modules: str) -> list[str]:
    """Return which of ``modules`` a fresh interpreter has loaded after ``statement``."""
    code = f"import sys; {statement}; print(*[m for m in {modules!r} if m in sys.modules])"
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout.split()


@pytest.mark.parametrize(("name", "home"), HOMES.items())
def test_each_name_is_its_home_modules_object(name: str, home: str) -> None:
    assert getattr(graphed_executors, name) is getattr(import_module(home), name)


def test_all_lists_exactly_the_runners_and_the_version() -> None:
    assert set(graphed_executors.__all__) == {*HOMES, "__version__"}


def test_dir_is_all() -> None:
    assert dir(graphed_executors) == sorted(graphed_executors.__all__)


def test_an_unknown_name_is_an_attribute_error() -> None:
    with pytest.raises(AttributeError, match="no_such_runner"):
        graphed_executors.no_such_runner  # type: ignore[attr-defined]  # noqa: B018


def test_importing_the_package_loads_no_backend() -> None:
    heavy = ("graphed", "graphed_executors.local", "dask", "distributed", "parsl", "htcondor2")
    assert loaded_after("import graphed_executors", "graphed_executors", *heavy) == [
        "graphed_executors"
    ]


def test_a_cluster_runner_loads_its_module_not_its_extra() -> None:
    assert loaded_after(
        "import graphed_executors; graphed_executors.dask_runner",
        "graphed_executors.dask_backend",
        "distributed",
        "graphed_executors.parsl_backend",
    ) == ["graphed_executors.dask_backend"]


@pytest.mark.parametrize(
    ("call", "extra"),
    [
        (lambda: graphed_executors.dask_runner(object()), "dask"),
        (lambda: graphed_executors.parsl_runner(object()), "parsl"),
        (lambda: graphed_executors.start_htex(workers=1, run_dir="unused"), "parsl"),
        (lambda: graphed_executors.htcondor_runner(workers=1), "htcondor"),
    ],
    ids=["dask_runner", "parsl_runner", "start_htex", "htcondor_runner"],
)
def test_a_missing_extra_is_named(
    call: Callable[[], Any], extra: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    for module in ("distributed", "parsl", "parsl.executors", "htcondor2"):
        monkeypatch.setitem(sys.modules, module, None)
    with pytest.raises(ImportError, match=re.escape(f"pip install 'graphed-executors[{extra}]'")):
        call()
