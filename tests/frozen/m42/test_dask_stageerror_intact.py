"""m42 M6 obligation on dask: a `StageError` raised inside a REAL worker process arrives at the
driver as the SAME StageError — full structured state, byte-equal, re-renderable to the user's
analysis line (plan §1.4 bullet 1; blueprint §3 `test_dask_stageerror_intact`).

Discriminates: wrapping worker errors in a dask-specific or generic error, degrading to an opaque
string, and any lossy re-raise that drops the source-frame chain (`format_traceback` must still
name user_analysis.py:42 after the process boundary)."""

from __future__ import annotations

import pickle

import pytest
from graphed.debug import StageError, format_traceback
from submit_backends import cluster_client, make_dask_runner, make_stage_error, stage_error_plan

pytest.importorskip("distributed")


@pytest.fixture(scope="module")
def client():
    # tier B: the error must cross a genuine process boundary (nanny workers, real pickling)
    with cluster_client(2, processes=True) as c:
        yield c


def test_stage_error_crosses_the_worker_boundary_byte_equal(client) -> None:
    expected = make_stage_error()
    with make_dask_runner(client, retries=0) as runner, pytest.raises(StageError) as excinfo:
        runner.run(stage_error_plan(4, "sei"))
    caught = excinfo.value

    assert type(caught) is StageError  # never a subclass-wrap, string, or RuntimeError
    assert caught == expected  # StageError.__eq__ compares the FULL structured __dict__
    assert caught.__dict__ == expected.__dict__  # byte-equal __reduce__ state, field by field
    assert pickle.loads(pickle.dumps(caught)) == expected  # still round-trippable driver-side

    rendered = format_traceback(caught)
    assert 'File "user_analysis.py", line 42, in my_cut' in rendered
    assert "pt > 30" in rendered  # the user's sub-expression, not an executor frame
    assert "ValueError: negative pt" in rendered
