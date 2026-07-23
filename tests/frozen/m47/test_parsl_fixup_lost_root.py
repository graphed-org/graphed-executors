"""m47 FIXUP — root loss-safety (plan §1.4: ``collect_peer_root`` timeout → ``_select_root``
raise, "No silent None anywhere"; obligation 12's ``_NO_ROOT`` disambiguation: a lost/withheld
reduce root must NEVER default to the identity value).

Scenario (deterministic, clock-free, black-box): the root OWNER fails at exactly the root
combine. ``RootLossCombine`` raises only when its operand sum equals the grand total — with
all-positive leaf values only the FINAL (root) combine can hit the full sum, so every earlier
combine on every peer completes cleanly, the non-root peers' node hand-offs are all delivered
(the root combine being REACHED proves it), and then the root is never minted anywhere: no
``("root", …)`` /msg delivery, no root in the owner's task-return triple. The driver holds the
surviving peers' clean work and NO root.

Witnesses: with ``epoch_restarts_allowed=0`` the run RAISES an attributed ``StageError`` whose
rendered cause names the marker (``RuntimeError: m47-fixup-root-loss-marker``) — and never
returns; the mark file written by the combine proves the loss sits exactly at the root combine
and carries a WORKER pid (the root combine genuinely ran peer-side, not in a driver-side fold);
a plain submit afterwards proves the pool survived (the error path released the peers).

Discriminates (the cheat each assertion kills): a driver that on a missing root returns
``empty()`` — here the identity 0 — "completes" instead of raising and fails ``pytest.raises``
loudly (ANY return fails it: 0, None, or a driver-side re-fold of the surviving partials); a raw
exception leaking to the user instead of the m6-attributed ``StageError`` (type + cause pins); a
release path that strands the peers on error (the liveness submit hangs the 2-slot pool).

Seam note (measured while authoring, recorded in README_FIXUP.md): the panel's intended
transport-flavored construction — withhold the ``("root",)`` delivery at the DRIVER endpoint —
is not constructible through the pinned seams (``inject_recv_failures`` is a no-op for
dest="driver", ``parsl_run_plan`` carries no ``registry_rewrite``, and a root withheld by a
still-RUNNING owner is an honest unbounded wait, not a loss). The root-owner-loss arc is the
sharpest reachable state where NO root exists anywhere; its cause is the user-marker error, not
``TransportDeliveryError``."""

from __future__ import annotations

import os
import sys
import tempfile

import pytest
from graphed.core.execution import Plan, Task
from graphed.debug import StageError
from parsl_transport_harness import (
    make_parsl_backend,
    p2p_double,
    p2p_htex_pool,
    p2p_leaf_value,
    p2p_partitions,
    p2p_zero,
    plan_run,
    run_bounded,
)

pytest.importorskip(
    "parsl", reason="graphed-executors[parsl] extra not installed (main matrix is parsl-free)"
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="parsl HTEX requires POSIX (Windows keeps the base package only — plan §4 G8)",
)

N_LEAVES = 12
# p2p_leaf_value = 7*entry_start + 3, all positive => ONLY the root combine sees the full sum
GRAND_TOTAL = sum(7 * i + 3 for i in range(N_LEAVES))
MARKER = "m47-fixup-root-loss-marker"


class RootLossCombine:
    """Module-level (HTEX workers unpickle it by reference off this directory's PYTHONPATH
    export): a combine that fails ONLY on the grand total, i.e. exactly at the root."""

    def __init__(self, threshold: int, mark_path: str) -> None:
        self.threshold = threshold
        self.mark_path = mark_path

    def __call__(self, a: int, b: int) -> int:
        s = a + b
        if s == self.threshold:
            with open(self.mark_path, "w") as f:
                f.write(str(os.getpid()))
            raise RuntimeError(MARKER)
        return s


def test_lost_root_raises_attributed_and_never_returns_the_identity() -> None:
    mark_path = os.path.join(tempfile.mkdtemp(prefix="m47-fixup-rootloss-"), "rootmark")
    plan = Plan(
        process=p2p_leaf_value,
        combine=RootLossCombine(GRAND_TOTAL, mark_path),
        empty=p2p_zero,
        tasks=tuple(Task(i, p) for i, p in enumerate(p2p_partitions(N_LEAVES, "m47fixloss"))),
    )
    with p2p_htex_pool(workers=2) as htex:
        backend = make_parsl_backend(htex)
        with pytest.raises(StageError) as excinfo:
            run_bounded(
                lambda: plan_run(plan, backend, epoch_restarts_allowed=0),
                timeout_s=300.0,
            )

        err = excinfo.value
        rendered = f"{getattr(err, 'cause_type', '')}: {err}"
        assert "RuntimeError" in rendered and MARKER in rendered, (
            f"the StageError must attribute the root-owner loss to the marker cause; got: {rendered}"
        )

        assert os.path.exists(mark_path), (
            "the root-combine mark is missing — the failure did not occur AT the root combine, "
            "so this run never constructed the no-root-anywhere state (scenario misfired)"
        )
        with open(mark_path) as f:
            mark_pid = int(f.read())
        assert mark_pid != os.getpid(), (
            "the root combine ran in the DRIVER process — a driver-side fold, not the peer "
            "reduction whose lost root this test pins"
        )

        fut = backend.submit(p2p_double, 21, key="m47-fixup-rootloss-liveness")
        assert run_bounded(lambda: fut.result(), timeout_s=120.0) == 42, (
            "the pool is dead after the root-loss error — the error path stranded the peers"
        )
