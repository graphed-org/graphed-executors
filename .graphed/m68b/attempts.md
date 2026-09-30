# m68b — attempts log

Milestone **m68b**: condor cluster-hosted services (B1: `ServiceJob`, `announce.py`, `/announce`,
`host_service`/`release_service`) and the DAG driverless run with SERVICE nodes and `SiteProfile.job_root` (B2).
Branch `m68b` from `main` 0e48380 (m68a merged, #39). Plan: `plan/plan-services.md` §3.3 (+ D1–D10, §6–§9 lines that
bind m68b) on `lgray/graphed-executors` `plan/m68` 1c22bc2 (B1 converged r23 clean delta + owner-directed M41 fold
confirmed r25; B2 converged r19/r20); implementer constraints `plan/reviews/m68b-exit-items.md`.

## Decisions (coordinator)
- Each part freezes first: `test(services): frozen m68b, attached hosting` (tag `freeze-m68b-b1`), then
  `test(services): frozen m68b, DAG driverless` (tag `freeze-m68b-b2`); then B1 feat, B2 feat, `docs(m68b)`.
- Local gates on CPython 3.12 (macOS arm64, venv built as `ci.yml` installs: `GRAPHED` pin d0ad16b, corpus, histogram,
  `-e .[dev,docs]`); the all-OS matrix is CI's.
- `htcondor_backend/**` is outside the `test` job's coverage and diff-cover; it is gated by `test-htcondor`. Its local
  mirror is a Linux container: `htcondor/mini:25.13.2-el9` + py3.12 with the same pins and `htcondor==25.13.2`
  bindings. Docker Hub has no 25.14.1 tag, and a 25.14.1 wheel fails FS authentication against the 25.13.2 schedd,
  so the bindings match the daemon there. CI's pool is whatever get.htcondor.org serves (25.14.1 at 0e48380).
- The frozen m68b harness has its own basename: frozen dirs are not packages, so a second `services_harness.py`
  would shadow m68a's.

## Freeze
- **B1** `freeze-m68b-b1` = `3eb2bfac0048cb5e0d4ed69d2412f10e86e114af` (66 items: `test_announce_route.py` 11,
  `test_cluster_service_job.py` 53, `test_cluster_services_live.py` 2). Sanity r1 NOT SANE (`wait_announce` calls
  unbounded: a timeout-ignoring mutant hung the suite); r2 SANE. On the unimplemented tree: Mac 64 failed + 2 skipped
  (live, no bindings), container 66 failed, all planned-name-missing; identical across two runs.
- **B2** `freeze-m68b-b2` = `b1a4d07cfcfbeefea3a321a677d898cadd120bbc` (36 items: `test_driverless_dag.py` 34,
  `test_driverless_dag_live.py` 2). Authored and sanity-checked in a worktree at the B1 freeze with no implementation.
  Sanity r1 NOT SANE (unbounded `submit_driverless`/`result()` in the live file; skip shapes); r2 SANE. On the
  unimplemented tree: Mac 33 failed + 3 skipped, container 36 failed.
- The precommit integrity scan hard-fails `pytest.skip(`/`@pytest.mark.skip…` in new frozen files too; environmental
  skips are name-assigned `pytest.mark.skipif` markers (m68a\x27s `needs_triton` form) with unchanged conditions.
- Integrity check for reviewers: `git diff b1a4d07 -- tests/frozen` must be empty.
