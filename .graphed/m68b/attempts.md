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
