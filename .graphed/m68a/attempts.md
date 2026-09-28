# m68a — attempts log

Milestone **m68a**: the engine service set (three legs, readiness checks, worker probe, recipes), driver-hosted
services, the site table's `services`, driverless endpoints. Branch `m68a` from `main` c2298d7 (m67 and m69a merged).
Plan: `plan/plan-services.md` §1, D1–D10, §3.1, §6–§9 on `lgray/graphed-executors` `plan/m68` eafd92f; implementer
constraints `plan/reviews/m68a-exit-items-r8-r10.md` (and r7 E1–E7).

## Decisions (coordinator)
- The brief says `git fetch origin plan/m68`; this clone had no `origin`, and `plan/m68` exists only on the fork, so `origin` = `https://github.com/lgray/graphed-executors` and `upstream` = graphed-org (the brief's diff-cover base).
- GRAPHED pin: graphed-org/graphed `cf4520d` (#61's merge; its parent is the current pin 49454d4, #58 is below both), the smallest move that carries `ServiceSpec`/`split_endpoint`/`bind_services`/`require_bound`.
- **Blocker, recorded, not routed around:** the plan's graphed "resolve walk" (§3.2: `graphed.services.resolve_services`, `Resolvable`, forwarding through `_Collated`/`_PartitionReduce`) is on no graphed branch (main 6e9e55e, all heads, no `freeze-preserve-m68-3`). graphed changes are out of this brief's scope. The engine calls `graphed.services.resolve_services` when it exists and otherwise resolves through the duck-typed hook on `plan.process` (the same call for a non-composite process). The frozen legs that need graphed's forwarding (the `collate` and `aggregate_plan` spies) are written to the plan and stay red until graphed lands the walk. See `blockers.md`.
- Commit author Lindsey Gray, no trailers: the brief's Rules (the owner's instruction) govern attribution.
- Local gates run on CPython 3.12 (the `test-htcondor`/`docs` interpreter); the all-OS/all-version matrix is CI's.
- Live legs (minicondor + Triton container): `get.htcondor.org` and `nvcr.io` are refused by this session's egress policy (403). A `htcondor/mini` container pool was tried: the condor CLI submits, but the pip `htcondor2` bindings fail FS authentication against it. The `test-htcondor` leg is therefore gated by the PR's CI, as the brief allows.
- Pre-existing, not m68a: under `pytest -n 8`, `tests/extra/m66/test_m66_ports.py::test_a_bind_failure_names_the_site_and_the_range` and `test_m66_server.py::...exits_at_once[mid-task]` fail on port contention; both pass serially (CI runs serially). Local gates run serially.

## Freeze
- Test author wrote `tests/frozen/m68a/` (144 tests). Sanity r1 was NOT SANE, with three required fixes: README contract readings, bounding every planned-API call, and a probe fault that discriminates a connect-only probe. Optional A/B/C/E were also applied. Sanity r2 was SANE: 130 failed, 12 passed and 2 skipped on the unimplemented tree, identical across two runs, with 0 collection errors; ruff, format and mypy clean.
- Deliberately not applied (sanity r2 optional nits, non-blocking): three `ServiceSet(...)` constructors sit outside `run_bounded`, but the plan's constructor only runs `split_endpoint`; and the README has no line saying the bare-endpoint refusal is graphed's `split_endpoint` text.
- The test author validated attainability against a throwaway prototype outside `src/`. It was deleted before the implementer started, and the implementer never saw it.
- **Freeze sha: `83ca76fbd0b39ecedbd651584f9573d059de71bd`** (`test(m68a): frozen m68a acceptance suite`). The annotated tag `freeze-m68a` exists locally at that commit. The session's git proxy drops every push that carries the tag (`send-pack: unexpected disconnect`, 5 tries with backoff), while the same commit pushed as a branch. Per the brief, the sha is recorded here. Integrity check for reviewers: `git diff 83ca76f -- tests/frozen` must be empty.
