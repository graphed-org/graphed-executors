# Dispute record: frozen README pinned-contract prose vs review-mandated F3 drop

**Artifact:** `tests/frozen/m42/README.md:49` (pinned execution contract block) and the
`tests/frozen/m42/submit_backends.py:64` harness docstring both write the constructor as
`DaskBackend(client, *, replicate_broadcast=False, default_retries=3)` (mirroring plan §1.3.2).

**Deviation:** the implementation omits `default_retries` (dropped in commit d24215a).

**Why this is sanctioned, not a route-around:**
- No frozen TEST passes or asserts on `default_retries` — the harness `make_dask_backend` forwards
  only `**kwargs` and no caller supplies it; the pin exists solely in prose/docstrings. The
  executable frozen suite is untouched and green (47/47; `git diff freeze-m42 -- tests/frozen` empty).
- The drop was mandated as an option by the m42 REVIEW follow-up **F3** ("either wire it as the
  submit default or drop it" — review record `dask-parallel-backends-plan-reviews.md`, m42 section),
  after three APPROVE verdicts. Wiring was rejected because `retries or self._default_retries` would
  silently override an explicit `retries=0` from a direct caller — a footgun worse than dead config.
- Rationale + evidence logged in `.graphed/m42/attempts.md` (REVIEW/REMEDIATION section, F3 entry).

**Disposition:** prose-only divergence, blessed by review. The m43 test-author should NOT re-pin
`default_retries`; if a backend-level retry default is ever wanted, it returns through a plan
revision, not through this parameter.

Recorded by the orchestrator (team-lead), 2026-07-21.
