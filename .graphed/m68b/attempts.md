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
  skips are name-assigned `pytest.mark.skipif` markers (m68a's `needs_triton` form) with unchanged conditions.
- Integrity check for reviewers: `git diff b1a4d07 -- tests/frozen` must be empty.

## Implementer iteration 1 (B1)
- **Changed** (+847/−12): `htcondor_backend/announce.py` (the prototype typed; `start` returns the ready
  `(child, port)` or the reason; argv tokens rendered by `str.replace` as the engine's `_render`, not `str.format`;
  the self-check bypasses environment proxies, as `check_ready` does; the self-check timeout is capped by the start's
  remaining budget; `reap` has no early return, since `Popen.terminate`/`kill` never signal a reaped child),
  `htcondor_backend/services.py` (`ServiceJob`: keys through `CondorPilots.submit_description`, then `initialdir`,
  the attached-only `MY.SendCredential` drop and the recipe image, then `extra_submit` again so the user's keys stay
  last; `ad()` = queue ad, else history `match=1`), `server.py` (announce registry on its own `Condition`;
  `forget_announce` also drops a pending record), `backend.py` (`host_service`/`release_service` iff attached over
  `CondorPilots` with `"cluster"`; module docstring's "later seam" sentence rewritten, E3), `launch.py` (absolute
  `log_dir` at `start`; `job_python` shared by `_stage` and `ServiceJob`), `ci.yml` `test-htcondor` (+ m68b dirs,
  one simulated GPU before the pool starts), `.coveragerc-htcondor` comment, `tests/extra/m68b` (10 legs).
- **Mac** (py3.12, full `tests/frozen tests/extra`, serial): 1033 passed, 101 skipped, 33 failed; every failure is
  in `tests/frozen/m68b/test_driverless_dag.py` (B2, unimplemented; the same 33 ids as B2 sanity). Per-file min
  93.02% (`local/shuffle.py`, unchanged), total 97.15%; diff-cover: no measured lines (all changed src is under
  the excluded `htcondor_backend/`).
- **Container** (`m68b-minicondor:local` + simulated GPU, `ci.yml`'s test-htcondor replay over m66/m67/m68a + B1
  frozen m68b + extras, B2 files `--ignore`d and run separately): 388 passed, 3 skipped (dask absent; two Triton legs
  without `GRAPHED_TRITON_GRPC`); scoped report 99%; per-file min 97.66% (`backend.py`, lines unchanged since
  0e48380), `announce.py` 99.21% (90.31% from the frozen legs alone), `services.py` 100%; diff-cover 100% of 391
  changed lines. B2 files there: 36 failed (all 36 B2 items).
- ruff, ruff format, mypy strict (also `--platform win32`, with a control: unguarded `hard_reap` gives 2
  `attr-defined`), prek, sphinx -W, precommit `--fast --no-coverage`: ok (advisory `ci_config_modified`).
- Extras discriminate: 13 mutants of `announce.py`/`services.py` (one per guarded branch), each killed by its leg.

## Implementer iteration 2 (B2)
- **Changed** (+309/−70): `sites.py` (`SiteProfile.job_root` after `jobs_can_submit`: lxplus `/afs`, generic `/`,
  lpc default `None`), `driverless.py` (`_SELF_SUBMIT_ROOT` gone; `_require_under` = `root == "/"` or a lexical
  `abspath` check, applied to the `log_dir` actually used (the `mkdtemp` one when `None`), then to every
  `user_modules` path and SERVICE-node input; `_service_nodes` = launched, not given, kind not served, image or GPUs,
  ids `svc<i>` in name order; the DAG: new `<log_dir>/graphed-<nonce>/`, `driver.sub` without the retry keys,
  `service-svc<i>/` from B1's watch-mode `ServiceJob.files` after `_stage`, `svc<i>.sub` with `periodic_remove`
  before `extra_submit`, `run.dag`, `from_dag(abs run.dag, DAG_OPTIONS)` submitted unspooled; `RunHandle.dag` and
  `_dag_status`; `result()` never retrieves for a DAG and names `run.dag.dagman.out` without a `result.pkl`),
  `driver.py` (announced runs bind the task server on the slot's `Machine` and `worker_ports`, then publish the
  announce secret and `driver.url` into `dag_dir` via `mkstemp` (0600) + `os.replace`), `backend.py` (`announced=`
  binds `host_service` = `wait_announce(node id, timeout_s)` and `release_service` = drop a pending announce; E3
  docstrings), `launch.py` (`_stage(..., placeholder)`: `printf` of the octal bytes to `result.pkl` and
  `: >> driver.log` before the tar/exec lines; `pilot.sh` unchanged), `announce.py` (`URL_FILE` shared with the
  driver), `tests/extra/m68b/test_m68b_dag.py` (2 legs: an unannounced node times out and a release drops a pending
  announce; a driver node held only while spooling reads `queued`).
- **Mac** (py3.12, full `tests/frozen tests/extra`): 1068 passed, 101 skipped, 0 failed (no expected failures left;
  the B2 skips are the `condor_dagman` fixture leg and the two live legs). Per-file min 93.02% (`local/shuffle.py`,
  unchanged), total 97.15%; diff-cover: no measured lines (all changed src is under the excluded `htcondor_backend/`).
- **Container** (`test-htcondor` replay, simulated GPU, all of `tests/frozen/m68b` + extras): 426 passed, 3 skipped
  (dask absent; the two Triton legs without `GRAPHED_TRITON_GRPC`); scoped 99%; per-file min 97.92% (`backend.py`:
  lines 129/168, m66/m68a code); `driverless.py`, `driver.py`, `sites.py` 100%; diff-cover 100% of 512 changed lines.
- **Trap met:** a host-side mutation sweep left the container importing a restored file's mutant (shared
  `__pycache__` of the same CPython minor over the bind mount); the container legs failed until
  `find src tests -name __pycache__ -exec rm -rf {} +`. Clear bytecode after any host-side source mutation.
- ruff, ruff format, mypy strict (also `--platform win32`), prek, sphinx -W, precommit `--fast --no-coverage`: ok.
  E2: no `triton|histserv` in `htcondor_backend/**` outside `sites.py` (control: 2 hits in `sites.py`).
- Extras and the frozen legs discriminate: 16 mutants of the B2 code (one per decision), each killed.

## Implementer iteration 3 (docs)
- `htcondor.rst`: leg 3 names cluster hosting (E3: the "does not have yet … refused naming `host_service`" text
  and the closing driverless paragraph rewritten); new "Cluster-hosted services" under "Services" (announce,
  timeouts and removal, `service-<key>/`, the `service/` rule with bare-name `model_repository`, `SO_REUSEADDR`,
  argv tokens, the driverless DAG, `job_root` with its lexical check and the held-node consequences, three ×
  `timeout_s`, the lxplus GPU walk-through, marked site-specific); the driverless section reads `job_root` for
  `pilots="condor"`, says a DAG is not spooled, and the exit table gains a "killed" row. `design.rst`: E3's
  "a later release fills" rewritten, the DAG and the placeholder added, "five fields" of `RunHandle` corrected.
  `api.rst`: `SiteProfile.job_root`. `changelog.rst`: cluster hosting, the DAG and `job_root`, the killed driver.
- The walk-through is not executable (lxplus); it parses and every call in it binds to the current signatures
  (`inspect.signature(...).bind`). sphinx -W ok; the two new `Cluster-hosted services`_ links resolve.

## Implementer iteration 4 (review r1)
- R1-1: `tests/extra/m68b`'s `listeners()` sets `SO_REUSEADDR`, binding as `free()` binds, and `free_ports()` scans
  from a per-process base. Closing check (`timewait_probe`: a server-side TIME_WAIT left on both ports the leg picks
  in the same process, a plain bind then refused with errno 48 on macOS / 98 on Linux): `-k exhausted` failed before
  the fix (errno 48 in `listeners`) and passes after, on both; a `start()` mutant that skips the `free()` scan still
  fails it.
- Nits: "(D10)" dropped from `backend.py`'s comment; `CondorPilots.job_python` → `_job_python` (the rendered
  `launch` API page listed it 3 times before, 0 after); `ServiceJob.submit()`'s assert now checks the launcher's
  schedd (what `_submit` needs) as well as `log_dir`, before any file, with a leg (a mutant checking only `log_dir`
  is killed). It stays an assert, the nit's first option: turning it into a `raise` trips the integrity scan's
  `assertion_removed` on a line of a pushed commit.
- Mac full: 1069 passed, 101 skipped, 0 failed; per-file min 93.02% (`local/shuffle.py`). Container: 427 passed,
  3 skipped; per-file min 97.92% (`backend.py`); diff-cover 100% of 513 lines. ruff, mypy (+win32), prek, sphinx -W,
  precommit: ok.
