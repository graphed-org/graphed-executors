# m47 FIXUP suite — facade join arms, root loss-safety, dedup mechanism witness

Append-style FIXUP section to the m47 frozen suite (the m44-fixup precedent), mandated by the
adversarial review of the m47 implementation. The facade-join and lost-root tests were authored
IMPL-BLIND against the review-remediated implementation at `f12693b`; every scenario below was
**measured before pinning** (black-box: entry points, signatures via `inspect.signature`, and
witness counters only — no `src/graphed_executors/**` read). Three behavioral gaps closed.
**Frozen — read-only after the fixup freeze tag.**

**Authorship exception (recorded, not hidden):** the dedup test's scenario was REDESIGNED by the
team-lead session **with source access** after the blind-authored both-edges-into-the-root variant
proved intermittently racy (a pure-duplicate retry could race endpoint teardown into a legitimate
epoch restart ~coin-flip — the pins were right, the scenario was not deterministic). The redesign
gates root completion on a clean slow edge so the race is closed by construction (see the test
docstring). Blindness for that ONE test is forfeited and compensated empirically: the mutation
re-verification (MUT5 — the dropped `(level,pos)` dedup) must kill it, proving its discrimination
is real rather than implementation-echoed.

## Files → gap → what each test witnesses / which cheat it kills

| Test | Gap | Witness | WRONG impl it FAILS |
|---|---|---|---|
| `test_parsl_fixup_facade_join.py::test_facade_transport_join_dispatches_the_p2p_engine_with_parity` | facade `run_join` valid-method dispatch was dark (the only frozen call used an invalid method) | spy == exactly k `_parsl_peer_main`, zero relay/tasks names; inner hashes == local + duplicating-oracle rows; `.transport` present, peer rows == {w0,w1}, Σ`pull_requests_served` ≥ 1, Σ`bytes_served` > 0, driver `recv_class_hello == k`; 1 nonce, 0 restarts | a transport branch that reruns the relay/local engine; a driver-relayed "peer" join (block-plane counters 0); a corrupted argument hand-off (parity) |
| `…::test_facade_transport_join_left_rows_are_exact` | same dark block, non-default `how` | `how="left"` through the facade transport arm: null-aware multiset + hashes == local; still k peers + `.transport` | the many-positional-argument hazard: a dispatch that drops/transposes `how`/`broadcast` into `transport_run_join`; sentinel nulls |
| `…::test_facade_join_tasks_and_auto_resolve_to_the_relay_shape[tasks,auto]` | join arms of `tasks` + the `auto` predicate | relay Counter (`_dask_map_write` > 0, `_dask_gather_join == parts`, zero peers/picks); `head_node_routed is True`; no `.transport`; `how="left"` rows/hashes == local | an `auto` resolving join to transport on parsl (m45 predicate); a hardcoded pick tier; an unlabeled relay; a dropped `how` |
| `test_parsl_fixup_lost_root.py::test_lost_root_raises_attributed_and_never_returns_the_identity` | no frozen scenario drove the no-root state (§1.4 "No silent None anywhere", obligation 12 `_NO_ROOT`) | root OWNER fails at exactly the root combine (value-gated: only the grand total triggers) ⇒ no root exists anywhere; run RAISES `StageError` naming the marker cause; mark file (worker pid ≠ driver) proves the loss sits AT the root, peer-side; pool-liveness submit after | a driver that returns `empty()` (identity 0), `None`, or a driver-side re-fold when no root arrives — ANY return fails `pytest.raises`; raw exception leakage; an error path that strands peers |
| `test_parsl_fixup_reduce_dedup.py::test_double_sibling_duplicates_are_deduped_with_exact_counters` | the single-dup dedup test cannot distinguish dedup from topology absorption | k=3: w1's hand-off into w0 delivered THREE times (`{w0:{w1:2}}` — both duplicates on ONE edge, from w1's real inline retry loop) while root completion stays gated on w2's clean SLOW edge (teardown race closed by construction); value == oracle in clean+injected; per-peer `n_combines` identical to the in-test clean baseline, Σ == 11; w0 `peer_recvs == 2` both runs (4 plane deliveries → 2 protocol consumptions; a dropped guard reads 4); Σ`processed == 12`; w0 `recv_duplicate_deliveries >= 2`; w1 `sends_retried >= 2`; w2 `sends_retried == 0` (negative control); 0 restarts both runs | a dropped `(level,pos)` dedup (`peer_recvs` reads 4; combine counters / value diverge on double-combine variants); wire-level digest dedup (< 2 dups witnessed — breaks at-least-once); an unwired seam (w1 retry counter < 2); leaf recompute masquerading as dedup; escalation instead of retry (restarts > 0) |

## Measured facts this suite pins (authoring session, parsl HTEX, py3.12, macOS, `f12693b`)

- Facade transport join (k=2, parts=8, `join_sides`): spy `{_parsl_peer_main: 2}` exactly;
  hashes byte-identical to local `run_join`; rows == the duplicating oracle; peer rows carried
  `pull_requests_served` 3+2 and `bytes_served` 432+320; driver row
  `{recv_class_hello: 2, recv_class_manifest: 2, recv_class_probe: 2, recv_class_gathered: 2}`.
- Facade `tasks` AND `auto` join (`how="left"`): spy `{_dask_map_write: 4, _dask_gather_join: 8}`,
  `head_node_routed=True`, no `.transport`, rows/hashes == local.
- k=3 / 12-leaf reduce topology: w1 `peer_sends=1`, w2 `peer_sends=1`, w0 `peer_recvs=2`;
  `n_combines` {w0: 5, w1: 3, w2: 3} (Σ = 11), IDENTICAL clean vs injected; injected
  `{w0:{w1:2}}` gave w0 `recv_duplicate_deliveries=2` exactly, w1 `sends_retried=2`, w2
  `sends_retried=0`, value 498 == SequentialRunner, 0 restarts. Flake posture: protocol-layer
  counters (`n_combines`/`peer_recvs`/`processed`/value) are pinned EXACT; the injected edge's
  plane-layer counters (`recv_duplicate_deliveries`, `sends_retried`) are pinned `>=` (a genuine
  network hiccup adds deliveries/retries without weakening what the pin discriminates); the clean
  edge pins `sends_retried == 0` as the negative control. The r1 scenario (`{w0:{w1:1,w2:1}}`,
  both edges into the root) was retired: with the injected messages being the root's LAST needs,
  pure-duplicate retries raced endpoint teardown and escalated to a legitimate epoch restart
  ~coin-flip. The r2 scenario measured six consecutive identical passes (fresh HTEX pool each
  run) on the authoring machine.
- Root-owner loss (value-gated raising combine, `epoch_restarts_allowed=0`): `StageError`,
  `cause_type=RuntimeError`, marker in the rendered message, mark written on a worker pid, pool
  alive after. (A `__main__`-defined combine class is serialized BY VALUE with broken globals —
  the test class is module-level in the test file, which rides the harness's PYTHONPATH export.)

## Seam findings — reported, not routed around (impl-blind protocol)

1. **The mandated fallback-through-`run_join` arm is NOT authorable.** `api.run_join` accepts no
   `registry_rewrite` (measured: `inspect.signature` — `run_repartition` has it, `run_join` does
   not), and the probe cannot be failed any other black-box way on a loopback pool. The
   `on_unreachable="fallback"` path through `run_join` — including its fallback argument plumbing
   — stays unwitnessed until the facade grows the same seam `run_repartition` already carries.
2. **`inject_recv_failures` is a no-op for dest `"driver"`.** Measured: `{driver: {w0: 2}}` and
   `{driver: {w0: 8}}, allowed=0` both completed cleanly with w0 `sends_retried == 0` — the seam
   arms peer endpoints only. The panel's transport-flavored lost-root construction (withhold
   `("root",)` at the driver; `StageError` naming `TransportDeliveryError`) is therefore not
   constructible black-box.
3. **`root_timeout_s` does not bound a still-RUNNING root owner.** Measured: a root withheld by a
   gate INSIDE the root combine (peers otherwise clean) hung `parsl_run_plan` past a 120 s hard
   timeout despite `root_timeout_s=5.0`, `allowed=0` — the driver honestly waits on a running
   peer future (a slow user combine is legal work, not a loss). Consequence of 2+3: the authored
   lost-root pin uses the root-owner-loss arc (no root exists ANYWHERE — not in /msg, not in the
   task-return triple), whose attributed cause is the user marker error rather than
   `TransportDeliveryError`. The never-return / never-identity contract — the actual §1.4
   loss-safety clause — is pinned identically either way.

## Non-vacuity + determinism evidence

- Every pin above is a measured behavior of `f12693b`, and every discriminating claim names the
  divergent observable (value, counter, spy shape, exception type/cause) a cheating
  implementation moves — no assertion is satisfiable by an absent mechanism (`.get(key, 0)`
  reads make a missing counter a FAILURE, not a skip).
- The facade-join and lost-root tests were run twice consecutively against `f12693b` with
  identical results (authoring report: `fixup_run1.txt` / `fixup_run2.txt`); the redesigned dedup
  test was run SIX times consecutively with a fresh HTEX pool per run, identical results; the
  full frozen m46+m47 tree stays green with all three files added.
- Clock discipline: bounded waits are scenario construction / hang guards only (R0.10a); every
  witness is a counter, hash, pid, spy shape, or exception attribution.
