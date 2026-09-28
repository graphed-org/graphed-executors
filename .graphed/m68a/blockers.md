# m68a blockers (upstream, not disputes)

## graphed's resolve walk is not merged
- Plan: `plan-services.md` §3.2 "The resolve walk" (a graphed PR "stacked on main before the release executors floor
  at") and §7 ("graphed m68 PR and the resolve walk before executors m68a").
- Observed (2026-09-28): `graphed-org/graphed` main `6e9e55e` has `services.py` with `ServiceSpec`, `Launch`,
  `UnboundService`, `split_endpoint`, `Bindable`, `referenced_services`, `bind_externals`, `require_bound`,
  `bind_services`, but no `resolve_services` or `Resolvable`. No branch carries `def resolve_services`, and there is
  no `freeze-preserve-m68-3` tag.
- Effect on m68a: in `test_services_protocol.py`, the legs where a `Resolvable` spy sits under `collate({...})` or as
  `aggregate_plan`'s `reduce` need graphed's composites to forward `resolve_services`. The engine cannot reach
  those spies without reaching into graphed's private composites, which the plan's design excludes
  (`probes/class/probe_resolve_traversal_r16.txt`: the engine's `getattr(plan.process, "resolve_services")` finds no
  hook on either).
- Unblocks when: graphed merges the resolve walk and `ci.yml`'s `GRAPHED` pin moves to that commit. No executors
  change is needed then; the engine already prefers `graphed.services.resolve_services`.
