# Query Engine

Queries execute **on the peer**, against an immutable `Db` value. The engine
lives in `corium-query`, pure library code with no I/O beyond the segment
fetches its iterators trigger.

## Surface

Full EDN Datalog in Datomic's dialect, plus Pull and the entity API:

- `:find` — relation, collection `[?x …]`, single tuple `[?x ?y]`, scalar
  `?x .`; aggregates `(count ?x)`, `count-distinct`, `sum`, `avg`, `median`,
  `min`/`max` (n-arity variants), `rand`/`sample`, custom via cljrs (post-v1).
- `:in` — scalars, tuples, collections `[?x …]`, relations `[[?x ?y]]`,
  database args `$`, `$hist`, rule sets `%`; multiple databases per query.
- `:where` — data patterns (any position wildcard/constant/var, optional
  leading db), predicate clauses `[(< ?a ?b)]`, function clauses
  `[(f ?x) ?y]` with tuple/collection/relation destructuring, `not`/`not-join`,
  `or`/`or-join`, rules (recursive, with `[?required …]` heads).
- `:with` — bag semantics for aggregation.
- **Pull**: full pattern grammar — attribute specs, `'*'`, reverse refs
  `:_attr`, nesting, `{:limit n}`/`:default`/`:as`, recursion `{:friend 6}` /
  `'…'`, component auto-recursion. Usable standalone (`pull`, `pull-many`) and
  as a `:find` spec `(pull ?e pattern)`.
- **Entity API**: lazy map-like `Entity` over EAVT with reverse-ref keys and
  component navigation (Rust API; the natural idiom in cljrs).
- **Direct index access**: `datoms(index, components…)`,
  `seek-datoms`, `index-range` — the escape hatch that makes the engine
  honest (anything the planner does badly, users can do by hand).

Query forms arrive as EDN (parsed with `cljrs-reader` at the boundary) or are
built with a typed Rust builder. Parsed queries are cached by value (the map
form is the cache key), as in Datomic.

## Compilation and planning

Pipeline: EDN → AST → validation/rewrites → logical plan → physical plan.

1. **Validation & normalization**: unbound-var checks, `or` branch variable
   agreement, rule head arity, rewrite map-form sugar, push constants into
   patterns.
2. **Clause ordering**: greedy selectivity ordering — start from the most
   selective bound pattern (constants in `a`+`v` on AVET beat `a`-only on
   AEVT, etc.), then repeatedly pick the clause with the most bound variables
   / smallest estimated fan-out. Estimates come from cheap index statistics
   (datom counts per attribute, sampled distinct-value counts) carried in
   tree metadata. Explicit clause order is respected as a tiebreak, matching
   the Datomic performance model users know.
3. **Physical operators**: index scans (pattern → best index + prefix),
   hash joins on shared variables, leapfrog-style merge join for
   multi-pattern star joins on the same sorted prefix (post-v1 optimization;
   the operator interface allows it), predicate/function filters, `not` as
   anti-join, `or` as union of subplans, aggregation as final grouping pass.
4. **Rules**: semi-naive fixpoint evaluation with per-rule memo tables;
   non-recursive rules inline as subplans.

The executor is iterator/batch-based (small tuple batches for cache
friendliness), synchronous, and cancellable (a fuel/deadline check per batch —
the same mechanism bounds runaway queries on peer servers).

## Time-view semantics

`as-of`/`history`/`since` views plug in beneath the scan operators (they
change which tree + filter a scan uses — see time-model.md). `history` scans
bind an extra `?added` pattern position (5-element patterns), exactly as in
Datomic.

## cljrs interop in queries

Predicate/function clauses resolve in this order: (1) built-in native set
(comparisons, arithmetic, `get-else`, `ground`, `missing?`, `tuple`/`untuple`,
string ops…), (2) cljrs functions via the sandbox host, under the same fuel
limits as database functions. The seam is the `ExternCall` hook on
`ExecOptions` (wired to the sandbox by `corium-cljrs::query` at M5, with
explicit registration); resolving fn clauses to user code stored in the
database remains post-v1. The native set covers the overwhelming majority of
real queries.

## Performance posture (v1 targets, not promises)

- Point lookup (entity by unique attr) — a few segment reads, sub-ms warm.
- The planner must never produce a full-index scan when any pattern has a
  bound `a` — enforced by test.
- Benchmarks in-repo from M3 on (criterion): musicbrainz-style dataset,
  pull-heavy and join-heavy suites, tracked per-commit.
