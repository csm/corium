# corium-query

The Corium query engine: EDN Datalog, Pull, the entity API, and direct index
access — executing on the peer against immutable `Db` values.

## What it does

Everything a peer needs to answer queries locally:

- **Datalog** — an EDN parser, compiler, planner, and executor for `:find` /
  `:where` queries with rules, aggregates, predicates, and function calls.
- **Pull** — `pull` / `pull_many` for hierarchical entity projection.
- **Entity API** — the lazy, navigable `Entity` view over an id.
- **Direct index access** — `datoms`-style scans over the covering indexes.
- **Query cache** (`QueryCache`) and a planner that uses `Db` statistics.
- **Boundary** helpers converting between EDN and `Value`.

## Dependencies

- `corium-core` — values, keywords, datoms.
- `corium-db` — the immutable `Db` values queries run against.
- `thiserror` for errors; `criterion`/`proptest`, `corium-log`, `corium-tx`
  (dev) for benches and conformance/property tests.

Pure, synchronous library code — no async or network dependencies, so the
engine is fully testable in the deterministic simulator. Custom function/
predicate resolution is exposed as a seam that `corium-cljrs` fills with
sandboxed cljrs evaluation.

## Architecture

All query execution happens on the peer, against an immutable `Db` value — the
engine never talks to the transactor. A query is parsed to an AST, compiled and
planned (join order informed by per-attribute statistics), then executed by
scanning the covering indexes and threading bindings through frames. Rules,
aggregates, and Pull are layered on the same execution core. Because the input
is an immutable value, results are stable for the life of that value and can be
memoized in the `QueryCache`. See
[`docs/design/query-engine.md`](../../docs/design/query-engine.md).
