# corium-db

The immutable `Db` value: time views, covering-index access, naming,
per-attribute statistics, and bootstrap metadata.

## What it does

Defines the central **database value** that queries run against. A `Db` is a
value — cheap to clone, never mutated in place. It provides:

- **Time views** — `Db::as_of`, `Db::since`, and `Db::history` (via `DbView`),
  each wrapping the same recorded datoms with a different fold policy; no facts
  are copied.
- **Covering-index access** — the four indexes for a view, materialized lazily
  on first read and shared by every clone.
- **Naming** — the `Idents` registry mapping `:db/ident` keywords to entity ids.
- **Statistics** — per-attribute counts used by the query planner.
- **Bootstrap** — the reserved-id layout (`FIRST_USER_ID`) and the initial
  schema every database starts with.

## Dependencies

- `corium-core` — value/datom/schema types and encoding.
- `corium-index` (transitively, via the transactor/peer that build the index
  data a `Db` merges).

Pure, synchronous library code — no async, network, or storage dependencies.
It is consumed by `corium-tx` (validation), `corium-query` and `corium-sql`
(reads), and produced by `corium-transactor`/`corium-peer`.

## Architecture

`Db` is the immutability boundary of the whole system. A view merges persistent
index segments with the in-memory log tail up to a basis-`t`; because that
merge is pure and the value is shared by `Arc`, obtaining a database value
never blocks on the transactor and never takes a lock. Time views are just a
different fold over the same datoms, so `as-of`/`since`/`history` cost nothing
to construct. The four covering indexes for a view are built lazily and cached
in the value, so the first read pays for materialization and every clone reuses
it. See [`docs/design/time-model.md`](../../docs/design/time-model.md).
