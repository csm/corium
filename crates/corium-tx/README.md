# corium-tx

Pure transaction expansion, entity resolution, and validation.

## What it does

Turns transaction data into a validated, ready-to-commit set of datoms. Given a
list of operations against a database value, it:

- Expands map-form entities (`EntityMap`) into individual assertions.
- Resolves entity positions (`EntityRef`): concrete ids, transaction-local
  **tempids**, and unique-attribute **lookup refs**.
- Applies the built-in operations — `Add`, `Retract`, `Cas` (compare-and-swap),
  and `RetractEntity` (recursive component retraction).
- Validates against the schema: value types, cardinality, uniqueness, and
  component/ref rules.
- Produces a `PreparedTx` (resolved datoms plus the tempid → entity-id map).

## Dependencies

- `corium-core` — value/datom/schema types.
- `corium-db` — reads the current `Db` value to resolve lookups and check
  uniqueness against existing facts.
- `thiserror` for errors.

Pure, synchronous library code — no async, network, or storage dependencies.
The transactor drives it; database functions (`:db/fn`) that emit further ops
are expanded on top of it by `corium-cljrs`.

## Architecture

Transaction handling is a pure function of `(current Db, tx ops)` →
`PreparedTx`, with no I/O. Keeping it side-effect-free means the transactor can
run it, validate, and either commit or cleanly abort without touching storage,
and the deterministic simulator can exercise every validation path. Native
built-ins like `:db/cas` live here as Rust; sandboxed user-defined `:db/fn`
code lives in `corium-cljrs` and feeds its emitted ops back through this
expansion. See
[`docs/design/log-and-transactor.md`](../../docs/design/log-and-transactor.md).
