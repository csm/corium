# corium-sql

Read-only SQL execution over immutable Corium database values.

## What it does

Projects a Corium `Db` value as SQL tables and runs read-only queries against
them with DataFusion:

- **`SqlSession`** — captures one `Db` time view and answers SQL queries.
- **Current / as-of / since** views expose one **wide table per attribute
  namespace** (e.g. `artist`), with cardinality-many attributes as list-valued
  columns.
- **History** views expose normalized **event relations** instead.
- A Corium-owned row API (`SqlRow`, `SqlColumn`, `SqlValue`, `SqlType`) so
  callers get typed rows without depending on Arrow directly.

Writes are impossible by construction: `SQLOptions` disallow DDL/DML, matching
the immutable-value model.

## Dependencies

- `corium-core`, `corium-db` — the value being projected.
- `datafusion` + `arrow` — SQL planning/execution and the columnar batches
  produced from a `Db` view.
- `futures`, `thiserror`; `tokio` (dev) for tests.

## Architecture

SQL is a **projection layer**, not a second storage engine. The catalog derives
table schemas from Corium's own schema — attribute namespaces become tables,
attributes become columns — and streams rows out of the same covering-index
scans the Datalog engine uses, adapted into Arrow `RecordBatch`es for
DataFusion. Everything runs peer-local against a fixed immutable view, so a
session sees a consistent database and can never mutate it. This is the crate
behind the `corium sql` shell. See [`docs/sql.md`](../../docs/sql.md) and
[ADR-0011](../../docs/adr/0011-sql-interface.md).
