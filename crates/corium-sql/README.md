# corium-sql

SQL execution and guarded autocommit mutation planning over immutable Corium
database values.

## What it does

Projects a Corium `Db` value as SQL tables, runs queries with DataFusion, and
translates a deliberately small DML subset into ordinary Corium transaction
forms:

- **`SqlSession`** — captures one `Db` time view and answers SQL queries.
- **Current / as-of / since** views expose one **wide table per attribute
  namespace** (e.g. `artist`), with cardinality-many attributes as list-valued
  columns.
- **History** views expose normalized **event relations** instead.
- A Corium-owned row API (`SqlRow`, `SqlColumn`, `SqlValue`, `SqlType`) so
  callers get typed rows without depending on Arrow directly.
- **`SqlMutation`** — plans `INSERT`, `UPDATE`, or `DELETE` against a current
  basis, exposes the transaction forms and expected basis, and produces
  `RETURNING` rows after the caller commits through a transactor.

`SqlSession::query` remains read-only by construction. `SqlSession::mutation`
is a separate planning path so this crate never bypasses the transactor,
authentication, authorization, or durability path.

## Mutation subset

- Existing `corium.<namespace>` projections only; no DDL or schema writes.
- Current views only, one autocommit statement at a time.
- `INSERT` requires an explicit column list. Omitting `e` allocates a tempid.
- `UPDATE` supports one plain target table and SQL expressions in assignments
  and predicates. `NULL` clears a scalar; an `ARRAY[...]` replaces a
  cardinality-many value.
- `DELETE` retracts the target namespace's attributes, not the entire entity.
- All three operations support `RETURNING`.
- A basis fence makes a stale read/modify/write plan fail instead of applying
  it to a newer database.

Joined/multi-table mutations, conflict clauses, ordered/limited mutations, new
keyword interning, explicit transaction blocks, and DDL are deferred.

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
DataFusion. Queries and mutation planning run peer-local against a fixed
immutable view; only the normal transactor path commits the generated forms.
The read-only query API is used by the `corium sql` shell, while
`corium-pgwire` connects mutation planning to a write-capable catalog. See
[`docs/sql.md`](../../docs/sql.md) and
[ADR-0011](../../docs/adr/0011-sql-interface.md), and
[ADR-0015](../../docs/adr/0015-guarded-autocommit-sql-dml.md).
