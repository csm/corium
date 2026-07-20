# ADR-0011: Peer-local read-only SQL over namespace projections

**Status:** Accepted (2026-07-19)

## Context

Corium stores entity/attribute/value datoms rather than tables. SQL access
must therefore define a relational projection without changing the storage
model or moving query execution to the transactor. Reimplementing SQL on top
of the Datalog AST would also require independently implementing SQL's bag,
NULL, ordering, window, and nested-value semantics.

## Decision

`corium-sql` embeds Apache DataFusion and executes locally against an immutable
`corium_db::Db` value. It is read-only. A session captures exactly one current,
as-of, since, or history view.

For current, as-of, and since views, schema attribute idents are grouped by
keyword namespace. Each namespace becomes a table in the `corium` SQL schema,
each attribute name becomes a column, and each entity with at least one value
in that namespace becomes a row. The entity id is available as `e`. Namespace
and attribute names are preserved exactly; names requiring SQL escaping must be
double-quoted rather than normalized into lossy aliases.

Cardinality-one attributes are nullable scalar columns. Cardinality-many
attributes are non-null Arrow `List<T>` columns: missing values are empty
lists, values are unique, and their deterministic Corium ordering is not
domain-significant. SQL therefore gets array syntax and functions while the
column retains set semantics.

The `corium_sys` schema exposes normalized `datoms`, `attributes`, and `idents`
relations. `datoms` uses one nullable value column per Corium value type rather
than a tagged JSON/string encoding. History sessions initially expose only
these event relations.
A later version may add wide history tables as entity states over half-open
`[_valid_from_t, _valid_to_t)` intervals.

The Rust API exposes Corium-owned column, type, and value types. Arrow remains
the internal execution format; a separately feature-gated Arrow adapter may be
added without making Arrow part of the default compatibility contract.

The workspace minimum Rust version is 1.88, shared by DataFusion 54 and the
toolchain used to publish Turso 0.7.

## Consequences

- SQL preserves the peer-local scaling and immutable-snapshot behavior fixed
  by ADR-0001 and ADR-0004.
- Namespace tables are projections, not exclusive entity types: one entity
  may appear in several tables.
- Wide-row construction pushes scalar comparisons and array-membership
  predicates into AVET when the attribute is covered and falls back to a
  bounded AEVT attribute scan otherwise. DataFusion rechecks pushed predicates.
- SQL DML and SQL transaction syntax are rejected; writes continue through
  Corium transaction data.
- History starts with an exact event model rather than an ambiguous wide-row
  encoding.
