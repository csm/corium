# SQL interface

Corium's SQL interface executes read-only queries inside a peer, against one
immutable `Db` value. It does not turn the storage model into tables and it
does not send SQL to the transactor.

## Rust API

Add `corium-sql` and create a session from the database view to query:

```rust,ignore
use corium_sql::SqlSession;

let session = SqlSession::new(&db.as_of(42))?;
let mut result = session
    .query("SELECT e, name FROM corium.artist ORDER BY name")
    .await?;

while let Some(row) = result.next_row().await? {
    println!("{row:?}");
}
```

`SqlSession` fixes both the basis and time view. Results stream as Corium-owned
`SqlColumn`, `SqlType`, and `SqlValue` values, keeping DataFusion and Arrow out
of the default public compatibility contract. Dropping a result stream cancels
the remaining execution. DDL, DML, and session-mutating SQL statements are
rejected.

## Relational projection

For current, as-of, and since sessions, attributes are grouped by keyword
namespace. Given `:artist/name`, `:artist/country`, and `:artist/tags`, SQL gets:

```text
corium.artist(e BIGINT, name TEXT, country TEXT, tags LIST<TEXT>)
```

The projection has these rules:

- `e` is the Corium entity id and is reserved.
- Cardinality-one columns are nullable scalars.
- Cardinality-many columns are non-null Arrow lists. An absent attribute is an
  empty list; values are unique and deterministically ordered, but list order
  is not domain-significant.
- An entity can occur in several namespace tables. These are projections, not
  entity-type declarations.
- Attributes without a namespace are grouped in `corium._global`.
- Namespace and attribute names are preserved exactly. Use SQL double quotes
  for names such as `release-group` rather than relying on normalized aliases.

List functions are available through DataFusion, for example:

```sql
SELECT e, name
FROM corium.artist
WHERE array_has(tags, 'ambient');
```

All views expose normalized metadata and fact relations:

- `corium_sys.datoms` contains `e`, `a`, `attr`, typed value columns, `tx`,
  `t`, and `added`.
- `corium_sys.attributes` describes the Corium schema.
- `corium_sys.idents` maps entity ids to keyword idents.

A history session initially exposes only `corium_sys` relations, so additions
and retractions remain unambiguous events. Wide history tables are reserved for
a later validity-interval design.

## CLI shell

Connect to the same peer-local database used by the Datalog console:

```console
corium sql my-database
corium sql my-database -c "SELECT * FROM corium.artist LIMIT 10"
corium sql my-database -f report.sql
```

Interactive statements end with a semicolon. The shell understands:

```text
\as-of t       fix subsequent sessions at t
\since t       use a since view
\history on    expose history events
\history off   return to the current view
\current       return to the current view
\basis         show basis and view
\dt            list relations
\d table       show the result columns for a relation
\timing on     show execution time
\q             quit
```

Each statement captures a fresh current `Db` value unless a time view is
selected. Pressing Control-C drops the running query future.

## PostgreSQL wire-protocol server

The same read-only SQL is reachable by standard PostgreSQL clients through the
`corium-pgwire` crate and the `corium postgres-server` command:

```console
corium postgres-server --listen 127.0.0.1:5432
psql 'host=127.0.0.1 port=5432 dbname=my-database' \
  -c "SELECT e, name FROM corium.artist ORDER BY name LIMIT 10"
```

One server exposes the transactor's whole database catalog. A connection picks
its database with the standard startup `database` parameter (`psql -d …`) and
can switch at any time with `USE <database>`; `SHOW DATABASES` lists what is
available. Databases are opened lazily and cached, so one peer connection is
shared across all clients using it. Restrict the exposed set with one or more
`--database <name>` flags.

Every query runs through the same `SqlSession` as the shell, so DDL, DML, and
session-mutating statements are rejected. The server speaks the v3 protocol in
the text format and supports both the simple and extended query sub-protocols;
bound parameters and the binary result format are not supported and are
reported as errors. Stateless control statements (`BEGIN`, `COMMIT`,
`ROLLBACK`, `SET`, `RESET`, `DISCARD`) are accepted as no-ops, since each query
already sees one immutable snapshot.

Pass `--password` to require a cleartext password; TLS is not terminated by the
server, so front it with a proxy when transport security is needed. The SQL
dialect is DataFusion's, not PostgreSQL's — wire compatibility does not imply
`pg_catalog` or dialect compatibility. See
[ADR-0013](adr/0013-postgres-wire-interface.md).

## Engine choice and tradeoffs

The implementation embeds DataFusion. This provides mature SQL semantics,
optimizers, functions, joins, aggregates, and an Arrow execution engine while
letting Corium implement tables as peer-local providers. It also raises compile
time and binary size, and makes DataFusion-to-Corium predicate translation an
explicit optimization layer.

Two alternatives were rejected for the initial implementation:

- A SQLite/Turso virtual-table adapter would produce a familiar SQL dialect
  and potentially reuse an existing dependency, but its scalar cell model is a
  poor fit for typed cardinality-many values and the virtual-table ABI would
  dominate the provider design.
- Translating SQL into Corium Datalog would reuse some planning machinery, but
  faithfully implementing SQL NULL, bag, ordering, window, and nested-value
  semantics would effectively create a second SQL engine.

Wide providers materialize Arrow batches at scan time, not session creation.
Entity-id equality uses EAVT lookups. Scalar equality and range comparisons,
plus `array_has`, produce candidate entity sets through AVET for indexed/unique
attributes and bounded AEVT scans otherwise. DataFusion rechecks every pushed
predicate for safety. The next performance steps are projection-aware row
assembly and provider statistics. An optional Arrow batch adapter can then be
added without changing the row API.

The decisions and longer-term history model are recorded in
[ADR-0011](adr/0011-sql-interface.md).
