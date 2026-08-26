# SQL interface

Corium's SQL interface executes queries inside a peer against immutable `Db`
values. It does not turn the storage model into tables. A separate mutation
planner translates a supported DML subset into ordinary transaction forms;
only a write-capable adapter sends those forms through the transactor.

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
the remaining execution. This `query` API rejects DDL, DML, and
session-mutating SQL statements.

## Mutations

`SqlSession::mutation` and `mutation_params` plan one `INSERT`, `UPDATE`, or
`DELETE` against a current view. A plan contains ordinary Corium transaction
forms and the basis they were derived from. The caller submits both through
the normal authenticated and authorized transactor path, then supplies the
committed `Db` and tempid map to `SqlMutation::finish` for any `RETURNING`
rows.

The initial writable subset is intentionally narrow:

- Only existing `corium.<namespace>` wide projections are writable.
  `corium_sys`, history/as-of/since views, DDL, and schema changes are
  read-only.
- Each statement is autocommit. The expected-basis fence rejects a stale
  read/modify/write plan before commit.
- `INSERT` requires an explicit column list and supports `VALUES` or a query
  source. Omit `e` for a Corium tempid; an explicit `e` must not already occur
  in that namespace projection. A `NULL` input omits that attribute.
- `UPDATE` supports a single plain target table, predicates, expressions, and
  `RETURNING`. Assigning `NULL` clears a cardinality-one attribute. Assigning
  `ARRAY[...]` replaces the full cardinality-many set.
- `DELETE` supports a single plain target table, predicates, and `RETURNING`.
  It retracts every attribute in the target namespace, but preserves attributes
  belonging to other namespaces on the same entity.
- `RETURNING` is supported for all three operations. Delete rows come from the
  pre-commit snapshot; insert and update rows come from the committed value.

Joined and multi-table mutations, conflict clauses/upserts, ordered or limited
mutations, new keyword interning, and DDL are deferred. The planning API still
produces one statement at a time; `corium-pgwire` can compose those plans into
one atomic explicit transaction. This is relational mutation over Corium's
projection, not an attempt to make entity namespace membership into a table
ownership rule.

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
- `corium_sys.sagas` is the saga registry (ADR-0023): one row per saga, with
  its id, status, opening basis, owner, deadline, whether its reservation set
  is sealed, and counts of what it declares. Statuses render as the bare name
  (`open`, `committed`, `aborted`, `expired`); a status the engine does not
  define renders as its whole keyword, so it can never pass for one of them.
- `corium_sys.saga_compensations` is the external-compensation ledger, one row
  per entry, joined to `corium_sys.sagas` on `saga_id`. The engine never
  executes an entry — it is the orchestrator's own record of reverse progress
  outside the database, and it outlives the saga that prompted it.

The two saga relations fold current values, so they are absent from a history
session, where a saga's assertions and retractions sit side by side and "the
status" is not a question with one answer; read `corium_sys.datoms` there.

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
\as-of t       fix subsequent sessions at t (or a UTC timestamp,
              e.g. \as-of 2026-07-25T09:30:00Z)
\since t       use a since view (also accepts a timestamp)
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

The same SQL is reachable by standard PostgreSQL clients through the
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

Reads run through the same `SqlSession` as the shell. A write-capable
`DbCatalog` additionally commits the mutation subset above through its Corium
transactor connection; a catalog that does not implement `transact` remains
read-only. The server supports both simple and extended query sub-protocols,
including PostgreSQL `$1` bound inputs. Common scalar parameters accept text
and binary input encodings, including ISO 8601 `timestamptz` text with an
explicit UTC offset. Results support both text and binary encodings. Array
inputs are not yet supported.

Explicit `BEGIN` blocks pin the first database snapshot. DML is staged against
a provisional value, so later reads and writes see earlier changes;
`ROLLBACK` discards the forms and `COMMIT` submits them through one guarded
Corium transaction. A concurrent basis change fails the commit with SQLSTATE
`40001`. `SET`, `RESET`, and `DISCARD` remain compatibility no-ops.

Hibernate ORM 7.4 with PgJDBC 42.7 is exercised by the runnable
[`postgres-hibernate`](../examples/postgres-hibernate/README.md) example. It
performs generated-id insert, entity reads, dirty update, HQL query, and delete
through ordinary Hibernate transactions, with automatic PostgreSQL dialect
detection. The server answers the PgJDBC metadata probes that bootstrap this
path. Broader `pg_catalog` introspection, DDL-based schema management,
savepoints, COPY, sequences, and array bind inputs are not yet implemented.

`corium postgres-server` is read-only by default. Pass `--allow-writes` to
enable the mutation path explicitly:

```console
corium postgres-server --listen 127.0.0.1:5432 --allow-writes
```

Pass `--password` to require one shared cleartext password. TLS is not
terminated by the server, so front it with a proxy when transport security is
needed. The SQL dialect is DataFusion's, not PostgreSQL's — wire compatibility
does not imply `pg_catalog` or dialect compatibility. See
[ADR-0013](adr/0013-postgres-wire-interface.md).

### Authentication and authorization

`postgres-server` takes the same `--serve-token`, `--oidc-*`, and `--authz-db`
flags as `peer-server`. Once any of them is set, **the password field carries
the caller's own bearer token** — `PostgreSQL` has no separate token field —
and the startup `user` is informational:

```console
corium postgres-server --listen 127.0.0.1:5432 \
  --oidc-issuer https://issuer.example --oidc-audience corium \
  --authz-db corium.authz --storage-key file:/etc/corium/pii.key
psql "host=127.0.0.1 port=5432 dbname=people user=alice password=$JWT"
```

> **The token travels in cleartext.** `postgres-server` does not terminate
> TLS — it rejects `--tls-cert`/`--tls-key` rather than accepting flags it
> cannot honour — so a client's bearer token crosses the wire unencrypted.
> Run it behind a TLS-terminating proxy, or bind it to loopback. The server
> prints this warning at startup whenever authentication is configured.

Every statement is then authorized as that principal: `SELECT` needs `Query`,
DML needs `Transact`, and `SHOW DATABASES` lists only what the principal may
inspect. Reads are answered through the principal's own view — a column its
policy hides keeps its declared type and reports `NULL`, and never takes a
pushed-down predicate — and through its own protection class keys, so one
key-holding server serves principals with different entitlements from one
database value. Pass `--key-policy server-wide` to keep the pre-`ADR-0021`
behaviour of hydrating every authorized caller with the server's whole keyring.
A principal whose view hides attributes may not write. See
[ADR-0021](adr/0021-contextual-read-authorization.md) and
[auth.md](design/auth.md).

Writes still *commit* through the catalog's own `corium-peer` connection, so
the transactor additionally applies that connection's bearer principal. TLS
termination and PostgreSQL role/catalog semantics remain separate work.

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
[ADR-0011](adr/0011-sql-interface.md); the guarded write path is recorded in
[ADR-0015](adr/0015-guarded-autocommit-sql-dml.md).
