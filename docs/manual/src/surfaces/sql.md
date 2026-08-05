# SQL shell and PostgreSQL server

Corium executes SQL inside a peer, against immutable database values. SQL does
not change the storage model into tables. The relations are a projection.

The SQL dialect is the DataFusion dialect. Wire compatibility with PostgreSQL
does not imply dialect compatibility or `pg_catalog` compatibility.

## The relational projection

Attributes are grouped by keyword namespace. Given `:artist/name`,
`:artist/country`, and `:artist/tags`, SQL sees:

```text
corium.artist(e BIGINT, name TEXT, country TEXT, tags LIST<TEXT>)
```

The rules of the projection are:

- `e` is the Corium entity id, and the name is reserved.
- A cardinality-one column is a nullable scalar.
- A cardinality-many column is a non-null list. An absent attribute is an
  empty list. Values are unique and ordered deterministically, but the order
  carries no meaning.
- One entity can occur in several namespace tables. These are projections, not
  entity types.
- An attribute without a namespace is grouped in `corium._global`.
- Names are preserved exactly. Use double quotes for a name such as
  `release-group`.

Three system relations are available in every view.

| Relation | Content |
|---|---|
| `corium_sys.datoms` | `e`, `a`, `attr`, typed value columns, `tx`, `t`, `added`. |
| `corium_sys.attributes` | The schema. |
| `corium_sys.idents` | Entity id to keyword ident. |

> **Partly implemented.** A history session exposes `corium_sys` relations
> only. Wide history tables are reserved for a later validity-interval design.

## The SQL shell

```sh
corium sql people
corium sql people -c "SELECT * FROM corium.artist LIMIT 10"
corium sql people -f report.sql
```

An interactive statement ends with a semicolon. Each statement captures a
fresh current database value, unless a time view is selected. `Ctrl-C` drops
the running query.

The shell is read-only.

| Command | Effect |
|---|---|
| `\as-of <t>` | Fix later sessions at `<t>`, or at a UTC timestamp. |
| `\since <t>` | Use a since view. Timestamps are accepted. |
| `\history on` | Expose history events. |
| `\history off` | Return to the current view. |
| `\current` | Return to the current view. |
| `\basis` | Print the basis and the view. |
| `\dt` | List relations. |
| `\d <table>` | Print the result columns of a relation. |
| `\timing on` | Report execution time. |
| `\q` | Quit. |

List functions come from DataFusion:

```sql
SELECT e, name FROM corium.artist WHERE array_has(tags, 'ambient');
```

The shell takes no key flag, so it prints `<redacted>` for a value on a
[protected attribute](../security/protection.md).

## The PostgreSQL wire server

```sh
corium postgres-server --listen 127.0.0.1:5432
```

One server exposes the whole database catalog of the transactor. A connection
picks its database with the standard startup `database` parameter. It can
switch at any time with `USE <database>`. `SHOW DATABASES` lists what is
available.

```sh
psql 'host=127.0.0.1 port=5432 dbname=people' \
  -c "SELECT e, name FROM corium.person ORDER BY name LIMIT 10"
```

| Flag | Default | Effect |
|---|---|---|
| `--listen <addr>` | `127.0.0.1:5432` | Listen address. |
| `--database <name>` | All | Restrict the exposed set. Repeatable. |
| `--password <secret>` | None | Require this cleartext password. Ignored once authentication is configured. |
| `--allow-writes` | Off | Enable guarded DML. |

The server also takes the [connection flags](../running/catalog.md), the
[serving flags](../security/authentication.md), and `--storage-key`.

Databases are opened lazily and cached. One peer connection is shared by every
client that uses that database.

The server supports the simple and the extended query sub-protocols, including
`$1` bound inputs. Common scalar parameters accept text and binary encodings.
Results support both encodings.

> **Not implemented.** Array inputs are not supported on the wire.

## Writes through SQL

`corium postgres-server` is read-only by default. `--allow-writes` enables a
narrow DML subset.

```sh
corium postgres-server --listen 127.0.0.1:5432 --allow-writes
```

In autocommit each statement is one transaction. An expected-basis fence
rejects a stale read-modify-write plan before it commits.

- Only existing `corium.<namespace>` projections are writable. `corium_sys`,
  the time views, DDL, and schema changes are read-only.
- `INSERT` requires an explicit column list. It supports `VALUES` or a query
  source. Omit `e` for a tempid. An explicit `e` must not already occur in
  that projection. A `NULL` input omits the attribute.
- `UPDATE` supports one plain target table, predicates, expressions, and
  `RETURNING`. Assigning `NULL` clears a cardinality-one attribute. Assigning
  `ARRAY[...]` replaces the whole cardinality-many set.
- `DELETE` supports one plain target table, predicates, and `RETURNING`. It
  retracts every attribute in the target namespace, and it preserves
  attributes of other namespaces on the same entity.
- `RETURNING` works for all three. Delete rows come from the pre-commit
  snapshot. Insert and update rows come from the committed value.

> **Not implemented.** Joined and multi-table mutations, conflict clauses and
> upserts, ordered or limited mutations, new keyword interning, and DDL are
> deferred.

## Explicit transactions

An explicit `BEGIN` block pins the database value of its first statement. DML
is staged against a provisional value, so a later statement in the block reads
what the earlier ones wrote.

`ROLLBACK` discards the staged forms. `COMMIT` submits them as one atomic
Corium transaction. A concurrent basis change fails the commit with SQLSTATE
`40001`, which a client reads as a serialization failure and retries.

`SET`, `RESET`, and `DISCARD` are compatibility no-ops.

## Object-relational mappers

The server answers the PgJDBC metadata probes for SQL keywords, current schema
and catalog, and transaction isolation. Hibernate therefore selects its
PostgreSQL dialect on its own.

The runnable
[`postgres-hibernate`](https://github.com/csm/corium/blob/main/examples/postgres-hibernate/README.md)
example exercises Hibernate ORM 7.4 with PgJDBC 42.7. It inserts with a
generated id, reads, updates, runs an HQL query, and deletes. Every step uses
an ordinary Hibernate transaction.

> **Not implemented.** Broader `pg_catalog` introspection, DDL-based schema
> management, savepoints, `COPY`, and sequences are absent. Declare the schema
> with [`corium schema update`](../running/schema.md) rather than with the
> schema tool of the mapper.

## Security of the wire server

> **CAUTION: The PostgreSQL wire server does not terminate TLS.** It rejects
> `--tls-cert` and `--tls-key` rather than accept flags it cannot honor. Put a
> TLS-terminating proxy in front of it, or bind it to loopback.

Restrict the exposed set with `--database` when only some databases must be
reachable.

### A SQL client is a Corium principal

Set any of `--serve-token`, `--oidc-*`, or `--authz-db`, and the server
authenticates each client for itself.

PostgreSQL has no bearer-token field, so **the password field carries the
token of the caller**. The startup `user` is informational.

```sh
corium postgres-server --listen 127.0.0.1:5432 \
  --oidc-issuer https://issuer.example --oidc-audience corium \
  --authz-db corium_authz

psql "host=127.0.0.1 port=5432 dbname=people user=alice password=$JWT"
```

> **CAUTION: The token crosses the wire in the clear.** The server prints this
> warning at startup whenever authentication is configured.

Every statement is then authorized as that principal. `SELECT` needs `query`.
DML needs `transact`. `SHOW DATABASES` lists only what the principal can
inspect.

Reads are answered through the view of the principal and through its own
protection class keys. A column that the policy hides keeps its declared type,
reports `NULL`, and never takes a pushed-down predicate. A principal whose
view hides attributes cannot write. Read
[authorization](../security/authorization.md) and
[attribute protection](../security/protection.md).

`--password` still applies when no authentication flag is set. It is one
shared secret and it maps to no principal.

> **Partly implemented.** A write still *commits* through the peer connection
> of the server, so the transactor additionally applies the bearer principal
> of that connection. Give that connection an identity that can transact every
> database the server exposes.
