# corium-pgwire

A PostgreSQL wire-protocol front end for Corium SQL.

## What it does

Speaks the PostgreSQL v3 frontend/backend protocol so ordinary PostgreSQL
clients (`psql`, JDBC, `psycopg`, `pgx`, BI tools) can query and mutate Corium
databases. Reads execute through [`corium-sql`](../corium-sql/README.md)'s
`SqlSession`; supported DML is planned there and committed through the
catalog's normal Corium transactor connection.

One server exposes a whole catalog of databases:

- **`serve`** — accepts connections on a `TcpListener` and handles each on its
  own task until a shutdown future resolves.
- **`DbCatalog`** — the async trait the server resolves databases through:
  `list()` enumerates the catalog and `db(name)` returns a fresh immutable
  snapshot. A write-capable implementation also provides `transact()`; its
  default implementation is read-only. Implementations open databases lazily
  and cache them, so one database (and its segment cache) is shared across all
  client connections.
- **`PgWireConfig`** — an optional required cleartext password and the reported
  `server_version`.

A connection picks its database with the standard startup `database` parameter
and can switch at any time with `USE <database>`; `SHOW DATABASES` lists the
catalog. The database is validated lazily, so a client may connect with an
unknown default (for example the conventional `postgres`) and then `USE` a real
one.

## Protocol coverage

- Startup with TLS/GSSAPI negotiation declined (`SSLRequest`/`GSSENCRequest`
  answered with `N`).
- Trust or cleartext-password authentication.
- The **simple** query sub-protocol (`Query`), including multiple
  semicolon-separated statements.
- The **extended** query sub-protocol (`Parse`/`Bind`/`Describe`/`Execute`/
  `Sync`/`Close`/`Flush`) with typed bound input parameters.
- Common scalar input parameters and results support PostgreSQL text or binary
  format. Array inputs remain unsupported.
- `INSERT`, `UPDATE`, and `DELETE` over existing namespace projections support
  `RETURNING` in autocommit or explicit transactions. Concurrent basis changes
  are reported as serialization failures.
- Explicit transactions pin their first snapshot, expose staged writes to
  later statements, discard them on `ROLLBACK`, and submit one atomic Corium
  transaction on `COMMIT`.
- PgJDBC metadata probes for SQL keywords, current schema/catalog, and
  transaction isolation are supported so Hibernate can select its PostgreSQL
  dialect automatically.
- `SET`, `RESET`, and `DISCARD` are accepted as compatibility no-ops.
- `USE <database>` switches the active database and `SHOW DATABASES` lists the
  catalog.

## Type mapping

Corium's `SqlType`/`SqlValue` are rendered into PostgreSQL types:

| Corium | PostgreSQL |
|---|---|
| `Boolean` | `bool` |
| `SignedInteger` | `int2` / `int4` / `int8` |
| `UnsignedInteger` | `int4` / `int8` / `numeric` (64-bit entity ids) |
| `Float` | `float4` / `float8` |
| `TimestampMillis` | `timestamptz` (UTC, ISO 8601) |
| `Text` | `text` |
| `Bytes` | `bytea` (hex output) |
| `List<T>` | the matching array type, e.g. `_text` |

### Parameter and result formats

Scalar parameters accept PostgreSQL text and binary encodings. Text
`timestamptz` parameters accept ISO 8601 forms and require an explicit UTC
offset because the server does not maintain a session time zone. Query results
support both text and binary formats; array parameters remain unsupported.

## Dependencies

- `corium-sql`, `corium-db`, `corium-core`, `corium-forms`, `corium-tx` — the
  database, SQL projection, and provisional transaction preparation.
- `tokio` — the async TCP server and framing I/O.
- `async-trait` — the `DbCatalog` trait; `thiserror`, `tracing`.

This is the crate behind the `corium postgres-server` command. See
[`docs/sql.md`](../../docs/sql.md#postgresql-wire-protocol-server) and
[ADR-0013](../../docs/adr/0013-postgres-wire-interface.md), and
[ADR-0015](../../docs/adr/0015-guarded-autocommit-sql-dml.md).

The CLI server remains read-only unless its operator passes `--allow-writes`.
When enabled, the catalog commits through its cached `corium-peer` connection,
so the configured Corium service principal and transactor authorization gate
remain authoritative. PostgreSQL usernames/passwords are not yet mapped to
Corium principals; per-user authentication and authorization parity is still
future work.
