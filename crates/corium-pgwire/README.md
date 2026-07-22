# corium-pgwire

A PostgreSQL wire-protocol front end for read-only Corium SQL.

## What it does

Speaks the PostgreSQL v3 frontend/backend protocol so ordinary PostgreSQL
clients (`psql`, JDBC, `psycopg`, `pgx`, BI tools) can run queries against a
Corium database. Every statement is executed through
[`corium-sql`](../corium-sql/README.md)'s `SqlSession`, so the read-only
guarantee is preserved end to end: DDL, DML, and session-mutating statements
are rejected exactly as they are in the `corium sql` shell.

- **`serve`** — accepts connections on a `TcpListener` and handles each on its
  own task until a shutdown future resolves.
- **`DbSource`** — supplies the immutable `Db` value each query runs against.
  A closure `Fn() -> Db` implements it directly, so the CLI wires a peer
  `Connection` in with `move || connection.db()`.
- **`PgWireConfig`** — advertised database name, an optional required cleartext
  password, and the reported `server_version`.

## Protocol coverage

- Startup with TLS/GSSAPI negotiation declined (`SSLRequest`/`GSSENCRequest`
  answered with `N`).
- Trust or cleartext-password authentication.
- The **simple** query sub-protocol (`Query`), including multiple
  semicolon-separated statements.
- The **extended** query sub-protocol (`Parse`/`Bind`/`Describe`/`Execute`/
  `Sync`/`Close`/`Flush`) for statements **without** bound parameters.
- All results use the **text** wire format. Bound parameters and the binary
  result format are reported as errors (`feature_not_supported`).
- Stateless control statements (`BEGIN`, `COMMIT`, `ROLLBACK`, `SET`, `RESET`,
  `DISCARD`) are accepted as no-ops so clients connect cleanly against the
  single immutable snapshot each query sees.

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

## Dependencies

- `corium-sql`, `corium-db`, `corium-core` — the database and its SQL
  projection.
- `tokio` — the async TCP server and framing I/O.
- `thiserror`, `tracing`.

This is the crate behind the `corium postgres-server` command. See
[`docs/sql.md`](../../docs/sql.md#postgresql-wire-protocol-server) and
[ADR-0013](../../docs/adr/0013-postgres-wire-interface.md).
