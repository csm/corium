# ADR-0013: PostgreSQL wire-protocol front end for read-only SQL

**Status:** Accepted (2026-07-22)

## Context

`corium-sql` (ADR-0011) executes read-only SQL locally against an immutable
`corium_db::Db` value, and the `corium sql` shell exposes it interactively.
That shell is Corium-specific: it requires the `corium` binary and speaks a
bespoke REPL. A large ecosystem of tools — `psql`, JDBC/ODBC drivers,
`psycopg`, `pgx`, and BI front ends — instead speaks the PostgreSQL v3 wire
protocol. Exposing the existing SQL projection over that protocol makes those
tools work against Corium without a new client library, while keeping the
storage model and query location unchanged.

## Decision

A new crate, `corium-pgwire`, implements the PostgreSQL v3 frontend/backend
protocol and answers every query by running it through a `corium_sql::SqlSession`
built from a `Db` value supplied by a `DbSource`. The read-only guarantee is
inherited rather than re-implemented: `SqlSession` already rejects DDL, DML,
and session-mutating statements, so the wire server adds no write path.

Scope of the first implementation:

- **Both query sub-protocols.** The simple protocol (`Query`, including
  multiple semicolon-separated statements) and the extended protocol
  (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`/`Close`/`Flush`) for statements
  without bound parameters, so drivers that always prepare paramless queries
  work.
- **Text format only.** Results are sent in the PostgreSQL text wire format.
  Bound parameters and the binary result format are reported as
  `feature_not_supported` errors rather than silently mishandled. Substituting
  bind parameters into SQL text safely is deferred; the immutable model makes
  read-only parameterization a pure future optimization, not a correctness gap.
- **Trust or cleartext-password authentication.** TLS and GSSAPI negotiation
  are declined during startup; transport security, if needed, is terminated
  ahead of the server. This mirrors the optional-auth posture of ADR-0012 and
  keeps the crate free of a TLS dependency.
- **Stateless control-statement no-ops.** `BEGIN`, `COMMIT`, `ROLLBACK`,
  `SET`, `RESET`, and `DISCARD` are accepted as no-ops. Each query already sees
  one immutable snapshot, so an explicit transaction block spans nothing the
  server can violate, and accepting these lets standard clients and pools
  connect cleanly.
- **Type mapping to `pg_type` OIDs.** Corium's `SqlValue`/`SqlType` map to
  PostgreSQL types: integers to `int2`/`int4`/`int8`, 64-bit unsigned entity
  ids to `numeric` (lossless in text), floats to `float4`/`float8`, instants to
  `timestamptz` in UTC, bytes to hex `bytea`, and cardinality-many lists to the
  matching array type.

The crate depends only on `corium-sql`, `corium-db`, `corium-core`, and
`tokio`; it does not depend on `corium-peer`, so the database source is a
trait. The `corium postgres-server` CLI command connects a peer and serves the
protocol with `move || connection.db()` as the source, mirroring how
`corium peer-server` hosts one database.

## Consequences

- Standard PostgreSQL clients can query Corium read-only, over the same
  peer-local, immutable-snapshot execution fixed by ADR-0001, ADR-0004, and
  ADR-0011.
- The protocol front end owns no storage or planning logic; it is a thin
  adapter over `SqlSession`, so SQL semantics stay defined in one place.
- Parameterized prepared statements and the binary format are explicit,
  reported limitations rather than partial behaviors; both can be added later
  without changing the wire contract already exposed.
- Corium's SQL dialect is DataFusion's, not PostgreSQL's. Wire compatibility
  does not imply dialect compatibility, and client-issued catalog probes that
  assume `pg_catalog` are not served.
