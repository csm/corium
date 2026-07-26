# ADR-0015: Guarded autocommit SQL DML through the transactor

**Status:** Accepted (2026-07-24)

## Context

ADR-0011 made SQL a read-only peer-local projection, and ADR-0013 exposed it
over PostgreSQL's wire protocol. That is useful for exploration and BI, but
ordinary application adoption also expects parameterized CRUD. Sending writes
directly to a peer would violate Corium's topology, durability, and
authorization model. Translating a read/modify/write statement against one
snapshot and committing it against a newer basis could also lose concurrent
changes.

Corium namespace tables are projections, not entity types. An entity can
appear in several namespace projections, so SQL deletion cannot safely imply
`retractEntity`.

## Decision

`corium-sql` gains a separate mutation-planning API for a bounded subset of
`INSERT`, `UPDATE`, and `DELETE` over current `corium.<namespace>` projections.
It produces ordinary transaction forms plus the exact basis used to derive
them. It does not commit. The transactor protocol accepts an optional expected
basis and rejects a mismatch before preparing or durably writing the
transaction. Because an older transactor would ignore the unknown optional
field and lose that safety property, this change advances Corium's checked
protocol version from 1 to 2; peers and transactors must be upgraded together.

`corium-pgwire` connects that planner to a new, optional `DbCatalog::transact`
operation. The CLI implementation sends the forms through its cached
`corium-peer` connection, preserving the configured Corium principal,
authorization gate, transaction validation, durability, and publication path.
Catalogs that do not implement the operation remain read-only. The CLI
PostgreSQL server also remains read-only by default; operators must pass
`--allow-writes` to opt into the shared service principal's write authority.

The first mutation contract is:

- one autocommit statement against an existing namespace projection;
- explicit insert columns, with a generated tempid when `e` is omitted;
- scalar replacement/clearing and whole-set replacement for
  cardinality-many update columns;
- namespace-scoped delete, preserving other attributes on the entity;
- `RETURNING` for insert, update, and delete;
- typed PostgreSQL bind inputs for common scalar types.

Writes in an explicit `BEGIN` block are rejected. This is preferable to
silently autocommitting them while claiming transaction semantics. DDL,
schema changes, joined/multi-table mutation forms, upsert/conflict clauses,
new keyword interning, and multi-statement transactions are deferred.

PostgreSQL wire authentication remains distinct from Corium authorization.
The server's configured Corium service principal currently authorizes
transactions; mapping each PostgreSQL login to a Corium principal is future
authn/authz work.

## Consequences

- PostgreSQL clients can use safe, parameterized autocommit CRUD without a
  second write engine or a bypass around the transactor.
- Concurrent snapshot races fail as serialization conflicts rather than
  overwriting newer state. Clients may retry the whole statement.
- SQL delete semantics are explicitly projection-scoped and do not erase
  unrelated entity attributes.
- SQL remains less expressive than native transaction data and Datalog. The
  narrow subset is an adoption interface, while native APIs retain schema,
  temporal, rule, pull, recursive, and entity-oriented capabilities.
- Atomic multi-statement application workflows still require native
  transaction data until a real SQL transaction buffer is designed.
