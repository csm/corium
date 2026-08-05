# PostgreSQL Hibernate example

This example maps a Jakarta Persistence entity onto the existing
`corium.person` namespace projection and runs it with Hibernate ORM over the
PostgreSQL wire server. It verifies generated identity values, insert and
find, dirty-checked update, an HQL prepared query, delete, and transaction
commit behavior.

Run the self-contained example from the repository root:

```sh
examples/postgres-hibernate/run.sh
```

The harness builds `corium`, starts an in-memory transactor, creates a database
from [`schema.toml`](schema.toml), starts a write-enabled PostgreSQL server,
and invokes the Maven application. It removes the temporary deployment when
it exits and prints server logs on failure.

Java 17 or newer and Maven are required. The example uses Hibernate ORM 7.4,
the current stable series as of August 2026, and PgJDBC 42.7. Override the
listen addresses with `CORIUM_TRANSACTOR_HOST`, `CORIUM_TRANSACTOR_PORT`,
`CORIUM_POSTGRES_HOST`, and `CORIUM_POSTGRES_PORT`. Set `CORIUM_SKIP_BUILD=1`
to reuse an existing debug binary.

To run only the Java client against an existing server:

```sh
cd examples/postgres-hibernate
CORIUM_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/people \
  mvn compile exec:java
```

The mapped table must already exist as a Corium schema projection. Hibernate
DDL generation is disabled because the pgwire surface currently exposes SQL
DML, not schema migration DDL.
