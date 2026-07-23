# PostgreSQL JDBC example

This Maven project runs ordinary Java JDBC queries against Corium's read-only
PostgreSQL wire-protocol server. Its one-command test harness:

1. builds `corium` and the MusicBrainz loader;
2. starts an in-memory transactor;
3. creates `mbrainz` with the MusicBrainz schema and loads the wasm demo's
   curated 1997 fixture (20 releases, 20 artists, 266 tracks, and their
   associated media and labels);
4. starts `corium postgres-server`; and
5. runs the Java program through Maven.

From the repository root:

```sh
examples/postgres-jdbc/run.sh
```

The program checks a release count, a release-to-artist join, and the
release-to-medium-to-track join for *OK Computer*. A failed result exits
non-zero, so the same command is used by CI. The harness removes its temporary
in-memory deployment on exit and prints the Corium process logs on failure.

Java 11 or newer and Maven are required. Set `CORIUM_SKIP_BUILD=1` to reuse
existing debug binaries. The listen addresses and database can be overridden
with `CORIUM_TRANSACTOR_HOST`, `CORIUM_TRANSACTOR_PORT`,
`CORIUM_POSTGRES_HOST`, `CORIUM_POSTGRES_PORT`, and `CORIUM_DATABASE`.

## Run only the Java client

Against an already-running Corium PostgreSQL server, set the JDBC connection
variables and invoke the example directly:

```sh
cd examples/postgres-jdbc
CORIUM_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/mbrainz \
  mvn compile exec:java
```

`CORIUM_JDBC_USER` defaults to `corium`, and `CORIUM_JDBC_PASSWORD` defaults to
the empty string. If `corium postgres-server` was launched with `--password`,
set the latter to that password.

The client deliberately uses plain `Statement` queries. Corium currently
supports JDBC's extended query protocol for statements without bound
parameters, but PostgreSQL bound parameters and binary result formats are not
yet implemented.
