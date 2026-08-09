# corium

A database system in the style of Datomic — immutable, time-aware, and
fact-oriented, with Datalog queries and peer-local query execution — written
in Rust, paired with [Clojurust](https://github.com/csm/clojurust) for
EDN/Clojure data handling and database function execution.

The peer also exposes SQL through the `corium-sql` Rust crate, a read-only
`corium sql` shell, and an opt-in PostgreSQL wire server with guarded
autocommit DML; see the [SQL interface](docs/sql.md).
`corium tui` opens a full-screen terminal dashboard — query workbench, live
store metrics, transaction feed, and schema browser; see the
[operations guide](docs/operations.md#terminal-dashboard-tui).

All roadmap milestones (M0–M7, through active/standby high availability)
are implemented. Start with the
[getting-started guide](docs/getting-started.md), work through the
[MusicBrainz example](examples/musicbrainz/README.md) for an end-to-end tour
(schema, data loader, and a Clojurust query REPL over in-memory, filesystem,
or Turso storage), use the [operations guide](docs/operations.md) for
PostgreSQL-backed deployment and recovery, and see [PLAN.md](PLAN.md) for
current status.
Design documents, the roadmap, and architecture decision records live in
[docs/](docs/).

## Getting started

Corium builds with Rust 1.88 or newer. From the repository root:

```sh
cargo build
./scripts/test-rust.sh
```

The test script runs the GC-mode Clojure crates separately from the
`no-gc` transaction-function runtime. A single `cargo test --workspace`
feature-unifies those incompatible allocator modes.

Start a local transactor (here fully in-memory, so there is nothing to clean
up afterwards):

```sh
cargo run -p corium-cli -- transactor --store mem --data-dir ./corium-data \
  --listen 127.0.0.1:4334
```

Then, in another terminal, create a database from a schema and open the
interactive query console:

```sh
cargo run -p corium-cli -- db create people --schema schema.toml
cargo run -p corium-cli -- console people
```

The console accepts EDN Datalog directly:

```clojure
[:find ?name ?age
 :where [?e :person/name ?name]
        [?e :person/age ?age]]
```

The [getting-started guide](docs/getting-started.md) has the full walkthrough
(schema file, other storage backends, transacting data); the
[MusicBrainz example](examples/musicbrainz/README.md) is a one-command
end-to-end tour.

## Workspace layout

Corium is a single Cargo workspace. Dependency edges point strictly downward:
`corium-core` at the base, the pure engine crates above it, then the async /
networked crates, with `corium-cli` composing everything into runnable
processes. Each crate has its own README.

| Crate | What it does |
|---|---|
| [`corium-core`](crates/corium-core/README.md) | `Value`, sortable encoding, `Datom`, ids, partitions, schema model, errors |
| [`corium-index`](crates/corium-index/README.md) | Immutable covering-index segments (EAVT/AEVT/AVET/VAET) whose leaves are the published chunks; incremental apply with structural sharing |
| [`corium-store`](crates/corium-store/README.md) | `BlobStore` + `RootStore` traits; memory/fs/postgres/turso/s3 backends; segment cache |
| [`corium-log`](crates/corium-log/README.md) | Durable append-only transaction log: format, append/replay, range scans |
| [`corium-tx`](crates/corium-tx/README.md) | Transaction expansion, tempid/lookup resolution, schema validation, built-in tx fns |
| [`corium-db`](crates/corium-db/README.md) | The immutable `Db` value: time views, covering-index access, naming, stats |
| [`corium-query`](crates/corium-query/README.md) | EDN Datalog compiler/planner/executor, rules, aggregates, Pull, entity API |
| [`corium-sql`](crates/corium-sql/README.md) | DataFusion SQL and autocommit mutation planning over peer-local `Db` values |
| [`corium-pgwire`](crates/corium-pgwire/README.md) | PostgreSQL wire-protocol front end for Corium SQL |
| [`corium-protocol`](crates/corium-protocol/README.md) | protobuf/gRPC definitions, wire value encoding, generated tonic stubs |
| [`corium-transactor`](crates/corium-transactor/README.md) | Transactor process: pipeline, indexing job, lease/HA, gRPC server, backup |
| [`corium-peer`](crates/corium-peer/README.md) | Peer library: connection, tx-report handling, segment cache, peer server |
| [`corium-client`](crates/corium-client/README.md) | Fluent async Datomic-style API over the peer library and peer-server gRPC; typesafe Datalog/Pull builders |
| [`corium-ffi`](crates/corium-ffi/README.md) | Owned, runtime-neutral facade for native language bindings |
| [`corium-jni`](crates/corium-jni/README.md) | JNI adapter that runs an in-process peer for the Java client |
| [`corium-cljrs`](crates/corium-cljrs/README.md) | Clojurust bindings: value conversion, `corium.api`, `:db/fn` sandbox host |
| [`corium-cli`](crates/corium-cli/README.md) | `corium` binary: launchers, admin commands, console, TUI, SQL shell |
| [`corium-sim`](crates/corium-sim/README.md) | Deterministic simulation harness for fault-injection tests (not published) |

Language clients live under `clients/`; [`clients/python`](clients/python/README.md)
provides the shared asynchronous local/remote Python API,
[`clients/java`](clients/java/README.md) provides the Java API, and
[`clients/clojure`](clients/clojure/README.md) provides synchronous and
core.async JVM Clojure APIs over the published Java client.

## Examples

- [`examples/musicbrainz`](examples/musicbrainz/README.md) — a corium port of
  the Datomic MusicBrainz sample: schema, a streaming data loader, and a
  Clojurust query REPL, with one-command scripts for in-memory, filesystem,
  and Turso storage.
- [`examples/postgres-jdbc`](examples/postgres-jdbc/README.md) — a Java/Maven
  client that starts an in-memory transactor, loads the 20-release MusicBrainz
  wasm fixture, and checks SQL queries through the PostgreSQL JDBC driver.
