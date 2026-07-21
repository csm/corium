# corium

A database system in the style of Datomic — immutable, time-aware, and
fact-oriented, with Datalog queries and peer-local query execution — written
in Rust, paired with [Clojurust](https://github.com/csm/clojurust) for
EDN/Clojure data handling and database function execution.

The peer also exposes read-only SQL through the `corium-sql` Rust crate and the
`corium sql` interactive shell; see the [SQL interface](docs/sql.md).
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

Corium builds with a recent stable Rust toolchain. From the repository root:

```sh
cargo build --workspace
cargo test --workspace
```

Start a local transactor (here fully in-memory, so there is nothing to clean
up afterwards):

```sh
cargo run -p corium-cli -- transactor --store mem --data-dir ./corium-data \
  --listen 127.0.0.1:4334
```

Then, in another terminal, create a database from a schema and open the
interactive query console:

```sh
cargo run -p corium-cli -- db create people --schema schema.edn
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
| [`corium-index`](crates/corium-index/README.md) | Immutable segment trees, EAVT/AEVT/AVET/VAET indexes, live index, merge iterators |
| [`corium-store`](crates/corium-store/README.md) | `BlobStore` + `RootStore` traits; memory/fs/postgres/turso/s3 backends; segment cache |
| [`corium-log`](crates/corium-log/README.md) | Durable append-only transaction log: format, append/replay, range scans |
| [`corium-tx`](crates/corium-tx/README.md) | Transaction expansion, tempid/lookup resolution, schema validation, built-in tx fns |
| [`corium-db`](crates/corium-db/README.md) | The immutable `Db` value: time views, covering-index access, naming, stats |
| [`corium-query`](crates/corium-query/README.md) | EDN Datalog compiler/planner/executor, rules, aggregates, Pull, entity API |
| [`corium-sql`](crates/corium-sql/README.md) | Read-only DataFusion SQL over peer-local `Db` values |
| [`corium-protocol`](crates/corium-protocol/README.md) | protobuf/gRPC definitions, wire value encoding, generated tonic stubs |
| [`corium-transactor`](crates/corium-transactor/README.md) | Transactor process: pipeline, indexing job, lease/HA, gRPC server, backup |
| [`corium-peer`](crates/corium-peer/README.md) | Peer library: connection, tx-report handling, segment cache, peer server |
| [`corium-cljrs`](crates/corium-cljrs/README.md) | Clojurust bindings: value conversion, `corium.api`, `:db/fn` sandbox host |
| [`corium-cli`](crates/corium-cli/README.md) | `corium` binary: launchers, admin commands, console, TUI, SQL shell |
| [`corium-sim`](crates/corium-sim/README.md) | Deterministic simulation harness for fault-injection tests (not published) |

## Examples

- [`examples/musicbrainz`](examples/musicbrainz/README.md) — a corium port of
  the Datomic MusicBrainz sample: schema, a streaming data loader, and a
  Clojurust query REPL, with one-command scripts for in-memory, filesystem,
  and Turso storage.
