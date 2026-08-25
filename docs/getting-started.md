# Getting started

Corium currently builds with Rust 1.88 or newer. From the repository root:

```sh
cargo build
./scripts/test-rust.sh
```

The test script keeps the GC-mode Clojure crates separate from the `no-gc`
transaction-function runtime. Cargo feature unification makes a single
`cargo test --workspace` invalid for this repository.

Start a local transactor in one terminal:

```sh
cargo run -p corium-cli -- transactor \
  --data-dir ./corium-data \
  --listen 127.0.0.1:4334 \
  --metrics-listen 127.0.0.1:9464
```

The transactor keeps its blobs, root pointers, and transaction logs in one
of the selected stores: `fs` (the default, under `--data-dir`), `mem` (fully
in-memory and ephemeral — handy for demos and tests), `postgres` (build with
`--features postgres` and pass `--postgres-url`), or `turso` (an
embeddable-SQLite database; build with `--features turso` and pass
`--turso-path`). `postgres` and `turso` store their transaction logs
natively alongside blobs and roots; `fs` uses versioned log files and `mem`
uses an in-process registry.

```sh
cargo run -p corium-cli -- transactor --store mem --data-dir ./corium-data
cargo run -p corium-cli --features postgres -- \
  transactor --store postgres --postgres-url "$DATABASE_URL" \
  --data-dir ./corium-data
cargo run -p corium-cli --features turso -- \
  transactor --store turso --data-dir ./corium-data
```

Create a schema file named `schema.toml`:

```toml
schema-version = 1

[[entity]]
name = "person"

[entity.attributes]
name = { type = "string", unique = "identity", index = true }
age = "long"
```

The `entity` block is schema-authoring sugar: it supplies the `person` group
for the `person/name` and `person/age` attributes, but does not install or
enforce an entity type. Flat `[[attribute]]` declarations and the original
Datomic-style EDN schema format are also supported. See
[TOML schemas](schema-toml.md).

Create the database and inspect it:

```sh
cargo run -p corium-cli -- db create people --schema schema.toml
cargo run -p corium-cli -- db list
cargo run -p corium-cli -- db stats people
cargo run -p corium-cli -- console people
```

The console accepts EDN Datalog directly:

```clojure
[:find ?name ?age
 :where [?e :person/name ?name]
        [?e :person/age ?age]]
```

Transactions can be submitted from the Rust peer API or from the
`corium.api/transact` Clojurust binding. See
[Clojurust integration](design/clojurust-integration.md) for the boundary API
and [Operations](operations.md) for production process flags, backup, metrics,
and recovery.

For a fuller worked example — a MusicBrainz schema, a streaming data loader,
a Clojurust query REPL, and one-command scripts for every store type — see
[`examples/musicbrainz`](../examples/musicbrainz/README.md).
