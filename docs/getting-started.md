# Getting started

Corium currently builds with Rust 1.85 or newer. From the repository root:

```sh
cargo build --workspace
cargo test --workspace
```

Start a local transactor in one terminal:

```sh
cargo run -p corium-cli -- transactor \
  --data-dir ./corium-data \
  --listen 127.0.0.1:4334 \
  --metrics-listen 127.0.0.1:9464
```

Create a schema file named `schema.edn`:

```clojure
[{:db/ident :person/name
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/unique :db.unique/identity
  :db/index true}
 {:db/ident :person/age
  :db/valueType :db.type/long
  :db/cardinality :db.cardinality/one}]
```

Create the database and inspect it:

```sh
cargo run -p corium-cli -- db create people --schema schema.edn
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
