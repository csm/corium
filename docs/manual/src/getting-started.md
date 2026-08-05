# Getting started

This chapter builds Corium, starts a local system, and runs a query. The whole
system is in memory. Nothing is written to disk, so there is nothing to clean
up.

The steps take about ten minutes. Use four terminals.

## Step 1 — Build the binary

Corium needs Rust 1.85 or newer. From the repository root, run:

```sh
cargo build -p corium-cli --release
```

The binary is `target/release/corium`. This manual writes `corium` for that
path.

To build the optional storage backends, add their features. The
[installation chapter](running/installation.md) lists every feature.

## Step 2 — Start a transactor

In terminal 1, run:

```sh
corium transactor --store mem --data-dir ./corium-data --listen 127.0.0.1:4334
```

The transactor prints the databases it serves. `--store mem` keeps everything
in the process. The process loses the whole database when it stops.

`--data-dir` is required for every store. The `mem` store does not write to
it.

## Step 3 — Write a schema file

Create `schema.toml` in terminal 2:

```toml
schema-version = 1

[[entity]]
name = "person"

[entity.attributes]
name = { type = "string", unique = "identity", index = true }
age  = "long"
```

This file declares two attributes, `:person/name` and `:person/age`. The
`[[entity]]` block is an authoring group. It does not create an entity type.
The [schema chapter](running/schema.md) explains the full format.

## Step 4 — Create the database

```sh
corium db create people --schema schema.toml
corium db list
corium db stats people
```

`db create` sends the schema to the transactor as an ordinary transaction.
`db stats` prints the basis, the datom count, and the transactor counters.

## Step 5 — Write some data

The CLI has no transact command. Writes come from a client library, or from
the PostgreSQL wire server. This step uses the wire server, because it needs
no code.

In terminal 3, start the server with writes enabled:

```sh
corium postgres-server --listen 127.0.0.1:5432 --allow-writes
```

In terminal 4, insert two rows with `psql`:

```sh
psql 'host=127.0.0.1 port=5432 dbname=people' \
  -c "INSERT INTO corium.person (name, age) VALUES ('Ada', 36), ('Grace', 45)"
```

Each statement is one transaction. The
[SQL chapter](surfaces/sql.md) states which statements the write path accepts.

> The Rust, Clojure, Python, and Java clients all transact directly against
> the transactor. Use them for real data loading. See
> [`clients/python`](https://github.com/csm/corium/blob/main/clients/python/README.md)
> and
> [`clients/java`](https://github.com/csm/corium/blob/main/clients/java/README.md).

## Step 6 — Query the data

Open the Datalog console:

```sh
corium console people
```

Enter a query:

```clojure
[:find ?name ?age
 :where [?e :person/name ?name]
        [?e :person/age ?age]]
```

The console also runs pull forms and time-view commands:

```clojure
(pull [:person/name :person/age] 1000)
```

Type `:basis` to see the current transaction number. Type `:quit` to leave.
The [console chapter](surfaces/console.md) lists every command.

Run the same question in SQL:

```sh
corium sql people -c "SELECT e, name, age FROM corium.person ORDER BY name"
```

Open the dashboard to watch the system live:

```sh
corium tui people
```

## Step 7 — Change the schema

Add an attribute to `schema.toml`:

```toml
[entity.attributes]
name  = { type = "string", unique = "identity", index = true }
age   = "long"
email = { type = "string", doc = "primary contact" }
```

Ask for the plan:

```sh
corium schema update people --schema schema.toml
```

The command writes nothing. It prints the plan and the digest of that plan.
Apply exactly the plan you read:

```sh
corium schema update people --schema schema.toml --apply --plan <digest>
```

The last line of the plan is the invocation to run. The
[schema chapter](running/schema.md) explains the execution classes and the
acknowledgement codes.

## Step 8 — Stop the system

Press `Ctrl-C` in each terminal. The `mem` store discards the database.

## Next steps

- To keep the data, restart with the default `fs` store. Read
  [storage backends](running/storage.md).
- To run a production process, read [the transactor](running/transactor.md).
- To understand what the system does with your data, read
  [how Corium works](theory/index.md).
