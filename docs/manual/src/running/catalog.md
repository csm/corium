# The database catalog

One transactor serves many databases. The `corium db` commands operate the
catalog. Every one of them talks to a running transactor.

## Connection flags

Every client command shares the same connection flags.

| Flag | Default | Effect |
|---|---|---|
| `--transactor <url>` | `http://127.0.0.1:4334` | Transactor endpoint. A comma-separated list gives failover order. |
| `--token <secret>` | Shared development token | Bearer token. `--token ""` connects anonymously. |
| `--ca <pem>` | None | CA certificate to trust. Enables TLS. |
| `--tls-domain <name>` | None | Domain expected on the server certificate. |
| `--peer-bootstrap` | Off | Read the published snapshot from storage instead of replaying the log from basis 0. |

`CORIUM_TOKEN` sets the token for every command.

Administrative commands use the first endpoint in the list. Peer connections
fail over across the whole list.

## Create a database

```sh
corium db create people --schema schema.toml
```

The command prints `{:db "people" :created true}`.

A database name holds 1 to 128 characters. Only ASCII letters, digits, `-`,
and `_` are allowed.

The schema file is EDN, or TOML when the path ends in `.toml`. Omit
`--schema` to create an empty database. See
[schema management](schema.md).

To encrypt every durable artifact of the database, add `--storage-key`:

```sh
corium db create people --schema schema.toml --storage-key file:/etc/corium/storage.key
```

Encryption is fixed at creation. See
[encryption at rest](../security/encryption.md).

> **`db create` is idempotent, and it does not update an existing database.**
> An existing name prints `{:db "people" :created false}`, and the schema file
> is ignored. Change the schema of a live database with a transaction from a
> client. See [schema management](schema.md).

## List databases

```sh
corium db list
```

The command prints the names that the transactor serves.

## Inspect a database

```sh
corium db stats people
```

The command connects a peer, syncs it, and prints one EDN map:

```clojure
{:basis-t 1240 :index-basis-t 1200 :datoms 91234 :entities 20114
 :attributes 37 :index-lag 40 :tx-count 1240 :tx-failures 2
 :tx-queue-depth 0 :gc-runs 17 :gc-swept-blobs 214}
```

| Field | Meaning |
|---|---|
| `:basis-t` | Newest committed transaction that the peer has seen. |
| `:index-basis-t` | Transaction covered by the published indexes. |
| `:datoms`, `:entities`, `:attributes` | Counts in the current value. |
| `:index-lag` | Transactions committed after the published index basis. |
| `:tx-count`, `:tx-failures` | Transactor counters since process start. |
| `:tx-queue-depth` | Commit queue depth now. |
| `:gc-runs`, `:gc-swept-blobs` | Garbage collection counters since process start. |

`db stats` replays from basis 0 unless `--peer-bootstrap` is given. On a large
database that is slow. Add `--peer-bootstrap` when the client can reach the
storage backend.

> **Partly implemented.** `db stats` does not print the lease owner. The
> `Metrics` panel of [`corium tui`](../surfaces/tui.md) shows lease ownership
> and the advertised endpoint, from the same `Status` call.

## Delete a database

```sh
corium db delete people
```

The command prints `{:db "people" :deleted true}`.

> **CAUTION: `db delete` asks for no confirmation, and it cannot be undone.**
> The command deletes the database root, the metadata root, the key manifest,
> and every log record at once. Blobs stay until garbage collection sweeps
> them. Take a [backup](../availability/backup.md) first.

## Fork a database

`corium db fork` creates a new database that duplicates an existing one at a
transaction basis. Use it for a writable sandbox against real data. See
[forking a database](../availability/fork.md).

## Index publication

`corium db request-index` and `corium db index-policy` control when the
transactor publishes fresh index trees. See [index publication](indexing.md).
