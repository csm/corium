# Command reference

This chapter lists every `corium` command. Run `corium <command> --help` for
the authoritative flag list of a build.

## Global

```text
corium [--log-format human|json] <command>
```

`--log-format` comes before the subcommand. `corium tui` writes no tracing
output, because it owns the terminal.

## Shared flag groups

**Connection flags** apply to `peer-server`, `postgres-server`, every `db`
subcommand, every `authz` subcommand, every `keys` subcommand, `console`,
`tui`, and `sql`.

| Flag | Environment | Default |
|---|---|---|
| `--transactor <url>` | | `http://127.0.0.1:4334` |
| `--token <secret>` | `CORIUM_TOKEN` | Shared development token |
| `--ca <pem>` | | None |
| `--tls-domain <name>` | | None |
| `--peer-bootstrap` | | Off |

**Serving flags** apply to `transactor` and `peer-server`.

| Flag | Environment |
|---|---|
| `--serve-token <secret>` | `CORIUM_SERVE_TOKEN` |
| `--require-auth` | |
| `--serve-open` | |
| `--oidc-issuer <url>` | |
| `--oidc-audience <aud>` | |
| `--oidc-jwks-file <path>` | |
| `--authz-db <name>` | `CORIUM_AUTHZ_DB` |
| `--authz-fresh-writes` | |
| `--authz-break-glass-role <role>` | |
| `--authz-max-depth <n>` | |
| `--tls-cert <pem>` | |
| `--tls-key <pem>` | |

**Key flags** apply to `transactor`, `peer-server`, `gc`, and `log`.

| Flag | Environment |
|---|---|
| `--storage-key <uri>` | `CORIUM_STORAGE_KEY` |

## Servers

### `corium transactor`

Runs a transactor over a data directory. See
[the transactor](../running/transactor.md).

Storage flags: `--config`, `--store`, `--data-dir`, `--turso-path`,
`--postgres-url`, `--postgres-read-only-url`, `--s3-bucket`, `--s3-prefix`,
`--s3-region`, `--s3-endpoint-url`, `--s3-read-only-access-key-id`,
`--s3-read-only-secret-access-key`, `--s3-read-only-session-token`,
`--s3-read-only-role-arn`, `--s3-read-only-role-session-name`,
`--s3-read-only-role-duration-seconds`, `--s3-read-only-role-external-id`.

Process flags: `--listen`, `--owner`, `--advertise`, `--metrics-listen`.

Lease flags: `--ha`, `--lease-ttl-ms`, `--lease-wait-ms`, `--heartbeat-ms`.

Index flags: `--index-interval-ms`, `--index-backoff`,
`--index-tail-threshold`, `--index-tail-deadline-ms`.

Collection flags: `--gc-interval`, `--gc-window`.

Function flags: `--db-fn-fuel`, `--db-fn-memory-bytes`.

### `corium peer-server`

Hosts one database for thin clients. See
[peer server and thin clients](../surfaces/peer-server.md).

Flags: `--db`, `--listen`, `--max-fuel`, `--metrics-listen`,
`--segment-cache-dir`, `--segment-cache-capacity`, `--segment-cache-memory`.

### `corium postgres-server`

Serves the catalog over the PostgreSQL wire protocol. See
[SQL shell and PostgreSQL server](../surfaces/sql.md).

Flags: `--database` (repeatable), `--listen`, `--password`, `--allow-writes`.

## Catalog

### `corium db create <name>`

Creates a database. Flags: `--schema <path>`, `--storage-key <uri>`.

### `corium db delete <name>`

Deletes a database. It asks for no confirmation.

### `corium db list`

Lists the databases that the transactor serves.

### `corium db stats <name>`

Connects a peer and prints statistics.

### `corium db fork <name> <target>`

Duplicates a database at a basis. Flag: `--as-of <t>`. See
[forking a database](../availability/fork.md).

### `corium db request-index <name>`

Publishes the indexes now, bypassing pacing.

### `corium db index-policy <name>`

Reads or overrides the pacing of one database. Flags: `--interval-ms`,
`--backoff`, `--tail-threshold`, `--tail-deadline-ms`. With no flags it prints
the current policy.

## Authorization

See [authorization](../security/authorization.md).

| Command | Effect |
|---|---|
| `corium authz init` | Creates the policy database with schema, permissions, and a first administrator. Flags: `--db`, `--admin`, `--provider`, `--no-admin`. |
| `corium authz grant <subject> <relation> <object>` | Asserts a relationship tuple. Flag: `--db`. |
| `corium authz revoke <subject> <relation> <object>` | Retracts a relationship tuple. Flag: `--db`. |
| `corium authz check <subject> <action>` | Prints what the policy decides. Flags: `--database`, `--provider`, `--role`, `--claim`, `--db`. |
| `corium authz status` | Prints the compiled basis and entity counts. Flag: `--db`. |

## Keys

See [encryption at rest](../security/encryption.md).

| Command | Effect |
|---|---|
| `corium keys status <db>` | Prints the KEK, the epochs, and the nonce budget. |
| `corium keys rotate <db>` | Opens a new storage-key epoch. |
| `corium keys rewrap <db> --kek <uri>` | Re-wraps the data keys under another KEK. |

## Data care

### `corium gc`

Sweeps unreachable blobs. Flags: `--data-dir` or `--transactor` (they
conflict), `--token`, `--ca`, `--tls-domain`, `--window`, `--storage-key`. See
[garbage collection](../availability/gc.md).

### `corium backup <db> <destination>`

Creates or refreshes a backup from a live transactor. Flags: `--transactor`,
`--token`, `--ca`, `--tls-domain`. See
[backup and restore](../availability/backup.md).

### `corium restore <source>`

Restores a backup offline. Flags: `--data-dir`, `--as-db`. Both are required.

## Interactive surfaces

| Command | Effect |
|---|---|
| `corium console <db>` | Datalog console. See [query console](../surfaces/console.md). |
| `corium tui <db>` | Dashboard. Flag: `--refresh-ms`. See [terminal dashboard](../surfaces/tui.md). |
| `corium sql <db>` | SQL shell. Flags: `-c/--command`, `-f/--file`. See [SQL](../surfaces/sql.md). |

## Offline inspection

### `corium log`

Prints committed transactions from a filesystem data directory. The transactor
does not need to be running.

Flags: `--data-dir`, `--db`, `--from`, `--to`, `--storage-key`.

`--from` is inclusive. `--to` is exclusive, and `0` means open-ended.

## Commands that do not exist

> **Not implemented.** There is no `corium transact` command. Writes come from
> a client library, or from `corium postgres-server --allow-writes`.

> **Not implemented.** There is no `corium dump` command. Human, JSON, and EDN
> export are deferred.

> **Not implemented.** There is no command that alters the schema of an
> existing database. See [schema management](../running/schema.md).
