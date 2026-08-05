# Storage backends

`--store` selects where the transactor keeps blobs, root records, and the
transaction log. Five backends are built in.

| Store | Cargo feature | Blobs and roots | Log | Shared between hosts |
|---|---|---|---|---|
| `mem` | Built in | Process memory | Process memory | No |
| `fs` (default) | Built in | `<data-dir>/store` | `<data-dir>/logs` | Only on a shared filesystem |
| `postgres` | `postgres` | PostgreSQL tables | PostgreSQL rows | Yes |
| `turso` | `turso` | Turso database file | Turso database file | No |
| `s3` | `s3` | S3 objects | S3 objects | Yes |

`--data-dir` is required for every store. The `mem` store does not write to
it.

A backend is a driver that enters a process-wide registry under a backend
kind. `mem` and `fs` live in the engine. The other three are separate crates
that a Cargo feature links in. A driver can also be loaded at run time. Read
[storage plugins](#storage-plugins) below.

## Read-only discovery credentials

A storage-aware peer, and an online backup, read storage directly. They ask
the transactor for connection details with the `GetStorageInfo` call.

**`GetStorageInfo` never returns the primary write credential of a service
backend.** For PostgreSQL and S3 you must provision a separate read-only
credential. Without it, storage-aware peer bootstrap and online backup fail
with an explicit error. They do not fall back to the write credential.

Local filesystem and Turso stores need no separate credential.

## `mem`

```sh
corium transactor --store mem --data-dir ./corium-data
```

Everything lives in one process. The database is lost on exit. Use `mem` for
demonstrations and tests.

An online backup cannot open a process-local memory store. The backup command
rejects it clearly.

## `fs`

```sh
corium transactor --store fs --data-dir /srv/corium
```

Blobs are files under `<data-dir>/store`. Logs are versioned files under
`<data-dir>/logs`.

For a high-availability pair, both members must see the same directory over a
shared filesystem. Never run two members against diverged copies of a data
directory.

## `postgres`

```sh
corium transactor --store postgres \
  --postgres-url 'postgresql://corium@db.example/corium?sslmode=require' \
  --postgres-read-only-url \
    'postgresql://corium_reader@db.example/corium?sslmode=require' \
  --data-dir /srv/corium
```

The backend creates `corium_blobs` and `corium_roots` in the current schema of
the connection. It stores log objects as fenced root records with `log:`
names. TLS uses the platform certificate store.

Provision the read-only role with `SELECT` on both tables. Pass its URL with
`--postgres-read-only-url`, or with `CORIUM_POSTGRES_READ_ONLY_URL`.

Readers use ordinary MVCC. They do not contend with the root compare-and-set
writer.

## `turso`

```sh
corium transactor --store turso --data-dir /srv/corium --turso-path /srv/corium/store.db
```

Turso is an embeddable SQLite database. `--turso-path` defaults to
`<data-dir>/store.db`.

Turso 0.7 needs its experimental multi-process write-ahead log when
independent processes open one file. Corium enables that mode. Every process
that touches the file must run the same mode.

## `s3`

```sh
AWS_REGION=us-east-1 \
corium transactor --store s3 \
  --s3-bucket corium-prod --s3-prefix corium/ \
  --s3-region us-east-1 \
  --s3-read-only-role-arn arn:aws:iam::123456789012:role/corium-reader \
  --data-dir /srv/corium
```

Blobs go under `{prefix}blobs/`. Roots, including versioned log objects with
`log:` names, go under `{prefix}roots/`.

Root publication is fenced with S3 conditional writes, `If-None-Match` and
`If-Match`. The bucket, or the S3-compatible substitute, must support them.

**Provision the bucket yourself.** Corium does not create it, because bucket
creation involves region and ownership choices that Corium must not make for
you.

Primary credentials come from the standard AWS configuration chain:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, and instance or
task roles. Region and endpoint come from that chain, or from `--s3-region`
and `--s3-endpoint-url`.

Read-only discovery credentials take one of two forms.

- **Static keys.** Pass `--s3-read-only-access-key-id` and
  `--s3-read-only-secret-access-key`, and an optional session token. Restrict
  these keys to reads of the Corium prefix yourself.
- **AWS STS role.** Pass `--s3-read-only-role-arn`. Corium generates
  short-lived credentials on every `GetStorageInfo` call, with a session policy
  limited to `s3:GetObject` and prefix-scoped `s3:ListBucket`. The generated
  token cannot write, even when the role has broader permissions. The AWS
  identity of the transactor must be allowed to assume the role.

The two forms conflict. Use one of them.

A custom `--s3-endpoint-url` implies path-style addressing.

## Storage plugins

A storage driver can be a dynamic library that the transactor loads at
startup. An external author can therefore add a backend without a rebuild of
the engine.

```sh
corium transactor \
  --store-plugin /opt/corium/plugins/libacme_gcs.so \
  --store 'acme-gcs:{"bucket":"corium-prod"}' \
  --plugin-read-only-config '{"bucket":"corium-prod","role":"reader"}' \
  --data-dir /srv/corium
```

| Flag | Environment | Effect |
|---|---|---|
| `--store-plugin <path>` | `CORIUM_STORE_PLUGINS` | Load a plugin library. Repeatable. |
| `--store <kind>:<json>` | | Select a registered backend and pass it a JSON configuration object. |
| `--plugin-store <kind>:<json>` | | The same form under an older name. It conflicts with `--store`. |
| `--plugin-read-only-config <json>` | `CORIUM_PLUGIN_READ_ONLY_CONFIG` | The read-only configuration returned by `GetStorageInfo`. |

`CORIUM_STORE_PLUGINS` takes a path-separator-delimited list of files and
directories. Corium searches a directory for platform dynamic libraries only.
It never adds the working directory.

The EDN [configuration file](config-file.md) carries the read-only
configuration under `:plugin-read-only-config`. It does not carry the plugin
paths or a `kind:{json}` store.

`--plugin-read-only-config` is what `GetStorageInfo` returns to a
storage-aware peer or an online backup. Corium never returns the primary
configuration of a plugin backend. Without the read-only configuration,
discovery against a plugin store fails.

### Verifying a backend

```sh
corium store verify acme-gcs '{"bucket":"verification"}' \
  --store-plugin /opt/corium/plugins/libacme_gcs.so
```

The command runs the blob and root conformance suite against a live backend.
It creates uniquely named objects, exercises idempotence, listing,
compare-and-set fencing, and deletion, and removes the objects afterward.

**Use a disposable namespace.** Do not run the verifier against a bucket or a
database that holds live Corium data.

### What a plugin costs

> **CAUTION: A plugin is native code that Corium runs in its own process.**
> Install plugins only in directories that you control, and name them by
> explicit path.

- A plugin can receive storage credentials, because backend configuration
  crosses the boundary as JSON.
- A loaded library is never unloaded.
- Each library carries its own async runtime and its own process globals, so a
  process that loads several drivers pays for each.
- Encryption sits above the storage interface. A driver receives ciphertext
  and never receives key material.

> **Partly implemented.** Only `corium transactor` and `corium store verify`
> load plugins. `corium peer-server`, `corium console`, and the other client
> commands do not, so `--peer-bootstrap` against a plugin backend fails with
> "storage backend is not available". Use a built-in backend where a
> storage-aware peer is needed.

The plugin contract is documented in
[storage-plugins.md](https://github.com/csm/corium/blob/main/docs/storage-plugins.md).

## Keeping secrets out of the process arguments

Static secret fields are also read from the environment:

- `CORIUM_S3_READ_ONLY_ACCESS_KEY_ID`
- `CORIUM_S3_READ_ONLY_SECRET_ACCESS_KEY`
- `CORIUM_S3_READ_ONLY_SESSION_TOKEN`
- `CORIUM_POSTGRES_READ_ONLY_URL`
- `CORIUM_PLUGIN_READ_ONLY_CONFIG`

Prefer the environment, or a protected [configuration
file](config-file.md), over a process argument.

## Choosing a backend

| Situation | Backend |
|---|---|
| Demonstration or test | `mem` |
| Single host, simple operation | `fs` |
| High availability without a shared filesystem | `postgres` or `s3` |
| Existing PostgreSQL operations practice | `postgres` |
| Large database, object storage economics | `s3` |
| Single-file embedded deployment | `turso` |
| A service Corium does not support | A [plugin](#storage-plugins) |

A high-availability pair on `fs` needs a shared filesystem. `postgres` and
`s3` remove that requirement, because they store the log natively.
