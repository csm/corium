# Encryption at rest

Every durable artifact of an encrypted database is sealed. Index blobs,
transaction-log record payloads, and cached segments are all covered.

The seal uses a per-database data key. That data key is itself wrapped by a
key-encryption key, written KEK, that Corium never stores.

## Encryption is fixed at creation

```sh
corium db create people --schema schema.toml --storage-key file:/etc/corium/storage.key
```

A database created without a storage key stays unencrypted forever. A database
created with one stays encrypted forever.

There is no in-place migration. Migrating an unencrypted database means a
backup and a restore into a new database.

> **Partly implemented.** `corium backup` refuses an encrypted database.
> Backup format 1 cannot carry the key manifest, so no restore can open the
> resulting archive. An encrypted database therefore has no supported backup
> path today. Protect it with storage-level replication and
> snapshots until backup format 2 lands.

## Key identities

A key identity is a URI.

| Scheme | Resolves | Content |
|---|---|---|
| `file:<path>` | Yes | 32 raw bytes, or 64 hexadecimal characters. |
| `env:<NAME>` | Yes | The same two forms, from an environment variable. |
| `awskms:`, `gcpkms:`, `vault:` | No | Recognized and rejected as unsupported. |

Surrounding whitespace is ignored, so a key file with a trailing newline works.

> **Not implemented.** KMS key identities are recognized, but no keyring
> resolves them. A process that names one fails at startup. Use `file:` or
> `env:` today.

Create a key:

```sh
head -c 32 /dev/urandom > /etc/corium/storage.key
chmod 400 /etc/corium/storage.key
```

> **CAUTION: Corium never stores the KEK.** If you lose it, the database
> cannot be read again. Keep the key off the machine that holds the data, and
> keep a copy in a separate system.

## Which processes need the key

Every process that reads storage directly needs the key.

```sh
corium transactor  --data-dir /srv/corium --storage-key file:/etc/corium/storage.key
corium peer-server --db people --peer-bootstrap --storage-key file:/etc/corium/storage.key
```

`--storage-key` is repeatable, because one node can host databases under
different KEKs. `CORIUM_STORAGE_KEY` sets it from the environment.

A process resolves every named key at startup. A process without a key that
its database needs therefore fails at open, and the error names the key. It
does not fail later at its first read.

Thin clients and peer-server callers need no key. They receive plaintext over
TLS.

`corium db create --storage-key` names the key for the *transactor* to
resolve. No key material leaves the transactor host.

## Offline commands

Two offline commands read blob and log content, so they need the key.

```sh
corium log --data-dir /srv/corium --db people --storage-key file:/etc/corium/storage.key
corium gc  --data-dir /srv/corium --storage-key file:/etc/corium/storage.key
```

Offline garbage collection refuses to run without the key. Without the key,
the command cannot follow the index chunks, and a sweep deletes them.

## Inspecting keys

```sh
corium keys status people
```

The command prints the KEK, the storage-key epochs, their states, and the
share of the nonce budget that each epoch has spent.

| Field | Meaning |
|---|---|
| `:encrypted` | Whether the database is encrypted at all. |
| `:kek` | The key-encryption key that the manifest names. |
| `:rotation-due` | `true` when the active epoch has spent half its nonce budget. |
| `:keys-unavailable` | This node cannot load a manifest change. |
| `:keys-fenced` | This node cannot load the epoch that the manifest opened. |
| `:storage-keys` | One map per epoch: state, algorithm, KEK epoch, opening `t`, records sealed, budget used, live objects. |

## Rotation

```sh
corium keys rotate people
```

Rotation opens a new storage-key epoch. New writes use it at once. It rewrites
no stored object.

An older epoch stays readable. It drains as ordinary re-indexing rewrites its
objects. An epoch retires only when no live object carries it.

Rotate when `corium keys status` reports `:rotation-due true`. That fires at
half the log-record nonce budget. A log record uses a random 96-bit nonce, so
an epoch must seal well under 2³² records. That count is the span of `t` that
the epoch covers.

## Re-wrapping

```sh
corium keys rewrap people --kek file:/etc/corium/storage-2026.key
```

Re-wrapping re-encrypts the data keys under a new KEK. It reads, rewrites, and
re-encrypts no stored object.

The transactor must resolve both KEKs at once. Follow this procedure.

1. Start the transactor with both `--storage-key` flags.
2. Run `corium keys rewrap <db> --kek <new>`.
3. Confirm the new KEK with `corium keys status <db>`.
4. Restart the transactor with the new key only.

## When a node cannot load a key change

A key change made elsewhere is picked up within one lease-renewal tick. When
that load fails, the effect depends on which change it was.

| State | Cause | Effect |
|---|---|---|
| `:keys-unavailable true` | The manifest changed, and this node cannot load it. Usually a re-wrap to a KEK it cannot resolve. | Warning only. Reads and writes continue under the keys it already holds. The `corium_keys_unavailable` gauge rises. |
| `:keys-fenced true` | The manifest opened an epoch that this node cannot load. | **Writes refuse** with `FAILED_PRECONDITION`, naming both epochs. Reads, index publication, and the lease continue. |

The difference is deliberate. A re-wrap leaves the data keys unchanged. A
write refusal therefore turns a key-service outage into a write outage for no
confidentiality gain. A rotation is different in kind: records sealed under a
closed epoch draw on a budget that has stopped counting them.

Both states clear as soon as a load succeeds. The fix is the same for both.

1. Give the process a `--storage-key` that resolves the KEK that the manifest
   now names.
2. Restart the process.

Only the fenced state stops writes while you do this.

## Attribute protection classes

> **Not implemented.** A second layer is specified, in which values on a
> protected attribute are sealed with a class key by the writing peer and
> hydrated only by a reader granted that key. A keyless reader still queries,
> with protected values redacted. No code implements it. See
> [ADR-0018](https://github.com/csm/corium/blob/main/docs/adr/0018-attribute-protection-classes.md).
