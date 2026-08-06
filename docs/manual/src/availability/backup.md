# Backup and restore

Backup is online. Restore is offline.

## How backup works

The backup contacts the running transactor once. That call fixes the current
transaction basis, and it returns the connection details of the underlying
storage.

The backup then reads the storage log independently, through that basis only.
Transactions committed while the backup runs are left for the next incremental
run.

```sh
corium backup --transactor http://127.0.0.1:4334 people /backups/people.corium
```

| Flag | Default | Effect |
|---|---|---|
| `--transactor <url>` | `http://127.0.0.1:4334` | Transactor used for storage discovery. |
| `--token <secret>` | Development token | Bearer token. `--token ""` connects anonymously. |
| `--ca <pem>`, `--tls-domain <name>` | None | TLS for the transactor connection. |
| `--storage-key <uri>` | None | Key-encryption key, for an encrypted database. Repeatable; `CORIUM_STORAGE_KEY` also sets it. |

The positional arguments are the database name and the destination file.

## Incremental backup

Run the same command with the same file for an incremental refresh.

The backup reads only the transaction records after its existing checkpoint,
and it appends one new checkpoint frame. The report prints
`:replayed-transactions`.

The first run embeds the immutable snapshot blobs and retains that index
snapshot as a replay base. Later runs do not repeat that work.

The report is one EDN map:

```clojure
{:db "people" :backup-format 2 :writer-version "…" :content-encryption :none
 :basis-t 1240 :index-basis-t 1200 :replayed-transactions 40 :copied-blobs 12
 :reused-blobs 480}
```

## The archive

A backup has exactly one representation: a binary `.corium` archive.

The header carries an independent backup-file format version, and the Corium
version that created it. Every incremental checkpoint records the version that
appended it.

An unsupported future format fails before restore, and the error names its
writer.

This release writes format 2 and reads formats 1 and 2. An archive keeps the
format it was created with, so an incremental run against an existing format 1
file appends a format 1 checkpoint rather than rewriting its header.

`--log-format human|json` controls diagnostic logging only. It never changes
the artifact.

> **Not implemented.** There is no `dump` command. Human, JSON, and EDN export
> belong in one, not in backup or restore.

## Where a backup can run

| Store | Requirement |
|---|---|
| `fs`, `turso` | Run where the absolute local storage path of the transactor is reachable. |
| `postgres`, `s3` | Connect to the same native storage that the transactor advertises. S3 credentials come from the standard AWS environment. |
| `mem` | Rejected. A separate process cannot open process-local memory storage. |

The advertised PostgreSQL connection is read and write in this version. A
future release can substitute read-only credentials without a protocol change.

## Encrypted databases

A database created with `--storage-key` backs up and restores through the same
two commands, with the key named on both ends.

```sh
corium backup  --transactor http://127.0.0.1:4334 \
               --storage-key file:/etc/corium/storage.key \
               people /backups/people.corium

corium restore /backups/people.corium --data-dir /srv/corium-restored \
               --as-db people --storage-key file:/etc/corium/storage.key
```

The archive is backup format 2 and holds ciphertext throughout: index segments
are copied byte for byte and transaction records stay sealed, so nothing is
decrypted on the way in. The header and each checkpoint also carry the
database's key manifest — a KEK identity and data keys already *wrapped* under
it, never key material — which is what lets a restore bootstrap itself from the
archive plus access to the KEK.

Each side needs the key for a different reason, and the reports say which:
`:content-encryption` is `:storage` rather than `:none`.

- **Backup** needs it only to follow index references, which live inside a
  blob's ciphertext. Without it the command refuses and names the key rather
  than writing an archive missing its segments.
- **Restore** needs it to rewrite the records. A sealed record authenticates
  the database it belongs to, so restoring — under the source's own name or a
  new one — opens the copied records and writes them again onto the restored
  database's lineage.

The restored database keeps the archive's data keys, so a clone shares key
material with its source. Run `corium keys rotate` or `corium keys rewrap` on
the clone when it must be independently revocable; `corium db fork` mints a
fresh manifest instead of copying one.

See [encryption at rest](../security/encryption.md).

## Restore

Restore is offline, and it refuses to overwrite a database. The target
transactor must be stopped.

```sh
corium restore /backups/people.corium --data-dir /srv/corium-restored --as-db people
```

Restoring under a new name creates a clone:

```sh
corium restore /backups/people.corium --data-dir /srv/corium --as-db people-staging
```

| Flag | Effect |
|---|---|
| `--data-dir <path>` | Target transactor data directory. Required. |
| `--as-db <name>` | Target database name. It can differ from the source name. Required. |
| `--storage-key <uri>` | Key-encryption key, required for an encrypted archive. |

Restore writes a filesystem data directory. It does not write to a `postgres`,
`turso`, or `s3` store directly.

Backup-container and database-storage versions are checked separately before
publication.

## After a restore

1. Start the target transactor on the restored data directory.
2. Wait until `:index-lag` in `corium db stats` reaches zero.
3. Compare the basis with `:basis-t` in the backup report.
4. Compare datom, entity, and attribute counts.
5. Run a known query and compare the result.
6. Redirect peers only after those checks pass.

## Backup policy

Four rules make a backup useful.

- Run `corium db request-index <db>` before a backup when the snapshot must be
  current. A backup reads through the published basis.
- Keep the first full archive and its incremental chain together. An
  incremental run needs the checkpoint in the same file.
- Test a restore on a schedule. An untested backup is not a backup.
- Store an encrypted database's KEK apart from its archives. The archive holds
  the data keys wrapped, so the two together are a database and either alone is
  not.
