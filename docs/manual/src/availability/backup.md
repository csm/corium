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
{:db "people" :backup-format 1 :writer-version "…" :basis-t 1240
 :index-basis-t 1200 :replayed-transactions 40 :copied-blobs 12 :reused-blobs 480}
```

## The archive

A backup has exactly one representation: a binary `.corium` archive.

The header carries an independent backup-file format version, and the Corium
version that created it. Every incremental checkpoint records the version that
appended it.

An unsupported future format fails before restore, and the error names its
writer.

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

> **Partly implemented.** `corium backup` refuses an encrypted database.
> Backup format 1 cannot carry the key manifest. See
> [encryption at rest](../security/encryption.md).

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

Three rules make a backup useful.

- Run `corium db request-index <db>` before a backup when the snapshot must be
  current. A backup reads through the published basis.
- Keep the first full archive and its incremental chain together. An
  incremental run needs the checkpoint in the same file.
- Test a restore on a schedule. An untested backup is not a backup.

For a database that `corium backup` refuses, back up the underlying storage
instead. See the [runbooks](../operations/runbooks.md).
