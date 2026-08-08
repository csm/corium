# Binary backup format

Corium backup artifacts use one appendable binary representation. This format
is independent of the database storage format: a reader must validate both
versions before restoring data.

Corium writes **version 2** and reads versions 1 and 2. An archive keeps the
version it was created with: an incremental run appends a checkpoint rather
than rewriting the header, so an existing version 1 archive stays a version 1
archive.

## Version 1

All integers are unsigned and big-endian. `bytes` means a `u64` byte length
followed by that many bytes. Text fields are UTF-8 `bytes`.

The archive begins with this fixed-order header:

| Field | Encoding |
|---|---|
| Magic | 16 bytes: `CORIUM_BACKUP` followed by three zero bytes |
| Backup format | `u32`: `1` here, `2` below |
| Creator version | text |
| Source database name | text |
| Database storage format | `u32` |
| Snapshot index basis | `u64` |
| Encoded database root | bytes |

The creator version identifies the Corium release that created the archive.
A reader that encounters a newer backup format reports this value with the
unsupported-version error.

The rest of the archive is a sequence of frames:

```text
tag: [u8; 4]
payload-length: u64
payload: [u8; payload-length]
```

`BLOB` frames contain one immutable index blob as raw bytes. They appear only
before the first checkpoint, with referenced children before parents. Blob
identities are recomputed and the snapshot tree is validated during restore.

Every successful full or incremental run ends at a `CKPT` frame. Its payload
has this layout:

| Field | Encoding |
|---|---|
| Writer version | text |
| Inclusive checkpoint basis `t` | `u64` |
| First transaction `t` | `u64`; zero when the range is empty |
| Transaction count | `u64` |
| Database catalog metadata | bytes |
| Transaction records | bytes containing Corium log-framed records |
| Commit marker | 4 bytes: `DONE` |

The first checkpoint covers `(0, basis]`. Each later checkpoint must begin at
the preceding checkpoint's basis plus one and end at its own basis. An
incremental run appends only that new transaction range. Restore concatenates
the ranges and rejects gaps, overlaps, regressions, mismatched counts, or
malformed record framing.

The writer version in each checkpoint identifies the Corium release that last
extended the archive. It can differ from the creator version after an
incremental backup made by a newer compatible release.

## Commit and recovery rules

A complete `CKPT` frame is the archive's durability boundary. Readers ignore a
trailing partial frame. Before appending an incremental checkpoint, the writer
truncates the file to the end of the last complete frame, writes the new frame,
and synchronizes it. A newly created archive is written and synchronized under
a temporary name, then atomically renamed into place.

An archive with no complete checkpoint is invalid. Unknown frame tags and
`BLOB` frames after the first checkpoint are also invalid.

## Version 2 (encryption at rest)

Version 2 carries an encrypted database ([encryption.md](encryption.md)). It
adds two header fields, after the encoded database root:

| Field | Encoding |
|---|---|
| Content encryption | `u32`: `0` cleartext, `1` per-database storage key |
| Key manifest | bytes: the encoded `keys:<db>` record, empty when cleartext |

and two checkpoint fields, after the transaction records and before the commit
marker:

| Field | Encoding |
|---|---|
| Log-version runs | bytes: repeated `u64` version, `u64` record count |
| Key manifest | bytes: the manifest as of this checkpoint |

Everything else is unchanged. `BLOB` frames carry stored objects **verbatim**,
so an encrypted database's segments enter the archive as the ciphertext the
store holds, and `CKPT` transaction records stay log-framed — the bytes the log
holds, still sealed. Nothing in the archive is ever decrypted on the way in.

The key manifest holds a KEK identity and data keys already wrapped under it,
never key material, which is what lets a restore bootstrap itself from the
archive plus access to the KEK. The header's copy is the manifest the archive
was created with; **the newest checkpoint's copy is authoritative**, because a
storage-key rotation between incremental runs must travel with the records
sealed under the new epoch.

The log-version runs cover the checkpoint's records in order and must sum to
its transaction count. An empty table means every record belongs to log version
0, which is what a version 1 archive and a cleartext single-file log both
carry. The table exists because a sealed record authenticates the lease version
of the file it was written to: without it a copied frame could not be opened
again. A reader rejects a table that does not cover its records rather than
handing a frame to the wrong version.

### What each side needs

**Backup** needs the storage key only to *walk* the index: a blob's child
references are inside its ciphertext. It never needs one to write, and it never
re-encodes a record — re-sealing would draw a fresh nonce from the epoch's
budget, which the manifest measures as one nonce per transaction.

**Restore** needs the storage key. Blobs are replaced verbatim, because blob
encryption binds no database identity, but a log record's AAD binds the
database lineage and the log version, so the records are opened and written
again onto the restored database's own lineage at log version 0. This is what
makes restore-as-clone work at all, and it is the same thing `db fork` does
when it copies a database under a new name.

The restored database keeps the archive's manifest, so it shares data keys with
its source until `corium keys rotate` (new epoch for new writes) or `corium
keys rewrap --kek` (new KEK) says otherwise. That is the cost of copying blobs
rather than rewriting them; a fork, which re-seals everything on the way in, is
the operation that yields an independently revocable copy.

One field of that manifest is rewritten: every epoch's `opened-at-t` becomes 0.
That number is the log-record nonce budget, measured as the span of `t` an epoch
covers — and the restored log is sealed entirely under the active epoch,
whatever epochs its source used. Zeroing the openings credits the active epoch
with the whole log and the retired ones with none, which is exactly what the
restored log holds. The retired epochs stay in the manifest because the copied
blobs still carry them.

Restoring where the KEK is unavailable fails at restore, naming the key, rather
than producing a database that fails at open. Restoring *without a protection
class's key* yields a fully functional database whose attributes in that class
are permanently redacted — which is exactly what you want when shipping
production data to a staging environment.

## Out of scope

Corium deliberately does not interpret the former directory-shaped backup
output or expose human, JSON, or EDN backup variants. A future dump/export
command can render a binary archive without weakening the backup contract.
