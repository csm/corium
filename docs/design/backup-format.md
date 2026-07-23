# Binary backup format

Corium backup artifacts use one appendable binary representation. This format
is independent of the database storage format: a reader must validate both
versions before restoring data.

## Version 1

All integers are unsigned and big-endian. `bytes` means a `u64` byte length
followed by that many bytes. Text fields are UTF-8 `bytes`.

The archive begins with this fixed-order header:

| Field | Encoding |
|---|---|
| Magic | 16 bytes: `CORIUM_BACKUP` followed by three zero bytes |
| Backup format | `u32`, currently `1` |
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

Corium currently reads and writes version 1 only. It deliberately does not
interpret the former directory-shaped backup output or expose human, JSON, or
EDN backup variants. A future dump/export command can render a binary archive
without weakening the backup contract.
