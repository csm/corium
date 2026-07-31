# The transaction log

## What the log is

The log is a totally ordered sequence of transactions. Each record holds `t`,
the transaction instant, and the datoms of that transaction.

The log is the source of truth. The indexes are a deterministic fold of the
log. Anything except the log can be rebuilt from the log.

## The commit point

A transaction is durable when its log record is durable. The transactor
acknowledges the caller after that point, and never before it.

Each record uses this frame:

```text
payload-length | (1 << 63): u64 big-endian
payload: [u8; payload-length]
crc32c: u32 big-endian
```

The CRC32C covers the length word and the payload. Replay rejects a corrupted
record even when the payload still decodes. A crash before the checksum
is durable leaves a torn tail, and recovery truncates the whole incomplete
frame.

## The transaction pipeline

One logical thread of control serves one database. That thread is the write
serialization point.

1. Receive the transaction data.
2. Resolve database functions. Built-in functions are native Rust. User
   `:db/fn` code runs in a sandboxed Clojure interpreter.
3. Expand map forms and nested entities into list form.
4. Resolve lookup references and tempids. A `:db.unique/identity` collision
   becomes an upsert.
5. Validate against the schema: types, cardinality, and uniqueness.
6. Retract the prior value of each cardinality-one attribute.
7. Assign the transaction entity id and `:db/txInstant`.
8. Append to the log and flush. **This is the durability point.**
9. Apply the datoms to the in-memory live index.
10. Acknowledge the caller.
11. Broadcast the transaction report to subscribed peers.

Steps 1 to 5 are pure functions of the database value and the input.

## Group commit

Concurrent transactions to one database commit as a batch under one durability
boundary. Each transaction keeps its own `t`, its own report, and its own
acknowledgement.

A caller enqueues its work and then contends to lead a flush. The leader
validates each transaction against a staging value that already includes its
predecessors. Uniqueness, cardinality-one retraction, and compare-and-set
therefore see the same state as a sequence of single transactions.

The batch is one atomic log object. A takeover keeps all of the batch or none
of it. A rejected transaction fails alone, and the rest of the batch still
commits.

Batch size is capped by count and by encoded bytes. Under no contention a
batch holds one transaction, so light-load latency is unchanged.

> **Partly implemented.** Group commit works. Three write-path items remain:
> optimistic-apply overlap across separate batches, more than one flush in
> flight, and an explicit bounded queue with fast-fail backpressure. See
> [write-path-scaling.md](https://github.com/csm/corium/blob/main/docs/design/write-path-scaling.md).

## Where the log is stored

The log layout follows the store.

| Store | Log layout |
|---|---|
| `mem` | An in-process registry. The log dies with the process. |
| `fs` | Versioned files under the data directory, named `<db>.v<N>.log`. |
| `postgres`, `turso` | One row per transaction, keyed by database, lease version, and `t`. |
| `s3` | One create-only object per transaction, with the same key. |

On the native backends each commit is one create-only write. Success of that
write is the durability point. An append is therefore O(1), and it does not
read and rewrite a growing object.

The create-only condition is the fence of the log. A given lease version and
`t` are written at most once.

> **Partly implemented.** Log sealing is future work on the native backends.
> The design concatenates the per-transaction tail into content-addressed
> chunks and reclaims the small objects. Without that step, replay cost and
> list cost grow with the tail since the last index publication. Frequent
> index publication keeps that tail short.

## Lease-versioned files and takeover

Each transactor appends only to the file, or the key prefix, of its own lease
version. A pre-HA log reads as version 0.

Readers merge the versions in order. A reader discards any record in an older
version whose `t` is at or after the first record of a later version. Those
records are exactly the never-acknowledged appends of a deposed writer.

> **CAUTION: Never edit or delete log files by hand.** Old lease-version files
> are inert history that readers need to merge correctly.

## Reading the log

Two surfaces read the log directly.

- `tx-range(from-t, to-t)` streams `(t, instant, datoms)` to any peer. It does
  not touch the covering indexes.
- `corium log --data-dir <dir> --db <name>` prints committed transactions from
  a filesystem data directory. The transactor does not need to be running.

An encrypted log needs the key. Pass `--storage-key` to `corium log`. See
[encryption at rest](../security/encryption.md).
