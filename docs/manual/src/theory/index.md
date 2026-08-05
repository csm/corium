# How Corium works

An operator who knows the data model can predict what Corium does under
load, after a crash, and during recovery. This part of the manual explains
the model. Every later chapter refers back to it.

Five ideas carry the whole system.

1. **A fact is a datom.** Nothing is updated in place. A change asserts a new
   fact, and it retracts the old one. See
   [datoms and the fact model](datoms.md).
2. **The log is the truth.** A transaction is durable when its record is
   durable. Everything else is derived. See
   [the transaction log](log.md).
3. **Indexes are a fold of the log.** They are immutable, content-addressed
   blobs. They are an optimization, never a durability requirement. See
   [indexes and storage](indexes.md).
4. **A database value is a snapshot.** Time views name a basis. They do not
   copy facts. See [time and database values](time.md).
5. **Writes and reads are separate roles.** One transactor writes. Many peers
   read locally. See [processes and roles](processes.md).

## The consequences an operator sees

The five ideas above produce the operational rules in this manual.

- Index publication can lag without risk to durability. Lag costs cold-start
  time and backup freshness.
- Garbage collection can strand garbage, but it cannot lose data, because it
  deletes only unreachable blobs after a retention window.
- Crash recovery equals startup. The transactor replays the log tail. There is
  no repair step.
- A peer never blocks on the transactor to get a database value.
- A deposed transactor cannot publish, because every root write is a
  compare-and-set on the record that holds the lease.

## Where the design documents are

This manual states what an operator needs. The design documents state why.
They are in
[`docs/design`](https://github.com/csm/corium/tree/main/docs/design), and the
decisions are recorded as ADRs in
[`docs/adr`](https://github.com/csm/corium/tree/main/docs/adr).
