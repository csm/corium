# Log and Transactor

## Transaction log

The log is the source of truth: a totally ordered sequence of transactions,
each a `(t, tx-instant, [datoms])` record. Indexes are a deterministic fold of
the log; anything else can be rebuilt from it.

Physical layout reuses the blob machinery: transactions are appended to a
**log chunk** (an in-progress block buffered by the transactor and flushed to
the blob store on every transaction — durability before ack), and sealed
chunks are linked into a small persistent tree keyed by `t` (`log-root` in the
DbRoot), giving `tx-range(t1, t2)` as a range scan. Chunk flush + root CAS is
the commit point.

To keep per-transaction latency independent of chunk size, the active chunk is
written as an append-only object where the backend supports it (filesystem:
append + fsync; object stores later: small per-tx objects compacted into
chunks by the indexing job). The `corium-log` crate hides this behind
`append(tx) -> t` and `replay(range)`.

## Transaction pipeline

One logical thread of control per database (the write serialization point):

```
receive tx-data (wire or in-proc)
  → resolve database functions (built-ins native; user fns via cljrs sandbox)
  → expand map forms / nested entities to list form
  → resolve lookup refs and tempids (upsert via :db.unique/identity)
  → validate against SchemaCache (types, cardinality, uniqueness vs AVET)
  → cardinality-one implicit retraction of prior values
  → assign tx entity id, :db/txInstant (max(now, last+1) for monotonicity)
  → append to log, fsync/flush                 ← durability point
  → apply datoms to in-memory live index
  → ack caller with {db-before, db-after basis, tempids, tx-data}
  → broadcast tx-report to subscribed peers
```

Steps up to validation are pure functions in `corium-tx`, taking a `Db` value;
the pipeline is a thin loop around them. Pipelining: expansion/validation of
tx N+1 may overlap the log flush of tx N, but log append order defines t.

Backpressure: a bounded queue in front of the pipeline; transact calls beyond
it fail fast with a busy error (clients retry with backoff).

## Background indexing job

The live index (a persistent in-memory sorted structure holding datoms since
`index-basis-t`) grows with every transaction. When it exceeds a threshold
(bytes or datom count) or a time limit, the indexing job:

1. Takes the current basis `t*` and the live-index snapshot up to `t*`
   (persistent structure ⇒ snapshot is free; pipeline keeps running).
2. Merges it into each covering index tree, writing new leaf/inner segments
   bottom-up, reusing unchanged subtrees by hash.
3. Uploads all new segments, then CASes a new DbRoot with
   `index-basis-t = t*` and the new roots.
4. Notifies peers (via the tx-report stream) that a new index basis is
   available; peers adopt it and drop their log tail below `t*`.
5. Marks the pre-`t*` live index droppable once adopted locally.

Indexing runs concurrently with transaction processing; only step 3's CAS
touches shared state. If the transactor crashes mid-index, the orphan segments
are garbage (collected later) and indexing restarts from the last published
root — no recovery logic needed. This "crash-only" property falls out of
immutability and is a design invariant: **no step of indexing or GC requires
cleanup to be correct.**

## Transactor lifecycle and recovery

Startup:
1. Acquire the write lease for the database (root store CAS, see below).
2. Load DbRoot, build SchemaCache, replay log tail `(index-basis-t, basis-t]`
   into the live index.
3. Start gRPC services, accept transactions.

Crash recovery is identical to startup — the log tail replay rebuilds exactly
the state that existed at the durability point of the last acked transaction.

## HA design (built later, designed now — ADR-0010)

The lease is a record in the root store: `{owner-id, lease-version, expiry}`,
renewed by CAS at interval `T/3` for expiry `T`. Fencing rule: **every DbRoot
CAS carries the lease-version the writer believes it holds; the root store
impl validates it in the same atomic operation.** A deposed transactor (GC
pause, partition) that wakes and tries to publish loses the CAS and shuts
down. This fencing check is in the v1 `RootStore` contract and both v1 impls,
so single-transactor deployments already exercise the code path with a
never-contested lease.

The later HA milestone adds: standby process that polls the lease, takes over
on expiry (replaying the log tail exactly as in crash recovery, since the log
in shared storage is the truth), and peer reconnect-and-resubscribe to
whichever owner holds the lease (discovered via the root store). No consensus
protocol is needed beyond root-store CAS; this matches Datomic's
active/standby model.

## Process embedding

`corium-transactor` is a library with a `main` wrapper in `corium-cli`. In
early milestones and in tests, transactor + peer run in one process over
in-memory channels implementing the same service traits as the gRPC layer, so
the distributed and embedded configurations execute identical logic.
