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

> **Status:** the filesystem layout (per-lease-version files under the data
> directory) and the shared-storage layout are both implemented. The native
> backends (PostgreSQL, Turso, S3) store the log through the root store as
> **one object per transaction**, keyed `(db, lease-version, t)`
> (`corium-log`'s `NativeVersionedLog`): each commit is a single create-only
> write — a row insert on the SQL backends, a create-only `PUT`
> (`If-None-Match: *`) on S3 — whose success is the durability point, so an
> append is O(1) instead of a read-modify-write of a growing object. The
> create-only condition is the log's fence: a `(lease-version, t)` is written
> at most once. Log durability is the storage service's and HA needs no shared
> data directory. Readers merge every version's records and apply the
> lease-version takeover cutoff. Logs written by earlier releases used a
> different layout — a sequence of chunk objects keyed `(db, lease-version,
> chunk)`, each packing framed records up to a size cap — and are still read
> back **read-only** (`NativeVersionedLog` reads both layouts and continues
> appending in the per-transaction one), so existing databases keep replaying
> after an upgrade with no migration. The object-store *sealing / compaction*
> step below — concatenating the per-transaction tail into content-addressed
> `log-root` chunks and reclaiming the small objects, which bounds replay and
> list cost as the tail grows — is still future work.

### Object-store log layout (future)

On S3-class backends the current log semantics port without append support,
using conditional writes:

- **Live tail:** one small object per transaction (or micro-batch of the
  queued pipeline — group commit), written with a create-only PUT
  (`If-None-Match`) at `log/<db>/v<lease-version>/<t>` (`t` zero-padded so
  listings sort). The PUT returning is the durability point; **no
  per-transaction root CAS is needed**, so commits never contend with the
  DbRoot record. *(Implemented — see the Status note above; this is now the
  live-tail layout for every native backend, not only object stores. The
  group-commit micro-batch and the sealing step below remain future work.)*
- **Fencing carries over:** the version prefix is the object-store image of
  the per-lease-version log files. Readers list every version prefix and
  apply the same merge cutoff rule, discarding a deposed writer's stale
  appends; the create-only condition rejects a duplicate `t` within a
  version; and the post-append ownership re-check before acknowledgement is
  unchanged.
- **Sealing:** the indexing job compacts the tail — concatenate records
  through `t*` into a content-addressed chunk, link it into the log tree,
  CAS the DbRoot with the new `log-root` and log basis, then delete the
  superseded tail objects. Every step is crash-only: orphan chunks are GC
  garbage and an undeleted tail object is inert below the log basis.
- **Roots on S3** rely on conditional writes (`If-Match` ETag CAS); for
  providers without them, split the root store onto a small strongly
  consistent KV (the DynamoDB pairing) — the `BlobStore`/`RootStore` trait
  split anticipates exactly that mix.

SQL backends (`PostgreSQL`, Turso) fit better as a **log table** keyed
`(db, lease-version, t)` with one insert per commit — the same merge cutoff
over rows, at lower latency than chunked blobs.

A shared-storage log removes HA's shared-data-directory requirement, lets a
standby take over from a node whose disk is gone, and unpins databases from
transactor machines — the prerequisite for partitioning a catalog across
concurrent transactors.

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

> **Status — group commit (implemented).** Concurrent transactions to one
> database are committed as a **batch under a single durability boundary**,
> keeping each transaction's external boundary intact — every transaction
> still gets its own `t`, report, and acknowledgement. A `transact` call
> enqueues its work and then contends to *lead* a flush; whichever caller
> holds the per-database commit lock drains the queue and, for the whole run:
> validates each transaction against a staging value that already includes its
> predecessors (so uniqueness, cardinality-one retraction, and CAS see the
> same state they would one-at-a-time), makes the batch durable with **one**
> `append_batch_async` (on native backends, one object / one row — see the
> log status note above), installs it in memory, runs **one** post-append
> ownership fence for the batch, then answers every queued caller. That fence
> is the batch's *only* lease check on the common path — the pre-append check
> is skipped, since a deposed leader's append lands harmlessly under its old
> lease version (discarded by the successor's cutoff) and the fence refuses to
> acknowledge it; a batch that interns new keywords is the one exception and
> re-checks ownership before publishing the unfenced metadata root. The whole
> batch is one atomic log object, so a takeover's cutoff keeps all or none of
> it. A transaction that interns new keywords ends the batch (its names must
> be durable in metadata before the next transaction can reference them); the
> remainder is requeued. A rejected transaction (validation error) fails alone
> and its batchmates still commit. Batch size is capped by count and encoded
> bytes (`NodeConfig::max_commit_batch` / `max_commit_batch_bytes`), so a
> larger cap trades a bigger per-batch log object for higher peak throughput.
> Under no contention a batch is size 1, so low-load latency is unchanged;
> under load the expensive durable write and fence amortize across the batch,
> lifting the single-writer throughput ceiling. Still open: **optimistic-apply
> overlap** (validating tx N+1's CPU work concurrently with tx N's flush
> across *separate* batches), **pipelined flushes** (more than one batch's
> durable write in flight at once), and an explicit **bounded queue with
> fast-fail backpressure** (below). See
> [write-path-scaling.md](write-path-scaling.md) for the measured performance
> arc and the prioritized next steps.

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

## High availability (implemented in M7 — ADR-0010)

The lease `{owner-id, lease-version, expiry, advertised-endpoint}` lives
**inside the DbRoot record** (storage format 2), renewed by CAS at interval
`T/3` for expiry `T`. Because the lease fields and the index roots share one
record, every mutation — acquisition, renewal, release, index publication —
is a CAS on the same bytes: acquiring a lapsed lease with a bumped version
*is* the fence, and a deposed transactor's next CAS necessarily carries
stale expected bytes and fails. This realizes the design rule "every DbRoot
CAS validates the lease in the same atomic operation" with nothing beyond
plain single-record CAS, so no cross-record atomicity is asked of any store
backend. Single-transactor deployments exercise the identical path with a
never-contested lease.

Two further mechanisms make takeover airtight against the races a lease
alone cannot see:

- **Post-append fence.** The pipeline re-verifies lease ownership *after*
  the durable log append and *before* the acknowledgement. If ownership was
  intact at that check, the append linearizes before any takeover's
  fence CAS, and the successor's log replay (which runs strictly after its
  fence) provably contains the record. If ownership was lost, the caller
  gets an error and the record is never acked — the same contract as a
  crash between durability and reply.
- **Lease-versioned log files.** Each owner appends only to
  `<db>.v<lease-version>.log` (pre-HA `<db>.log` reads as version 0).
  Readers merge files in version order and discard any record in an older
  file whose `t` is at or past the first record of a later file: those are
  exactly a deposed writer's stale, never-acked appends, which therefore
  cannot interleave with — or fork — the successor's history.

The standby is a transactor started with the same shared storage in HA
mode: it polls the lease at the renewal cadence, rescans the catalog for
new databases, and on expiry performs ordinary startup — acquire (fence),
replay the log tail, serve. Takeover is crash recovery; there is no
separate code path. A deposed active refuses further work and returns to
standby by itself. Peers hold an endpoint preference list, rotate on
failure (a standby rejects subscriptions with a `standby` status), detect
silent death via handshake-advertised heartbeat intervals, and can
rediscover the current holder's advertised endpoint from the root record.
No consensus protocol is needed beyond root-store CAS; this matches
Datomic's active/standby model.

The M7 acceptance suite drives this: a deterministic simulation injects a
complete takeover at every boundary of the commit/publish/renew protocol
(every shared-store operation plus the append) and asserts zero
acked-transaction loss, no duplicates, and no post-takeover installs; a
process-level battery kill -9s the active under load and asserts the
standby serves writes within the lease-expiry bound with peers failing
over transparently.

## Process embedding

`corium-transactor` is a library with a `main` wrapper in `corium-cli`. In
early milestones and in tests, transactor + peer run in one process over
in-memory channels implementing the same service traits as the gRPC layer, so
the distributed and embedded configurations execute identical logic.
