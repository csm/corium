# Indexes and Storage

## Covering indexes

Corium maintains Datomic's four covering indexes, each a total sort of (a
subset of) all datoms. Every index contains whole datoms, so any query answer
comes from one index without row lookups.

| Index | Sort order | Contains | Serves |
|---|---|---|---|
| **EAVT** | e, a, v, tx | all current datoms | entity access, pull |
| **AEVT** | a, e, v, tx | all current datoms | column-style scans, datalog clauses with known `a` |
| **AVET** | a, v, e, tx | datoms of `:db/index` and `:db/unique` attributes | value lookups, ranges, uniqueness, lookup refs |
| **VAET** | v, a, e, tx | ref-typed datoms | reverse refs, graph walk, `:db/isComponent` traversal |

The **history** variant of each index additionally keeps retractions and
superseded assertions (the current indexes keep only live datoms plus the
retraction pairs needed by `since`; exact retention rules in
[time-model.md](time-model.md)). `:db/noHistory` attributes skip history
retention.

## Immutable segment trees

Each index is a persistent B+-tree-like structure:

- **Leaf segments**: a sorted run of encoded datoms (target ~50–100 KB
  compressed; prefix-compressed keys, zstd block compression), addressed by
  the BLAKE3 hash of their bytes.
- **Inner segments**: sorted (separator-key → child-hash) arrays, same
  addressing.
- **Index root**: the hash of the top node per index, plus tree metadata.

Segments are **write-once**. An indexing job producing a new tree reuses
unchanged subtrees by hash (structural sharing), so consecutive index roots
share the vast majority of their segments. Because a hash names its content
forever, segments are cacheable at every layer — peer memory, peer disk,
CDN, anything — with no invalidation protocol.

Readers navigate: root hash → fetch inner segments → fetch leaf → binary
search. Iterators (`datoms`, `seek-datoms`, range scans) stream leaves lazily.

## Database root

A database is named by its **database root**, a small record in the root
store:

```
DbRoot {
  db-name, db-id,
  basis-t,                 // t covered by the durable log
  index-basis-t,           // t covered by the index trees
  index-roots: {eavt, aevt, avet, vaet, eavt-hist, aevt-hist, avet-hist, vaet-hist},
  payload-root,            // proposed: append-only set of committed payload blobs (see below)
  log-root,                // hash of the log chunk tree (see log-and-transactor.md)
  keyword-table-root,
  schema-rev,
  gc-epoch,
  format-version,
  lease,                   // {owner, lease-version, expiry, advertised endpoint}
}
```

Since M7 the write lease is part of this record rather than a sibling root:
every lease acquisition/renewal and every index publication CAS the same
bytes, which is what makes the HA fencing rule a single atomic operation
(see log-and-transactor.md).

The current db value seen by any reader = `index trees at index-basis-t`
merged with `log tail (index-basis-t, basis-t]` replayed into an in-memory
live index. Peers hold the live index incrementally via tx-reports; a cold
reader replays the tail from storage.

The implemented publication (storage format 3) is a first cut of the
segment-tree design: each covering index is stored as a manifest blob
naming content-defined leaf chunks (`corium-store`'s `snapshot` module),
so consecutive roots share every untouched chunk. Inner tree levels (and
with them seek-without-full-download) are still future work; readers
concatenate a manifest's chunks and accept pre-format-3 flat snapshots.

The consequence on the read side is worth being explicit about: because a
reader cannot yet seek into a published index, **a peer materializes the whole
thing in memory**. Its database value keeps every datom it has seen — the full
history, retractions included — and folds four covering indexes over that log
per time view, lazily and cached. Facts are allocated once and shared by handle
across the log and every index, so the indexes cost keys and pointers rather
than copies, but nothing is evicted and a cold time view costs a fold of the
entire history rather than of the view. So a peer today is an in-memory
database whose durable storage reconstructs its state rather than bounds it:
size peers against total history, and expect first-touch latency on a distinct
`as-of`/`since`/`history` view to scale with history rather than with the
answer. Lazy descent through the segment cache is what closes this, and
`docs/design/time-model.md` records the current costs per view.

One rule (`corium-core`'s `chunk` module) decides where a sorted key stream
is cut, and both the published format and the in-memory segment
(`corium-index`) obey it — so **a segment's leaf is exactly one published
chunk**. That is what makes the indexing job incremental rather than a
rebuild: the transactor keeps the segments it last published, folds the log
tail since that basis into them (`Segment::apply`, the same current-value
fold the `Db` value applies to its own covering indexes), and gets back
segments whose untouched leaves are the *same allocations*. A leaf carried
over by handle keeps the blob id it was published under, so the pass encodes,
hashes, and uploads only the leaves the tail rebuilt. Work per pass tracks
the tail plus one pointer per leaf, not the size of the database.

A fold keeps only the key each datom encodes to, and the transactor holds the
datoms it is indexing already, so it hands them over by reference
(`Segment::apply_ref`, `Segment::from_sorted_ref`): neither the tail nor — on
a rebuild — the database's own covering index is copied to be indexed.

Because boundaries depend only on the boundary key, a transactor that has to
rebuild a segment from scratch — its first publication, or after a takeover —
reproduces the chunks the previous process published and re-uploads only what
genuinely changed.

Rebuilding is also the safety valve, and the rule that decides when it is
required is a garbage-collection rule. A carried leaf is published *by id*,
without its bytes being uploaded, and nothing but the root that names it keeps
it from being swept. So a pass may carry chunks over only while it can prove
that root is live throughout: it requires the published root to be the one
this transactor last installed, and then **pins that index state through the
root CAS** — the CAS re-checks the pin against the record it read immediately
before writing, and is conditional on exactly those bytes, so there is no
window in which another publisher can replace the root between the check and
the write. If the pin fails, nothing is installed and the pass retries from a
rebuild, which names no chunk it did not upload and so cannot be raced this
way. The pin deliberately ignores the lease fields of the same record: an
ordinary renewal rewrites them without dropping a chunk.

Without that pin the sequence *read root A → plan and upload → another
publisher installs B → GC marks from B and sweeps chunks reachable only
through A → install C naming those chunks* leaves the live root pointing at
deleted blobs, and the basis-monotonicity check alone does not prevent it,
because C's basis can legitimately exceed B's.

The implemented peer bootstrap follows that rule for the current value: a
peer initialized with a blob/root storage connection reads `meta:<db>` and
`db:<db>`, materializes the published EAVT snapshot at `index-basis-t`, and
subscribes to the transactor from that basis. A peer without storage
credentials uses the compatibility path and subscribes from basis zero.
Published v1 segments contain current facts only, so a snapshot-bootstrapped
peer does not reconstruct transactions that ceased contributing live facts
before the snapshot; complete pre-snapshot historical views still require
full-log replay (or the future history roots described above).

A recovering **transactor** uses the same published snapshot: it opens a
database from the EAVT root plus the log tail since `index-basis-t` instead
of replaying the whole log (see the recovery item in
[roadmap.md](../roadmap.md)). Because the snapshot drops entities retracted
before it, the DbRoot also carries two recovery hints the current-facts
segments cannot supply — `next_entity_id` (the entity-allocator high-water,
so a retracted id is never reused) and `last_tx_instant` (for
`:db/txInstant` monotonicity when the tail is empty). A root written before
these hints existed leaves them at their sentinels, which forces the exact
full-log replay used previously.

Filesystem, PostgreSQL, Turso, and S3 implement the same peer read interface.
PostgreSQL readers use ordinary MVCC and do not contend with the root CAS
writer. Turso 0.7 requires its experimental multi-process WAL for independent
processes opening one local file; Corium enables it in `TursoBlobStore::open`,
but every process touching that file must run the same coordinated mode. S3
readers rely on the conditional-write CAS described below and are the
implementation of the "S3 conditional writes" option anticipated in this
design.

## Storage traits

```rust
pub trait BlobStore: Send + Sync {
    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>>;
    async fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()>; // idempotent
    async fn delete(&self, hash: &Hash) -> Result<()>;            // GC only
    async fn contains(&self, hash: &Hash) -> Result<bool>;
    async fn list(&self) -> Result<BlobStream>;
}

pub trait RootStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, Version)>>;
    async fn compare_and_set(&self, key: &str, expected: Option<Version>, value: Bytes)
        -> Result<CasOutcome>;                                // roots + lease
    async fn list(&self) -> Result<Vec<String>>;              // database catalog
}
```

Design constraints on the traits (so future backends fit without change):

- Blob operations are idempotent and require no ordering guarantees —
  eventual consistency is fine because content addressing makes stale reads
  impossible (you either have the bytes or you don't). Writers upload
  segments **before** publishing any root that references them.
- All coordination (root publication, transactor lease) funnels through
  `RootStore::compare_and_set`, the single primitive that must be strongly
  consistent. S3 conditional writes, Postgres, DynamoDB, etcd all provide it.
- Traits are asynchronous and support dynamic dispatch. Blocking v1 backends
  execute their synchronous bodies on the async runtime's blocking pool; future
  networked backends can implement the same traits with native async drivers.
  Blob enumeration is streamed so garbage collection does not require a full
  identifier vector in memory. v1 impls:
  - **MemoryStore** — `DashMap`, for tests and the simulator.
  - **FileStore** — segments as `objects/ab/cdef…` files (write-temp +
    rename), roots as files updated by lock-file-guarded atomic rename.

Encryption at rest ([encryption.md](encryption.md),
[ADR-0017](../adr/0017-encryption-at-rest.md)) is proposed as a `BlobStore`
decorator above the cache: blobs are sealed under a per-database data key, and
a blob id becomes the digest of the stored *encrypted* object. Because that
encryption is deterministic for a given (epoch, content), idempotent `put`,
structural sharing, keyless integrity verification, GC, and backup all keep
working exactly as described above.

A read-through, size-bounded **segment cache** wraps `BlobStore` reads. The
peer's optional SSD tier, LRU and capacity semantics, crash behavior, operator
configuration, and metrics are specified in
[peer-segment-cache.md](peer-segment-cache.md). The cache never covers mutable
roots and is not part of storage durability.

## Payload blobs

*Proposed; [ADR-0020](../adr/0020-blob-value-type.md).* A `:db.type/blob` datom
carries a 32-byte digest and a length
([data-model.md](data-model.md#value-model)); the bytes live in the same blob
store as everything else, and readers fetch them lazily.

The digest names a **blob manifest**, not the payload:

```
corium-blob-manifest-v1
<total byte length>
<chunk id>
<chunk id>
…
```

Chunk boundaries are content-defined, which buys the same three things the
index snapshot format gets from chunking: a large payload streams and reads by
range instead of being materialized whole, near-identical payloads share
chunks, and an unchanged payload re-uploads as nothing. This needs a **new**
boundary rule beside the one in `corium-core`'s `chunk` module: that one hashes
a whole index key and counts entries, which does not apply to a raw byte
stream. A rolling-window hash over the bytes (64-byte window, target 1 MiB
chunks, clamped to 256 KiB…4 MiB) plays the same role. A payload below the
minimum is a single chunk, but it is still wrapped in a manifest, so every blob
has one shape and the reachability walk has one case.

That walk is why the manifest exists in this form. `index_blob_children` — the
function GC and backup share *specifically* so their reachability can never
diverge — learns a second magic and returns a blob manifest's chunks the way it
already returns an index manifest's. No second walk, no second place to forget.

**Writes go peer-side.** A peer with storage credentials uploads the chunks,
then the manifest, then transacts the reference: the same
upload-before-you-name-it ordering the segment publisher obeys, for the same
reason. Bytes never cross the transactor, which sees a fixed-width reference —
41 encoded bytes, whatever the payload weighs — and does everything it does
today. The upload cannot move into `corium-tx`'s `prepare`,
which is a pure function of `(db, tx-data)` that the simulator and unit tests
call directly — so the convenience form (transacting raw bytes against a blob
attribute) is rewritten to a reference at the *peer boundary*, before tx-data
is built. Thin clients hold no storage credentials and stream their bytes to a
peer server, which uploads on their behalf ([protocol.md](protocol.md)).

**Reads go through the segment cache.** Query results, pull, the entity API,
and tx-reports carry the reference and nothing else; hydration is a separate
call. Payload chunks are read through the same read-through, size-bounded
cache as index chunks ([peer-segment-cache.md](peer-segment-cache.md)) — a blob
is content-addressed like a segment, so it needs no invalidation and no second
cache.

Under `EncryptedBlobStore` a payload's digest is the digest of the *encrypted*
object, exactly as it already is for index chunks. Payload confidentiality is
that encryption ([encryption.md](encryption.md)); attribute protection classes
seal the reference, which hides which payload a datom names rather than the
payload itself, and so do not apply to blob values.

## Garbage collection

Old index roots keep old segments alive only until no reader needs them.
GC is epoch-based and never urgent:

1. Transactor bumps `gc-epoch` and records the set of live roots.
2. Mark: walk live roots, collect reachable hashes (cheap: inner segments
   only, leaves counted via tree metadata; full walk is streaming).
3. Sweep: delete unreachable segments older than a retention window generous
   enough to cover any in-flight reader (default: days).

Because deletion is the only mutation and it only touches unreachable data, a
GC bug can strand garbage but a conservative window makes data loss a
non-risk. `deleteDatabase` = delete root, then sweep.

### Payload blobs are permanent, and expunge is the way out

*Proposed; [ADR-0020](../adr/0020-blob-value-type.md).* A committed payload is
live **forever**. The log is never truncated, so history is complete, so a
retracted blob datom is still a valid historical fact and its bytes must still
be fetchable at that basis. Nothing — not retraction, not an `as-of` window,
not an indexing pass — makes a committed payload collectable.

Published snapshots hold live facts only, though, so a retracted blob datom
has no pointer left in any index. Reachability therefore needs a record of its
own: the **payload root**, published by the indexing pass beside the four
index roots. It is an append-only set of every payload manifest ever
committed, sorted and chunked like an index, so a publish costs the new
references rather than the set. It is built by unioning the previously
published set with the references in the log tail — which is what keeps it
correct across a restart, where the in-memory database holds no pre-snapshot
history to re-derive the set from.

Everything downstream then works unchanged, because everything downstream
walks roots: GC marks payloads from the payload root, and backup copies them
from it.

`DbRoot` grows one trailing field for it — the record already extends by
appending lines older readers stop before — and the storage format goes to 4,
so a pre-blob binary refuses the database rather than sweeping payloads it
cannot see. A database with no blob attributes never grows a payload root.

Uploads whose transaction never committed are the only ordinary garbage here:
unreachable from any root, swept by the retention window that already exists,
with no new machinery.

**Expunge** is the single operation that removes payload bytes. It is
explicit, operator-driven, irreversible, and runs as an approved job
([operator-service.md](operator-service.md)):

- It drops the id from the payload set and sweeps what that makes unreachable
  — a mark-and-sweep restricted to the payload root, *not* a direct delete of
  a manifest's chunks. Payloads are deduplicated by content, so chunks a
  surviving payload still shares must survive with it.
- **The datom stays.** History remains honest that the fact was asserted; only
  the bytes go. Hydrating an expunged reference yields a defined `Expunged`
  outcome rather than a corrupt-store error — the shape ADR-0018 uses for an
  absent key — so queries that never hydrate are unaffected and one that does
  gets a clear answer instead of a storage fault.
- It does not reach backups taken before it. Erasing for real means expunging
  in the backup set too; the runbook in [operations.md](../operations.md) says
  so.

## Consistency argument (why this is safe)

- Segments are immutable ⇒ no read ever sees a torn or stale segment.
- A root is published only after every referenced segment is durably in the
  blob store ⇒ any root a reader obtains is fully dereferenceable.
- Root updates are CAS with the lease fenced by version ⇒ a deposed
  transactor's late publish fails cleanly (see log-and-transactor.md).
