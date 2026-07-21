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
and only chunks absent from the store are uploaded, so consecutive roots
share every untouched chunk. Inner tree levels (and with them seek-without-
full-download) are still future work; readers concatenate a manifest's
chunks and accept pre-format-3 flat snapshots.

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

A read-through, size-bounded **segment cache** (in-memory ARC/LRU, optional
disk tier) wraps any `BlobStore` on the peer and transactor side.

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

## Consistency argument (why this is safe)

- Segments are immutable ⇒ no read ever sees a torn or stale segment.
- A root is published only after every referenced segment is durably in the
  blob store ⇒ any root a reader obtains is fully dereferenceable.
- Root updates are CAS with the lease fenced by version ⇒ a deposed
  transactor's late publish fails cleanly (see log-and-transactor.md).
