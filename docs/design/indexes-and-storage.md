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
