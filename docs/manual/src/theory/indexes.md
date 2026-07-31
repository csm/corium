# Indexes and storage

## The four covering indexes

Corium keeps four covering indexes. Each one is a total sort of all datoms, or
of a subset of them. Every index holds whole datoms, so an answer comes from
one index without a second lookup.

| Index | Sort order | Contains | Serves |
|---|---|---|---|
| EAVT | e, a, v, tx | All current datoms | Entity access, pull |
| AEVT | a, e, v, tx | All current datoms | Column scans, clauses with a known attribute |
| AVET | a, v, e, tx | Datoms of `:db/index` and `:db/unique` attributes | Value lookups, ranges, uniqueness, lookup refs |
| VAET | v, a, e, tx | Reference-typed datoms | Reverse references, graph walks, component traversal |

Each index has a history variant. The history variant keeps retractions and
superseded assertions. Attributes marked `:db/noHistory` are not retained in
the history indexes.

The operational rule follows from the table. An attribute needs `:db/index`
or `:db/unique` before a query can seek it by value. Without one of them, a
value lookup is a bounded scan of AEVT.

## Immutable segments

Each index is a persistent tree. A leaf segment holds a sorted run of encoded
datoms, about 50 KB to 100 KB compressed. An inner segment holds separator
keys and child hashes.

A segment is addressed by the BLAKE3 hash of its bytes. Segments are
write-once. A new tree reuses every unchanged subtree by hash.

Content addressing has three operational effects.

- A segment is cacheable anywhere with no invalidation protocol.
- A publication uploads only the leaves that changed, not the whole database.
- A stale read is impossible. A reader either has the bytes or does not.

## The database root

A database is named by its root record in the root store. The root holds the
database name and id, the basis `t`, the index basis `t`, the eight index
roots, the log root, the keyword table root, the schema revision, the garbage
collection epoch, the format version, and the write lease.

The lease lives inside this record. Every lease acquisition, every renewal,
and every index publication is a compare-and-set on the same bytes. One atomic
operation therefore fences the writer, which is why no consensus protocol is
needed.

The current database value equals the index trees at the index basis, merged
with the log tail after that basis.

## What a peer holds today

> **Partly implemented.** The published format names content-defined leaf
> chunks under a manifest, so consecutive roots share every untouched chunk.
> The inner tree levels are future work. A reader therefore cannot seek into a
> published index, and it materializes the whole index in memory.

This limit has three consequences that an operator must plan for.

- A peer keeps every datom it has seen, including retractions. Its memory
  tracks total history, not the size of the live database.
- A cold time view costs a fold of the whole history, not of the view. First
  touch of a distinct `as-of`, `since`, or `history` view is slow.
- Facts are allocated once and shared by handle, so the four indexes cost keys
  and pointers rather than copies.

Size a peer against total history. The
[time chapter](time.md) states the cost of each view.

## Incremental publication

One rule decides where a sorted key stream is cut. The published format and
the in-memory segment obey the same rule, so a segment leaf is exactly one
published chunk.

The indexing job therefore folds the log tail into the segments of the last
publication. It rebuilds only the leaves that the tail touched, and it carries
every other leaf across by handle. Work per pass tracks the tail, not the size
of the database.

A pass that cannot reuse the last publication rebuilds each segment from the
database value. A rebuild is a full re-encode, but not a full re-upload,
because content-defined boundaries reproduce the chunks of the previous
process.

The pass can carry chunks over only while it can prove that the root that
names them stays live. It pins the index state through the root
compare-and-set. If the pin fails, nothing is installed, and the pass retries
from a rebuild.

## Storage traits

The storage service has two parts.

- The **blob store** holds immutable, content-addressed objects. Its
  operations are idempotent and need no ordering guarantee.
- The **root store** holds a small mutable map of named pointers. It is
  updated by compare-and-set, and it is the only strongly consistent state in
  the system.

A writer uploads every segment before it publishes a root that names the
segment. Any root that a reader obtains is therefore fully dereferenceable.

## Garbage collection

Old roots keep old segments alive until no reader needs them. Collection is
epoch-based and never urgent.

1. The transactor bumps the collection epoch and records the live roots.
2. Mark: walk the live roots and collect every reachable hash.
3. Sweep: delete unreachable segments older than the retention window.

Deletion is the only mutation, and it touches only unreachable data. A bug in
collection can therefore strand garbage, but a generous window makes data loss
a non-risk. See [garbage collection](../availability/gc.md).

## Segment cache

A read-through, size-bounded cache wraps blob store reads. A peer server can
add an SSD tier with `--segment-cache-dir` and `--segment-cache-capacity`.
The cache never covers mutable roots, and it is not part of durability.

## Why this is safe

- Segments are immutable, so no read sees a torn or stale segment.
- A root is published only after every referenced segment is durable.
- A root update is a compare-and-set fenced by the lease version, so a late
  publish by a deposed transactor fails cleanly.
