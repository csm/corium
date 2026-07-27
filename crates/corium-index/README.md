# corium-index

Immutable ordered covering-index segments for datoms.

## What it does

Holds the published snapshot each covering index is made of, and folds a
transaction tail into it without rebuilding it:

- **`Segment`** — an immutable, sorted segment for one `IndexOrder`, holding
  current facts with `O(log n)` seek by key. A covering-index key encodes its
  whole datom, so a segment is just its key stream.
- **`Segment::apply`** — the current-value fold (assert replaces the fact,
  retract removes it) of a transaction tail into a segment, rebuilding only
  the leaves the tail touches. A fold keeps only the key each datom encodes
  to, so `apply_ref`/`from_sorted_ref` take a tail — or a whole covering
  index — the caller keeps, and publication indexes it without copying it.
- **`Leaf`** — one content-defined run of keys. Leaves are `Arc`-shared, and
  `Leaf::id` reports which ones a fold carried across untouched.

The same structures back all four covering indexes (EAVT, AEVT, AVET, VAET).
The *live* index — everything since the published basis — is the `Db` value
itself (`corium-db`), which already folds each commit into its own covering
index maps; this crate is the durable side that the indexing job merges into.

## Dependencies

- `corium-core` — for `Datom`, `IndexOrder`, the sortable key encoding, and
  the chunk-boundary rule.
- `proptest` (dev) for model and chunk-boundary property tests.

Pure, synchronous library code — no async, storage, or network dependencies.

## Architecture

Segments are immutable and content-addressable in spirit: once built they are
never mutated, so they can be cached anywhere without invalidation. Index keys
come from `corium-core`'s sortable encoding, so a byte-wise comparison is a
correct datom comparison in the chosen order.

A segment's leaves are cut at the boundaries in `corium-core`'s `chunk`
module — the same ones the published snapshot format cuts its chunks at, so
**one leaf is one published chunk**. That is what makes the transactor's
indexing job incremental: after `apply`, a leaf the tail did not touch is the
same allocation as before, so the publisher reuses the blob id it already has
and encodes only what it rebuilt. Boundaries depend only on the boundary key,
so a segment rebuilt from scratch lands on the same chunks a previous process
published. See
[`docs/design/indexes-and-storage.md`](../../docs/design/indexes-and-storage.md).
