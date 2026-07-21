# corium-index

Immutable ordered segment trees and in-memory live indexes for datoms.

## What it does

Provides the covering-index machinery that both the transactor's indexing job
and the peer's local database value are built on:

- **`Segment`** — an immutable, sorted, deduplicated index segment for one
  `IndexOrder`, with `O(log n)` seek by key.
- Live, in-memory indexes for the recent log tail that has not yet been folded
  into persistent segments.
- Merge iterators that present persistent segments and the live tail as a
  single ordered stream, which is what queries scan.

The same structures back all four covering indexes (EAVT, AEVT, AVET, VAET).

## Dependencies

- `corium-core` — for `Datom`, `IndexOrder`, and the sortable key encoding.
- `proptest` (dev) for ordering/merge property tests.

Pure, synchronous library code — no async, storage, or network dependencies.

## Architecture

Segments are immutable and content-addressable in spirit: once built they are
never mutated, so they can be cached anywhere without invalidation. Index keys
come from `corium-core`'s sortable encoding, so a byte-wise comparison is a
correct datom comparison in the chosen order. A database view is the ordered
merge of zero or more persistent segments with the in-memory live index; the
merge iterators handle assertion/retraction resolution so callers see the
resolved fact set. See
[`docs/design/indexes-and-storage.md`](../../docs/design/indexes-and-storage.md).
