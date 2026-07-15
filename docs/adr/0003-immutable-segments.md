# ADR-0003: Custom immutable content-addressed segments

**Status:** Accepted (2026-07-15)

## Context

The index layer could sit on an embedded KV store (RocksDB/redb/LMDB) with
datoms as sorted keys, or replicate Datomic's actual design: covering indexes
as persistent trees of immutable, content-addressed segments in a dumb blob
store. The peer model (ADR-0001) requires index data to be remotely readable
and cacheable without invalidation; embedded KV stores are neither.

## Decision

Build the segment design: prefix-compressed, zstd-block-compressed leaf
segments of encoded datoms; inner segments of separator keys; BLAKE3 content
addressing; structural sharing across index roots; a CAS'd root record as the
only mutable state. No embedded KV dependency in the core.

## Consequences

- Enables the peer model, trivially correct caching at every tier, O(shared)
  incremental backups, restore-as-clone, and crash-only recovery.
- We own a real storage engine: tree merge, segment sizing, compression, GC —
  the largest single engineering item in the plan (M1), with property/model
  tests as the safety net.
- Write amplification is managed by the live-index + periodic-indexing split
  (log absorbs writes; trees rebuilt in bulk), which must be built rather
  than borrowed.
