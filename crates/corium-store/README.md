# corium-store

The storage-service abstraction: a content-addressed blob store plus a fenced,
compare-and-swap root store, with pluggable backends.

## What it does

Defines the two traits that make up Corium's passive storage service, the
runtime backend registry, and the two dependency-free built-in implementations:

- **`BlobStore`** — immutable, content-addressed segments (index tree nodes,
  log chunks). Write-once, never mutated, safe to cache anywhere:
  `put(hash, bytes)`, `get(hash)`, `delete(hash)` (GC only).
- **`RootStore`** — a tiny mutable map of named pointers (database roots, the
  transactor lease) updated by **compare-and-swap**. This is the only mutable,
  strongly consistent state in the system.
- A local **segment cache** and the index-manifest snapshot format used to
  enumerate a root's reachable blobs (backup, GC, snapshot bootstrap).

Backends are separate delivery units:

- **memory** — ephemeral, for tests and demos.
- **filesystem** — single-node dev/prod (default).
- **postgres** — `corium-store-postgres`.
- **turso** — `corium-store-turso`, also shipped as the reference dynamic plugin.
- **s3** — `corium-store-s3`.

Every driver registers a `StorageBackend` factory. Engine code carries only a
backend kind, redacted JSON configuration, and `Arc<dyn FullStore>`; it does
not enumerate driver types. A host may link a driver crate and call its
`register` function, or load an ABI-compatible shared library through
`corium-store-plugin`.

## Dependencies

- `corium-core` for shared types; `blake3` for content-address digests;
  `async-trait`, `tokio`/`tokio-stream` for the async trait surface; `fs2` for
  filesystem locking; `thiserror` for errors.
- Driver dependencies remain in their driver crates. In particular, this core
  crate has no AWS SDK, PostgreSQL, or Turso dependency.

## Architecture

The store is deliberately "dumb": it knows nothing about datoms, transactions,
or queries. All correctness lives above it. Blobs are keyed by their BLAKE3
digest, so identical content deduplicates and no write ever invalidates a
cached read. The only coordination primitive is the root store's CAS, which is
what fences the write lease and publishes new index roots atomically. The trait
is sized for S3-class object stores, which is why the same interface serves the
in-memory, filesystem, and cloud backends. See
[`docs/design/indexes-and-storage.md`](../../docs/design/indexes-and-storage.md).
