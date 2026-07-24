# Peer local segment cache

## Status and scope

This document specifies an optional, Valcache-like SSD cache in front of the
native `BlobStore` used by Corium peers. It is a peer optimization, not a new
storage backend: the native store remains authoritative and peers continue to
read the same content-addressed segment IDs. The first implementation targets
`corium peer-server`; the same component must also be usable by an embedded
`corium-peer` connection when its application supplies a cache configuration.

The existing `SegmentCache` is an unbounded, process-memory map. It is useful
as a prototype but does not meet this contract. The production cache replaces
it with a bounded memory front tier plus the SSD tier described here. Query
result caches, mutable roots, transaction reports, and log-tail state are out
of scope. In particular, roots must always be read from the native
`RootStore`; caching a root would change Corium's consistency semantics.

## Goals

- Make repeated segment reads local and fast, especially when the native store
  is S3 or a remote database.
- Behave as a real cache: enforce a configured maximum byte capacity and evict
  the least-recently-used objects until back under that capacity.
- Be transparent. Enabling the cache must not change query results, basis,
  storage format, or the `BlobStore` contract; disabling it must retain the
  current native-store path.
- Remain disposable and self-healing after process crashes, partial writes,
  manual deletion, and index garbage collection.
- Expose enough Prometheus data to size the cache and distinguish useful hits,
  cold misses, eviction pressure, corruption, and native-store failures.

Non-goals are a shared cache service, write-back, pinning a working set, CDN
replacement, caching non-segment blobs by arbitrary user key, and making a
cache hit a durability guarantee.

## Operator interface

The peer-server command gains the following flags. The cache is disabled
unless both directory and non-zero capacity are supplied.

| Flag | Default | Meaning |
|---|---:|---|
| `--segment-cache-dir <path>` | unset | Dedicated local directory for cached segment files and metadata. |
| `--segment-cache-capacity <bytes>` | `0` | Hard accounting limit; accepts byte-size suffixes such as `256GiB`. |
| `--segment-cache-memory <bytes>` | `64MiB` when enabled | Optional bounded memory front tier; `0` disables it. |

Configuration is invalid when only one of directory/capacity is set, capacity
cannot be represented as `u64`, the directory is not writable, or the memory
tier exceeds total capacity. A peer fails startup on invalid configuration; it
must never silently run without an explicitly requested cache. Library users
receive the equivalent `SegmentCacheConfig { directory, capacity_bytes,
memory_capacity_bytes }` through the connection/storage builder.

The directory should be on a local SSD and must not be the native filesystem
store's directory. One directory is owned by one peer process. Multiple
databases hosted in that process may share it safely because blob IDs are
content hashes; no database name or tenant label is needed in a key. Sharing a
directory between processes is rejected with an exclusive lock rather than
attempting distributed LRU coordination.

Capacity is based on stored segment bytes plus cache metadata that grows per
entry; temporary files have a separately bounded allowance of at most one
in-flight object per loader. Admission may transiently exceed the configured
limit by the size of one completed object while the eviction lock is held, but
the public `get` does not complete until usage is at or below the limit. An
object larger than the total capacity is served from native storage and not
admitted.

## Read path and transparency

The cache wraps a `BlobStore` with the same read behavior:

1. Look up the segment ID in the bounded memory tier. A hit updates its LRU
   position and returns the immutable bytes.
2. Look up the ID in the SSD index. On a hit, open and read the file, verify its
   length and BLAKE3 digest, update recency, optionally admit it to memory, and
   return it.
3. Coalesce concurrent misses for the same ID so only one task fetches it from
   the native store. Other tasks await that result; unrelated IDs proceed in
   parallel.
4. On a native hit, verify that the returned bytes hash to the requested ID,
   return the bytes, and attempt SSD admission. Cache admission failure is
   logged and counted but does not fail a successful native read.
5. Preserve a native `None` and native-store errors exactly. Missing objects
   are not negatively cached, since a root can become observable immediately
   after its referenced blobs are uploaded on eventually consistent stores.

Every layer keys by the complete `BlobId`, not a shortened filename. Segment
immutability means there is no TTL and no invalidation message. The wrapper's
`put`, `delete`, `contains`, and `list` operations, if exposed, delegate to the
native store; only a successful `get` populates the peer cache. A native GC
delete does not have to delete a cached copy: the copy is disposable and will
eventually be evicted. Reads made through a still-live root remain safe because
the bytes are verified by content hash.

If a cached file is missing, truncated, unreadable, or fails digest
verification, the entry is removed, corruption is counted, and the request is
retried once as a cache miss against native storage. Cache I/O errors therefore
degrade performance rather than availability. Native-store errors still reach
the caller; the cache must not turn them into `None`.

## On-disk layout and crash recovery

Objects use fan-out paths such as `objects/ab/<remaining-hex-digest>`. A miss
is written to a uniquely named file under `tmp/`, flushed, atomically renamed
into `objects/`, and only then made visible in metadata. Concurrent admission
of identical content is idempotent. Cache files are read-only after rename.

The metadata index records ID, byte length, and a monotonically increasing
access generation. It may use an embedded database or an append/checkpoint
format, but updates to object visibility and accounted size must be atomic.
Recency writes should be batched so a hot-hit workload does not turn every
read into an SSD sync. Batched generations may make recently read entries look
slightly older after a crash; this affects eviction choice, never correctness.

At startup the cache acquires its ownership lock, removes abandoned temporary
files, and reconciles metadata with object files before serving reads:

- metadata without a file is dropped;
- files without metadata are validated and imported, or deleted if invalid;
- stored lengths and digests are checked lazily on first read (with an optional
  sampled/eager scrub), not by reading the entire cache at every startup; and
- if reconciled usage is over capacity, the oldest entries are evicted before
  startup completes.

An unrecoverable metadata database is quarantined and rebuilt by scanning
object filenames. Operators can always stop the peer and delete the entire
directory. No cache recovery procedure may require access to mutable roots or
modify the native store.

## LRU and admission

SSD eviction is exact LRU with respect to the access generations committed to
metadata. Both SSD hits and successful native admissions count as access. The
evictor repeatedly removes the entry with the smallest generation, atomically
removes it from the index/accounting, and unlinks its file until
`used_bytes <= capacity_bytes`. Failed unlink attempts are retried and remain
accounted so the configured maximum cannot drift silently.

The memory tier independently uses byte-bounded LRU. An SSD eviction also
invalidates the same memory key so reported tier occupancy is intelligible,
although an already returned `Arc<[u8]>` may live until its query completes.
Memory bytes held by callers are not part of SSD capacity and are exposed
separately.

The initial policy admits every object that fits. This is deliberately simpler
than frequency admission and gives scans predictable behavior. If scans cause
harmful churn, a later policy (for example, admit-on-second-read) can be added
behind an explicit option without changing the read-through contract.

## Concurrency and lifecycle

Lookup does not hold the global eviction/metadata lock during native I/O or
file reads. Per-key single-flight state has a bounded lifetime and is removed
after success or failure. Cancellation of the loading task wakes waiters, who
may elect a new loader. Eviction never removes an open file from under a read
on platforms where that is unsafe: an entry has a short-lived reader lease,
and eviction skips/queues leased entries.

Shutdown stops new admissions, waits for admitted-file renames and the current
metadata batch to finish, then releases the directory lock. Forced termination
is safe because startup reconciliation handles every intermediate state.

## Prometheus metrics

When `--metrics-listen` is enabled, peer metrics include the following. The
`tier` label is bounded to `memory` or `disk`; there are no database, segment,
path, or error-string labels.

| Metric | Type | Meaning |
|---|---|---|
| `corium_peer_segment_cache_requests_total{result="hit|miss",tier="memory|disk"}` | counter | Tier lookups; a request can miss memory then hit disk. |
| `corium_peer_segment_cache_native_fetches_total{result="found|not_found|error"}` | counter | Fetches that reached the authoritative store after single-flight coalescing. |
| `corium_peer_segment_cache_bytes_read_total{source="memory|disk|native"}` | counter | Bytes returned by each source. |
| `corium_peer_segment_cache_admissions_total{result="admitted|too_large|io_error"}` | counter | SSD admission outcomes. |
| `corium_peer_segment_cache_evictions_total` | counter | Objects evicted from SSD by capacity enforcement. |
| `corium_peer_segment_cache_evicted_bytes_total` | counter | Bytes evicted from SSD. |
| `corium_peer_segment_cache_corruptions_total` | counter | Invalid files/metadata discovered and discarded. |
| `corium_peer_segment_cache_coalesced_waiters_total` | counter | Requests that joined an existing native fetch. |
| `corium_peer_segment_cache_used_bytes{tier="memory|disk"}` | gauge | Currently accounted bytes in each tier. |
| `corium_peer_segment_cache_entries{tier="memory|disk"}` | gauge | Currently accounted objects in each tier. |
| `corium_peer_segment_cache_capacity_bytes{tier="memory|disk"}` | gauge | Configured bounds (zero when that tier is disabled). |

Metrics are registered only with the peer's existing metrics handle and are
rendered on the existing `/metrics` endpoint. When the segment cache is
disabled, capacity/usage gauges may report zero and event counters remain zero;
no separate listener is created. A useful operator hit ratio is native fetches
avoided divided by logical reads; summing per-tier `hit` values is not a valid
ratio because a single read probes multiple tiers.

Recommended alerts are sustained disk usage above 95% accompanied by a high
eviction rate (undersized cache), corruption increasing (disk or software
fault), and native fetch errors increasing. Full capacity alone is normal for
an LRU cache.

## Security and operations

Cached segments contain database data at rest. The peer creates directories
and files owner-only, does not expose the path in metrics, and relies on the
host's filesystem encryption and access controls. Authorization remains at the
peer request boundary: sharing by content hash cannot grant a caller a way to
name or read a segment. Operators must include cache SSD bandwidth, inode
availability, and wear in capacity planning, but must exclude the cache from
backup and native-store garbage-collection procedures.

Changing capacity on restart is supported: shrinking performs startup
eviction; growing preserves existing entries. Changing the directory starts a
cold cache. Graceful disabling leaves files intact for a later re-enable, while
deleting the directory is the explicit purge operation.

## Implementation and acceptance plan

1. Introduce a cache-neutral segment-reader interface in `corium-store`, move
   the current memory map behind a byte-bounded implementation, and add metrics
   hooks that do not make the storage crate depend on Prometheus exposition.
2. Implement the locked SSD directory, crash-safe admission, metadata index,
   reconciliation, per-key single flight, and LRU eviction. Unit tests use a
   fake clock/generation source and injected file/native-store failures.
3. Thread optional configuration through `corium-peer` and peer-server CLI,
   and merge its counters into `corium_peer::metrics::Metrics::prometheus`.
4. Add integration and property tests proving:
   - disabled behavior is byte-for-byte equivalent to direct `BlobStore` reads;
   - a cold read fetches native once and a warm read succeeds with native
     storage unavailable;
   - concurrent cold reads fetch native once;
   - deterministic accesses evict the least-recently-used entry and usage is
     at or below capacity before calls return;
   - oversized objects bypass admission;
   - corrupt/truncated entries self-heal from native storage;
   - kills at every write/rename/metadata boundary reconcile on restart; and
   - all metrics have the stated increments, gauges, and bounded label sets.
5. Benchmark cold-native, warm-SSD, and warm-memory reads plus a mixed scan at
   several concurrency levels. The feature is ready when warm SSD improves
   remote-store latency, adds no correctness failures under fault injection,
   and never exceeds the documented transient capacity bound.

