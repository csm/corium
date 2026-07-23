# Write-path scaling

How the transactor's single-writer commit path was scaled, what it measures
today, and the next steps. Companion to
[log-and-transactor.md](log-and-transactor.md); that document is the
authoritative description of the pipeline, this one records the performance
arc and the open work.

## The bottleneck

The transactor is one logical writer per database (the serialization point
that defines transaction order `t`). Every commit ran the whole critical
section — validate, durable append, ownership fence — serially, so
per-database write throughput was bounded by the *sum* of those stages on one
core plus the I/O wait, and offered concurrency only queued behind it.

## What was done

Three changes, each independently measured (all through
`crates/corium-transactor/examples/tx_throughput.rs`, see
[tx-throughput.md](../benchmarks/tx-throughput.md)):

1. **Per-transaction native log.** The `PostgreSQL`/Turso/S3 log stopped
   rewriting a growing chunk object per commit (a read-modify-write whose cost
   grew with the chunk) and now writes **one create-only object per
   transaction** — a row insert on SQL, a create-only `PUT` on S3. Append went
   from O(chunk) to O(1). Older chunk-format logs are still read read-only, so
   existing databases replay after an upgrade with no migration.
2. **Group commit.** Concurrent transactions to one database commit as a
   **batch under one durable write and one ownership fence**, while keeping
   each transaction's external boundary intact (its own `t`, report, and ack).
   A batch is one atomic log object, so a takeover's cutoff keeps all or none
   of it. The prepare step stays serial (each transaction validates against a
   staging value that already includes its predecessors), so semantics are
   unchanged.
3. **Configurable cap + one fence on the common path.** Batch size is bounded
   by `NodeConfig::max_commit_batch` (count, default 256) and
   `max_commit_batch_bytes` (default 4 MiB). The pre-append lease check was
   dropped from the common path — the post-append fence is the safety-critical
   one — removing a lease round trip per batch; only a keyword-interning batch
   re-checks ownership first (it publishes the unfenced metadata root).

## Measured arc

Relative baseline from one setup: a laptop client driving a LAN `PostgreSQL`
(and a MinIO S3 endpoint) on a small home server, so the remote round trips
are real network + disk, not loopback. Numbers are hardware/network specific
and single-shot — treat the *shape* as the result, not the absolute figures.
Re-record on a change of machines.

**Peak write throughput (transactions/second):**

| stage | PostgreSQL | S3 (MinIO) |
|---|---:|---:|
| original (chunk-rewrite log, no batching) | ~7 (flat) | ~4.8 (flat) |
| + per-transaction log | 26 (conc 1) | 38 (conc 1) |
| + group commit (cap 64) | 1,598 (conc 64) | 912 (conc 32) |
| + configurable cap, dropped pre-check | **~6,500** (conc 256) | (not re-swept) |

The latency story matters as much as throughput. Originally, latency grew
linearly with offered concurrency (queueing): `PostgreSQL` p50 at concurrency
32 was ~4,700 ms. With group commit, p50 stays roughly **flat** as concurrency
rises (~25–40 ms) because a batch amortizes the durability cost across its
transactions — throughput scales with concurrency while per-commit latency
does not. Under no contention a batch is size 1, so low-load latency is
unchanged.

Two rules the sweep established:

- **Set `max_commit_batch` ≥ expected peak concurrency.** A cap below the
  offered concurrency splits the queued work into serialized batches, doubling
  latency and cutting throughput; above it, the batch is bounded by offered
  load anyway.
- **The cap only matters once concurrency exceeds it.** At concurrency 64,
  throughput was identical across caps 64–512, because a batch was ≤ 64
  regardless.

## Current bound

Throughput has gone sublinear at high concurrency (on the reference setup,
concurrency 128→256 was ~1.3× rather than 2×) and p50 rises with batch size.
A batch of 256 commits in ~32 ms → a theoretical ~8,000 tx/s, but ~6,500 is
observed. Two costs inside the leader's serial section account for the gap:

1. **Serial flushes.** One batch's entire commit (prepare + durable write +
   fence + install) holds the commit lock before the next batch starts, so
   throughput is capped at `batch_size / flush_time`, not
   `concurrency / op_latency`.
2. **Per-transaction expansion overhead.** With `:db/fn` enabled (the
   default), every transaction in a batch runs its own
   `spawn_blocking(expander.expand)` — N blocking-pool hops per batch, even
   for transaction data containing no function call — inflating the serial
   prepare loop as batches grow.

## Next steps

Roughly in increasing effort / decreasing certainty of payoff:

1. **Collapse batch expansion.** Run a batch's whole expand → convert →
   prepare loop in **one** `spawn_blocking` (or skip the expander for
   transactions with no `:db/fn`), removing the N blocking-pool hops per batch.
   Contained, directly attacks the large-batch prepare cost above. Likely the
   quickest remaining win.
2. **Pipelined flushes.** Let batch N+1's durable write start while batch N's
   is still in flight (on a separate backend connection), so throughput tracks
   `concurrency / op_latency` rather than `batch_size / flush_time`. This is
   the structural ceiling-lifter, but the larger change: transaction-number
   assignment stays serial while durable writes overlap, and the ownership
   fence and in-memory install must still apply in `t` order. Depends on a
   backend that supports concurrent writes (natural for S3's independent
   `PUT`s; a small connection pool for SQL).
3. **Optimistic-apply overlap across batches.** Validate batch N+1's CPU work
   against a staged value while batch N's flush is pending, rolling the stage
   back if N fails. Marginal while I/O dominates CPU, but it composes with
   pipelined flushes.
4. **Bounded queue with fast-fail backpressure.** The pending queue is
   currently unbounded; cap it and reject beyond the bound with a retriable
   busy error (clients back off), so a saturated transactor sheds load instead
   of growing memory.
5. **Filesystem batch durability.** `NativeVersionedLog` batches on the native
   backends; the filesystem `VersionedLog` still fsyncs per record under the
   default `append_batch_async`. A batch override (write all framed records,
   one `fsync`) would extend group commit's win to the `fs` backend.
6. **Object-store log sealing/compaction.** The per-transaction S3 tail grows
   one object per batch; the indexing job should periodically concatenate the
   tail into content-addressed `log-root` chunks and reclaim the small
   objects, bounding replay and list cost (see the object-store log layout in
   [log-and-transactor.md](log-and-transactor.md)).

Measurement note: the write-throughput comparison is only meaningful across
independent machines (client separate from store) with the store's real
durability (`synchronous_commit=on` for `PostgreSQL`, enforced conditional
writes for S3). Record the round-trip latency alongside each run.
