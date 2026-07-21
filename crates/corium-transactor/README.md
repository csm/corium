# corium-transactor

The single-writer transaction pipeline and index publisher, plus the networked
transactor process (lease, gRPC services, indexing job, HA).

## What it does

Corium's write side. Two layers:

- **Embedded pipeline** — serialize transactions, expand/validate them (via
  `corium-tx`, including sandboxed `:db/fn` through `corium-cljrs`), assign the
  tx entity and timestamp, append to the durable log, ack the caller, and emit
  a `TxReport`.
- **Networked node** — the standalone transactor process: acquire and renew the
  write **lease** (CAS-fenced in the root record), serve the Transactor/Catalog
  gRPC services, stream tx-reports to peers with gapless backfill, and run the
  background **indexing job** that folds the log tail into fresh covering-index
  trees and publishes a new index root.

Also provides offline **backup**/restore, Prometheus **metrics**, storage
`backend`/`StoreSpec` selection, and `--ha` standby mode (lease polling,
takeover-as-crash-recovery, depose-to-standby).

## Dependencies

- Engine: `corium-core`, `corium-db`, `corium-index`, `corium-log`,
  `corium-query`, `corium-store`, `corium-tx`.
- Network: `corium-protocol`, `tonic`, `tokio`/`tokio-stream`, `async-trait`.
- `tracing` for observability; `thiserror` for errors.
- Feature-gated storage backends: `postgres`, `turso`, `s3` (forwarded to
  `corium-store`).

## Architecture

Exactly one transactor holds a database's write lease at a time; the lease is
folded into the CAS-fenced database root (storage format 2), so acquisition,
renewal, and index publication are all fenced by a single atomic root update. A
deposed writer's stale log appends are discarded on merged replay, and the
commit pipeline re-verifies ownership after the durable append and before every
ack — so a standby takeover loses no acked transaction and produces no
duplicate. Indexing is asynchronous and idempotent: the log is authoritative,
index roots are just published folds of it. See
[`docs/design/log-and-transactor.md`](../../docs/design/log-and-transactor.md)
and the HA runbook in [`docs/operations.md`](../../docs/operations.md).
