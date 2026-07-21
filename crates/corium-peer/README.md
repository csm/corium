# corium-peer

The peer library: a live connection to a transactor, tx-report handling,
local immutable database values, sync, and the segment cache.

## What it does

Embedded in an application process, a peer gives you database values to query
locally without round-tripping the transactor:

- **`Connection`** — subscribes to the transactor's tx-report stream and folds
  every report into an immutable `Db` value; queries never block on the
  transactor.
- **Reconnect / resubscribe** — on disconnect it reconnects from its basis and
  the server backfills the gap from the durable log; with an HA pair it fails
  over across an endpoint preference list.
- **Segment cache** (`segment`) — reads immutable index segments directly from
  storage through a local cache for snapshot bootstrap and large scans.
- **`sync`**, tx-report queue, `Admin` operations, and index-policy settings.
- **`server`** — the peer server that hosts a peer as a standalone gRPC
  endpoint for thin clients.

## Dependencies

- Engine: `corium-core`, `corium-db`, `corium-log`, `corium-query`,
  `corium-store`.
- Network: `corium-protocol`, `tonic`, `tokio`/`tokio-stream`, `async-trait`.
- `tracing`, `thiserror`.

## Architecture

The peer is the read side of the topology. All query execution happens here,
against immutable `Db` values, so obtaining a value never blocks on and never
coordinates with the writer. State a peer holds is either immutable (segments,
log tail up to a basis-`t`) or disposable (caches), so crash/restart loses
nothing — it resubscribes from its last basis and the transactor backfills.
Failover is client-side: subscriptions and transactions walk an endpoint
preference list and rediscover the lease holder from the root record. The peer
server wraps this same library so non-Rust clients reach it over gRPC. See
[`docs/architecture.md`](../../docs/architecture.md) and
[`docs/design/protocol.md`](../../docs/design/protocol.md).
