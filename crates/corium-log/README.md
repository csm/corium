# corium-log

Durable, append-only transaction logs with replay and range scans.

## What it does

The transaction log is Corium's source of truth — indexes are a deterministic
fold of it. This crate defines:

- **`TxRecord`** — one committed transaction: monotonic `t`, `tx_instant`
  timestamp, and the asserted/retracted `datoms`.
- **`TransactionLog`** — the append/replay/range-scan trait: durably append the
  next transaction, replay from the start, and scan a `t` range.
- Log chunk format plus append-only file implementations, including the
  per-lease-version split used for HA (a deposed writer's stale appends are
  discarded during merged replay).

## Dependencies

- `corium-core` — for `Datom`, `EntityId`, and value encoding.
- `thiserror` for errors; `tempfile` (dev) for tests.

Pure, synchronous library code — no async or network dependencies.

## Architecture

Logs are append-only and replayable: given the log, the entire index state is
reconstructible, which is what makes peer crash/restart lossless and GC safe.
Records are length-framed and value-encoded with `corium-core`'s codec so a
partial trailing write is detectable (`LogError::Corrupt`) rather than silently
accepted. For high availability the log is split into per-lease-version files;
merged replay orders them by lease version and drops appends from a fenced-out
writer, so a standby that takes over never replays a deposed transactor's
uncommitted tail. See
[`docs/design/log-and-transactor.md`](../../docs/design/log-and-transactor.md).
