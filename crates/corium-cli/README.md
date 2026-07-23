# corium-cli

The `corium` binary: process launchers and admin commands for the whole
distributed topology.

## What it does

Ships the `corium` command. Subcommands:

- **`transactor`** — run a transactor process over a storage backend (`mem`,
  `fs`, `postgres`, `turso`, `s3`), with lease/HA (`--ha`, `--advertise`) and
  indexing options.
- **`peer-server`** — host a peer as a gRPC endpoint for thin clients.
- **`postgres-server`** — serve the database catalog over the PostgreSQL wire
  protocol (read-only) for standard PostgreSQL clients and drivers; clients
  pick a database with `USE`/the startup parameter and list them with
  `SHOW DATABASES`.
- **`db create` / `delete` / `fork` / `list` / `stats`** — database
  administration, including restore-as-clone forks.
- **`db request-index` / `index-policy`** — drive and tune background indexing.
- **`console`** — interactive query console with as-of/since/history views,
  schema/stats/basis inspection, timing, and live tx-report watch.
- **`tui`** — a full-screen terminal dashboard (query workbench, live store
  metrics, transaction feed, schema browser).
- **`sql`** — read-only SQL shell over the peer-local database.
- **`backup` / `restore`** — offline full and hash-incremental backup and
  guarded restore.
- **`gc`** — retention-aware garbage collection (online or offline).
- **`log`** — inspect the transaction log.

## Dependencies

- Every Corium library crate (`corium-core`, `corium-db`, `corium-peer`,
  `corium-pgwire`, `corium-protocol`, `corium-query`, `corium-sql`,
  `corium-store`, `corium-transactor`, `corium-log`).
- `clap` (arg parsing), `tokio` + `tonic` + `rustls` (networking/TLS),
  `ratatui` + `rustyline` (TUI/console line editing), `tracing`-subscriber
  (human/JSON logs).
- Storage backends are feature-forwarded: `postgres`, `turso`, `s3`. The
  `cljrs` feature (default) forwards the transactor's built-in `:db/fn`
  runtime; `--db-fn-fuel` and `--db-fn-memory-bytes` set its budgets.

## Architecture

This crate is a thin composition layer — it wires the library crates into
runnable processes and interactive tools but holds little logic of its own. The
transactor and peer-server subcommands construct and run `corium-transactor` and
`corium-peer` servers; `postgres-server` serves the `corium-pgwire` protocol
over a catalog of lazily cached peer connections; the `console`, `tui`, `sql`,
and `db *` subcommands drive a peer `Connection` and render results. Storage-backend selection and TLS/token
auth are surfaced as flags and forwarded down to the relevant crate. See
[`docs/getting-started.md`](../../docs/getting-started.md) and
[`docs/operations.md`](../../docs/operations.md).
