# Clients and Operations

## `corium` CLI

One binary (`corium-cli`) with subcommands:

```
corium transactor --config corium.edn      # run a transactor
corium peer-server --config corium.edn     # run a peer server for thin clients
corium db create|delete|list <uri>
corium db stats <uri>                      # datom counts, index sizes, basis
corium gc <uri> [--window 72h]             # segment garbage collection
corium backup <uri> <dest>                 # see below
corium restore <src> <uri>
corium log <uri> --from t1 --to t2         # dump tx-range as EDN
corium console <uri>                       # interactive query console
corium sql <uri>                           # interactive read-only SQL shell
corium tui <uri>                           # full-screen dashboard (queries + metrics)
```

Config files are EDN (read via cljrs-reader): storage backend + credentials,
listen addresses, TLS, memory/fuel budgets, index thresholds.

## Query console

`corium console` embeds a peer and a line editor (rustyline):

- Enter Datalog/pull forms as EDN; results pretty-printed as EDN tables.
- Console commands: `:as-of t`, `:since t`, `:history on|off`, `:basis`,
  `:schema [attr]`, `:stats`, `:watch` (tail tx-reports live), timing output.
- Doubles as the smoke-test surface for every milestone demo.

A full cljrs REPL with `corium.api` loaded (via the cljrs nREPL/REPL tooling)
is the richer alternative for Clojure-fluent users and costs us nothing beyond
the M5 bindings — the console exists so the database is explorable without
knowing Clojure.

## Terminal dashboard

`corium tui` embeds a peer and a ratatui full-screen interface — the moral
equivalent of Datomic's web console for the terminal:

- **Query** panel: the console's Datalog/pull/`:command` surface with an
  inline editor, query history, and tabular relation results (headers from
  `:find`), reporting latency and datoms scanned per run.
- **Metrics** panel: data-store statistics from the transactor `Status` RPC
  polled on an interval — counts, basis/index lag, queue depth, tx totals
  and failure rate, GC, lease state — with sparklines for transaction
  frequency, status round-trip latency, and index lag.
- **Transactions** panel: live tx-report feed with a datom detail pane.
- **Schema** panel: filterable attribute browser.

It shares `ClientFlags` (and the console `Session`) with `corium console`,
so time views (`:as-of`, `:since`, `:history`) work identically.

## SQL shell

`corium sql` embeds the peer-local SQL engine and uses the same connection
flags as `corium console`. It supports interactive, `-c` command, and `-f` file
modes. Backslash commands select current/as-of/since/history sessions, inspect
the basis and registered relations, and enable timing. SQL is read-only; see
[../sql.md](../sql.md) for the relational projection and Rust API.

## Backup and restore

Backups exploit immutability: a backup is (a copy of every segment reachable
from a DbRoot) + (that root), written to a directory/object tree in blob-store
layout. Incremental backups copy only hashes absent from the destination —
structural sharing makes dailies cheap. Restore = copy back + install root
under a (possibly new) database name; it is also the storage-migration path
(fs → S3 later) and the fork/clone primitive (restore under a new name into
the same store is O(root) thanks to sharing).

## Observability

- `tracing` throughout; JSON or human log output.
- Metrics (prometheus endpoint on transactor and peer server): tx throughput
  and latency histograms, queue depth, log flush latency, indexing job
  duration/lag (basis-t minus index-basis-t), segment cache hit rates, blob
  store op latencies, per-query fuel spend on peer servers.
- `corium db stats` and `Status` RPC expose the same numbers ad hoc.

## Versioning and compatibility posture

- `format-version` in the DbRoot gates storage compatibility; v1 promises
  read-compat within a major version and provides `backup`/`restore` as the
  migration path across majors.
- The gRPC protocol carries an explicit version; peer/transactor mismatch
  degrades to a clear error, never silent misbehavior.
