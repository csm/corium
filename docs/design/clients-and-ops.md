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
