# corium-cljrs

Clojurust integration for Corium: boundary value conversion, the `corium.api`
client namespace, and the sandboxed database-function host.

## What it does

Bridges Corium and [Clojurust](https://github.com/csm/clojurust) (`cljrs`) so
EDN/Clojure is the language at the API boundary and inside database functions:

- **`convert`** — bidirectional conversion between cljrs values and Corium
  `Value`s (with a `cljrs-reader` text bridge for reading EDN source).
- **`api`** — the `corium.api` namespace bound to `corium-peer`:
  `connect`/`transact`/`q`/`pull`/`entity`/`datoms`/`as-of`/`since`/`history`/
  `tx-range`/`tx-report-queue`/`sync`.
- **`dbfn` / `sandbox`** — the host that runs `:db/fn` code on the transactor in
  a restricted interpreter: allowlisted environment, fuel/allocation/
  call-depth budgets, and a watchdog deadline.
- **`query`** — fills `corium-query`'s function/predicate resolution seam with
  sandboxed cljrs evaluation.

## Dependencies

- Corium: `corium-core`, `corium-db`, `corium-peer`, `corium-query`,
  `corium-transactor`.
- Clojurust: `cljrs-builtins`, `cljrs-env`, `cljrs-gc`, `cljrs-interop`,
  `cljrs-interp`, `cljrs-reader`, `cljrs-value`.
- `tokio` (peer/transactor async), `thiserror`.

## Architecture

cljrs isolates own per-thread GC heaps, so every cljrs value is confined to the
thread that created it. This crate enforces two rules throughout: (1) anything
crossing between an engine thread and a cljrs isolate travels as plain boundary
EDN, never as a live cljrs handle; and (2) each sandbox owns a dedicated worker
thread that doubles as the watchdog boundary for runaway user code — the
interpreter's pluggable call hook enforces the fuel/depth budgets inline, with
the worker-thread deadline as a hard backstop. Sandbox escapes (I/O, interop,
namespace manipulation, unbounded loops) fail safely. See
[`docs/design/clojurust-integration.md`](../../docs/design/clojurust-integration.md),
[ADR-0002](../../docs/adr/0002-clojurust-boundary.md), and
[ADR-0008](../../docs/adr/0008-sandboxed-db-functions.md).
