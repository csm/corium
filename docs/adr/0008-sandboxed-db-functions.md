# ADR-0008: Sandboxed Clojurust database functions

**Status:** Accepted (2026-07-15); amended (2026-07-22) — the transactor-path
runtime is now the GC-less `cljrs-tx` interpreter (see Addendum).

## Context

Datomic's `:db/fn` transaction functions run arbitrary code on the transactor
inside transaction expansion. Options: native Rust built-ins only (defer user
functions), an unrestricted cljrs environment (trusting deployments), or a
restricted cljrs interpreter. The transactor is the availability bottleneck
of the whole system; code running inside its pipeline must be bounded.

## Decision

User transaction functions are cljrs code stored as datoms, executed on the
transactor in a sandboxed `cljrs-interp` environment: pure-`clojure.core` +
read-only db API namespace allowlist, no I/O or interop escape, no mutable
concurrency primitives, per-invocation fuel/allocation budgets with clean
transaction abort on exhaustion. Functions must be deterministic in
`(db, args)`; the log stores expanded datoms so replay never re-executes
functions. Core primitives (`:db/cas`, `:db/retractEntity`) are native Rust,
not sandboxed code.

## Consequences

- Users get Datomic's most distinctive extension point with the transactor
  protected against runaway or effectful functions.
- Requires fuel/step hooks in `cljrs-interp` — an upstream dependency risk
  tracked at the M5 checkpoint (fallback: watchdog-thread enforcement with
  namespace restriction only).
- The same sandbox host is reusable for user functions in query clauses
  (post-v1) on peers.

## Addendum (2026-07-22): `cljrs-tx` transactor runtime

Upstream clojurust now ships `cljrs-tx`, a purpose-built transaction-function
runtime: a tree-walker-only interpreter where each invocation runs in a fresh
bounded arena (no GC) under gas, managed-memory, and call-depth budgets, with
the whole environment destroyed when the call returns. The transactor's
`:db/fn` expansion now uses it directly (`corium-transactor` feature `cljrs`,
on by default), replacing the worker-thread sandbox from the original
decision in the transactor path:

- **Isolation by construction.** Only pure boundary data crosses into an
  invocation; the database never enters the arena. Functions receive an
  opaque token as their `db` argument, and the read-only `corium.api` host
  functions (`q`, `pull`, `entity`, `datoms`, time views, `basis-t`)
  interpret the token against the database value they close over — the
  `HostFn` seam serializes and validates every crossing value.
- **No watchdog.** Gas bounds execution deterministically, so the wall-clock
  deadline and abandoned-worker machinery are unnecessary; each invocation
  runs on a scoped thread whose stack is sized for the call-depth budget.
- **Global feature caveat.** `cljrs-tx` requires the cljrs stack's `no-gc`
  mode, a cargo feature that unifies onto every cljrs crate in the same
  build. `corium-cljrs` (the GC-mode Clojure client API) is therefore
  excluded from default workspace builds and is built standalone; it remains
  the client-side embedding surface and no longer sits in the transactor
  path.
