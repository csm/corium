# ADR-0008: Sandboxed Clojurust database functions

**Status:** Accepted (2026-07-15)

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
