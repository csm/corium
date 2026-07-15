# ADR-0004: Full Datalog + Pull executing on the peer

**Status:** Accepted (2026-07-15)

## Context

Query capability could be phased (index API first, Datalog later) or
delivered whole. Query could execute on a server (client-server model) or on
the peer against local immutable data (Datomic peer model). The peer model is
already fixed by ADR-0001; the open question was v1 query scope.

## Decision

The first query milestone (M3) delivers the full EDN Datalog dialect —
patterns, predicates, native function clauses, not/or, recursive rules,
aggregates, multiple database args — plus the complete Pull grammar, the
entity API, and direct index access. Execution is always local to a peer
(thin clients get a server-hosted peer, ADR-0006/protocol.md).

## Consequences

- The query engine is a first-class deliverable with its own planner,
  statistics, and conformance corpus — the second-largest engineering item.
- No interim "index API only" public period; users meet the real API first.
- Horizontal read scaling and query isolation come free (queries consume
  application-process resources, not database-server resources).
- User-defined cljrs functions in query clauses are the one deferral
  (post-v1); the native built-in set covers common usage.
