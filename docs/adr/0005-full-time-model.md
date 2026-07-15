# ADR-0005: Full time model in the first milestone set

**Status:** Accepted (2026-07-15)

## Context

Time-travel views (as-of, since, history), the log API, and tx-report queues
could ship incrementally after a current-view-only v1. But the datom `(tx,
added)` fields and the durable log must exist from the start regardless —
the question is only whether the views are exposed.

## Decision

M3 exposes the complete time model: `as-of`, `since`, `history` database
views, `tx-range`/log access, `sync`, and tx-report queues. History index
variants are maintained by the same indexing pass as current indexes from M2.
Excision is explicitly excluded (design space reserved in time-model.md).

## Consequences

- Index design must settle history-retention rules early (retraction pairs,
  `:db/noHistory`), avoiding a later reindex-the-world migration.
- Marginal cost over a current-only v1 is mostly in the query layer's view
  plumbing and conformance vectors — the storage cost is paid either way.
- Storage growth is unbounded by default (as in Datomic); `:db/noHistory` and
  (later) excision are the pressure valves, and GC only collects unreachable
  segments, not history.
