# ADR-0002: Clojurust at the boundary only

**Status:** Accepted (2026-07-15)

## Context

Clojurust provides persistent collections, keywords, EDN reading, and an
interpreter. The engine could use `cljrs-value` as its native value type
throughout (maximum idiom fidelity), or keep an engine-owned representation
and convert at the edges. Index/storage hot paths need a compact,
order-preserving byte encoding and cheap comparisons; a general dynamic value
type there couples engine performance and layout stability to an external
project's internals.

## Decision

The engine core (`corium-core` through `corium-db`) uses its own `Value` enum
and sortable binary encoding. cljrs appears in exactly three places: value
conversion at the public API (`corium-cljrs`, via `cljrs-interop`
FromValue/IntoValue), EDN text parsing everywhere (`cljrs-reader` is the only
EDN reader in the system), and the sandboxed interpreter for database
functions (ADR-0008).

## Consequences

- Hot paths (segment compare, merge, join) are memcmp/integer operations,
  fully under Corium's control; storage format is independent of cljrs
  versioning.
- A conversion layer must exist and be property-tested for round-trip
  fidelity; per-call conversion cost at the API edge (amortized — one
  conversion per call, not per datom touched during execution).
- Rust consumers get a first-class typed API rather than a dynamic-value API.
