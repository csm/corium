# ADR-0006: gRPC control plane + custom tagged value encoding

**Status:** Accepted (2026-07-15)

## Context

Transactor↔peer and thin-client communication needs RPC, server streaming
(tx-reports), TLS/auth, and versioning. Candidates: a fully custom framed
binary protocol (Fressian-over-TCP in Datomic's spirit), EDN/Transit over
WebSocket, or gRPC. EDN's open value set does not map cleanly onto protobuf's
closed message model, so a pure-protobuf data plane was never on the table.

## Decision

gRPC (tonic/prost) carries the control plane: service/method structure,
framing, HTTP/2 streaming, TLS, deadlines, versioned APIs. Values — tx-data,
query args/results, datoms — travel as protobuf `bytes` holding a
Corium-defined tagged binary encoding that shares its tag space with the
sortable segment encoding (one codec module, two variants). Index segments
never travel over gRPC; peers read the blob store directly.

## Consequences

- Mature Rust ecosystem for the hard networking parts (streaming, TLS, load
  shedding); thin clients in any language get transport for free and need
  only implement the value codec, which ships with test vectors.
- Two encodings in one module (sortable vs. tagged-composite) with
  cross-consistency property tests; the codec spec becomes a public,
  versioned artifact.
- Less "Clojure-native" wire debuggability than EDN-over-WebSocket; mitigated
  by CLI tooling (`corium log`, console) that renders everything as EDN.
