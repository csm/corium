# ADR-0001: Full Datomic topology from day one

**Status:** Accepted (2026-07-15)

## Context

A Datomic-style system can start as an embedded library (Datalevin-style) and
grow distribution later, or commit to the transactor/peer/storage-service
separation immediately. Retrofitting the split is expensive: it changes what
the storage interface must guarantee (remote, cacheable, eventually
consistent blobs vs. local KV), where query runs, and how peers learn about
new transactions.

## Decision

Design the three roles — single-writer transactor, query-executing peers,
passive storage service — as distinct components with narrow interfaces from
the first commit. Early milestones run them in one process over an in-process
transport implementing the same service traits as the eventual gRPC layer
(M4); no engine code may assume co-location.

## Consequences

- The storage trait must be object-store-shaped (immutable blobs +
  CAS roots) from M0, which forces the content-addressed segment design
  (ADR-0003) rather than an embedded-KV shortcut.
- Slower to first end-to-end demo than an embedded-first plan; mitigated by
  keeping M0–M3 single-process behind the same interfaces.
- Pure engine crates stay free of tokio/network dependencies, which is what
  makes the deterministic simulator (testing-strategy.md) possible.
