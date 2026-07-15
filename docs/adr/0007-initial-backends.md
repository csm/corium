# ADR-0007: In-memory + filesystem backends first

**Status:** Accepted (2026-07-15)

## Context

The storage service (ADR-0003) needs concrete backends. Production-grade
options (S3-compatible object stores, Postgres, DynamoDB) each bring
credentialing, consistency quirks, and operational surface that would slow
the engine milestones without changing the engine's shape.

## Decision

v1 ships exactly two backends behind the `BlobStore`/`RootStore` traits: an
in-memory store (tests, simulator) and a local-filesystem store (single-node
dev and small production), with CAS-fenced root updates in both. The traits
are constrained now so later backends need no core changes: blobs are
idempotent/eventually-consistent; all coordination funnels through one CAS
primitive; lease fencing is validated inside the root-store CAS.

## Consequences

- Engine milestones proceed without cloud dependencies; the simulator gets a
  first-class in-memory backend with fault injection.
- Single-node filesystem deployments are a supported v1 configuration.
- S3-compatible and Postgres backends are post-v1 backlog items whose
  feasibility is guarded by trait-level contract tests (a backend test kit
  runs the same suite against every implementation).
