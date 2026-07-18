# Corium — Project Plan

Corium is a database system in the style of [Datomic](https://www.datomic.com/):
an immutable, time-aware, fact-oriented database with Datalog queries, a
single-writer transactor, and read scaling through peers that query locally
against cached immutable index segments. It is written in Rust and pairs with
[Clojurust](https://github.com/csm/clojurust) (`cljrs`) for EDN/Clojure data
handling at the API boundary and for executing database functions.

This document is the entry point to the plan. The design is elaborated in
`docs/`:

| Document | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | System topology, process roles, crate layout |
| [docs/design/data-model.md](docs/design/data-model.md) | Values, sortable encoding, datoms, schema |
| [docs/design/indexes-and-storage.md](docs/design/indexes-and-storage.md) | Covering indexes, immutable segments, blob store, roots, GC |
| [docs/design/log-and-transactor.md](docs/design/log-and-transactor.md) | Transaction log, transaction pipeline, background indexing, HA design |
| [docs/design/time-model.md](docs/design/time-model.md) | as-of, since, history, log API, tx-report queue |
| [docs/design/query-engine.md](docs/design/query-engine.md) | Datalog compiler/planner, rules, aggregates, Pull, entity API |
| [docs/design/protocol.md](docs/design/protocol.md) | gRPC services, value wire encoding, peer sync, thin-client protocol |
| [docs/design/clojurust-integration.md](docs/design/clojurust-integration.md) | Boundary conversion, sandboxed database functions, cljrs client API |
| [docs/design/clients-and-ops.md](docs/design/clients-and-ops.md) | CLI, query console, backup/restore, metrics |
| [docs/getting-started.md](docs/getting-started.md) | Build and first local database walkthrough |
| [docs/operations.md](docs/operations.md) | Process, console, backup/restore, metrics, GC, recovery runbook |
| [docs/thin-client-protocol.md](docs/thin-client-protocol.md) | Public v1 thin-client interoperability contract |
| [docs/design/testing-strategy.md](docs/design/testing-strategy.md) | Property tests, deterministic simulation, conformance suite |
| [docs/roadmap.md](docs/roadmap.md) | Milestones M0–M7 with acceptance criteria |
| [docs/adr/](docs/adr/) | Architecture Decision Records for the choices below |

## Decisions to date

These were settled at project initialization and are recorded as ADRs:

1. **Full Datomic topology from day one** — transactor, peers, and storage
   service are distinct roles with network-capable boundaries from the first
   commit, even though early milestones run them in one process.
   ([ADR-0001](docs/adr/0001-datomic-topology.md))
2. **Clojurust at the boundary only** — the engine core uses its own compact
   Rust value and datom representation; cljrs values appear at the public API
   and inside database functions. ([ADR-0002](docs/adr/0002-clojurust-boundary.md))
3. **Custom immutable, content-addressed index segments** — Datomic's actual
   storage design: a log plus persistent covering-index trees whose nodes are
   immutable blobs, cacheable anywhere. No embedded KV dependency in the core.
   ([ADR-0003](docs/adr/0003-immutable-segments.md))
4. **Queries execute on the peer** — full EDN Datalog (rules, aggregates,
   predicates), Pull, and the entity API are first-class deliverables.
   ([ADR-0004](docs/adr/0004-peer-query.md))
5. **Full time model from the start** — as-of, since, history, tx-range/log,
   and tx-report queues. ([ADR-0005](docs/adr/0005-full-time-model.md))
6. **gRPC + custom tagged binary value encoding** — tonic/protobuf for the
   control plane; a purpose-built order-preserving/tagged encoding for values,
   shared with the segment format. ([ADR-0006](docs/adr/0006-grpc-protocol.md))
7. **In-memory and filesystem storage backends first** — behind a storage
   trait sized for S3-class object stores later.
   ([ADR-0007](docs/adr/0007-initial-backends.md))
8. **Sandboxed Clojurust transaction functions** — `:db/fn` code runs on the
   transactor in a restricted cljrs interpreter (no I/O, fuel budget); core
   built-ins like `:db/cas` are native Rust.
   ([ADR-0008](docs/adr/0008-sandboxed-db-functions.md))
9. **Core Datomic schema scope for v1** — idents, value types, cardinality,
   uniqueness, components, indexing, partitions, lookup refs, tempids;
   fulltext and tuple types deferred. ([ADR-0009](docs/adr/0009-schema-scope.md))
10. **Single transactor now, lease-based HA designed in** — the root store,
    log, and reconnect protocol are shaped for active/standby failover, which
    lands as its own milestone. ([ADR-0010](docs/adr/0010-ha-later.md))

## Current status

Milestones M0–M6 are complete: core types and sortable encoding, the
immutable segment store, the embedded transaction pipeline, the query
engine (Datalog, Pull, entity API, time views, planner statistics, query
cache) with a 194-vector conformance corpus, differential model tests, and
a recorded benchmark baseline
([docs/benchmarks/m3-baseline.md](docs/benchmarks/m3-baseline.md)),
distribution: the composite wire codec, the transactor as a process
(Transactor/Catalog gRPC services, lease acquisition with fenced root
publication, tx-report streams with gapless backfill), the peer library
(reconnect/resubscribe, sync, tx-report queue, direct segment reads), the
peer server for thin clients, TLS/bearer-token auth, and the `corium` CLI
(`transactor`, `peer-server`, `db *`, `gc`, `log`) — with an M4 acceptance
battery of real multi-process integration tests (peer convergence,
kill -9 recovery, deposed-transactor fencing) and a thin-client replay of
the conformance corpus — and Clojurust integration (`corium-cljrs`):
bidirectional value conversion (plus a `cljrs-reader` text bridge), the
`corium.api` namespace (connect/transact/q/pull/entity/datoms/as-of/
since/history/tx-range/tx-report-queue/sync) bound to `corium-peer`,
sandboxed `:db/fn` database functions on the transactor (allowlisted
environment, fuel/allocation/call-depth budgets, watchdog deadline —
risk checkpoint resolved via the interpreter's pluggable call hook with
the worker-thread watchdog as backstop, see
[docs/design/clojurust-integration.md](docs/design/clojurust-integration.md)),
and the query fn/pred resolution seam wired to the sandbox. The M5
acceptance battery re-runs the full conformance corpus driven from cljrs
through a live transactor with identical results, exercises cas-like/
invariant/recursive database functions with clean aborts on fuel and
deadline exhaustion, and verifies that sandbox escape attempts (I/O,
interop, namespace manipulation, unbounded loops) all fail safely. M6 adds
the operations surface: an interactive `corium console` with
as-of/since/history views, schema/stats/basis inspection, timing, and live
tx-report watch; full and hash-incremental offline backup plus guarded
restore-as-clone with format-version checks; human/JSON tracing,
Prometheus endpoints on transactor and peer server, and expanded Status/db
stats counters; retention-aware GC as both a scheduled transactor duty and
a manual online/offline operation; and getting-started, operations, console
demo, and public thin-client protocol documentation. The M6 acceptance
battery verifies basis/datom preservation across backup and clone restore,
measures both a no-change incremental copying zero segments and a delta
incremental copying only newly addressed segments,
exercises every console time view, and covers scheduled GC retention. Next
step is Milestone M7 (High availability) per
[docs/roadmap.md](docs/roadmap.md).
