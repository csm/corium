# Architecture Overview

Corium follows Datomic's decomposition of a database into three cooperating
roles plus a family of clients. The roles are separated by narrow, versioned
interfaces from the first commit; early milestones run them all in one process,
but nothing in the core may assume co-location.

```
                    ┌────────────────────┐
   transact ───────►│     Transactor     │──── append ────► ┌─────────────┐
                    │  (single writer)   │──── segments ──► │   Storage   │
                    │  tx pipeline       │                  │   service   │
                    │  indexing job      │                  │ (blob store │
                    └─────────┬──────────┘                  │  + roots)   │
                              │ tx-report stream            └──────┬──────┘
              ┌───────────────┼───────────────┐                    │
              ▼               ▼               ▼            read segments
        ┌──────────┐    ┌──────────┐    ┌───────────┐              │
        │   Peer   │    │   Peer   │    │Peer server│ ◄────────────┘
        │ (in-proc │    │          │    │ (hosts db │
        │  query)  │    │          │    │ for thin  │
        └──────────┘    └──────────┘    │  clients) │
                                        └─────┬─────┘
                                              │ gRPC query/transact
                                        ┌─────┴─────┐
                                        │Thin client│ (any language)
                                        └───────────┘
```

## Roles

### Storage service

A passive, dumb store with two parts:

- **Blob store** — immutable, content-addressed segments (index tree nodes,
  log chunks). Write-once, never mutated, safe to cache anywhere without
  invalidation. Interface: `put(hash, bytes)`, `get(hash)`, `delete(hash)`
  (GC only).
- **Root store** — a tiny mutable map of named pointers (database roots,
  transactor lease) updated by compare-and-swap. This is the only mutable,
  strongly consistent state in the system.

Initial implementations: in-memory (tests) and local filesystem (single-node
dev/prod). The trait is sized so S3-class object stores and SQL backends slot
in later (see [design/indexes-and-storage.md](design/indexes-and-storage.md)).

### Transactor

The single writer for a database. It serializes transactions, expands and
validates them (tempids, lookup refs, schema, uniqueness, database functions),
assigns the tx entity and timestamp, appends to the durable log, acks the
caller, and streams tx-reports to connected peers. A background indexing job
periodically folds the log tail into fresh covering-index trees and publishes a
new index root. Exactly one transactor holds the write lease for a database at
a time; the lease lives in the root store (see
[design/log-and-transactor.md](design/log-and-transactor.md)). The future
fleet topology preserves that per-database serialization point while placing
different databases on different nodes behind one service address; see
[design/transactor-fleet.md](design/transactor-fleet.md).

### Peer

A library embedded in the application process. It maintains a live connection
to the transactor for tx-reports, reads segments directly from storage through
a local cache, and merges (persistent index trees + in-memory log tail) into an
immutable database value on which **all query execution happens locally**:
Datalog, Pull, entity API, index scans, time-travel views. Getting a database
value never blocks on the transactor.

### Peer server + thin clients

A peer hosted as a standalone process exposing query/transact/pull over gRPC
for languages without the peer library. The gRPC surface is documented as a
public protocol (see [design/protocol.md](design/protocol.md)).

## Data flow invariants

- Facts are **datoms** `[e a v tx added]`; nothing is updated in place, ever.
- The log is the source of truth; indexes are a deterministic fold of the log.
- Any state a peer holds is either immutable (segments, log tail up to a
  basis-t) or disposable (caches). Peer crash/restart loses nothing.
- The current database root is the only coordination point for readers, and
  readers never take locks.

## Crate layout

A single Cargo workspace. Dependency edges point strictly downward.

| Crate | Contents |
|---|---|
| `corium-core` | `Value`, sortable encoding, `Datom`, entity/tx ids, partitions, schema model, errors |
| `corium-index` | Persistent segment trees, EAVT/AEVT/AVET/VAET, in-memory live index, merge iterators |
| `corium-store` | `BlobStore` + `RootStore` traits; memory and filesystem impls; segment cache |
| `corium-log` | Log chunk format, append/replay, tx-range access |
| `corium-tx` | Transaction data expansion, tempid resolution, schema validation, built-in tx functions |
| `corium-query` | Datalog parser/compiler/planner/executor, rules, aggregates, Pull, entity API |
| `corium-db` | The immutable `Db` value: basis, index merge, as-of/since/history views |
| `corium-sql` | Read-only DataFusion SQL over peer-local `Db` values; namespace projections and system relations |
| `corium-transactor` | Transactor process: pipeline, indexing job, lease, gRPC server |
| `corium-peer` | Peer library: connection, tx-report handling, segment cache, `Connection`/`Db` public API |
| `corium-protocol` | protobuf definitions, wire value encoding, generated tonic stubs, request identity/authorization model |
| `corium-authz` | Self-hosted relationship-based (ReBAC) authorization: reserved policy schema, compiled policy snapshots, bounded relationship search, `SystemDbAuthorizer` |
| `corium-cljrs` | Clojurust bindings: value conversion, `(d/q …)` API, db-function sandbox host |
| `corium-cli` | `corium` binary: admin commands, query console, standalone transactor/peer-server launchers |
| `corium-sim` | Deterministic simulation harness for tests (not published) |

`corium-core`, `corium-index`, `corium-store`, `corium-log`, `corium-tx`,
`corium-query`, and `corium-db` are pure library code with no tokio/network
dependencies; async and gRPC enter only in `corium-transactor`, `corium-peer`,
`corium-protocol`, and `corium-cli`. This keeps the engine testable in the
deterministic simulator.

## Technology choices

- **Rust** stable toolchain, edition 2024.
- **tokio + tonic/prost** for the transactor, peer connection, and peer server.
- **DataFusion + Arrow** for peer-local, read-only SQL execution.
- **cljrs** (`cljrs-value`, `cljrs-reader`, `cljrs-interp`, `cljrs-interop`)
  for EDN at the boundary and database function execution.
- **proptest** for property tests; the `corium-sim` harness for whole-system
  fault-injection tests.
- Hashing for content addressing: BLAKE3 (fast, incremental, 32-byte digests).
