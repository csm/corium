# Architecture Decision Records

Numbered, immutable once accepted; superseding decisions get new ADRs that
reference the old. Format: Context / Decision / Consequences.

| # | Decision | Status |
|---|---|---|
| [0001](0001-datomic-topology.md) | Full Datomic topology from day one | Accepted |
| [0002](0002-clojurust-boundary.md) | Clojurust at the boundary only | Accepted |
| [0003](0003-immutable-segments.md) | Custom immutable content-addressed segments | Accepted |
| [0004](0004-peer-query.md) | Full Datalog + Pull executing on the peer | Accepted |
| [0005](0005-full-time-model.md) | Full time model in the first milestone set | Accepted |
| [0006](0006-grpc-protocol.md) | gRPC control plane + custom tagged value encoding | Accepted |
| [0007](0007-initial-backends.md) | In-memory + filesystem backends first | Accepted |
| [0008](0008-sandboxed-db-functions.md) | Sandboxed Clojurust database functions | Accepted |
| [0009](0009-schema-scope.md) | Core Datomic schema scope for v1 | Accepted |
| [0010](0010-ha-later.md) | Single transactor now, lease-based HA designed in | Accepted |
| [0011](0011-sql-interface.md) | Peer-local read-only SQL over namespace projections | Accepted |
| [0012](0012-optional-authn-authz.md) | Optional, request-scoped authentication and authorization | Proposed |
