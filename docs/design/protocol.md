# Wire Protocol

Control plane: gRPC (tonic/prost). Data plane values: a Corium-defined tagged
binary encoding carried in protobuf `bytes` fields. Segments never travel over
gRPC — peers read the blob store directly.

## Value wire encoding

The sortable segment encoding (data-model.md) is also the wire encoding for
single values; composite payloads (tx-data, query args, results) use a
length-prefixed tagged variant of the same tag space extended with container
tags (list, vector, map, set) and an interning table per message for keywords
and repeated strings. One `corium-protocol::codec` module owns both variants;
round-trip and cross-variant property tests keep them honest.

Rationale (ADR-0006): protobuf handles framing, streaming, auth, and
versioning where it is strong; EDN's open value set lives in one codec we
control, rather than being contorted into protobuf messages.

## Services

### TransactorService (peers → transactor)

```proto
service Transactor {
  rpc Transact(TransactRequest) returns (TransactResponse);      // tx-data bytes → tempids, basis, tx-data
  rpc Subscribe(SubscribeRequest) returns (stream TxReport);     // declares client basis; server backfills then streams
  rpc Sync(SyncRequest) returns (SyncResponse);                  // wait for basis ≥ t
  rpc Status(StatusRequest) returns (StatusResponse);            // basis, index-basis, lease info, stats
}
```

- `Subscribe` is the peer's lifeline: tx-reports, index-basis announcements,
  and heartbeats are multiplexed on this stream. The handshake advertises
  the server's heartbeat interval; a stream silent for three intervals is
  presumed dead and dropped even when the transport has not noticed.
  Disconnect ⇒ peer reconnects, rotating through its endpoint preference
  list (an HA standby rejects the subscription with a `standby`
  FAILED_PRECONDITION until it holds the lease; peers with storage
  credentials can also rediscover the holder's advertised endpoint from the
  root record) and resubscribes from its basis; the transactor backfills
  from the log if the gap is large.
- On a cold storage-aware connection, the initial subscription basis is the
  `index-basis-t` of the immutable snapshot the peer just loaded. The root
  selects a complete snapshot, and the subscription supplies the gap through
  the handshake basis, so concurrent index publication does not require a
  cross-service transaction.
- All requests carry the database name and a protocol version; the transactor
  rejects mismatched `format-version` roots with a clear upgrade error.
- Protocol v2 adds `TransactRequest.expected_basis_t`. When present, the
  transactor rejects a stale request before transaction preparation or durable
  append; peer-local read/modify/write adapters use this fence.
- Version checks use a supported range to permit server-first rolling
  upgrades. A v2 server accepts v1 clients (which cannot request the new
  fence); a v2 client still sends version 2 and is rejected by a v1 server
  before that older server can ignore the fence. Upgrade transactors first,
  then peers and clients.
- The proposed schema-migration protocol adds `schema_basis_t` and
  `schema_generation` to the handshake and `schema_generation_after` to tx
  reports. The handshake schema is the snapshot effective at the subscriber's
  `from_basis_t`, not the server's newest schema. Reports with
  `t > from_basis_t` advance data and schema together. A subscriber from basis
  0 receives the pre-basis schema seed in the handshake, never as a `t = 0`
  report. These semantics require a protocol-version bump so an older peer
  cannot silently install a current schema before replaying older data.

#### Future fleet routing

The current peer connects to an ordered transactor endpoint list. The proposed
[transactor fleet design](transactor-fleet.md) replaces that deployment
contract with one fleet endpoint while preserving the database field as the
authoritative request target.

The SDK will duplicate a canonical database routing key in gRPC metadata so an
L7 load balancer can apply advisory consistent-hash affinity. Any transactor
ingress may receive the request; owner-dependent work is executed locally or
forwarded once to the lease holder. Structured `NotOwner` details replace
parsing `standby`/`deposed` message text. Affinity never grants ownership.

Transparent retry after a request reaches the owner additionally requires a
durable transaction request ID and result deduplication. Until that protocol
exists, an in-flight connection loss remains ambiguous exactly as it is
today.

### CatalogService (admin)

`CreateDatabase`, `DeleteDatabase`, `ForkDatabase`, `ListDatabases`,
`GcDeletedDatabases`, index controls, and `GetBackupInfo`. Most are thin
wrappers over root-store operations plus transactor bootstrap datoms.
`ForkDatabase` creates a new database duplicating an existing one at a
transaction basis by copying the log prefix; the fork replays it and publishes
indexes of its own. `GetBackupInfo` briefly serializes with commit to return a
definite current basis and the underlying storage connection; the backup
client then leaves the transactor and reads the bounded native log range
directly.

Schema migration adds two administrative calls:

```proto
rpc PlanSchemaUpdate(PlanSchemaUpdateRequest) returns (SchemaUpdatePlan);
rpc ApplySchemaUpdate(ApplySchemaUpdateRequest) returns (SchemaUpdateResult);
```

`PlanSchemaUpdate` is read-only. It returns the normalized logical steps and the
installed-schema fingerprint. It also returns the observed basis, stable
acknowledgement codes, and advisory impact observations. `ApplySchemaUpdate`
carries the desired schema, `--prune` mode, plan digest, fingerprint, observed
basis, and supplied acknowledgements. The transactor recomputes the logical
digest and validates safety preconditions in the writer queue. Advisory counts
are not part of the digest. The response either records a completed short
schema transaction or returns the remaining long-job specifications. The CLI
submits those to the operator service when configured. Otherwise, it executes
the same job contract in-process. `Catalog` never depends on the operator
service. The exact messages and stable error codes land with the implementation
and protocol-version bump described in
[schema-migrations.md](schema-migrations.md).

Published-root and status metadata gain the schema generation plus per-attribute
AVET readiness basis. A migration-triggered backfill invokes the existing index
publisher immediately, bypassing interval/tail pacing, and readiness is never
reported ahead of the root that proves coverage.

### OperatorService (operator tools → operator peer service) *(proposed)*

Job submission, planning, approval, cancellation, and watching; schedules; and
fleet, database, storage, and key inspection — with a JSON/HTTP gateway over the
same surface for the UI and for scripting. It is a separate service from
`Catalog` on purpose: the transactor's latency belongs to the commit pipeline,
so it is called by the operator service rather than extended into it. See
[operator-service.md](operator-service.md).

### PeerServerService (thin clients → peer server)

For languages without the peer library; queries run server-side on a hosted
peer:

```proto
service PeerServer {
  rpc Query(QueryRequest) returns (stream QueryResultChunk);
  rpc Pull(PullRequest) returns (PullResponse);
  rpc Transact(TransactRequest) returns (TransactResponse);      // proxied
  rpc Datoms(DatomsRequest) returns (stream DatomChunk);
  rpc TxRange(TxRangeRequest) returns (stream TxChunk);
  rpc DbStats(DbStatsRequest) returns (DbStatsResponse);
  rpc Subscribe(SubscribeRequest) returns (stream TxReport);     // relayed
}
```

Requests name a db view as
`{db-name, as-of?, since?, history?, as-of-instant?, since-instant?}` so thin
clients get the full time model, by basis or by wall clock. Result streams are chunked with a
server-enforced fuel/deadline per query. This service definition plus the
codec spec **is** the public thin-client protocol; a conformance doc and test
vectors ship with it so third parties can write clients.

## Security

- TLS via tonic/rustls everywhere; mTLS or bearer-token auth per endpoint
  (pluggable `Authenticator` trait; static tokens in v1).
- Request-scoped identity and authorization live in
  `corium-protocol::authz` (optional per-surface enforcement, external
  identity providers, and per-principal view decisions). Guards are wired into
  the transactor and peer gRPC services; a filtered decision is rejected on
  surfaces that cannot enforce it yet. See [auth.md](auth.md) and
  [ADR-0012](../adr/0012-optional-authn-authz.md).
- Peer servers enforce per-request fuel, result-size, and concurrency limits;
  the transactor enforces tx-size and queue limits.
- The blob store is assumed private to the deployment (peers have direct
  credentials to it, as in Datomic). Encryption at rest
  ([encryption.md](encryption.md)) is proposed to remove that assumption for
  blobs, log records, backups, and cached segments.
- Attribute protection classes (proposed) seal values under per-class keys
  before they leave the writing peer, so tx-data, tx-reports, datom streams,
  and query results may carry sealed values that only a key-holding reader can
  hydrate. This adds a value tag to the codec and a thin-client protocol
  version; a peer server either hydrates per request or forwards sealed values
  (`--seal-through`) for end-to-end protection.

## Embedded transport

The same service traits have an in-process implementation over channels
(`corium-peer` talks to `corium-transactor` directly). Tests and the
simulator run the identical pipeline code both ways; only the transport
differs. This is the mechanism that lets us build "full topology" logic from
day one while running single-process until M4.
