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
`GcDeletedDatabases` — thin wrappers over root-store operations plus
transactor bootstrap datoms. `ForkDatabase` creates a new database
duplicating an existing one at a transaction basis by copying the log
prefix; the fork replays it and publishes indexes of its own.

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

Requests name a db view as `{db-name, as-of?, since?, history?}` so thin
clients get the full time model. Result streams are chunked with a
server-enforced fuel/deadline per query. This service definition plus the
codec spec **is** the public thin-client protocol; a conformance doc and test
vectors ship with it so third parties can write clients.

## Security

- TLS via tonic/rustls everywhere; mTLS or bearer-token auth per endpoint
  (pluggable `Authenticator` trait; static tokens in v1).
- Request-scoped identity and authorization are a spike in `corium-protocol::authz`
  (optional per-surface enforcement, external identity providers, per-principal
  views for multi-tenant serving); see [auth.md](auth.md) and
  [ADR-0012](../adr/0012-optional-authn-authz.md). Not yet wired into the servers.
- Peer servers enforce per-request fuel, result-size, and concurrency limits;
  the transactor enforces tx-size and queue limits.
- The blob store is assumed private to the deployment (peers have direct
  credentials to it, as in Datomic).

## Embedded transport

The same service traits have an in-process implementation over channels
(`corium-peer` talks to `corium-transactor` directly). Tests and the
simulator run the identical pipeline code both ways; only the transport
differs. This is the mechanism that lets us build "full topology" logic from
day one while running single-process until M4.
