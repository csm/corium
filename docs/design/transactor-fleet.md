# Transactor fleet routing and placement

**Status:** Proposed future direction. This is post-v1 work, not the behavior
of the current CLI or wire protocol.

Corium already has the safety mechanism needed to run more than one
transactor: each database has exactly one CAS-fenced write lease, transaction
logs live in shared storage, a new owner recovers from the published index
plus log tail, and the old owner checks ownership after append and before
acknowledgement. The current HA topology applies those mechanisms as an
active/standby pair serving the whole catalog.

The next topology should make HA a **per-database property of a transactor
fleet**. A node can be active for some databases, standby for others, and
unassigned for the rest. Clients configure one fleet endpoint. Requests may
arrive at any fleet member; operations requiring an owner are routed to the
database's current lease holder.

This design extends, rather than supersedes, the fencing and recovery protocol
in [log-and-transactor.md](log-and-transactor.md). It changes placement and
routing, not transaction ordering or the meaning of ownership.

## Goals

- Spread many databases across many transactor machines while preserving one
  logical writer per database.
- Give peers and thin clients one stable service address rather than an
  active/standby endpoint list.
- Let a fleet member serve eligible reads locally while routing
  owner-dependent operations correctly.
- Retain transparent per-database failover, gapless subscription recovery, and
  zero loss of acknowledged transactions.
- Keep the storage root and write lease as the authority. Load-balancer state,
  placement preferences, and routing caches must be disposable hints.
- Avoid recovering, polling, and renewing every database on every fleet
  member.

## Non-goals

- Multiple concurrent writers for one database. A hot database retains one
  serialization point; this design scales across databases.
- Moving Datalog query execution back into the transactor. Embedded peers
  still query locally, and hosted peer servers remain the main read-scaling
  tier.
- Replacing the root-store CAS or introducing consensus between transactor
  processes. The existing lease is sufficient to decide the writer.
- Requiring a load balancer to track per-database lease ownership.

## Topology

```text
                               shared blob/root/log storage
                                         ▲
                                         │
client ── one fleet address ──► L7 load balancer
                                  │ advisory database affinity
                                  ▼
                         any transactor ingress
                            │             │
                    local owner/read     │ one internal hop
                            │             ▼
                            └──────► current lease holder
```

Every externally reachable fleet member can act as ingress. It may also host
database state and own leases; a separate router tier is not required. An
operator may nevertheless deploy dedicated ingress nodes later without
changing the protocol.

The ingress path is deliberately redundant with load-balancer affinity:

1. Affinity normally sends a database to a stable ingress and distributes
   different databases across the fleet.
2. The ingress verifies actual ownership and either executes locally or
   routes to the current owner.
3. The owner verifies its lease before accepting owner-dependent work.

Only steps 2 and 3 affect correctness.

## Database identity and the route hint

All database-specific RPCs already carry a database name in their protobuf
body. Generic gRPC load balancers cannot conveniently inspect that body, so
the Corium SDK should also attach canonical routing metadata:

```text
corium-database-route: <stable database routing key>
```

Initially the routing key may be the canonical database name. A stable opaque
database ID is preferable if database rename or tenant-scoped naming is
introduced. The SDK adds the metadata automatically; it is not another
application configuration field.

An L7 load balancer may consistently hash the metadata for transaction and
subscription traffic. This is **cooperative affinity**, not owner selection:

- fleet membership and database placement can temporarily disagree;
- failover changes the lease holder without changing the routing key;
- ordinary load balancers do not have useful per-database health;
- a deployment may not support per-RPC hashing at all.

The server treats the protobuf database field as authoritative, rejects a
metadata/body mismatch, and never derives authorization from the route hint.
An edge proxy should overwrite rather than trust a caller-supplied hint where
tenants must not influence fleet load distribution.

Read traffic need not use the same affinity policy. For example,
`database + client` hashing or round-robin routing can spread reads among
sufficiently current replicas, while transaction and subscription traffic
benefits from database stickiness.

## Placement and lease ownership

Placement answers **which nodes should try to host a database**. The lease
answers **which one may write it now**. These are separate decisions.

Each database receives a small ordered candidate set, normally two or three
nodes in different failure domains:

```text
database-a → [node-1, node-2]
database-b → [node-2, node-3]
database-c → [node-3, node-1]
```

The first eligible candidate normally acquires the lease. Other candidates
observe the lease and take over after it lapses. A returning preferred node
does not preempt a healthy current owner; rebalancing is an explicit,
graceful handoff to avoid churn.

The first implementation should use an operator-supplied placement map or
database filters. Once fleet membership has a durable definition, weighted
rendezvous hashing is a suitable automatic policy because it:

- assigns a deterministic, bounded candidate set without per-database
  scheduler state;
- moves only a fraction of databases when membership changes;
- can incorporate capacity weights and failure-domain constraints.

Non-candidates do not recover the database, poll its lease, or renew it.
Candidates should open databases on demand and evict idle state after a
configured interval. Eviction releases an owned lease gracefully before
dropping the in-memory state. This bounds recovery memory and prevents one
root CAS renewal per cold database from dominating storage traffic.

Placement is never a fence. If two nodes temporarily compute different
candidate sets, only the node holding the root-store lease can acknowledge a
transaction or publish a root.

## Authoritative routing

Each ingress keeps a short-lived cache:

```text
database → {owner ID, lease version, internal route endpoint}
```

The cache is populated from the database root and, where needed, a fleet
registry mapping stable owner IDs to internal endpoints. The current
`advertised-endpoint` can serve both purposes in a flat trusted network, but a
production fleet should distinguish public client addresses from mutually
authenticated internal routing addresses.

For owner-dependent unary requests:

1. If the ingress holds the database lease, execute locally.
2. Otherwise resolve the current owner and forward directly to it.
3. Include the database, observed lease version, original deadline, and
   authenticated caller context.
4. The receiver accepts the request only if it currently holds that lease
   version.
5. On a structured `NotOwner` response, the ingress invalidates its cache,
   resolves the root again, and may make one more routing attempt.

Forwarded requests carry a hop count and are never forwarded again. This
prevents loops when routing caches disagree. A node that cannot reach storage
or the owner returns `UNAVAILABLE`; it must not guess.

An idle database may have no unexpired owner. In that case the ingress routes
an activation request to the highest-ranked reachable candidate (or activates
locally when it is a candidate). That node attempts the ordinary lease
acquisition and recovery path, then handles the request only after it owns the
lease. Competing activation attempts are harmless because the root CAS chooses
one owner. Non-candidate ingress nodes never acquire a lease merely because
they received first contact.

Trusted, storage-aware peers may optionally consume the same structured owner
hint and connect directly to the owner, avoiding the proxy hop. Forwarding is
the default for public and thin clients because it preserves the single fleet
address and does not expose internal topology.

The current string-based `standby`/`deposed` errors should become structured
protocol details containing at least:

```text
database, owner ID, lease version, retryability, optional route endpoint
```

These details are hints. The hinted receiver still verifies its lease.

## Transaction idempotency

Server forwarding adds another response boundary: a transaction can commit at
the owner and then lose its reply between the owner, ingress, and client.
Transparent retry after such a failure is unsafe with the current protocol.

Before automatic retry of an in-flight request, `TransactRequest` should gain
a client-generated request ID. The committed log record must durably associate
that ID with enough result data to reproduce the original
`TransactResponse`, including tempid resolution. The owner then behaves as
follows:

- an unseen request ID is validated and committed normally;
- a duplicate committed request ID returns the original response;
- concurrent submissions of the same ID join or serialize behind one result;
- a request ID reused with different transaction bytes is rejected.

The deduplication index is part of recoverable database state, not an
ingress-local cache. A bounded in-memory recent-ID cache may accelerate it,
but recovery and failover must retain the duplicate decision.

Until durable request IDs exist, an ingress may retry only failures proven to
occur before the owner accepted the request. Connection loss after forwarding
remains ambiguous and must surface to the caller, matching the current peer
contract.

## Operation classes and read consistency

Read-only does not automatically mean "any server": the server also needs a
database value sufficiently current for the operation's contract.

| Operation | Required destination |
|---|---|
| `Transact` | Current lease holder |
| `Subscribe` live tail | Current lease holder initially |
| `Sync` with current-owner meaning | Current lease holder |
| `Sync(min_basis)` | Any replica known to have reached `min_basis` |
| Lease/placement status | Any node able to read the root |
| Live basis, queue, and process status | Current owner |
| `ListDatabases` | Any node able to read the catalog |
| Query, Pull, Datoms | Any peer replica satisfying the requested basis |
| `TxRange` | Any node able to read the durable log range |
| `RequestIndex` / index-policy mutation | Current lease holder |
| Create, delete, fork | Fleet-aware catalog path; database work routes to its owner |

Corium should make consistency explicit rather than silently serving a stale
local value:

- **minimum basis**: the request supplies `min_basis_t`; any replica may wait
  until it reaches that basis and serve the read;
- **current owner**: route to the lease holder for operations whose meaning
  is "current at the serialization point";
- **snapshot/as-of**: any replica or storage-aware peer able to materialize
  that immutable view may serve it.

Hosted peer servers can maintain on-demand read replicas and evict them when
idle. This is the primary mechanism for spreading query work. Transactor
fleet members need not all tail every database merely to make a few
administrative reads local.

## Subscriptions

A subscription is read-only but depends on the live transaction broadcast, so
the first fleet version routes it to the current owner. An ingress may proxy
the stream; a trusted peer may follow a structured redirect.

When either hop fails, the client reconnects through the fleet address and
supplies its last fully applied basis. The new owner backfills from the
durable log before streaming live reports, preserving the current gapless
contract. Affinity keeps healthy long-lived streams stable but is not needed
for recovery.

Follower-served subscriptions are a possible later optimization. They require
followers to tail durable log records promptly and define an acceptable lag;
they are not necessary for distributing databases across transactors.

## Catalog operations

Catalog reads can run anywhere. Mutations require fleet-aware behavior:

- create installs metadata and placement, then allows an eligible candidate
  to acquire and initialize the database;
- delete prevents new ownership before releasing or deposing the current
  owner;
- fork serializes target creation globally, then routes source/target work
  according to placement;
- database-specific maintenance requests route exactly like transactions.

A catalog request arriving at an arbitrary ingress must not make that ingress
the database's accidental long-term owner. Placement is selected before the
new database is opened.

## Security

Internal forwarding crosses a trust boundary even when every node belongs to
one deployment:

- internal routes use mTLS and stable node identities;
- ingress propagates the authenticated principal and authorization context in
  a signed or mutually authenticated envelope rather than blindly replaying
  an external bearer token;
- the owner performs the final authorization check;
- route hints and owner endpoints never bypass database-name validation;
- deadlines, size limits, queue limits, and cancellation propagate to the
  owner;
- ingress and owner apply coordinated rate limits so forwarding cannot double
  the allowed work.

## Failure behavior

The existing lease and post-append fence remain the safety proof:

- **stale ingress cache:** the receiver returns `NotOwner`; ingress refreshes
  and tries once more;
- **owner crash:** another candidate acquires the expired lease, recovers the
  log tail, and becomes routable;
- **deposed owner:** its post-append fence prevents acknowledgement and it
  returns to standby;
- **ingress crash before forwarding:** no transaction reached the owner and
  the client can retry;
- **ingress or connection crash after forwarding:** retry is transparent only
  with durable request IDs; otherwise the outcome is ambiguous;
- **storage partition:** a node unable to verify ownership cannot accept
  owner-dependent work;
- **placement disagreement:** may add a routing hop or failed acquisition but
  cannot create two acknowledged writers.

## Observability

Fleet operation needs database- and hop-aware telemetry:

- owned, standby, assigned-cold, and cached-read database counts per node;
- routing-cache hit/miss and stale-owner counts;
- local versus forwarded request counts and forwarding latency;
- `NotOwner`, routing-loop prevention, and owner-resolution failures;
- database placement, current owner, lease version, and candidate ranks;
- recovery/open and idle-eviction duration;
- transaction deduplication hit/conflict counts;
- end-to-end latency separated from owner execution latency.

Logs for a forwarded request carry one correlation/request ID across client
ingress and owner.

## Delivery sequence

1. **Placement boundary.** Add configured database eligibility/candidate
   policy so nodes no longer race to host the whole catalog. Exercise mixed
   active/standby ownership across at least three nodes.
2. **Structured ownership errors.** Replace message parsing with typed
   `NotOwner` details and separate public from internal advertised endpoints.
3. **Single fleet ingress.** Allow any node to resolve an owner and forward
   unary owner-dependent requests once. Preserve deadlines, identity, and
   backpressure.
4. **Subscription routing.** Proxy or redirect streams and prove gapless
   reconnect through the fleet address.
5. **Durable request IDs.** Make ambiguous transaction replay idempotent, then
   enable automatic retry across the owner hop.
6. **Affinity optimization.** Add SDK routing metadata and documented
   load-balancer hashing. Correctness tests must also run with random routing
   and affinity disabled.
7. **On-demand hosting and read replicas.** Add idle eviction, explicit
   minimum-basis reads, and weighted automatic placement when fleet
   membership is mature.

## Acceptance properties

The fleet design is complete only when tests demonstrate:

- many databases distribute across nodes rather than accumulating on the
  first process;
- every database has at most one acknowledging writer under crash,
  partition, stale-routing, and placement-change schedules;
- killing any owner under mixed-database load loses no acknowledged
  transaction and does not interrupt unrelated databases;
- a client configured with only the fleet endpoint reconnects and continues
  without learning the active/standby set;
- random ingress routing and deliberately stale owner caches cannot violate
  fencing;
- subscription recovery remains gapless across ingress and owner failure;
- an in-flight transaction retried with the same durable request ID commits
  exactly once and returns the same result;
- non-candidates do not recover or renew unassigned databases;
- cold-database eviction bounds memory and lease-renewal traffic.

## Open decisions

- Source of truth for fleet membership and capacity weights: deployment
  configuration, a storage-backed registry, or an external scheduler.
- Whether the first placement implementation is an explicit map or
  rendezvous hashing over a fixed configured member list.
- Public redirect support versus forwarding-only for each client surface.
- Durable request-ID representation and its lookup index in log/index
  storage.
- Which read surfaces need minimum-basis, current-owner, or explicitly stale
  consistency contracts.
