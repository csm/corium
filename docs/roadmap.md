# Roadmap

Milestones are sequential but each leaves the system in a demonstrable state.
"Full topology from day one" (ADR-0001) is realized as: the *boundaries* exist
from M0 (service traits, pure crates, abstract transport/storage), and the
*network* arrives in M4 without touching engine logic.

Estimates are deliberately omitted; ordering and acceptance criteria are the
contract.

## M0 — Foundations

Workspace scaffolding (crate layout per architecture.md), CI, `corium-core`:
`Value`, sortable encoding, `Datom`, entity ids/partitions, schema model,
keyword interning; `corium-sim` skeleton with abstract clock/storage traits.

**Accept:** encoding property tests (order-preservation, round-trip) pass;
datom key composition tested for all four index orders; clippy/fmt gates on.

## M1 — Storage engine

`corium-store` (BlobStore/RootStore traits, memory + filesystem impls with
CAS-fenced roots, segment cache) and `corium-index` (immutable segment trees:
build, merge with structural sharing, iterators/seek; live in-memory index;
merged live+durable iterator).

**Accept:** tree property tests vs model; structural-sharing bound test;
crash-during-publish simulation shows either old or new root, both fully
dereferenceable; GC mark/sweep on a synthetic history strands nothing
reachable.

## M2 — Transactions (embedded)

`corium-tx` (expansion, tempids/upsert, lookup refs, schema validation,
cardinality handling, native built-ins `:db/cas`/`:db/retractEntity`),
`corium-log` (append/replay/tx-range), transactor pipeline + background
indexing job as a library (`corium-transactor`, in-process transport),
`corium-db` (Db value with basis; bootstrap schema datoms). Single process:
open a database on the filesystem, transact, read datoms back, crash-recover.

**Accept:** model-based tx tests pass; sim battery — crash at every pipeline
stage loses no acked tx and duplicates none; indexing job publishes correct
roots under concurrent writes; `db stats` counts match model.

## M3 — Query engine + time model

`corium-query` complete per query-engine.md: Datalog (patterns, predicates,
functions-native set, not/or, rules, aggregates, multiple dbs), Pull, entity
API, direct index access; `as-of`/`since`/`history` views and `tx-range` in
`corium-db`; query cache; statistics for the planner; criterion benchmark
suite; first cut of the conformance corpus (≥150 vectors).

**Accept:** conformance corpus green; model-based random-query differential
tests green; planner never full-scans with bound `a` (tested); benchmarks
recorded as baseline.

## M4 — Distribution

`corium-protocol` (codec + proto + tonic), transactor as a process
(Transactor/Catalog services, lease acquisition with fencing, tx-report
stream with backfill), `corium-peer` (remote connection, segment cache,
reconnect/resubscribe, sync, tx-report queue), peer server + thin-client
protocol (PeerServerService), TLS/auth, `corium` CLI: `transactor`,
`peer-server`, `db *`, `gc`, `log`.

**Accept:** multi-process integration tests — N peers converge on every tx;
kill -9 transactor mid-load, restart, zero acked-tx loss; peer reconnect
backfills gaplessly; deposed-transactor fencing test (paused process cannot
publish); thin-client conformance kit passes against peer server.

## M5 — Clojurust

`corium-cljrs`: value conversion, `corium.api` namespace (connect/transact/
q/pull/entity/as-of/history/tx-report-queue/sync), sandboxed database
functions (`:db/fn` storage, compile cache, allowlist env, fuel budget),
query fn/pred clause resolution seam wired to the sandbox.

**Accept:** the M3 conformance corpus re-runs driven from cljrs with identical
results; db-function tests (cas-like fn, invariant fn, recursion, fuel
exhaustion aborts cleanly); sandbox escape attempts (I/O, interop, unbounded
loop) all fail safely. **Risk checkpoint:** cljrs-interp fuel hooks
(clojurust-integration.md) — resolve by upstream contribution or watchdog
fallback before this milestone completes.

## M6 — Operations

`corium console` (interactive query console with time-travel commands),
backup/restore (full + incremental, restore-as-clone), metrics/tracing per
clients-and-ops.md, GC as a scheduled transactor duty, docs: getting-started,
operations guide, thin-client protocol spec.

**Accept:** backup → wipe → restore round-trip preserves basis and passes
conformance; incremental backup copies only new segments (measured); console
demo script exercises the full time model.

## M7 — High availability

Active/standby transactor: standby lease polling and takeover, peer
lease-holder rediscovery and failover reconnect, heartbeat tuning, runbook.
(Design already fixed in log-and-transactor.md; this milestone is
implementation + simulation coverage.)

**Accept:** sim: takeover under every crash/partition timing preserves all
acked txs and never double-publishes (fencing); integration: kill active under
load, standby serves writes within lease-expiry bound, peers fail over without
error surfacing to callers beyond retry latency.

## Post-v1 backlog (unordered)

Scaling and durability (see
[log-and-transactor.md](design/log-and-transactor.md) for the log design):

- **Durable log in shared storage.** *(Done.)* The transaction log lives in
  the storage service for every non-filesystem backend: `corium-log`'s
  `NativeVersionedLog` keeps a `(db, lease-version, t)` record per commit
  through the `RootStore`, so PostgreSQL, Turso, and S3 nodes need no shared
  data directory and a standby can take over from a dead node's database.
  The lease-version prefix carries the same merge-cutoff fencing as the
  filesystem layout. Object-store *chunk sealing* (compacting the tail into
  content-addressed `log-root` chunks) remains future work; until then the
  native backends keep one record per transaction.
- **Recovery from the index root.** *(Done for the current value.)* A
  transactor now opens a database from its published EAVT snapshot plus the
  log tail since `index-basis-t` (`TransactorNode::recover_transactor` →
  `EmbeddedTransactor::recover_from_snapshot`), so open and restart time are
  proportional to the tail, not the whole history. The `DbRoot` carries two
  recovery hints a current-facts snapshot cannot reconstruct — the entity
  allocator high-water (`next_entity_id`, so ids of entities retracted
  before the snapshot are never reused) and the last `:db/txInstant`
  (`last_tx_instant`, preserving transaction-time monotonicity across an
  empty tail); a root missing them (or a snapshot that fails to load) falls
  back to full-log replay, which is always correct. Still open: **complete
  pre-snapshot `history`/`as-of` views**, which need the history trees
  ("future history roots" in
  [indexes-and-storage.md](design/indexes-and-storage.md)) — published v1
  segments carry current facts only, so those views still require full-log
  replay.
- **Transactor fleet placement and routing.** Pursue the
  [fleet design](design/transactor-fleet.md): assign each database a small
  candidate set so nodes are active for some databases and standby for
  others; put one load-balanced address in client configuration; use a
  database routing header for advisory affinity; and have any ingress
  forward owner-dependent work once to the CAS-fenced lease holder.
  Structured owner hints replace message-text parsing. Durable transaction
  request IDs are required before an ingress can retry ambiguous in-flight
  failures transparently. Open-on-demand with idle eviction bounds memory
  and root-store lease-renewal traffic for cold databases. The shared durable
  log and recovery-from-index work above are already the prerequisites.
- **Copy-free fork.** `db fork` currently copies the log prefix and
  rebuilds indexes; share the parent's index roots behind an as-of ceiling
  in the DbRoot (format bump) to make fork cost independent of database
  size. Depends on published history roots (rewinding below the parent's
  index basis needs retracted facts), and needs explicit semantics for
  `:db/noHistory` attributes, whose pre-retraction values cannot be
  faithfully rewound.
- **S3-compatible storage backend.** *(Done.)* `S3BlobStore` implements both
  `BlobStore` and `RootStore` against an S3 (or S3-compatible) bucket; root
  CAS uses S3 conditional writes (`If-None-Match: *` for a first publish,
  `If-Match: <etag>` for a fenced update), so no separate KV is required on
  providers that support them. Selectable via the `s3` Cargo feature and the
  transactor's `StoreSpec::S3`.

Security and multi-tenancy:

- **Optional request-scoped authn/authz.** *(Landed.)* The network surfaces
  derive a `Principal` per request
  ([`corium-protocol::authz`](../crates/corium-protocol/src/authz.rs)),
  authenticate it in the interceptor (static tokens, OIDC/JWT behind the `oidc`
  feature, an mTLS-shaped `TokenVerifier` seam), and authorize the concrete
  `Access` in each handler. Policy is either permit-all (the default), a
  role→grant table, an external async oracle (OpenFGA / Auth0 FGA), or
  Corium's own relationship database — see [auth.md](design/auth.md) and
  [ADR-0012](adr/0012-optional-authn-authz.md).
- **Self-hosted ReBAC authorization.** *(Landed.)*
  [`corium-authz`](../crates/corium-authz/src/lib.rs) stores relationship
  policy — principals, tuples, permissions, rewrites, views — in an ordinary
  Corium database, compiles it into an immutable snapshot keyed by its basis
  `t`, and answers checks with a bounded, cycle-safe graph walk in memory.
  Transactor and peer server enable it with `--authz-db`; `corium authz
  init|grant|revoke|check|status` operates it. Remaining work: entity- and
  value-level view filtering in the query engine (executor predicate plus
  query-cache keying), which is what an `AllowFiltered` decision needs before a
  read path can serve it, and mTLS subject extraction.

Engine and API:

- Fulltext (`tantivy`) and tuple value types; excision (design reserved in
  [time-model.md](design/time-model.md)); query fn clauses in user cljrs
  code; leapfrog join; HTTP/JSON gateway; adaptive index statistics; disk
  tier for peer segment cache; `:db/ensure` entity specs.
