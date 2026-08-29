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
CAS-fenced roots, segment cache) and `corium-index` (immutable segments:
build, incremental apply with structural sharing, iterators/seek). The live
in-memory index is the `Db` value itself (`corium-db`), which already folds
each commit into its four covering indexes; segments hold the published
snapshot the indexing job merges that tail into.

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
- **Recovery from the index root.** *(Done, including historical views.)* A
  transactor now opens a database from its published current and history EAVT
  snapshots plus the log tail since `index-basis-t`
  (`TransactorNode::recover_transactor` →
  `EmbeddedTransactor::recover_from_snapshot`). This avoids replaying the log
  prefix; until lazy segment descent lands, opening still materializes the
  retained-history EAVT root. The `DbRoot` carries two
  recovery hints a current-facts snapshot cannot reconstruct — the entity
  allocator high-water (`next_entity_id`, so ids of entities retracted
  before the snapshot are never reused) and the last `:db/txInstant`
  (`last_tx_instant`, preserving transaction-time monotonicity across an
  empty tail); a root missing them (or a snapshot that fails to load) falls
  back to full-log replay, which is always correct. Storage format 5 publishes
  four retained-history covering roots alongside the four current roots.
  Snapshot-bootstrapped peers and recovered transactors use the history EAVT
  root for exact pre-snapshot `history`/`as-of` views of retained attributes;
  roots from older formats fall back to full-log replay and upgrade on
  publication. Values discarded under `:db/noHistory` remain the explicit
  copy-free-fork semantics question below.
- **Lazy segment descent on the read side (peer resident set).** Today a
  peer is an in-memory database that storage reconstructs rather than
  bounds. Its `Db` value keeps every datom it has seen — the full history,
  retractions included — and folds four covering indexes over that log per
  time view. Datoms are allocated once and shared by handle across the log
  and every index of every view, so the indexes cost encoded keys and
  pointers rather than duplicate facts, and `since` narrows the live set
  before projecting the four orders instead of rebuilding finished indexes;
  views that select exactly the datoms of an already-folded one share its
  fold. What none of that fixes: nothing is evicted, so the resident set
  tracks total history rather than the live database, and the first read of
  a genuinely distinct `as-of`/`since`/`history` view costs a fold of the
  whole history rather than of the view. The fix is the segment-tree read
  path — inner tree levels in the published format so a reader can seek
  without materializing an index, then descent through `corium-store`'s
  bounded segment cache, so a peer's memory tracks its working set and view
  latency tracks the answer. The published history-root prerequisite is now
  present. See
  [indexes-and-storage.md](design/indexes-and-storage.md) and
  [time-model.md](design/time-model.md).
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
  size. The published history-root prerequisite is now present (rewinding
  below the parent's index basis needs retracted facts); this still needs
  explicit semantics for
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
- **Encryption at rest.** *(In progress.)* Envelope-encrypt every durable
  artifact — index blobs, log record payloads, backup archives, cached
  segments — under a per-database data key wrapped by a KMS or operator file,
  with a new `corium-crypt` crate and a `Keyring` seam. Encryption is a
  `BlobStore` decorator above the segment cache, and a blob id becomes the
  digest of the stored encrypted object, so idempotent `put`, structural
  sharing, keyless integrity verification, GC, and backup are all preserved.
  Storage format 4, backup format 2. Done: the primitives, the blob-store
  decorator, log-record payload encryption, the `keys:<db>` manifest with
  storage format 4, backup format 2 (`corium backup` and `corium restore` take
  `--storage-key`, and the archive holds ciphertext end to end), `--storage-key`
  on the transactor, peer server, `corium log`, and offline `corium gc`,
  `corium db create --storage-key`, and `corium keys status|rotate|rewrap`.
  Remaining: KMS-backed keyrings; today a key identity resolves through `file:`
  or `env:`. See
  [encryption.md](design/encryption.md) and
  [ADR-0017](adr/0017-encryption-at-rest.md).
- **Attribute protection classes.** *(Specified.)* Per-attribute confidentiality
  under separate keys: a class names a key id, the writing peer seals values
  before tx-data leaves it, and only a reader whose keyring resolves that class
  hydrates them — the transactor never holds a class key and cannot forge a
  protected fact. Sealing is deterministic so retraction pairing, supersession,
  deduplication, and `:db/cas` keep working bytewise; protected datoms are
  excluded from AVET and VAET, so filtering one means scanning it. Protecting,
  unprotecting, and re-classifying a populated attribute are legal and
  forward-only — old datoms keep the form they were asserted in — with legacy
  plaintext redacted on read by default, a sweep that seals the current values,
  and `corium keys audit` reporting what plaintext remains. Needs
  `Value::Sealed` and its encoding, schema validation, peer-side sealing,
  hydration in `ExecOptions` with key-set-aware query caching, `ReserveEntityIds`
  plus a basis fence for entity-scoped classes, thin-client protocol v3, SQL
  redaction, and the `corium keys` surface. See
  [ADR-0018](adr/0018-attribute-protection-classes.md).

- **Operator peer service.** *(Specified.)* A peer whose workload is operations:
  backup, restore, fork, GC, index publication, and the encryption migrations
  become resumable, idempotent, singleton-per-target **jobs** with progress,
  cancellation, plan/apply, and two-person approval for the irreversible ones.
  Its registry — jobs, schedules, approvals, fleet observations, audit — is an
  ordinary Corium database, so operational history is backed up and
  time-travelable. An `Operator` gRPC service plus a JSON/HTTP gateway (the
  first customer for the gateway item below) carries it, the CLI becomes a
  client that still works with no service configured, and a web UI follows once
  the API has been stable through a release. Nothing in the data plane may ever
  depend on it — which is also what keeps multi-tenant operations open, since a
  component nothing depends on can be run once per slice. Tenancy itself is not
  designed; the design instead keeps the implicitly global things out (per-target
  leases, globally unique job ids, recorded job scope, approval checked on the
  target, per-database key configuration, a scoped fleet view). See
  [operator-service.md](design/operator-service.md) and
  [ADR-0019](adr/0019-operator-peer-service.md).

Engine and API:

- **Schema migration planning and execution.** *(Specified.)* Add
  `corium schema update` as a plan-first declarative diff against the installed
  schema, with exact impact counts and additive, validate/reindex, rewrite, and
  destructive classes. Before apply is enabled, close the gap between the data
  model and creation-only schema metadata. Schema vocabulary becomes
  basis-versioned datoms. Peers apply schema generations from tx reports.
  Index and unique activation waits for a validated backfill. Old databases
  retain their creation metadata as a deterministic pre-basis seed. This seed
  preserves basis 0 and attribute ids. Removal means retirement. In-place type
  change and hard deletion remain rejected. See
  [schema-migrations.md](design/schema-migrations.md) and
  [ADR-0020](adr/0020-planned-schema-migrations.md).
- **Long-running transactions (sagas).** *(Registry and branches implemented;
  merge and the expiry sweep specified.)* A saga is a database
  branch plus a registry entry: steps run as ordinary durable transactions on
  a lightweight overlay branch of the database, and the whole branch merges
  into the parent as one conflict-checked commit or aborts without ever
  touching canonical state. Engine-installed `:db.saga/*` vocabulary makes
  in-flight work discoverable with plain Datalog; branch `Db` values give
  opt-in readers the full query surface over partial progress; leased
  entity-id blocks keep ids stable across the merge; mandatory expiry keeps
  abandoned branches from pinning segments. What ships today is the registry —
  the vocabulary, the lifecycle transitions the writer holds to legal moves,
  the peer API, `corium_sys.sagas`, and the `corium saga` commands — plus
  branches: opening a saga leases it an id block, its branch is hosted beside
  the parent as an overlay database, steps are ordinary transactions held to
  the saga's reservations, and tier-2 readers query it through an ordinary
  connection (`corium saga step|log`, `corium console --saga`). Merging that
  novelty back is the next phase. See
  [long-running-transactions.md](design/long-running-transactions.md) and
  [ADR-0023](adr/0023-saga-branch-transactions.md).
- Fulltext (`tantivy`) and tuple value types; excision (design reserved in
  [time-model.md](design/time-model.md)); query fn clauses in user cljrs
  code; leapfrog join; HTTP/JSON gateway; adaptive index statistics; disk
  tier for peer segment cache; `:db/ensure` entity specs.
