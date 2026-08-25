# ADR-0023: Long-running transactions as branch-and-merge sagas

**Status:** Proposed (2026-08-25). Design:
[`docs/design/long-running-transactions.md`](../design/long-running-transactions.md).
Builds on [ADR-0016](0016-transaction-time-as-data.md) (transaction
metadata), follows the plan/apply pattern of
[ADR-0020](0020-planned-schema-migrations.md), and assigns its expiry sweep
to the operator service of [ADR-0019](0019-operator-peer-service.md).

## Context

A Corium transaction is one atomic batch, serialized and durable in
milliseconds. Atomicity, isolation, and brevity are welded together, and
real workloads need the first two without the third: multi-step business
processes that run for days, human-reviewed repairs and backfills,
incrementally prepared imports published in one motion. Such work must be
durable across crashes, must commit as a whole or leave canonical state
untouched, and must not hold anything resembling a lock in a system whose
readers are coordination-free by invariant.

The visibility requirement is the tension that shaped the design. A saga
must not be invisible to the wider system — outside readers may
legitimately want to know work is in flight, and some want to read its
partial progress — yet readers must not be *required* to join the saga or
even know sagas exist, and a reader who did observe partial progress needs
a way to adapt when the saga rolls back.

Existing mechanisms each fail alone: `Db::with_transaction` is speculative
but ephemeral and process-local; `db fork` is durable and writable but
heavyweight (log-prefix copy), lives in the user catalog, and has no merge;
transaction metadata labels work but cannot make many transactions atomic.

Three alternative models were rejected in design iteration (recorded in
full in the design doc): classic interleaved-commit sagas with
compensations make partial state canonical for *every* reader and reduce
atomicity to best-effort compensation; long-lived write intents are locks
held across days and violate the coordination-free invariant; tentative
datoms in the parent log with status-flipped visibility make `as-of` views
status-dependent — visibility would no longer be a pure fold of the log
prefix — and put a saga filter on every hot read path.

## Decision

A saga is a database branch plus a registry entry in the parent database.

- **Branch.** Opening a saga creates a lightweight overlay branch — the
  parent's published state as of opening basis `t₀` plus the branch's own
  log — on the parent's transactor. O(1) creation, segments shared by
  content address, parent's encryption keys, not in the user catalog.
  Steps are ordinary fully-validated durable transactions against the
  branch; schema changes on a branch are refused.
- **Registry.** Engine-installed vocabulary (`:db.saga/*`: id, status,
  basis, owner, expiry, id grants, advisory footprint, outcome refs) in
  every database. Open, extend, commit, abort, and expire are ordinary
  parent transactions on the saga entity.
- **Entity-id grants.** The parent's writer leases the branch disjoint
  per-partition sequence blocks, recorded in the registry; branch
  allocations survive the merge verbatim, so ids resolved by the saga's
  application — or seen by outside readers of the branch — never change
  identity at commit. Abandoned blocks are simply holes in a 42-bit
  space.
- **Merge.** Commit squashes the branch's net novelty into **one** parent
  transaction, validated inside the single-writer path against the
  parent's *current* state and schema: write–write conflict scan over
  `(t₀, now]`, uniqueness, dangling refs, retraction misses, plus
  explicit caller-supplied guards (CAS-shaped preconditions, guard
  queries) for read dependencies. The registry flip to `:committed` rides
  in the same transaction — atomicity is structural, retries are
  idempotent. Effects are replayed, not inputs: tx functions are never
  silently re-evaluated against state the owner didn't observe; drift
  fails loudly with an EDN conflict report, and a retry may carry
  per-conflict resolutions (accept-parent or override). No splicing of
  branch transactions into parent history — the parent log records what
  happened on the parent's timeline; step-level history stays queryable
  in the branch, retained post-commit under a retention policy.
- **Visibility by tiers.** Unaware readers see canonical facts only and
  pay nothing. Registry-aware readers discover in-flight sagas — and
  advisory footprints — with plain Datalog and watch status transitions
  in ordinary tx-reports. Branch readers get the branch's full `Db` value
  (Datalog, Pull, SQL, time views) without locks, registration, or effect
  on the saga; their adaptation contract is: everything read from a
  branch is provisional under that saga id until the registry says
  `:committed`, and the merge transaction's saga id maps what they saw
  onto what landed.
- **Expiry is mandatory.** Every saga carries an owner-extendable
  deadline; an operator-service sweep (in-transactor fallback) expires
  overdue sagas and reclaims branches after a grace window, so abandoned
  sagas cannot pin `t₀`-era segments forever. `:expired` is distinct from
  `:aborted`.
- **External effects stay a layer above.** The branch makes the
  *database* side atomic; workflows with outside side effects use the
  registry as durable orchestrator state and the retained branch as the
  record of what needs compensating — classic saga orchestration over an
  atomic core, not instead of one.

V1 excludes: read-set tracking (serializability beyond write-write is
opt-in via guards), nested and cross-database sagas, base refresh and
rebase-commit modes, and any pgwire `BEGIN` mapping.

## Consequences

- Parent readers can never observe state that later rolls back — abort
  needs no compensation in the database and no reader ever adapts to a
  retraction it didn't opt into. The cost is that partial progress is
  only visible to those who ask, which is the requirement, but means
  tier-0 writers can race an in-flight saga; the conflict scan at merge,
  not any reservation, is what protects the saga, so long sagas over hot
  entities will see conflict reports and must resolve or re-do work.
- The transactor grows real machinery: branch bookkeeping, id-block
  leasing in the allocator, and the merge scan — O(branch novelty +
  parent novelty since `t₀`) inside the writer path. Merge of a large
  saga is an observable pause for that database, like index activation
  in ADR-0020; the conflict scan bounds it, splicing was rejected partly
  to keep it one append.
- An open branch pins parent segments at `t₀` and holds id grants; GC
  pressure and grant consumption are bounded by mandatory expiry rather
  than by trusting owners.
- Squashing trades parent-log granularity for timeline honesty: parent
  history shows one labeled commit; auditors needing step grain must
  consult the retained branch, and retention policy becomes an
  operational knob with storage cost.
- Effects-replay semantics mean a merge can commit a value computed from
  stale reads if the owner declared no guard — the same explicit-optimism
  trade ADR-0015 and ADR-0020 already chose; the remedy is guards, and
  the failure mode is a loud conflict report, never a quiet recompute.
- Every surface grows a saga face: bootstrap vocabulary, peer API,
  protocol, CLI, console, a `corium_sagas` SQL relation, authz applied
  to branch views (ADR-0021 unchanged), operator-service sweep job.
  The registry-first delivery order keeps each phase shippable and the
  data plane free of operator-service dependence.
- Fork remains what it was — an independent database with an independent
  life; sagas do not replace it, and `with_transaction` remains the
  zero-cost speculative tool for single-process what-ifs.
