# Long-Running Transactions (Sagas)

Status: **specified, not implemented.**
[ADR-0023](../adr/0023-saga-branch-transactions.md) records the decision.

A saga is a durable unit of work that spans many transactions and an
arbitrary amount of wall-clock time, and that either lands in the database
as one atomic commit or leaves no mark on the canonical state at all. This
document works through the design space and specifies the chosen model:
**a saga is a database branch plus a registry entry** — steps run as
ordinary transactions on a cheap branch of the database, in-flight progress
is discoverable (and readable) by anyone who asks but imposed on no one,
and the whole branch merges into the parent as a single validated commit or
aborts without ever having touched it.

## The problem

A Corium transaction today is a single submitted batch: the transactor
expands it, validates it, assigns one `t`, appends one log record, and the
work is over in milliseconds. Three properties are welded together in that
model — atomicity, isolation, and *brevity* — and several real workloads
need the first two without the third:

- A data repair or backfill that a human reviews midway before it becomes
  visible to applications.
- A business process (an order, an onboarding, a settlement) that takes
  hours or days, touches many entities across many steps, may consult
  external systems between steps, and must either complete as a whole or
  come apart cleanly.
- A bulk import prepared incrementally, validated as it goes, and published
  atomically.

What such work needs, stated as requirements:

1. **Durable.** Progress survives process and machine crashes; a saga is
   resumable from its last completed step. Nothing rides on one process
   staying alive for days.
2. **Atomic outcome.** Commit makes every step's effect canonical at one
   basis `t`; abort leaves canonical state exactly as if the saga had never
   run. No third outcome.
3. **Isolated by default, observable on demand.** Readers who have never
   heard of sagas must see only canonical facts and pay nothing. But the
   saga must not be invisible to the wider system: a reader who wants to
   know that work is in flight — or wants to read its partial state — can,
   without acquiring locks and without becoming a saga participant.
4. **Rollback-safe for observers.** A reader who *did* look at partial
   progress must be able to tell, afterwards, what became canonical and
   what evaporated — and adapt.
5. **No reader coordination.** Corium readers never take locks
   (architecture invariant); a saga open for a week must not block or slow
   anyone.
6. **Honest about external effects.** Steps that call out to other systems
   cannot be rolled back by the database; the design must give the classic
   compensation-style saga a durable home rather than pretend the problem
   away.

## What already exists to build on

| Mechanism | What it gives | Why it is not the answer alone |
|---|---|---|
| `Db::with_transaction` | Speculative application of tx data to an immutable `Db` value | Ephemeral and process-local; nothing survives a crash, nobody else can see it |
| `corium db fork` | A durable, writable copy of a database at a basis | Heavyweight (copies the log prefix), lands in the user catalog, no way back: there is no merge |
| Transaction metadata (ADR-0016) | Arbitrary datoms on the tx entity | Vocabulary for labeling saga work, but no atomicity across transactions |
| `:db/cas`, guards in tx fns | Optimistic concurrency inside one transaction | One transaction only |
| Plan/apply with basis fencing (ADR-0020) | The pattern: observe at a basis, validate assumptions at apply time inside the writer | Built for schema; the pattern generalizes |
| Operator service jobs (ADR-0019) | Resumable, auditable long-running work with leases and progress | Jobs run *operations*; sagas are user data work — but expiry sweeps belong here |

The chosen design is, in one sentence: make `with_transaction` durable and
shared by backing it with a branch, and make fork mergeable by adding the
merge the fork never had — with the transactor's single-writer pipeline
validating the merge exactly the way it validates any transaction.

## Design iteration

Four models were worked through. The first three fail a requirement each in
an instructive way; the record of *why* is part of the decision.

### Option A — classic sagas: interleaved commits plus compensations

Each step commits to the database immediately, tagged with a saga id on its
transaction entity; abort runs compensating transactions in reverse order.
This is the textbook saga, and ADR-0016 makes the tagging trivial.

Rejected as the core model. It abandons requirement 2 and inverts
requirement 3: partial state is not merely *observable*, it is **canonical**
— every reader sees it, whether or not they can cope, and any writer can
build on facts that a later compensation retracts. Compensation is
best-effort semantics, not atomicity: retracting a datom does not retract
the decisions other transactions made from it. In an immutable-history
database the aborted saga also stays in every history view and `since`
window as ordinary user data, indistinguishable from work that was meant
to last. Classic sagas *do* solve the external-effects problem, though, and
the design keeps them — as an orchestration layer above the database
mechanism (see [External effects](#external-effects-and-the-classic-saga-layer)),
not as the database mechanism.

### Option B — long-lived write intents (pessimistic)

Let a saga reserve entities or attributes in the main database and block or
fail conflicting writers until it completes.

Rejected without much struggle. Corium's reader model is coordination-free
and its writer is a serialized pipeline measured in milliseconds; a
days-long reservation is a lock by another name, held across the exact
time scales where owners crash, leases expire, and humans go home for the
weekend. It violates requirement 5 for writers and imports every deadlock
and starvation question the architecture was designed to never ask.

### Option C — tentative datoms in the main log

Commit saga steps to the parent log, but mark their transactions as
*pending*; the default `Db` view filters datoms whose transaction belongs
to an open or aborted saga, and commit is one small transaction flipping
the saga's status — atomically revealing everything.

This is seductive: one log, atomic reveal, cheap abort, and partial
progress sits right there for saga-aware views. It was rejected on
invariants:

- **Visibility stops being a pure fold of the log prefix.** Whether a datom
  at `t=100` is visible comes to depend on a status transaction at
  `t=250`. `as-of 200` must answer differently before and after `t=250`
  exists — either time views become status-dependent (an `as-of` value
  that changes retroactively is a contradiction in terms in this system)
  or `as-of` shows pending data that `db` hides, which is worse.
- **Every read path pays.** Index scans, Datalog, Pull, SQL projections,
  peer live-tail merges, and published segments would all need a
  tx-status filter on the hottest paths, for a feature most databases
  never use. Requirement 3 says unaware readers pay nothing.
- **Uniqueness turns ambiguous.** If a pending datom holds a unique value,
  an aborted saga retroactively blocked real writers; if it does not,
  commit needs full re-validation anyway — at which point the pending
  datoms bought nothing that a branch does not buy cleaner.

The one idea worth keeping is the *atomic reveal via one small status
transaction*; the chosen design keeps exactly that, but with the pending
data outside the parent log.

### Option D — branch and merge (chosen)

Fork-without-the-weight plus the merge fork never had. A saga:

1. **opens** — the transactor records a saga entity in the parent database
   and creates a *branch*: a lightweight database rooted at the parent's
   state as of the opening basis `t₀`;
2. **steps** — the saga owner runs ordinary transactions against the
   branch, each fully validated and durably logged there, over days if need
   be;
3. **commits** — the transactor folds the branch's accumulated novelty into
   the parent as **one** transaction, re-validated against the parent's
   *current* state, with explicit conflict detection; or
4. **aborts** — one status transaction in the parent; the branch is
   discarded. Canonical state was never touched.

Every requirement lands somewhere concrete. Durability: branch log +
registry are ordinary durable state. Atomicity: the merge is one parent
commit. Default isolation: parent readers see nothing until merge, so
nothing they read can ever be rolled back. Observability: the registry
entry is ordinary data in the parent — discoverable with plain Datalog —
and the branch is a real database value, so partial progress is queryable
with the full surface (Datalog, Pull, SQL, time views) by anyone who asks
for it. No reader coordination: branches share immutable segments;
nobody blocks. The rest of this document specifies the pieces.

## The saga registry

The engine installs a small reserved vocabulary in every database
(`:db.part/db` sequence range, alongside `:db/txInstant`), so the registry
needs no application schema and travels with backup, restore, and
replication like any data:

| Attribute | Type | Card | Notes |
|---|---|---|---|
| `:db.saga/id` | uuid | one | unique identity; the saga's name everywhere |
| `:db.saga/status` | keyword | one | `:open` → `:committed` \| `:aborted` \| `:expired` |
| `:db.saga/basis-t` | long | one | parent basis `t₀` the branch was rooted at |
| `:db.saga/description` | string | one | human-readable purpose |
| `:db.saga/owner` | string | one | authenticated principal that opened it |
| `:db.saga/expires-at` | instant | one | deadline; extendable by the owner while `:open` |
| `:db.saga/id-grants` | ref (component) | many | entity-id blocks leased to the branch; grant entities carry `:db.saga.grant/partition`, `/start`, `/length` |
| `:db.saga/footprint` | ref | many | *advisory* declared touch-set — entities, or attribute entities to name whole attributes |
| `:db.saga/merged-tx` | ref | one | on commit: the merge transaction |
| `:db.saga/steps` | long | one | on commit: number of branch transactions squashed |
| `:db.saga/conflict-report` | string (EDN) | one | on a failed merge attempt: what collided (latest attempt) |

Opening, expiry extension, commit, and abort are ordinary parent
transactions touching this entity — which is the whole visibility story:

- **Tier 0 — unaware readers.** See canonical facts only. The registry adds
  a handful of datoms per saga; no read path changes.
- **Tier 1 — registry-aware readers.** Ask, in plain Datalog, "is anything
  in flight? does anything declare a footprint over entity X?" and decide
  to wait, proceed, or look closer. Because status transitions are ordinary
  transactions, they arrive in tx-reports: a peer can watch saga state
  changes exactly the way it watches any data.
- **Tier 2 — branch readers.** Obtain the branch's `Db` value and query
  partial progress with the full read surface. They have "joined the saga"
  for reads without any protocol: no locks, no registration, no effect on
  the saga.

Rollback-safety for tier 2 falls out of two facts. Everything a branch
reader saw was labeled — it came from a `Db` whose identity *is* the saga
id — and the registry transition is data they can observe. On
`:committed`, the merge transaction carries the saga id and the merged
novelty is exactly the branch novelty with entity ids unchanged (see id
grants below), so "what I saw" maps one-to-one onto "what became
canonical." On `:aborted`, nothing they saw was ever canonical, and they
know precisely which facts those were. The adaptation contract for a tier-2
reader is one sentence: *treat everything read from a branch as provisional
under that saga id until the registry says `:committed`.*

The **footprint** is deliberately advisory. A binding footprint would be
Option B's locks through the back door. As a declaration it lets tier-1
readers and tooling warn about overlapping in-flight sagas at open time,
without ever being load-bearing for correctness — correctness lives in
merge validation.

## Branches

A saga branch is *not* a `db fork`, though it reuses its machinery where it
can:

| | `db fork` | saga branch |
|---|---|---|
| Creation cost | copies the log prefix through `t` — O(history) | overlay: parent's published root as of `t₀` + an empty branch log — O(1) |
| Catalog | a full user database | internal (`<db>` sub-namespaced by saga id); listed via saga surfaces, not `db list` |
| Lifecycle | independent forever | tied to the saga: merged then retained-or-deleted, or discarded on abort/expiry |
| Entity ids | replays parent counters; diverges freely | allocates from leased blocks so ids survive merge verbatim |
| Transactor | any | the parent's transactor (id grants and merge live in its writer) |
| Encryption | fresh data key, no shared ciphertext | shares the parent's data key — same trust domain, shares segments by construction |
| Schema | full database, may evolve | data transactions only; schema changes refused on a branch |

The overlay construction is the same shape a peer already uses for its own
view — published index trees plus a log tail replayed on top — pointed at
the parent's index root for `t₀` plus the branch's own log. GC reachability
needs no new mechanism: the branch root reaches the parent segments it
shares, and the sweep already collects only what no live root reaches. A
branch does pin the parent's `t₀`-era segments for as long as it lives,
which is one reason sagas carry deadlines (below).

Branch steps are ordinary transactions with ordinary guarantees: tempids,
lookup refs, upsert, uniqueness, `:db/cas`, database functions, tx
metadata — each step validated against the branch's current state and
durably appended to the branch log before ack. A saga with a hundred steps
is a hundred real transactions whose interleaved reads and writes the
owner performed and observed; that observation is what merge preserves
(and what distinguishes the chosen merge semantics from a rebase — see
below).

### Entity-id grants

Fork's id story does not survive a merge: branch and parent both continue
the counters from `t₀`, so each allocates the same ids to different
entities, and merged datoms would collide. Two fixes were considered:

- **Remap at merge** — allocate fresh parent ids for branch-created
  entities and rewrite refs in the novelty. Sound, but it breaks the one
  thing tier-2 visibility promised: ids the saga's own application resolved
  from tempids — and possibly stored externally, and possibly showed to
  branch readers — silently change identity at commit.
- **Leased blocks** *(chosen)* — at open (and again on demand), the parent
  transactor leases the branch disjoint sequence blocks per partition,
  recorded in `:db.saga/id-grants`; the branch allocates only from its
  blocks and the parent's allocator skips granted ranges. Merged datoms
  keep every id verbatim. Sequences are 42-bit per partition; granting
  generous blocks (say 2²⁰ ids a grant) to even thousands of sagas costs
  nothing that matters, and blocks of aborted sagas can simply be
  abandoned — a hole in the sequence space is already normal.

Grants are durable (registry datoms) and fenced by the same single writer
that allocates parent ids, so a transactor crash between granting and
branching cannot double-issue a block.

## Merge

The merge is where the design earns atomicity, and it is specified in the
image of ADR-0020's plan/apply: optimistic work observed at a basis,
re-validated inside the single-writer path at apply time, with drift
surfaced rather than absorbed.

**Semantics: replay the *effects*, not the *inputs*.** The merge takes the
branch's novelty — the net datom effect of branch transactions
`(t₀, t_branch]` — and applies it to the parent's current value as one
transaction. It does **not** re-run the original transaction data (tx
functions, `:db/cas` forms) against the parent. The saga's owner ran those
steps against branch state, observed the results, possibly took external
actions on the strength of them, possibly had a human review them; a
rebase that re-evaluates functions against different state could silently
commit something nobody observed. When the parent has changed in a way
that matters, the right behavior is to *fail loudly*, not to recompute
quietly. (A `--rebase` merge mode that replays inputs is noted under
future work; it is a different promise and must be asked for by name.)

**The merge algorithm**, inside the parent's writer:

1. Load the branch novelty: fold branch transactions into net assertions
   and retractions (an intra-branch assert-then-retract cancels; the last
   write per `(e, a)` on cardinality-one wins — this is the squash).
2. **Conflict scan** against parent changes in `(t₀, now]`:
   - *write–write*: the parent asserted or retracted any datom on an
     `(e, a)` the branch also wrote (cardinality-one), or the exact
     `(e, a, v)` (cardinality-many);
   - *uniqueness*: a branch-asserted unique value now collides in the
     parent;
   - *dangling refs*: the branch references an entity the parent has since
     retracted (`:db/retractEntity`);
   - *retraction misses*: the branch retracts `(e, a, v)` whose `v` is no
     longer the parent's value (the CAS-shaped conflict).
3. **Guard evaluation**: the commit request may carry explicit guards —
   `:db/cas`-shaped preconditions and boolean guard queries — evaluated
   against the parent's current value. Guards are how a saga makes its
   *read* dependencies explicit; the engine does not track read sets (see
   Limits), so serializability beyond write-write is opt-in and visible in
   the request.
4. **Full transaction validation** of the novelty against the parent's
   current schema (which may have migrated since `t₀` — ADR-0020 makes
   schema basis-versioned, and merge validates against *now*, exactly as a
   fresh transaction would).
5. If anything failed: no parent write except an updated
   `:db.saga/conflict-report` on the registry entry; the saga stays
   `:open` and the branch is untouched. The report is EDN naming each
   conflicting `(e, a)` with both sides' values — a plan document, in
   ADR-0020's sense.
6. If everything passed: append **one** parent transaction containing the
   novelty **plus** the registry flip to `:committed` plus
   `:db.saga/merged-tx`/`:db.saga/steps`, with the saga id asserted on the
   transaction entity. One log record; atomicity is structural, and a
   crashed-and-retried commit is idempotent because the retry finds
   `:committed`.

**Squash, not splice.** Branch transactions are not replayed as individual
parent transactions. Splicing would fabricate parent history — `t` values
interleaving a past that did not happen on the parent's timeline — and
break `:db/txInstant` monotonicity (ADR-0016). The parent's log tells the
truth: one commit, at commit time, labeled with the saga id. Step-level
history — who did what, when, in which order, with what tx metadata —
remains fully queryable in the branch, which can be **retained** after
commit as a read-only audit annex (subject to a retention policy) or
deleted once nobody needs it. History-minded readers get:
`[?tx :db.saga/id ?saga]` in the parent to find the merge, and the branch
`as-of`/`history` views for the fine grain.

**Conflict resolution and retry.** A failed merge is information, not a
dead end. The commit request accepts per-conflict resolutions: for a named
`(e, a)`, either *accept-parent* (drop the branch's write) or *override*
(the branch's value wins even though the parent moved). A retry validates
that no conflicts appeared beyond the resolved set, then proceeds. The saga
owner can also simply keep working — resolve the divergence with further
branch transactions informed by the report, and commit again. A general
*base refresh* (folding parent novelty into the branch to re-root it at a
newer `t₀`) is future work; it is a merge in the opposite direction and
earns its own design pass.

## Lifecycle, failure, and expiry

States: `:open → :committed | :aborted | :expired`. All transitions are
single parent transactions; there is no `:committing` limbo because the
flip rides inside the merge transaction itself.

| Failure | Outcome |
|---|---|
| Crash between registry open and branch creation | Registry entry exists, branch does not; open is resumable/idempotent by saga id (branch creation is a deterministic function of the registry entry) |
| Crash mid-step | Step either fully in the branch log or absent — ordinary transaction durability, per step |
| Crash during merge, before append | No parent effect; saga `:open`; retry re-runs the scan against then-current state |
| Crash during merge, after append | Saga is `:committed` (same record); retry observes the status and reports success |
| Owner disappears | `:db.saga/expires-at` passes; the expiry sweep aborts the saga |
| Transactor failover | Registry, branch log, and grants are durable storage state; the new lease holder sees all of it — nothing lived only in memory |

**Expiry is mandatory.** An open saga pins parent segments from `t₀` and
holds id grants; an abandoned one must not do so forever. Every saga
carries `:db.saga/expires-at`; the owner extends it (heartbeat at whatever
period suits the workload — an interactive saga might extend daily); a
sweep — an operator-service job (ADR-0019), with an in-transactor fallback
so the data plane does not depend on the service — transitions overdue
sagas to `:expired` and queues their branches for deletion. `:expired` is
distinguished from `:aborted` so a returning owner knows the system, not a
decision, ended the work; the branch retention window gives them a grace
period to salvage (re-open as a fresh saga from the old branch's contents)
before deletion.

**Concurrent sagas** compose without new rules: each has its own branch
and disjoint id grants; merges serialize through the parent's writer;
the first to merge wins, and later merges see its effects in their
conflict scan. Footprint overlap between open sagas can be *warned about*
at open time; it is never an error, because footprints are advisory.

## External effects and the classic-saga layer

The branch commits or aborts database effects atomically. It cannot
un-send an email. Workflows whose steps have external side effects still
need compensation logic — the classic saga — and the design gives that
layer a durable home rather than a competing mechanism:

- The registry entry is the orchestrator's durable state: status,
  progress, deadline — crash-safe and queryable, so a restarted
  orchestrator resumes from data.
- Step transactions on the branch carry ordinary tx metadata; a workflow
  records each external action and its compensation handle
  (application-defined attributes) alongside the data it justified.
- On abort or expiry, the branch (retained through its retention window)
  is the complete, queryable record of which external actions were taken
  and need compensating — the orchestrator reads it and drives the
  compensations *outward*; the database side needs none, because the
  database side never happened.

This split is the honest division: atomicity where the system can actually
promise it (its own state), durable bookkeeping where it cannot (everyone
else's).

## Surfaces

- **Peer API.** `Connection::saga_open(db, opts) → Saga` (opts:
  description, footprint, expiry, id-grant sizing);
  `Saga::transact(...)` / `Saga::db()` (ordinary `Db` value of the
  branch); `Saga::commit(guards, resolutions) → Result<MergeReport,
  ConflictReport>`; `Saga::abort()`; `Saga::extend(expiry)`;
  `Connection::saga_resume(db, id)`; `Connection::saga_view(db, id) → Db`
  for tier-2 readers. Registry queries are just queries.
- **Protocol.** gRPC additions mirroring the peer API; the branch is
  served over the existing database-view machinery (a `DbViewSpec`-shaped
  reference naming the saga), so thin clients get tier-2 reads for free.
- **CLI.** `corium saga open|list|status|extend|commit|abort <db> ...`;
  `corium console <db> --saga <id>` to point a console at a branch;
  `corium saga log <id>` for step history.
- **SQL.** A `corium_sagas` system relation over the registry (id, status,
  basis, owner, expiry, description) beside the existing system
  relations; branch reads via the console/session db-view selection.
  Mapping pgwire interactive `BEGIN`/`COMMIT` onto sagas is explicitly out
  of scope for v1 (ADR-0015's guarded autocommit stands); it is an
  obvious later customer.
- **AuthZ.** Opening a saga requires transact rights on the database;
  branch reads are authorized like parent reads (same policy database,
  same contextual authorization — ADR-0021 applies to branch views
  unchanged); the merge is authorized as a transact by the saga owner at
  commit time against then-current policy. The registry's own attributes
  are ordinary data under the same model.

## Limits and non-goals (v1)

- **No read-set tracking, so no silent serializability.** Write-write
  conflicts are detected; read-write anomalies are the saga's to declare
  via guards. This is the same honesty ADR-0015 chose for guarded DML —
  optimistic, explicit, validated at the writer.
- **No nested sagas.** A branch of a branch has no parent-merge story yet.
- **No cross-database sagas.** One saga, one database; multi-database
  atomicity is a different (two-phase) problem.
- **No schema changes on a branch.** Schema migration has its own
  plan/apply lifecycle (ADR-0020); mixing the two inside a merge is
  needless coupling.
- **No base refresh / rebase modes.** Future work, each with its own
  semantics to specify: base refresh re-roots the branch on a newer
  parent basis; rebase-commit re-runs original tx *inputs* at merge.
- **Branch writes go through the parent's transactor.** A saga is not a
  write-throughput feature; see
  [write-path-scaling.md](write-path-scaling.md) for that problem.

## Delivery sketch

1. **Registry + vocabulary** — bootstrap attributes, open/abort/expiry as
   transactions, `corium_sagas` relation, no branches yet (a saga with no
   steps is already useful as a durable workflow record).
2. **Branches** — overlay construction, id grants, step transacting, peer
   and CLI read surfaces (tier 2).
3. **Merge** — squash, conflict scan, guards, resolutions, the atomic
   commit-and-flip; conflict reports.
4. **Expiry sweep + retention** — operator-service job with in-transactor
   fallback; branch GC.
5. **Protocol/thin-client surfaces and SQL polish.**

Each phase leaves the system consistent and shippable; nothing in the data
plane depends on a later phase.
