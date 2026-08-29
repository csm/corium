# Long-Running Transactions (Sagas)

Status: **registry and branches implemented; merge and the expiry sweep are
still specification.** Phases 1 and 2 of the [delivery
sketch](#delivery-sketch) are in the tree: the `:db.saga/*` vocabulary, the
lifecycle transitions and the rules the transactor holds them to
([`corium_tx::saga`](../../crates/corium-tx/src/saga.rs)), the read model
([`corium_db::saga`](../../crates/corium-db/src/saga.rs)), the peer API
([`corium_peer::saga`](../../crates/corium-peer/src/saga.rs)),
`corium_sys.sagas`, and `corium saga`; then id-block leasing in the allocator,
the branch overlay and its pipeline
([`corium_transactor::branch`](../../crates/corium-transactor/src/branch.rs),
[`corium_tx::branch`](../../crates/corium-tx/src/branch.rs)), step transacting,
and tier-2 reads. So a saga today is a durable, expiring workflow record whose
partial progress is real, queryable, and isolated — what is missing is the way
back: merge, guards, and conflict reports, plus the expiry sweep that reclaims
branches. [ADR-0023](../adr/0023-saga-branch-transactions.md) records the
decision.

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
   run. No third outcome. (A saga may register a *compensation* — a
   deliberately authored failure record applied atomically with the abort
   — see [Compensation](#compensation); what never happens is a partial
   landing of the saga's own writes.)
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
mechanism (see [Compensation](#compensation)), not as the database
mechanism.

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

The engine installs a small reserved vocabulary in every database —
attributes in `:db.part/db`, allocated from the reserved sequence range
below the first user-installable id, the same register `:db/txInstant`
lives in — so the registry needs no application schema and travels with
backup, restore, and replication like any data (what does *not* travel
with it is the branch; see the liveness invariant under
[Lifecycle](#lifecycle-failure-and-expiry)):

| Attribute | Type | Card | Notes |
|---|---|---|---|
| `:db.saga/id` | uuid | one | unique identity; the saga's name everywhere |
| `:db.saga/status` | keyword | one | `:db.saga.status/open` → `…/committed` \| `…/aborted` \| `…/expired` — namespaced like every enum in the `:db` vocabulary (`:db.cardinality/one`); prose below abbreviates to `:open` etc. |
| `:db.saga/basis-t` | long | one | parent basis `t₀` the branch was rooted at |
| `:db.saga/description` | string | one | human-readable purpose |
| `:db.saga/owner` | string | one | authenticated principal that opened it |
| `:db.saga/expires-at` | instant | one | deadline; extendable by the owner while `:open` |
| `:db.saga/id-grants` | ref (component) | many | entity-id blocks leased to the branch; grant entities carry `:db.saga.grant/partition`, `/start`, `/length` |
| `:db.saga/footprint` | ref | many | *advisory* declared touch-set — entities, or attribute entities to name whole attributes |
| `:db.saga/reserves` | ref | many | *checked* reservation set — entities, or attribute entities; binds the saga's own writes, never other writers (see [Footprints and reservations](#footprints-and-reservations)) |
| `:db.saga/sealed` | boolean | one | when true, the reservation set is fixed at open and cannot grow |
| `:db.saga/merged-tx` | ref | one | on commit: the merge transaction |
| `:db.saga/steps` | long | one | on commit: number of branch transactions squashed |
| `:db.saga/conflict-report` | string (EDN) | one | on a failed merge attempt: what collided (latest attempt) |
| `:db.saga/on-abort-tx` | string (EDN) | one | compensation: static tx data applied atomically with the abort/expiry flip (see [Compensation](#the-compensating-transaction)) |
| `:db.saga/on-abort-fn` | ref | one | compensation: a `:db/fn` entity invoked at abort/expiry with the parent and branch values; at most one of `/on-abort-tx`, `/on-abort-fn` |
| `:db.saga/on-abort-error` | string (EDN) | one | why a system-time compensation did not land: validation failure at expiry, or the branchless-expiry skip |
| `:db.saga/compensations` | ref (component) | many | external-compensation ledger entries (`:db.saga.compensation/key`, `/status`, `/detail`, `/completed-at`, `/error`) — written by the orchestrator, never executed by the engine |
| `:db.saga/guard` | string (EDN) | many | *step tx metadata, used in the branch*: a guard declared by the step that established the read dependency (see [Merge](#merge)) |

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

### Footprints and reservations

The **footprint** is deliberately advisory. A binding footprint would be
Option B's locks through the back door. As a declaration it lets tier-1
readers and tooling warn about overlapping in-flight sagas at open time,
without ever being load-bearing for correctness — correctness lives in
merge validation.

An advisory declaration has a corresponding weakness: readers cannot
*rely* on it. A saga may touch entities it never declared, so "X is not
in any footprint" answers nothing. For sagas that know their shape up
front, the registry therefore offers a second, **checked** mode:
`:db.saga/reserves` names the exact pre-existing entities (and/or whole
attributes) the saga will operate on, and the engine enforces the
declaration — *against the saga itself*. The direction of the binding is
the whole trick: a reservation constrains the saga's own branch writes
and never touches any other writer, so it is a contract, not a lock.
Option B stays rejected.

Enforcement is a step-time check in the branch pipeline
([`corium_tx::branch`](../../crates/corium-tx/src/branch.rs)), after expansion
and tempid resolution, refusing the step (like any validation error) on
violation:

- an assertion or retraction whose `e` is a pre-`t₀` entity must have
  `e` in the reserved entities or `a` in the reserved attributes;
- a **ref value** targeting a pre-`t₀` entity must target a reserved
  entity (or be asserted under a reserved attribute). This closure rule
  is not pedantry: corium indexes refs in reverse (VAET), so a new
  entity merely *pointing at* X changes what reverse-ref navigation from
  X returns after merge. Without the rule, "X is outside the reserved
  set" would be false in exactly the way a reader can't see coming;
- branch-created entities (ids in granted blocks) are unrestricted among
  themselves — new structure grows at will, attached to the parent graph
  only through reserved entities, which is what makes the reserved set a
  complete boundary of the saga's effect.

What a reserved saga buys each party:

- **Tier-1 readers** get a *reliable* answer: an entity outside the
  reserved set (of an entity-reserved saga) is untouched by that saga —
  writes and reverse-refs included — as of the registry basis they read.
  They can also see conflict *brewing*: a parent tx-report touching a
  reserved entity while the saga is open is an early warning the merge
  scan will later confirm, available to tooling before anyone commits.
- **The merge** narrows: write–write, dangling-ref, and retraction-miss
  scanning confine to the reserved set (plus granted blocks), since the
  step checks guarantee novelty touches nothing else pre-existing.
  Uniqueness stays global — a unique value can collide with any entity.
- **The saga owner** trades flexibility for the above: an unreserved
  entity discovered mid-flight needs a registry extension first — an
  ordinary parent transaction adding to `:db.saga/reserves`, visible to
  readers with a basis, exactly like any other registry change. A saga
  opened `:db.saga/sealed` forgoes even that: its set is fixed at open,
  which is the strongest statement a reader can lean on. (The basis-
  relative contract is the honest one either way: "complete as of the
  registry I read," with extensions arriving as observable tx-reports.)

Reservation granularity matters at scale. Reserved entities are registry
datoms in the parent — reserving a million entities is a million datoms,
which is the wrong shape. Bulk work reserves **attributes** instead
("this saga writes only `:order/status`"), trading per-entity precision
for a set that stays small; entity-level reservation is for the
workflow-shaped sagas that motivated this design, whose write sets are
tens of entities known by name. The two combine, and both remain
optional: a saga with no reservations behaves as before, advisory
footprint and all.

## Branches

A saga branch is *not* a `db fork`, though it reuses its machinery where it
can:

| | `db fork` | saga branch |
|---|---|---|
| Creation cost | copies the log prefix through `t` — O(history) | overlay: parent's published root as of `t₀` + an empty branch log — O(1) |
| Catalog | a full user database | internal (`<db>` sub-namespaced by saga id); listed via saga surfaces, not `db list` |
| Lifecycle | independent forever | tied to the saga: merged then retained-or-deleted, or discarded on abort/expiry |
| Entity ids | replays parent counters; diverges freely | allocates from leased blocks so ids survive merge verbatim |
| Transactor | any | the parent's transactor *process*, but its own writer pipeline — see below |
| Timeline | continues the source's `t` counters, diverging freely | its own `t` counters from `t₀` — see [Branch time](#branch-time) |
| Encryption | fresh data key, no shared ciphertext | shares the parent's data key — same trust domain, shares segments by construction; if protection classes land (ADR-0017/0018), class keys gate protected attributes on a branch exactly as on the parent |
| Schema | full database, may evolve | data transactions only; schema changes refused on a branch |

The overlay construction is the same shape a peer already uses for its own
view — published index trees plus a log tail replayed on top — pointed at
the parent's index root for `t₀` plus the branch's own log. GC reachability
needs no new mechanism: the branch root reaches the parent segments it
shares, and the sweep already collects only what no live root reaches. A
branch does pin the parent's `t₀`-era segments for as long as it lives,
which is one reason sagas carry deadlines (below).

*As implemented:* a branch never publishes an index and never writes a
segment — it is excluded from the startup, standby, and indexer scans by
name — so it holds the parent's storage keys as a snapshot taken when it
opened and does nothing with them afterwards. A parent key rotation replaces
the segment key store, not the record cipher, so a branch's log keeps sealing
under the cipher it opened with and a rotation mid-saga changes nothing about
it; the branch picks the parent's current keys up the next time it is opened.
Key *fencing* is not a snapshot: a branch asks its parent, so a fenced parent
stops its branches too.

*As implemented:* a branch is hosted as an ordinary `DbState` beside its
parent, under the name `<parent>.saga.<id>` — a name no database can be
created under, since database names are alphanumeric, so a branch is never
listed, never stood by for, and never mistaken for one. Making it a database
is what buys the read surface: a step is an ordinary `Transact`, a tier-2
reader an ordinary `Subscribe`, and Datalog, Pull, SQL, and the time views
follow without a line of new read-path code. What it does *not* have of its
own is a lease or a data key: branch commits are fenced by the parent's write
lease, because a node that has lost the parent has no business acking a step,
and branch records are sealed under the parent's key, because a branch shares
the parent's segments by construction. Its base is built exactly as described
above — the newest published parent root with index basis `≤ t₀` plus the
parent log records closing the gap — falling back to replaying the parent's
prefix when no such root is available or a `t₀`-era segment has already been
swept, which is always correct because the log is the source of truth. And it
is opened on demand rather than when the saga opens: creation is a
deterministic function of the registry entry, so "open the branch if it is not
hosted" is both the ordinary path and the crash-recovery path.

Branch steps are ordinary transactions with ordinary guarantees: tempids,
lookup refs, upsert, uniqueness, `:db/cas`, database functions, tx
metadata — each step validated against the branch's current state and
durably appended to the branch log before ack. A saga with a hundred steps
is a hundred real transactions whose interleaved reads and writes the
owner performed and observed; that observation is what merge preserves
(and what distinguishes the chosen merge semantics from a rebase — see
below).

Steps do **not** pass through the parent's writer queue. The branch is
hosted in the parent's transactor process, but it is its own little
database with its own serialized pipeline (and its own group-commit
batching): because id blocks are leased up front, a step needs no
per-step coordination with the parent at all. The parent's writer is
entered only where the parent's state actually changes — open (grant
allocation plus the registry transaction), extend, abort, and merge. A
chatty saga therefore costs the parent shared process resources (CPU,
storage I/O, cache), never queueing latency in its commit path; the
one parent-writer pause a saga causes is the merge scan at commit.

### Branch time

A branch keeps its own timeline. Its log numbers transactions from
`t₀ + 1` upward, independently of the parent — exactly as a fork does —
and branch transaction entities take ids in `:db.part/tx` from those `t`
values. No id grants cover the tx partition, because **branch transaction
entities never merge**: the squash rewrites every novelty datom's `tx` to
the single merge transaction, and step tx-entity datoms (`:db/txInstant`,
step metadata, `:db.saga/guard` declarations) stay behind in the branch,
which is precisely where the step-grain story reads them (see *Squash,
not splice* below). Branch tx ids may therefore numerically coincide with
parent tx ids issued after `t₀`; the two never meet in one database, so
no ambiguity arises — but surfaces that display both (a console pointed
at a branch, `corium saga log`) should label ids with their timeline.

*As implemented:* the branch's durable log is an ordinary log numbered from
one, wrapped in a `RootedLog` that presents it at `t₀ + 1` upward. Every log
backend, recovery path, and torn-tail truncation therefore works on it
unchanged, and nothing about the branch's numbering reaches storage. Reads
splice: a range below `t₀` is answered from the parent's log and a range above
it from the branch's, so the concatenation is contiguous in `t` and a peer
subscribing to a branch folds the parent's prefix and the branch's novelty
into one value — which is how `as-of t ≤ t₀` on a branch answers exactly what
the parent answers, without copying a byte of history.

Time views on a branch `Db` are the branch's own: `as-of t` for `t ≤ t₀`
answers identically to the parent's `as-of t` (shared prefix, shared
segments); above `t₀` it walks branch history. `:db/txInstant` follows
the ordinary monotonic rule (`max(now, last + 1)`) *within the branch*,
seeded from the parent's clock at `t₀`, so instant-named views on the
branch resolve sensibly even though branch instants and parent instants
after `t₀` are unrelated sequences.

### Read-path cost

What does reading in-flight sagas cost, layer by layer? Never `N + 1`
log merges, and nothing at all for those who don't ask:

- **Tier 0/1: zero change.** A parent `Db` remains exactly today's
  two-layer construction — published index trees plus the parent's log
  tail `(index-basis, basis-t]` — with registry datoms as ordinary data
  inside it, however many sagas are open.
- **Tier 2, one branch: at most three layers, and the third is frozen.**
  A branch `Db` is the latest published parent root with index-basis
  `≤ t₀`, plus the parent's log records `(index-basis, t₀]` closing the
  gap, plus the branch's own tail. The gap slice is fixed at open — the
  branch's base never moves — and retained history roots keep it small
  when a publication sits near `t₀`. So a branch read is the ordinary
  peer construction with one bounded, immutable slice inserted.
- **And three collapses back to two.** A branch is a database; the
  existing indexing job pointed at it folds the frozen gap and the
  branch tail into published branch trees (structurally shared with the
  parent's segments by content address), after which branch reads are
  ordinary two-layer reads. Publication triggers on the same tail-size
  heuristics as anywhere — precisely the long, chatty, actually-watched
  sagas earn it; a short saga's tail is a handful of records nobody
  needs trees for.
- **`N` sagas cost per-branch, on consultation only.** No view unions
  branches. A reader consulting three sagas holds three `Db` values; a
  multi-database Datalog query over them takes them as separate inputs,
  not as one `N`-way merged log. The per-open-saga cost that accrues to
  the *system* rather than to opting-in readers is the transactor
  hosting `N` branch pipelines and pinning `N` sets of `t₀`-era
  segments — named in the consequences, bounded by mandatory expiry.

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
branching cannot double-issue a block. One guard closes the explicit-id
loophole: a transaction may name an arbitrary entity id directly, so
while a grant is live the parent's writer refuses assertions naming ids
inside granted blocks. This is allocator integrity, not a lock — leased
id space names no user-visible entity, and no legitimate parent
transaction has business writing there.

*As implemented:* opening stays ordinary transaction data any client can
compose, and the block is minted inside the writer as part of preparing that
very transaction — `:db.saga/id-grants` is refused as submitted data, because
carving a block out of the allocator's counter is the allocator's job. The
grant datoms therefore ride in the transaction that opens the saga: leased
but unrecorded is not a state that exists. Two consequences follow in the
allocator. Recovery floors the parent's next id past every block the registry
records, open or finished, because a leased block is spent whether or not a
datom uses it yet and an allocator trusting only the ids it can see would
reissue the range it promised. And the refusal above covers both positions of
a datom: a ref *value* naming a granted id would attach parent data to an
entity the branch has not created yet. Both end when the saga does — a
committed saga's ids are ordinary entities in the parent, and writing them is
ordinary work.

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
   Branch tx-entity datoms are excluded: step transaction entities never
   merge (see [Branch time](#branch-time)).
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

   For a saga with reservations, the write–write, dangling-ref, and
   retraction-miss scans confine to the reserved set plus granted blocks
   — the step-time checks guaranteed novelty touches nothing else
   pre-existing (see [Footprints and
   reservations](#footprints-and-reservations)); uniqueness stays global.

   One non-conflict is a deliberate choice, not an omission:
   **cardinality-many assertions union.** Parent and branch each asserting
   different values on the same many-cardinality `(e, a)` merge to the
   union — that is what a set-valued attribute means — and both asserting
   the *same* `(e, a, v)` is idempotent. Only the exact-triple races above
   (parent retracted a triple the branch asserts, or the reverse) conflict.
   A saga that needs "nobody else added to this set" is asserting a read
   dependency, and declares it as a guard.
3. **Guard evaluation**: guards are `:db/cas`-shaped preconditions and
   boolean guard queries evaluated against the parent's current value —
   how a saga makes its *read* dependencies explicit. The engine does not
   track read sets (see Limits), so serializability beyond write-write is
   opt-in and visible. Guards come from two places: the commit request,
   and — so the contract is durable rather than living only in one
   process's memory — `:db.saga/guard` metadata asserted on the branch
   step that established the dependency, at the owner's option. Commit
   evaluates the union of both by default; a step-declared guard that
   fails is reported with the step that declared it, which makes the
   conflict traceable to the read that mattered. A crashed owner, or a
   different process resuming the saga, inherits the declared guards for
   free.
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
dead end. The commit request accepts per-conflict resolutions, each fenced
to the conflict report it answers: a resolution names the parent-side
value the report showed, and holds only while that value still stands —
further parent drift on a resolved `(e, a)` is a fresh conflict, never
silently absorbed. Reading the report *is* the observation that makes
resolving consistent with effects-replay: the owner is no longer deciding
against unobserved state.

Which resolutions exist depends on the conflict class, and the asymmetry
is principled:

- *accept-parent* — drop the branch's conflicting write from the novelty —
  is available for **every** class. It only ever removes something from
  the merge.
- *override* — the branch's value wins — is available **only for
  write–write conflicts on cardinality-one `(e, a)`**, where it has an
  exact expansion: retract the parent's reported current value, assert the
  branch's. Both datoms name state the owner has seen (one in the branch,
  one in the report), and the write stays within the saga's own footprint.
- *uniqueness*, *dangling-ref*, and *retraction-miss* conflicts are **not
  override-able.** Each override would fabricate a write outside what the
  owner observed or outside what the saga touched: evicting the parent's
  claimant of a unique value edits an entity the saga never wrote,
  overriding a dangling ref resurrects a retracted entity, and overriding
  a retraction miss retracts a value the branch never held. The design's
  first merge principle — replay what was observed, fail loudly on the
  rest — outranks resolution convenience here. The recourse is
  accept-parent, or a decision made where decisions belong: an ordinary
  follow-up transaction (or fresh saga) that does the disputed write in
  the open, after this merge lands.

A retry validates that no conflicts appeared beyond the resolved set, then
proceeds. The saga owner can also simply keep working — resolve the
divergence with further branch transactions informed by the report, and
commit again. A general *base refresh* (folding parent novelty into the
branch to re-root it at a newer `t₀`) is future work; it is a merge in
the opposite direction and earns its own design pass.

## Lifecycle, failure, and expiry

States: `:db.saga.status/open → :db.saga.status/committed |
:db.saga.status/aborted | :db.saga.status/expired` (abbreviated `:open`
etc. throughout, per the registry table). All transitions are single
parent transactions;
there is no committing limbo because the flip rides inside the merge
transaction itself. Abort and expiry likewise carry any registered
compensating transaction inside the same flip (see
[Compensation](#the-compensating-transaction)); the failure table below
includes its edges.

| Failure | Outcome |
|---|---|
| Crash between registry open and branch creation | Registry entry exists, branch does not; open is resumable/idempotent by saga id (branch creation is a deterministic function of the registry entry) |
| Crash mid-step | Step either fully in the branch log or absent — ordinary transaction durability, per step |
| Crash during merge, before append | No parent effect; saga `:open`; retry re-runs the scan against then-current state |
| Crash during merge, after append | Saga is `:committed` (same record); retry observes the status and reports success |
| Abort races an in-flight merge | The parent's writer serializes them; whichever lands first wins, and the loser gets a status error — an abort arriving after the merge committed fails with "already `:committed`", it does **not** report success |
| Owner disappears | `:db.saga/expires-at` passes; the expiry sweep expires the saga, applying any registered compensation |
| Compensation fails at explicit abort | The abort fails like any invalid transaction; the saga stays `:open` — fix the tx data or abort without it |
| Compensation fails at expiry | Liveness outranks: the sweep expires the saga *without* the compensation datoms and records `:db.saga/on-abort-error`; branch retention is the grace period to record by hand |
| Transactor failover | Registry, branch log, and grants are durable storage state; the new lease holder sees all of it — nothing lived only in memory |
| Restore, fork, or replica finds `:open` registry entries with no branch | The saga is expired on first open — see the liveness invariant below |

**A saga is live only where its branch lives.** The registry travels with
the parent's data — backup, restore, `db fork`, replication — but the
branch does not: backups do not capture saga branches (v1), and a fork
copies the parent's log prefix, registry datoms included, never the
branches. So a database can find itself holding `:open` registry entries
whose branches are absent — a restored parent, a fork taken while sagas
were in flight. One rule covers every such case: on first open, the
transactor expires any `:open` saga whose branch does not exist. `:expired`
already means "the system, not a decision, ended this," which is exactly
what happened; the entries remain as honest history, and nothing dangles.
A branchless expiry also never applies a registered compensation: a
restored or forked database shares its timeline's past with the original
but not its future, and applying the same failure record on both sides of
the divergence would double every externally visible consequence — the
skip is recorded in `:db.saga/on-abort-error`.

**Expiry is mandatory.** An open saga pins parent segments from `t₀` and
holds id grants; an abandoned one must not do so forever. Every saga
carries `:db.saga/expires-at`; the owner extends it (heartbeat at whatever
period suits the workload — an interactive saga might extend daily); a
sweep — an operator-service job (ADR-0019), with an in-transactor fallback
so the data plane does not depend on the service — transitions overdue
sagas to `:expired` and queues their branches for deletion. `:expired` is
distinguished from `:aborted` so a returning owner knows the system, not a
decision, ended the work; the branch retention window gives them a grace
period to salvage before deletion. (Salvage means application-driven
replay: open a fresh saga at a new `t₀`, query the old branch, and
re-assert what is still wanted. It is not an engine operation, and it does
not depend on the future base-refresh work — the application decides what
survives, with both `Db` values in hand.)

**Retention is a per-database policy knob** — how long committed and
expired branches are kept before deletion — with a per-saga override at
open for workloads whose audit or salvage needs differ. The operator
service administers it like any schedule; the in-transactor fallback
applies the per-database default.

**Concurrent sagas** compose without new rules: each has its own branch
and disjoint id grants; merges serialize through the parent's writer;
the first to merge wins, and later merges see its effects in their
conflict scan. Footprint overlap between open sagas can be *warned about*
at open time; it is never an error, because footprints are advisory.

## Compensation

Abort's default is total: one status transaction, the branch discarded,
canonical state untouched. That default is correct and it stays the
default — but it leaves two real needs unserved, one on each side of the
database boundary:

- a failed saga often should leave a **user-facing record** — an order
  marked failed with a reason, a repair flagged rejected-after-review — in
  ordinary application attributes that tier-0 readers see, not buried in
  registry state only saga-aware readers consult;
- a saga whose steps took external actions needs its outward unwinding
  tracked somewhere durable that outlives the branch.

The design serves each with an explicit mechanism, and keeps them
distinct, because they are distinct: one is a transaction the database can
apply atomically, the other is bookkeeping about work only the application
can perform.

### The compensating transaction

A saga may register a **compensation**: tx data applied to the parent
atomically with the abort — one parent transaction carrying the flip to
`:aborted` (or `:expired`), the compensation's datoms, and the saga id on
the transaction entity. Observers get both in one tx-report: no window
exists in which the registry says the saga failed but the failure record
is missing, and `[?tx :db.saga/id ?saga]` finds the record forever.

Two registration forms, at most one per saga, both durable registry data
settable at open or while `:open`:

- `:db.saga/on-abort-tx` — static EDN tx data, for the common case of
  asserting a few known failure facts;
- `:db.saga/on-abort-fn` — a ref to an ordinary `:db/fn` entity, invoked
  at abort as `(parent-db, branch-db, saga-id) → tx-data` in the same
  sandboxed runtime, fuel budgets, and determinism contract as any
  transaction function (ADR-0008), with one extension: it receives the
  branch's `Db` value alongside the parent's, so it can summarize what
  the saga was doing — steps completed, entities touched, external-action
  metadata — into the record it writes.

Registration is durable for the same reason step-declared guards are: the
contract must not live only in one process's memory. A crashed owner's
saga reaches its deadline and the expiry sweep applies the registered
compensation with no owner present. An explicit abort may also supply
compensation tx data at call time — the aborter has observed current
state and decides — and call-time data replaces the registered form for
that abort.

The semantics are the merge's principles pointed at failure:

- **A fresh transaction, not a replay and not a leak.** The compensation
  is evaluated against the parent's *current* state and validated exactly
  like any transaction — expansion, uniqueness, schema, the lot. Branch
  facts are *inputs* to it (readable via `branch-db`), never outputs:
  branch novelty is not filtered into it, and ids from the saga's granted
  blocks are refused in its tx data — the explicit-id guard still holds
  at the moment of abort, and the abandoned block stays what abandoned
  blocks always are, a hole. New entities the compensation creates take
  ordinary parent ids.
- **Fail loudly where an owner can hear; never block liveness where none
  can.** A compensation that fails validation at explicit abort fails the
  abort — the saga stays `:open`, and the owner fixes the tx data or
  aborts without it. At expiry the sweep must not be held hostage by a
  buggy compensation: it expires the saga *without* the compensation
  datoms and records the failure as `:db.saga/on-abort-error`; the branch
  retention window is the owner's grace period to write the record by
  hand.
- **Requirement 2 bends but does not break.** "As if the saga had never
  run" holds for the saga's novelty — nothing half-lands, ever. The
  compensation is not surviving novelty: it is a new, deliberately
  authored transaction that is part of the abort decision, and labeled as
  such. An aborted saga with a compensation leaves exactly what its owner
  chose to record about the failure, and nothing else.

One shape was considered and rejected: a compensation that *selects which
branch facts to keep* — a filtered merge. It drags the entire merge
apparatus (conflict scan, guards, grant retention, resolution semantics)
into the abort path, for a novelty subset chosen under weaker observation
guarantees than commit demands, and the motivating need does not require
it: a failure record is a handful of fresh facts, and re-asserting a
salvageable value into the compensation covers small keeps (with parent
ids — identity does not survive, which is honest, because the entity's
branch history does not merge either). Genuine salvage of large partial
work already has its path — query the retained branch, re-assert under a
fresh saga — and a subset-merge, if it ever earns its keep, is future
work beside base refresh.

### The external-compensation ledger

The branch commits or aborts database effects atomically. It cannot
un-send an email. Workflows whose steps have external side effects still
need compensation logic — the classic saga — and the design gives that
layer a durable home rather than a competing mechanism:

- The registry entry is the orchestrator's durable state for *forward*
  progress: status, progress, deadline — crash-safe and queryable, so a
  restarted orchestrator resumes from data.
- Step transactions on the branch carry ordinary tx metadata; a workflow
  records each external action and its compensation handle
  (application-defined attributes) alongside the data it justified.
- On abort or expiry, the branch (retained through its retention window)
  is the complete, queryable record of which external actions were taken
  and need compensating — the orchestrator reads it and drives the
  compensations *outward*; the database side needs none, because the
  database side never happened.

*Reverse* progress gets the same treatment as forward progress. Driving
compensations outward is itself long-running, crash-prone work, and
without a home for its state every orchestrator invents the same
bookkeeping schema. The registry therefore carries an
**external-compensation ledger** — `:db.saga/compensations`, component
entities with:

| Attribute | Type | Card | Notes |
|---|---|---|---|
| `:db.saga.compensation/key` | string | one | application-chosen identifier; opaque to the engine |
| `:db.saga.compensation/status` | keyword | one | `:db.saga.compensation.status/pending` → `…/done` \| `…/failed` \| `…/skipped` |
| `:db.saga.compensation/detail` | string (EDN) | one | the handle/payload the compensating task needs — what to undo, and how |
| `:db.saga.compensation/completed-at` | instant | one | when it resolved |
| `:db.saga.compensation/error` | string (EDN) | one | why it failed, if it did |

The engine never executes these — the honest split stands. They are a
standard ledger the orchestrator writes with ordinary parent transactions
as it works. What the vocabulary buys:

- **Resumability from data, again.** A compensating task that crashes
  resumes from the ledger, not from its own files, and a different
  process can take over by reading the parent.
- **It outlives the branch.** The branch is the record of what was
  *done*, and it has a retention window; the ledger of what must be
  *undone* lives in the parent and does not die with it.
- **Atomic seeding.** The compensating transaction above is the natural
  writer of the initial ledger: an `on-abort-fn` reads the branch's
  external-action metadata and asserts the pending entries — atomically
  with the `:aborted` flip. From the first instant any reader sees the
  saga failed, the complete undo list is durably in the parent, whether
  or not the orchestrator ever wakes again.
- **Observability.** `corium saga status` (and the SQL relation) can show
  "aborted, two of five compensations pending"; tier-1 readers can
  distinguish an aborted-and-unwound saga from an aborted one whose
  external effects still stand — a real difference to downstream systems.

No new saga status arises. `:aborted` and `:expired` stay terminal for
the database side; unwinding progress belongs to the ledger, and "fully
compensated" is a derived fact tooling computes. Retention may consult
it — a branch whose saga's ledger is fully resolved can be reclaimed
early — but a pending ledger never extends retention past its window
(liveness again), because the ledger, unlike the branch, persists.

This split is the honest division: atomicity where the system can actually
promise it (its own state), durable bookkeeping where it cannot (everyone
else's).

## Surfaces

*As implemented:* the rules are read from the parent's registry per commit
batch rather than cached when the branch opens, which is what lets an unsealed
saga widen its reservation set with an ordinary parent transaction while its
branch is running. Two further step-time refusals fall out of the same place:
a step may not write the `:db.saga/*` registry (it lives in the parent, and
smuggling an entry through the merge would be a nested saga by accident) and
may not mint an entity in `:db.part/db` (schema belongs to the parent's own
plan/apply lifecycle; `alter-schema` against a branch is refused outright).

- **Peer API.** `Connection::saga_open(db, opts) → Saga` (opts:
  description, footprint, reservations and `sealed`, expiry, id-grant
  sizing, on-abort compensation); `Saga::reserve(...)` to extend an
  unsealed reservation set; `Saga::transact(...)` / `Saga::db()`
  (ordinary `Db` value of the branch); `Saga::commit(guards, resolutions)
  → Result<MergeReport, ConflictReport>`; `Saga::abort()` /
  `Saga::abort_with(tx_data)` (call-time compensation replacing the
  registered form); `Saga::extend(expiry)`;
  `Connection::saga_resume(db, id)`; `Connection::saga_view(db, id) → Db`
  for tier-2 readers. Registry queries are just queries, and the
  external-compensation ledger is written with ordinary transactions —
  no dedicated API. *As implemented:* `Connection::saga_branch(id) →
  SagaBranch` is both of the last two — it opens a second connection, with
  this one's endpoints, credentials, and keys, to the branch database. Steps
  go through `SagaBranch::step`, the branch value through `SagaBranch::db`
  (or `sync`), the step grain through `SagaBranch::steps`, and everything
  else through the `Connection` underneath, because a branch is a database.
- **Protocol.** gRPC additions mirroring the peer API; the branch is
  served over the existing database-view machinery (a `DbViewSpec`-shaped
  reference naming the saga), so thin clients get tier-2 reads for free.
- **CLI.** `corium saga open|list|status|extend|commit|abort <db> ...`
  (`open --on-abort <fn-ident|edn>`, `abort --compensate <edn>`;
  `status` includes the compensation ledger); `corium console <db>
  --saga <id>` to point a console at a branch; `corium saga log <id>`
  for step history. *As implemented:* all of these except `commit`, plus
  `corium saga step <db> <id> <edn|->`, which transacts one step.
- **SQL.** A `corium_sys.sagas` system relation over the registry (id, status,
  basis, owner, expiry, description) and a `corium_sys.saga_compensations`
  relation over the ledger, beside the existing system
  relations; branch reads via the console/session db-view selection.
  Mapping pgwire interactive `BEGIN`/`COMMIT` onto sagas is explicitly out
  of scope for v1 (ADR-0015's guarded autocommit stands); it is an
  obvious later customer.
- **AuthZ.** Opening a saga requires transact rights on the database;
  branch reads are authorized like parent reads (same policy database,
  same contextual authorization — ADR-0021 applies to branch views
  unchanged); the merge is authorized as a transact by the saga owner at
  commit time against then-current policy. An expiry-time compensation is
  authorized as a transact by the recorded saga owner against then-current
  policy — policy drift fails it like any validation failure (recorded in
  `:db.saga/on-abort-error`, never blocking expiry). The registry's own
  attributes are ordinary data under the same model.

## Limits and non-goals (v1)

- **No read-set tracking, so no silent serializability.** Write-write
  conflicts are detected; read-write anomalies are the saga's to declare
  via guards. This is the same honesty ADR-0015 chose for guarded DML —
  optimistic, explicit, validated at the writer.
- **No engine-driven external compensation.** The ledger is data; the
  orchestrator is the actor. Likewise no filtered merge on abort — a
  compensation is a fresh transaction, never a novelty subset (see
  [Compensation](#the-compensating-transaction)).
- **No nested sagas.** A branch of a branch has no parent-merge story yet.
- **No cross-database sagas.** One saga, one database; multi-database
  atomicity is a different (two-phase) problem.
- **No schema changes on a branch.** Schema migration has its own
  plan/apply lifecycle (ADR-0020); mixing the two inside a merge is
  needless coupling.
- **No base refresh / rebase modes.** Future work, each with its own
  semantics to specify: base refresh re-roots the branch on a newer
  parent basis; rebase-commit re-runs original tx *inputs* at merge.
  The sharpest consequence of this gap: a parent schema migration that
  invalidates branch novelty (merge validates against *current* schema)
  strands the saga through no fault of its own — the merge fails loudly,
  and with no base refresh the recourse is salvage-and-redo. Migration
  planning (ADR-0020) should treat open sagas as an advisory input, the
  way it already reports affected data.
- **Branch writes go through the parent's transactor.** A saga is not a
  write-throughput feature; see
  [write-path-scaling.md](write-path-scaling.md) for that problem.

## Delivery sketch

1. **Registry + vocabulary** *(done)* — bootstrap attributes, open/abort/expiry
   as transactions, `corium_sys.sagas` relation, no branches yet (a saga with no
   steps is already useful as a durable workflow record). Compensation
   vocabulary included: static `:db.saga/on-abort-tx` and the
   external-compensation ledger need no branch at all. Two details the
   implementation settled: the entity-id grants of phase 2 are refused as
   ordinary transaction data, since minting them is the allocator's job, and
   `:db.saga/owner` is still declared by the client — the transactor stamps
   the authenticated principal when saga authorization lands.
2. **Branches** *(done, less one piece)* — overlay construction, id grants,
   step transacting, peer and CLI read surfaces (tier 2). One detail the
   implementation settled: a branch's naming is the parent's, copied when the
   branch is first opened and durable from then on, because a step may mint
   keyword names the parent has never seen and the branch's own log records
   cannot be decoded without them. Two consequences are known and left for
   the merge phase to close, because merge is where schema reconciliation is
   actually decided:

   * *The snapshot is taken at first open, not at `t₀`.* Naming is not
     versioned by `t`, so the parent's naming as of `t₀` is not
     reconstructible without replaying its log from zero. A branch first
     opened long after `t₀` therefore sees attributes installed since — which
     is harmless for reading (its base has no datoms under them) but means
     the same saga can get different schemas depending on when someone first
     touched its branch. It is deterministic per branch, not per saga.
   * *A parent migration does not reach an open branch.* Steps naming
     post-snapshot attributes fail to resolve, and steps already taken were
     validated against the older schema. The merge is the checkpoint that
     matters — it validates the branch's novelty against the parent's
     *current* schema — so migrations are not refused while sagas are open:
     a schema change that takes days to roll out should not be blocked by a
     saga that takes days to finish. What a long-lived saga owes is a
     re-validation at merge, which is exactly what it gets.

   `:db.saga/on-abort-fn` invocation, which needs the branch value, is
   deferred to the phase that owns abort and expiry: applying a compensation
   atomically with the flip means the transactor, not the client, composing
   that transaction.
3. **Merge** — squash, conflict scan, guards, resolutions, the atomic
   commit-and-flip; conflict reports.
4. **Expiry sweep + retention** — operator-service job with in-transactor
   fallback; branch GC.
5. **Protocol/thin-client surfaces and SQL polish.**

Each phase leaves the system consistent and shippable; nothing in the data
plane depends on a later phase.
