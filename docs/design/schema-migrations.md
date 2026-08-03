# Schema Migration Planning and Execution

## Scope

Corium needs a declarative schema update path that compares a desired schema
file with the schema installed in a database, explains the operational impact,
and applies only changes whose preconditions still hold. The first user-facing
surface is:

```sh
# Inspect only. This is the default.
corium schema update people --schema schema.toml

# Apply the exact plan after reviewing it.
corium schema update people --schema schema.toml \
  --apply --plan <plan-digest>
```

`schema update` is deliberately plan-first. Reading a schema file is not
permission to remove data, collapse cardinality, or reinterpret values. A plan
is deterministic at an observed database basis and carries a digest. A schema
change or failed safety precondition invalidates it. Ordinary data writes do not
automatically make an additive plan stale.

This design covers attribute schema. TOML `[[entity]]` blocks are authoring
groups that supply keyword namespaces. Corium does not persist entity types.
Adding an empty group therefore changes no database state, while adding a group
with attributes is a set of attribute additions.

The decision is recorded in
[ADR-0020](../adr/0020-planned-schema-migrations.md).

## The implementation gap

The intended data model says schema is data and is installed through ordinary
transactions. The current implementation has only the creation half of that
model:

- `corium_forms::schemaform::schema_from_edn` assigns attribute ids once when
  `CreateDatabase` runs.
- The schema and ident registry are stored in the database metadata root and
  sent to peers in the subscription handshake.
- `Db::with_transaction_at` applies datoms against a fixed `Schema`. It does not
  derive a new schema from schema datoms.
- When the parent already has cached indexes, `Db::apply_transaction` folds new
  datoms into those indexes with the parent's schema. Granting AVET coverage
  without invalidating that fold therefore produces a silently partial
  index containing only post-change datoms.
- A reconnect may replace the handshake schema, but a live tx report cannot
  announce a schema generation or make a peer rebuild affected indexes.

A CLI-only diff would consequently be unsafe: it could describe an update but
there is no atomic, durable, peer-visible operation that performs one. Schema
transactions and basis-aware schema caches are therefore the first phase of
the work, not an implementation detail of the command.

## Desired-schema semantics

Both TOML and EDN inputs normalize to an ordered map keyed by canonical ident.
The normalized form contains value type, cardinality, uniqueness, index,
component, no-history, documentation, and (when attribute protection is
enabled) protection-class properties. Installed state also carries facts the
desired file cannot erase, such as whether an attribute has ever been
protected. Attribute entity ids are taken from the installed schema and never
inferred from file order after initial creation.

Matching is exact by ident. The planner must not guess that one removed ident
and one added ident are a rename, even when their definitions match. An
incorrect rename aliases two meanings permanently. A future explicit migration
directive can rename an ident while preserving its attribute entity id. Until
then, the safe recipe is add, copy, cut over, and retire.

By default, a desired file manages the declarations it contains. Installed
user attributes absent from the file are reported as `unmanaged`. The command
does not change them. `--prune` changes absent attributes into retirement
requests. Engine attributes are never managed by a user file. This makes it
safe to update a database from a partial schema while still supporting complete
manifests.

`schema-version` remains the TOML file-format version. It is not a database
migration number. A later optional `schema-revision` may provide an
application-owned monotone label, but correctness depends on the installed
schema and plan digest, not that label.

## Diff and impact model

The planner produces property-level changes rather than an attribute-level
`changed` flag. A single attribute can, for example, need both uniqueness
validation and AVET backfill. Every change reports:

- installed and desired values.
- the basis and schema generation inspected.
- affected current datoms, entities, and distinct values.
- known constraint violations and representative entity ids.
- index work and estimated scan size.
- history limitations, including when an exact historical count is unavailable
  from current-state segments.
- execution class, risk, preconditions, and required acknowledgement.
- dependencies on other plan steps.

Impact analysis is server-side or peer-local against a fixed `Db`. The initial
implementation can scan AEVT for one affected attribute. Later index and
statistics support can make the same questions cheaper without changing plan
semantics. Counts are exact unless explicitly labeled estimates. Samples are
bounded. The counts are not bounded.

The plan uses four execution classes:

| Class | Meaning | Typical examples |
|---|---|---|
| `additive` | No existing fact must be inspected or rewritten | add an attribute, or change cardinality from one to many |
| `validate-reindex` | Existing facts remain valid, but a bounded scan, constraint validation, or covering-index rebuild is required | add `index`, add `unique` with no duplicates, or change uniqueness mode |
| `rewrite` | Current facts must change before the desired schema can become active | resolve many-to-one conflicts, copy values to a replacement typed attribute, or retract current values during retirement |
| `destructive` | Information or historical interpretation would be lost | hard deletion, excision, or pretending old values always had a new type |

Risk is reported separately from execution class. Changing `isComponent` can
be metadata-only yet high impact when many live refs will acquire cascade
semantics. Conversely, an AVET backfill may be operationally expensive but
semantically low risk.

### Change matrix

| Change | Inspection | Planned behavior |
|---|---|---|
| Add attribute | ident collision | Allocate a stable db-partition id and install it atomically. No entity-group record is created. |
| Remove attribute from file | none by default. Live/history counts with `--prune` | Report as unmanaged. With `--prune`, retire it as a high-risk `validate-reindex` change. Never hard-delete it. Optional current-fact cleanup is a separate `rewrite` step. |
| `cardinality one -> many` | current count for reporting | Online additive change. Existing values already satisfy it. |
| `cardinality many -> one` | entities having more than one current value | If none conflict, validate and apply. Otherwise require a rewrite with an explicit value-selection policy. Never choose a winner implicitly. |
| Add `index` | current datom count, index capacity estimate, ever-protected flag | Reject an attribute that has ever been protected. Otherwise activate only after AVET backfill is complete, or keep the planner on AEVT until completion. |
| Remove `index` | current AVET coverage | Stop planning new reads through AVET. A rebuild reclaims stale coverage. The planner never treats stale coverage as authoritative. |
| Add `unique` | duplicate value groups, their entities, and ever-protected flag | Reject an attribute that has ever been protected or has duplicate values. Otherwise install a pending constraint, backfill AVET, and activate after tail validation. |
| Remove `unique` | whether `index` remains requested | Change future write semantics. Rebuild AVET only when coverage is no longer requested. |
| `unique identity <-> value` | no data rewrite. Report all users | Change upsert/conflict behavior only after explicit acknowledgement. |
| Toggle `isComponent` | count live refs and component fan-out/cycles | Change future pull and retract-entity semantics. Existing facts are not rewritten. The high-impact semantic change requires acknowledgement. |
| Toggle `noHistory` | presence of existing history and omitted-history interval | Forward-only. Enabling does not erase old history. Disabling cannot reconstruct history already omitted. The plan states the incomplete interval. |
| Change `doc` | none | Add, replace, or retract documentation as a metadata-only change. |
| Protect, unprotect, or re-classify | current index/unique/ref state, live plaintext/sealed counts, protection timeline, lookup-ref use | Forward-only high-risk `validate-reindex` change requiring `protection-forward-only`. Protection requires removing `index`/`unique` in the same schema transaction and is forbidden for refs. The plan reports lookup-ref breakage and offers the current-value sweep as separate `rewrite` work. |
| Change value type | current value counts and types | Direct mutation is `destructive` and never executable. The planner emits a separate replacement-attribute recipe whose copy and cleanup steps are `rewrite` work under an explicit conversion. |
| Rename ident | exact explicit source/target directive | Preserve the attribute id and history. Never infer from similarity. Deferred until the directive is specified. |
| Hard delete/excise | live and historical datom count, backup reachability | Unsupported by schema update. Excision is a separate destructive facility with its own design and approval. |

`index` and `unique` activation use a two-stage state: requested, then ready at
a basis. The query planner and uniqueness validator must not assume coverage
before the backfill reaches that basis. A small database may finish both stages
inside one command. This distinction prevents that optimization from becoming
a correctness assumption.

Uniqueness has an additional pending state that closes the scan/activation
race. The transactor validates current values and installs the pending
constraint in one turn of the writer queue at `scan_basis`. New writes then
enforce uniqueness through a correct AEVT fallback while AVET is rebuilt. The
activation turn re-validates `(scan_basis, activation_basis]` inside the writer
queue and changes the constraint to ready only when the rebuilt AVET covers
`activation_basis`. An implementation without pending-constraint enforcement
must block writes for that final validation pass. Validation of only the schema
fingerprint is not sufficient.

Backfill uses the existing index publisher and bypasses normal index-policy
pacing, as `corium db request-index` does. Apply reports the forced
publication's progress and does not mark readiness until the published root
carries both the target schema generation and readiness basis. A failure leaves
the attribute pending and unready. Queries use the correct AEVT fallback. The
ordinary interval, tail threshold, and deadline never decide correctness.

Protection changes reuse the forward-only model in
[encryption.md](encryption.md#changing-protection). `:db/protection` cannot
coexist with `:db/index`, `:db/unique`, or `:db.type/ref`. An attribute that has
ever held sealed datoms can never later gain index or uniqueness coverage.
Protecting an indexed or unique attribute retracts those properties in the same
schema transaction. The plan prominently reports that lookup refs through it
stop working. The schema cache retains the `(t, class)` protection timeline
needed to validate historical retractions and CAS operands.

## Retirement and type changes

An immutable database cannot make an attribute disappear from history. Schema
removal therefore means retirement:

- the ident and attribute metadata remain readable at every later basis.
- new assertions are rejected.
- retractions of existing values remain legal.
- queries and historical views continue to decode old facts.
- optional cleanup retracts current facts as a resumable rewrite job.
- physical erasure is not implied.

Changing an attribute's value type in place has the same historical problem and
an additional transactional one: old retractions and CAS operands retain their
old type. Direct type mutation is therefore `destructive`, not `rewrite`, and
`--allow rewrite` can never enable it. The planner rejects that change and emits
a shadow-attribute recipe instead:

1. Add a new attribute with the desired type.
2. Convert and assert current values under an explicit conversion function.
3. Verify counts, rejected values, and application dual-read/dual-write state.
4. Cut application reads and writes to the new ident.
5. Retire the old attribute, optionally retracting its current facts.

The migration may be automated later, but its conversion policy cannot be
inferred from the two type names. `long -> string`, for example, still needs a
declared formatting contract, while `double -> long` needs rounding and range
rules.

## Schema as basis-versioned data

Corium will install the schema vocabulary itself in the reserved db partition:
`:db/ident`, `:db/valueType`, `:db/cardinality`, `:db/unique`, `:db/index`,
`:db/isComponent`, `:db/noHistory`, `:db/doc`, `:db/protection` when enabled,
and a retirement marker. Attributes supplied at database creation form an
immutable **pre-basis schema seed**. The seed has no transaction id, is not a
datom view, and is never returned by `datoms`, `since`, `history`, or
`tx-range`. Basis 0 therefore keeps its existing meaning: `as-of 0` contains no
facts and `since 0` contains every transacted fact. Later attribute metadata
changes travel as ordinary schema datoms in the transaction log.

`Db::with_transaction_at` derives the next immutable `Schema` and `Idents`
before it indexes user datoms from the same transaction. Schema validation
rejects illegal transitions, protects engine attributes, and makes the result
independent of datom input order. A transaction that both installs an attribute
and uses it is legal only because schema effects are derived first.

The authority for schema at basis `t` is the immutable pre-basis seed plus
schema datoms through `t`. The metadata root stores that seed and can also carry
a recoverable current-schema snapshot/cache with the generation and basis it
represents. Replay of the log tail advances the cache exactly as replay advances
data. Existing databases already have the information needed for the seed in
their creation metadata. The compatibility upgrade labels and preserves it
rather than inventing a transaction. The operation is deterministic and
idempotent and preserves attribute ids, basis 0, and every application
transaction number.

Peers receive schema changes in tx reports. Applying such a report replaces the
schema cache, invalidates affected in-memory index folds and planner statistics,
and publishes the new schema generation with the resulting `Db`. The next
protocol version makes the handshake schema snapshot explicitly effective at
`schema_basis_t = SubscribeRequest.from_basis_t`, with its generation and the
server's target generation carried separately. A cold peer subscribing from 0
receives the pre-basis seed in that snapshot. It is not backfilled as a `t = 0`
report. Reports with `t > from_basis_t` then advance data and schema together,
so a reconnect never applies an older data transaction against the server's
newest schema.

`Db` retains enough schema history to derive the effective schema and ident map
for `as-of` views. This history includes the protection timeline from
[encryption.md](encryption.md#changing-protection). Retirement therefore does
not hide an attribute from an older basis. A future explicit rename can resolve
the ident that was active at that basis. Current and historical index readiness
are tracked independently when their coverage differs.

Published index roots carry the schema generation used to build them. If a
root's generation is behind the current schema, uncovered orders remain usable
where their semantics did not change, while affected orders rebuild or fall
back. A root must never claim AVET completeness for a newly indexed or unique
attribute until backfill has completed.

The schema generation is a monotone database-local counter, separate from
transaction basis. It advances once for a committed transaction containing one
or more schema changes. The basis says when the change happened. The generation
shows whether two otherwise different database values use the same schema.

## Implementation boundaries

- `corium-forms` owns format-specific parsing and a normalized desired
  attribute model that does not allocate entity ids. Database creation keeps
  its positional allocation so embedded, WASM, authz bootstrap, and existing
  fixtures remain reproducible. Dynamic updates allocate above the durable
  maximum installed db-partition id.
- `corium-core` owns installed attribute state, retirement/readiness metadata,
  the optional `:db/doc` value (not represented by `Attribute` today),
  protection timelines, schema generations, and property-level change/plan
  types shared across clients and the transactor.
- `corium-db` derives schema timelines from datoms, exposes fixed-basis impact
  scans, and invalidates only the index/statistic folds affected by a schema
  generation.
- `corium-tx` validates schema datoms and transition rules before ordinary
  transaction validation. Retractions remain legal for retired attributes and
  for historically valid value representations.
- `corium-protocol` adds versioned plan/apply messages and carries schema
  generation/readiness in recovery and subscription state. Applying a plan is
  an administrative catalog action, not an unrestricted transaction form. The
  required protocol, public thin-client, and authorization changes are also
  recorded in [protocol.md](protocol.md),
  [the thin-client contract](../thin-client-protocol.md), and
  [auth.md](auth.md).
- `corium-transactor` allocates db-partition ids, verifies plan preconditions in
  the writer queue, appends the schema transaction, and coordinates final
  activation with index jobs that force the existing publisher past normal
  index-policy pacing. Read-only impact scans operate on an immutable `Db`
  snapshot outside the commit lock.
- `corium-peer` applies schema tx reports in order and makes the installed
  schema/impact planner available to local clients.
- `corium-cli` renders human and stable JSON plans, enforces acknowledgement
  flags, and routes long steps to the operator service when configured.

## Plan and apply protocol

Planning is read-only:

1. Parse and normalize the desired file.
2. Obtain an immutable current database value and its basis/schema generation.
3. Match exact idents and compute property changes.
4. Run impact queries and construct a dependency graph.
5. Canonically encode the desired digest, installed-schema fingerprint,
   normalized step set, execution classes, safety preconditions, and `--prune`
   mode. Hash that encoding as the plan digest.
6. Print human output by default and stable JSON with `--json`.

Observations — counts, samples, and work estimates at the observed basis — are
carried beside the logical plan as advisory review and audit data. They are not
hashed into the plan digest. Otherwise one unrelated new datom can change a
count and invalidate the additive plan that the drift rule intentionally keeps
valid.

Apply submits the desired schema, plan digest, observed basis, installed-schema
fingerprint, and explicit acknowledgements to a schema-update endpoint. The
transactor recomputes the canonical logical digest from the submitted desired
schema and current installed-schema fingerprint. It does not treat the digest
as an opaque server-side token. It does not recompute advisory observations.
It independently validates every safety-critical precondition under its
single-writer commit queue. A changed schema fingerprint, a newly introduced
constraint violation, or a change in execution class returns a stale/blocked
plan error and changes nothing. Data-basis drift that preserves all
preconditions is allowed. Thus, a busy database can add an attribute. Additive
metadata changes commit in one schema transaction with
transaction metadata recording the source digest, plan digest, observed basis,
tool version, and requester.

Reindex and rewrite steps are jobs. Each step is idempotent and checkpointed by
attribute and basis. The CLI may execute the initial local implementation, but
the job contract matches the operator service in
[operator-service.md](operator-service.md), so the same plan can later run in a
durable operator process. Constraint activation is a final short,
basis-fenced transaction after the job verifies its result.

Applying the same desired digest twice is a no-op when the installed properties
already match. A partially completed plan is resumed by its step keys, not
replayed blindly. A different desired digest creates a new plan.

## CLI contract

The initial surface is intentionally narrow:

```text
corium schema update <db> --schema <path>
    [--prune] [--json]
    [--detailed-exit-code]
    [--apply --plan <digest>]
    [--allow validate-reindex|rewrite]
    [--ack <change-code>...]
    [connection flags]
```

- Without `--apply`, the command never writes.
- `--apply` requires the digest printed by the plan, preventing an unnoticed
  plan/apply mismatch.
- Additive changes need no `--allow` flag. Higher classes require their exact
  allowance. `destructive` has no allowance because this command cannot run it.
- High semantic risks, such as acquiring component cascade semantics or
  retiring an attribute with live facts, also require the stable change code in
  `--ack`. Allowing an execution class alone does not acknowledge its meaning.
- Stable codes are kebab-case semantic names such as `component-enable`,
  `retire-live-attribute`, `unique-mode-change`, `no-history-enable`, and
  `protection-forward-only`. Both human and JSON plans print the exact code next
  to every change that requires it.
- `--prune` requests retirement of absent installed attributes and is included
  in the digest.
- `--json` is a versioned machine contract and includes stable change codes.
  Scripts must not parse the human rendering.
- A normal read-only plan exits 0 whether or not it finds changes, so shell
  `&&` chains remain useful. `--detailed-exit-code` requests 0 for no change and
  2 for changes planned. Parse, stale-plan, blocked, and apply failures exit 1
  and carry stable JSON error codes when `--json` is set.

`schema` is a deliberate top-level group rather than another `db` verb. Its
planned surface includes `update`, `status`, `history`, and job inspection.
`corium db` remains the group for catalog lifecycle and index-policy operations.

Example human plan:

```text
database: people                    basis: 418  schema-generation: 3
desired:  sha256:7c…                plan:  sha256:91…

ADDITIVE
  + :person/email string cardinality-one
  ~ :person/tags cardinality one -> many       current datoms: 12,204

VALIDATE-REINDEX
  ~ :person/email unique none -> identity      duplicate values: 0
                                                AVET backfill: 0 current datoms
  ~ :person/address component false -> true    live refs: 8,109
                                                [ack: component-enable]

DESTRUCTIVE (blocked)
  ~ :person/age long -> string                  current datoms: 8,109
    direct type mutation is not executable
    rewrite recipe: add :person/age-text, copy explicitly, cut over, retire old

UNMANAGED
    :legacy/import-id                           use --prune to retire
```

## Authorization and audit

Planning requires inspect access to the database. Applying schema metadata
requires a new `AlterSchema` action, separate from ordinary `Transact`, so an
application writer cannot silently broaden its own schema. `AlterSchema` is an
Admin-class, database-scoped action with wire name `alter-schema`. The built-in
permission defaults therefore grant it only to database owners. Rewrite jobs
also require ordinary database-level `Transact` authority. Corium has no
attribute-scoped transact permission today. This design does not add this
permission. Hard deletion/excision, when designed, belongs to the operator
service's irreversible two-person approval path.

Every applied schema transaction records requester identity, desired and plan
digests, CLI/protocol version, execution class, and acknowledgements on the
transaction entity. Plans may contain samples for diagnosis but audit records
store counts and digests rather than application values.

## Failure and concurrency behavior

- A changed installed-schema fingerprint or failed plan precondition aborts
  before any schema change. The caller replans.
- A failed additive transaction changes nothing.
- A failed backfill leaves the requested constraint inactive and may be
  resumed. Queries continue through a correct fallback path.
- Concurrent ordinary writes during a rewrite are either captured by a
  high-water/tail pass or rejected by a final basis fence. The first
  implementation may choose the simpler write-blocked final pass.
- Peers that do not understand a schema generation fail the protocol-version
  check instead of applying data with stale validation/index rules.
- Cancellation stops at a checkpoint. It never activates a partially verified
  constraint.

## Delivery plan

### Phase 1: diff-only planner

**Implemented.** `corium_forms::desired` normalizes both syntaxes,
`corium_db::impact` runs the fixed-basis scans, `corium_forms::planner`
produces the plan, `corium_core::migration` owns the change/plan types and
digests, and `corium-cli` renders it. Operator documentation is in
[operations.md](../operations.md#schema-updates).

- Extract normalized desired attributes from both TOML and EDN without
  allocating ids.
- Add a read-only planner and impact analyzer over `Db`.
- Ship `corium schema update` without `--apply`, stable JSON, fixtures, and
  property tests for change classification.
- Report the current implementation limitation clearly: all applies are
  blocked until schema transactions land.
- Read the installed protection timeline so an ever-protected attribute is
  blocked from gaining index or unique coverage, and report a file's
  `:db/protection` as unplanned rather than dropping it: the class is not part
  of the normalized desired model yet, so a file that names one must not read
  as "no changes".

### Phase 2: transactional additive schema

- Bootstrap the schema vocabulary and add dynamic db-partition id allocation
  for updates while retaining positional allocation at database creation.
- Derive basis-versioned schema/id maps from schema datoms in `corium-db`.
- Persist/stream schema generations through recovery roots, handshakes, and tx
  reports. Label existing creation metadata as the pre-basis seed without
  changing basis 0 or application transaction numbers.
- Add the basis-fenced schema-update RPC, authorization action, audit metadata,
  and `--apply` for new attributes and one-to-many changes.

### Phase 3: validation and indexes

- Add exact duplicate/cardinality impact scans and index readiness state.
- Make AVET backfill/rebuild resumable and force publication independently of
  index-policy pacing. Teach query planning and uniqueness validation to
  respect pending state and readiness basis.
- Enable index and uniqueness changes plus conflict-free many-to-one changes.
- Enforce protection timelines and the permanent ever-protected prohibition on
  later `index`/`unique` coverage.

### Phase 4: retirement and rewrites

- Add attribute retirement while preserving reads and retractions.
- Add checkpointed current-fact rewrite jobs and the explicit conversion/
  conflict-resolution interface.
- Route long jobs through the operator service when configured, keeping the
  in-process CLI fallback.

### Phase 5: explicit rename and excision designs

- Specify stable-id ident rename syntax and compatibility policy.
- Design excision separately, including history roots, backups, replicas,
  approval, and proof of completion. Schema update continues to reject hard
  deletion.

## Testing

- Golden diff tests cover every property transition, partial manifests,
  `--prune`, protection timelines, engine attributes, stable acknowledgement
  codes, deterministic ordering, and plan digests.
- Digest tests prove that advisory count or sample drift leaves the logical
  digest unchanged. They also prove that logical plan changes alter the digest.
- Property tests generate installed/desired schema pairs and assert that every
  difference is classified exactly once and that unsupported changes never
  reach apply.
- Model tests compare impact counts with brute-force EAVT/AEVT scans.
- Transaction tests assert a schema install and first use in one transaction,
  stale-schema and constraint-drift rejection, harmless data-basis drift for an
  additive plan, pending-unique enforcement during AVET backfill, tail
  revalidation, idempotent re-apply, and atomic failure.
- Peer tests keep a connected peer live across each schema change and compare
  it with a freshly connected peer at the same basis.
- Crash simulation injects failure before and after schema-log append,
  metadata snapshot publication, index backfill checkpoints, and constraint
  activation.
- Compatibility tests open a current-format database and label its metadata as
  the pre-basis seed. They preserve basis 0, every application transaction
  number, and all attribute ids. They reopen it with identical queries and
  transaction validation. Subscription tests from basis 0 assert that the seed
  arrives only in the handshake and is never emitted as a tx report.
