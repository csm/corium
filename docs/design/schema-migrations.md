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
change or failed safety precondition invalidates it; ordinary data writes do not
automatically make an additive plan stale.

This design covers attribute schema. TOML `[[entity]]` blocks are authoring
groups that supply keyword namespaces; Corium does not persist entity types.
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
- `Db::with_transaction_at` applies datoms against a fixed `Schema`; it does
  not derive a new schema from schema datoms.
- A reconnect may replace the handshake schema, but a live tx report cannot
  announce a schema generation or make a peer rebuild affected indexes.

A CLI-only diff would consequently be unsafe: it could describe an update but
there is no atomic, durable, peer-visible operation that performs one. Schema
transactions and basis-aware schema caches are therefore the first phase of
the work, not an implementation detail of the command.

## Desired-schema semantics

Both TOML and EDN inputs normalize to an ordered map keyed by canonical ident.
The normalized form contains value type, cardinality, uniqueness, index,
component, no-history, and documentation properties. Attribute entity ids are
taken from the installed schema and never inferred from file order after
initial creation.

Matching is exact by ident. The planner must not guess that one removed ident
and one added ident are a rename, even when their definitions match. An
incorrect rename aliases two meanings permanently. A future explicit migration
directive can rename an ident while preserving its attribute entity id; until
then, the safe recipe is add, copy, cut over, and retire.

By default, a desired file manages the declarations it contains. Installed
user attributes absent from the file are reported as `unmanaged`; they are not
changed. `--prune` changes absent attributes into retirement requests. Engine
attributes are never managed by a user file. This makes it safe to update a
database from a partial schema while still supporting complete manifests.

`schema-version` remains the TOML file-format version. It is not a database
migration number. A later optional `schema-revision` may provide an
application-owned monotone label, but correctness depends on the installed
schema and plan digest, not that label.

## Diff and impact model

The planner produces property-level changes rather than an attribute-level
`changed` flag. A single attribute can, for example, need both uniqueness
validation and AVET backfill. Every change reports:

- installed and desired values;
- the basis and schema generation inspected;
- affected current datoms, entities, and distinct values;
- known constraint violations and representative entity ids;
- index work and estimated scan size;
- history limitations, including when an exact historical count is unavailable
  from current-state segments;
- execution class, risk, preconditions, and required acknowledgement;
- dependencies on other plan steps.

Impact analysis is server-side or peer-local against a fixed `Db`. The initial
implementation may scan AEVT for one affected attribute. Later index and
statistics support can make the same questions cheaper without changing plan
semantics. Counts are exact unless explicitly labeled estimates. Samples are
bounded; the counts are not.

The plan uses four execution classes:

| Class | Meaning | Typical examples |
|---|---|---|
| `additive` | No existing fact must be inspected or rewritten | add an attribute; cardinality one to many |
| `validate-reindex` | Existing facts remain valid, but a bounded scan, constraint check, or covering-index rebuild is required | add `index`; add `unique` with no duplicates; change uniqueness mode |
| `rewrite` | Current facts must change before the desired schema can become active | many to one with conflicts; copy values to a replacement typed attribute; retract all values during retirement |
| `destructive` | Information or historical interpretation would be lost | hard deletion, excision, or pretending old values always had a new type |

Risk is reported separately from execution class. Changing `isComponent` can
be metadata-only yet high impact when many live refs will acquire cascade
semantics. Conversely, an AVET backfill may be operationally expensive but
semantically low risk.

### Change matrix

| Change | Inspection | Planned behavior |
|---|---|---|
| Add attribute | ident collision | Allocate a stable db-partition id and install it atomically. No entity-group record is created. |
| Remove attribute from file | none by default; live/history counts with `--prune` | Report as unmanaged. With `--prune`, retire it as a high-risk `validate-reindex` change; never hard-delete it. Optional current-fact cleanup is a separate `rewrite` step. |
| `cardinality one -> many` | current count for reporting | Online additive change. Existing values already satisfy it. |
| `cardinality many -> one` | entities having more than one current value | If none conflict, validate and apply. Otherwise require a rewrite with an explicit value-selection policy; never choose a winner implicitly. |
| Add `index` | current datom count and index capacity estimate | Activate only after AVET backfill is complete, or keep the planner on AEVT until completion. |
| Remove `index` | current AVET coverage | Stop planning new reads through AVET. Stale coverage is reclaimed by a rebuild; it is never treated as authoritative. |
| Add `unique` | duplicate value groups, including their entities | Reject apply when duplicates exist. With no duplicates, validate under a basis fence, backfill AVET, then activate the constraint. |
| Remove `unique` | whether `index` remains requested | Change future write semantics. Rebuild AVET only when coverage is no longer requested. |
| `unique identity <-> value` | no data rewrite; report all users | Change upsert/conflict behavior only after explicit acknowledgement. |
| Toggle `isComponent` | count live refs and component fan-out/cycles | Change future pull and retract-entity semantics. Existing facts are not rewritten; the high-impact semantic change requires acknowledgement. |
| Toggle `noHistory` | presence of existing history and omitted-history interval | Forward-only. Enabling does not erase old history; disabling cannot reconstruct history already omitted. The plan states the incomplete interval. |
| Change `doc` | none | Add, replace, or retract documentation as a metadata-only change. |
| Change value type | current values convertible under an explicit conversion | Never mutate in place in the first implementation. Plan a replacement attribute, conversion, application cutover, and retirement of the old attribute. |
| Rename ident | exact explicit source/target directive | Preserve the attribute id and history. Never infer from similarity. Deferred until the directive is specified. |
| Hard delete/excise | live and historical datom count, backup reachability | Unsupported by schema update. Excision is a separate destructive facility with its own design and approval. |

`index` and `unique` activation use a two-stage state: requested, then ready at
a basis. The query planner and uniqueness validator must not assume coverage
before the backfill reaches that basis. A small database may finish both stages
inside one command; preserving the distinction prevents that optimization from
becoming a correctness assumption.

## Retirement and type changes

An immutable database cannot make an attribute disappear from history. Schema
removal therefore means retirement:

- the ident and attribute metadata remain readable at every later basis;
- new assertions are rejected;
- retractions of existing values remain legal;
- queries and historical views continue to decode old facts;
- optional cleanup retracts current facts as a resumable rewrite job;
- physical erasure is not implied.

Changing an attribute's value type in place has the same historical problem and
an additional transactional one: old retractions and CAS operands retain their
old type. The first implementation rejects direct type mutation and emits a
shadow-attribute recipe:

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
`:db/isComponent`, `:db/noHistory`, `:db/doc`, and a retirement marker.
Attribute metadata then travels as ordinary datoms in the transaction log.
The attributes supplied at database creation form a genesis schema record at
`t = 0`; it does not consume an application transaction number or advance the
database basis.

`Db::with_transaction_at` derives the next immutable `Schema` and `Idents`
before it indexes user datoms from the same transaction. Schema validation
rejects illegal transitions, protects engine attributes, and makes the result
independent of datom input order. A transaction that both installs an attribute
and uses it is legal only because schema effects are derived first.

The creation-time metadata root remains a recoverable snapshot/cache, not the
authority. It records the schema generation and basis it represents. Replay of
the log tail advances it exactly as replay advances data. Databases written by
the current format are upgraded by synthesizing the `t = 0` genesis schema
record from their existing metadata; the operation is deterministic and
idempotent, preserves attribute ids and the current basis, and must be completed
before the first dynamic update.

Peers receive schema changes in tx reports. Applying such a report replaces the
schema cache, invalidates affected in-memory index folds and planner statistics,
and publishes the new schema generation with the resulting `Db`. A reconnecting
peer still receives a complete schema snapshot in the handshake, followed by
the ordinary log tail. Thus live and reconnect paths converge on the same
basis-versioned state.

`Db` retains enough schema history to derive the effective schema and ident map
for `as-of` views. Retirement therefore does not hide an attribute from an
older basis, and a future explicit rename can resolve the ident that was active
at that basis. Current and historical index readiness are tracked independently
when their coverage differs.

Published index roots carry the schema generation used to build them. If a
root's generation is behind the current schema, uncovered orders remain usable
where their semantics did not change, while affected orders rebuild or fall
back. A root must never claim AVET completeness for a newly indexed or unique
attribute until backfill has completed.

The schema generation is a monotone database-local counter, separate from
transaction basis. It advances once for a committed transaction containing one
or more schema changes. The basis says when the change happened; the generation
cheaply detects whether two otherwise different database values use the same
schema.

## Implementation boundaries

- `corium-forms` owns format-specific parsing and a normalized desired
  attribute model that does not allocate entity ids.
- `corium-core` owns installed attribute state, retirement/readiness metadata,
  schema generations, and property-level change/plan types shared across
  clients and the transactor.
- `corium-db` derives schema timelines from datoms, exposes fixed-basis impact
  scans, and invalidates only the index/statistic folds affected by a schema
  generation.
- `corium-tx` validates schema datoms and transition rules before ordinary
  transaction validation. Retractions remain legal for retired attributes and
  for historically valid value representations.
- `corium-protocol` adds versioned plan/apply messages and carries schema
  generation/readiness in recovery and subscription state. Applying a plan is
  an administrative catalog action, not an unrestricted transaction form.
- `corium-transactor` allocates db-partition ids, verifies plan preconditions in
  the writer queue, appends the schema transaction, and coordinates final
  activation with index jobs. Read-only impact scans operate on an immutable
  `Db` snapshot outside the commit lock.
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
5. Canonically encode the desired digest, observations, steps, and
   preconditions; hash that encoding as the plan digest.
6. Print human output by default and stable JSON with `--json`.

Apply submits the desired schema, plan digest, observed basis, installed-schema
fingerprint, and explicit acknowledgements to a schema-update endpoint. The
transactor recomputes or verifies every safety-critical precondition under its
single-writer commit queue. A changed schema fingerprint, a newly introduced
constraint violation, or a change in execution class returns a stale/blocked
plan error and changes nothing. Ordinary data-basis drift is allowed when it
does not invalidate a precondition: otherwise a busy database could never add
an attribute. Additive metadata changes commit in one schema transaction with
transaction metadata recording the source digest, plan digest, tool version,
and requester.

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
    [--apply --plan <digest>]
    [--allow validate-reindex|rewrite]
    [--ack <change-code>...]
    [connection flags]
```

- Without `--apply`, the command never writes.
- `--apply` requires the digest printed by the plan, preventing an unnoticed
  plan/apply mismatch.
- Additive changes need no `--allow` flag. Higher classes require their exact
  allowance; `destructive` has no allowance because this command cannot do it.
- High semantic risks, such as acquiring component cascade semantics or
  retiring an attribute with live facts, also require the stable change code in
  `--ack`; allowing an execution class alone does not acknowledge its meaning.
- `--prune` requests retirement of absent installed attributes and is included
  in the digest.
- `--json` is a versioned machine contract and includes stable change codes;
  scripts must not parse the human rendering.
- Exit status distinguishes no change, changes planned, stale plan, blocked
  violations, and apply failure.

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

REWRITE (blocked)
  ~ :person/age long -> string                  current datoms: 8,109
    direct type mutation is unsupported; add/copy/cut-over/retire

UNMANAGED
    :legacy/import-id                           use --prune to retire
```

## Authorization and audit

Planning requires inspect access to the database. Applying schema metadata
requires a new `AlterSchema` action, separate from ordinary `Transact`, so an
application writer cannot silently broaden its own schema. Rewrite jobs also
require the existing transact authority over their target attributes. Hard
deletion/excision, when designed, belongs to the operator service's
irreversible two-person approval path.

Every applied schema transaction records requester identity, desired and plan
digests, CLI/protocol version, execution class, and acknowledgements on the
transaction entity. Plans may contain samples for diagnosis but audit records
store counts and digests rather than application values.

## Failure and concurrency behavior

- A changed installed-schema fingerprint or failed plan precondition aborts
  before any schema change. The caller replans.
- A failed additive transaction changes nothing.
- A failed backfill leaves the requested constraint inactive and may be
  resumed; queries continue through a correct fallback path.
- Concurrent ordinary writes during a rewrite are either captured by a
  high-water/tail pass or rejected by a final basis fence. The first
  implementation may choose the simpler write-blocked final pass.
- Peers that do not understand a schema generation fail the protocol-version
  check instead of applying data with stale validation/index rules.
- Cancellation stops at a checkpoint. It never activates a partially verified
  constraint.

## Delivery plan

### Phase 1: diff-only planner

- Extract normalized desired attributes from both TOML and EDN without
  allocating ids.
- Add a read-only planner and impact analyzer over `Db`.
- Ship `corium schema update` without `--apply`, stable JSON, fixtures, and
  property tests for change classification.
- Report the current implementation limitation clearly: all applies are
  blocked until schema transactions land.

### Phase 2: transactional additive schema

- Bootstrap the schema vocabulary and dynamic db-partition id allocation.
- Derive basis-versioned schema/id maps from schema datoms in `corium-db`.
- Persist/stream schema generations through recovery roots, handshakes, and tx
  reports; upgrade existing metadata deterministically.
- Add the basis-fenced schema-update RPC, authorization action, audit metadata,
  and `--apply` for new attributes and one-to-many changes.

### Phase 3: validation and indexes

- Add exact duplicate/cardinality impact scans and index readiness state.
- Make AVET backfill/rebuild resumable; teach query planning and uniqueness
  validation to respect readiness basis.
- Enable index and uniqueness changes plus conflict-free many-to-one changes.

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
  `--prune`, engine attributes, deterministic ordering, and plan digests.
- Property tests generate installed/desired schema pairs and assert that every
  difference is classified exactly once and that unsupported changes never
  reach apply.
- Model tests compare impact counts with brute-force EAVT/AEVT scans.
- Transaction tests assert a schema install and first use in one transaction,
  stale-schema and constraint-drift rejection, harmless data-basis drift for an
  additive plan, idempotent re-apply, and atomic failure.
- Peer tests keep a connected peer live across each schema change and compare
  it with a freshly connected peer at the same basis.
- Crash simulation injects failure before and after schema-log append,
  metadata snapshot publication, index backfill checkpoints, and constraint
  activation.
- Compatibility tests open a current-format database, synthesize its `t = 0`
  genesis schema record, preserve its basis and all attribute ids, and reopen
  it with identical queries and transaction validation.
