# ADR-0020: Plan and apply schema migrations as basis-versioned data

**Status:** Accepted (2026-08-03). Schema is basis-versioned data, and
additive, `validate-reindex`, protection, and retirement changes apply through
`corium schema update --apply`; `rewrite` and `destructive` changes remain
refused, and the delivery plan records what is left. Design:
[`docs/design/schema-migrations.md`](../design/schema-migrations.md). Relates
to [ADR-0009](0009-schema-scope.md), which fixes the supported attribute model,
[ADR-0018](0018-attribute-protection-classes.md), whose forward-only protection
timeline is schema migration work, and
[ADR-0019](0019-operator-peer-service.md), which supplies the eventual home for
long-running migration jobs.

## Context

Corium accepted a schema only when a database was created. The implementation
stored the resulting `Schema` and ident registry in creation-time metadata and
sent that snapshot in a peer handshake. Ordinary transactions neither altered
the schema cache nor told connected peers to replace it. This fell short of
the project's intended model that schema is data and made a CLI-only update
command impossible to implement safely.

Schema differences also have radically different costs. Adding an unused
attribute needs no data work. Adding uniqueness needs a duplicate scan and an
index backfill. Collapsing cardinality may need conflict resolution. Removing
an attribute or changing its type cannot erase or reinterpret immutable
history. Treating all of these as equivalent map edits would conceal the exact
operational risk the command needs to expose.

## Decision

Schema updates use a declarative, basis-fenced plan/apply workflow, and schema
becomes basis-versioned data before any update is applied.

- `corium schema update <db> --schema <file>` is read-only by default. It
  normalizes the desired schema, reads the installed schema at one basis,
  computes a property-level diff, measures affected facts and constraint
  violations, and emits a deterministic plan digest. Advisory counts, samples,
  and estimates accompany the plan. The digest excludes these observations, so
  harmless data drift does not contradict the rule below.
- Apply requires `--apply --plan <digest>`. The request carries the observed
  basis for audit. The transactor validates the installed-schema fingerprint,
  desired digest, and safety preconditions inside its single-writer path.
  Schema or safety-relevant drift aborts the whole apply and requires a new
  plan. Harmless ordinary data writes do not prevent an additive update.
- Changes are classified as additive, validate/reindex, rewrite, or
  destructive. Risk and affected-data counts are reported independently.
  Higher classes require explicit acknowledgement. This command cannot run
  destructive changes.
- File absence is not deletion by default. Installed attributes outside a
  partial desired file remain unmanaged. `--prune` explicitly requests
  retirement, which preserves schema metadata and history while refusing new
  assertions. Hard deletion and excision are separate operations.
- Idents match exactly. Rename inference is forbidden. Direct value-type
  mutation is destructive and cannot be allowed as a rewrite. The planner uses
  an add, convert, cutover, and retirement recipe. Copy and cleanup steps in
  this recipe are rewrite work.
- Attribute schema is represented by datoms using engine-installed schema
  vocabulary. Creation installs an immutable pre-basis schema seed with no
  transaction id. Datom, history, and log views do not return this seed, so
  basis 0 remains empty. Later changes are ordinary transactions. Immutable
  `Db` values derive their `Schema` and idents from the seed and later changes.
  This rule also applies to time views. Peers learn changes through ordinary tx
  reports.
- Index and uniqueness changes distinguish requested state from ready-at-basis
  state. A pending uniqueness constraint protects writes through an AEVT
  fallback while AVET builds. Activation validates the tail inside the writer
  queue. Queries and validation never assume that a backfill is complete.
- Protection changes follow ADR-0018's forward-only timeline. Protection is
  incompatible with index, uniqueness, and refs, and an attribute that has ever
  been protected can never later gain AVET coverage. The planner measures and
  acknowledges these effects rather than treating protection as an unrelated
  key operation.
- Long validation, reindex, and rewrite steps use resumable, idempotent job
  semantics compatible with the operator peer service. The CLI retains an
  in-process fallback. The data plane does not depend on the operator service.

## Consequences

- A reviewed plan names both semantic risk and actual installed-data impact.
  The apply operation rejects a stale or different plan.
- Additive changes stay cheap, while index builds and rewrites are observable
  work rather than hidden transaction latency.
- Schema history follows Corium's ordinary time model. A peer at basis `t`
  validates and interprets data with the schema at `t`, and live peers converge
  without reconnecting.
- Existing databases need a deterministic compatibility upgrade. It labels
  their creation metadata as the pre-basis seed. It preserves basis 0,
  application transaction numbers, and attribute entity ids. This is
  foundational work before the first applied update.
- Retirement preserves immutable history and permits cleanup retractions, but
  it does not provide physical erasure. Users who need erasure must wait for a
  separately designed excision facility with backup and replica semantics.
- Direct value-type mutation remains inconvenient on purpose. Shadow
  attributes require application cutover work, but avoid mixed-type ambiguity,
  implicit conversion policy, and false claims about historical schema.
- The protocol, recovery metadata, peer apply path, index publisher, schema
  cache, authorization model, and CLI all gain schema-generation awareness.
  The cross-cutting cost is required to make updates correct rather than a
  catalog-only illusion.
