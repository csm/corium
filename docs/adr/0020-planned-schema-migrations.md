# ADR-0020: Plan and apply schema migrations as basis-versioned data

**Status:** Proposed (2026-07-31); design in
[`docs/design/schema-migrations.md`](../design/schema-migrations.md). Relates
to [ADR-0009](0009-schema-scope.md), which fixes the supported attribute model,
and [ADR-0019](0019-operator-peer-service.md), which supplies the eventual home
for long-running migration jobs.

## Context

Corium accepts a schema only when a database is created. The implementation
stores the resulting `Schema` and ident registry in creation-time metadata and
sends that snapshot in a peer handshake. Ordinary transactions neither alter
the schema cache nor tell connected peers to replace it. This falls short of
the project's intended model that schema is data and makes a CLI-only update
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
  violations, and emits a deterministic plan digest.
- Apply requires `--apply --plan <digest>`. The request carries the observed
  basis for audit; the transactor verifies the installed-schema fingerprint,
  desired digest, and safety preconditions inside its single-writer path.
  Schema or safety-relevant drift aborts the whole apply and requires a new
  plan; harmless ordinary data writes do not prevent an additive update.
- Changes are classified as additive, validate/reindex, rewrite, or
  destructive. Risk and affected-data counts are reported independently.
  Higher classes require explicit acknowledgement; destructive changes are
  not executable by this command.
- File absence is not deletion by default. Installed attributes outside a
  partial desired file remain unmanaged. `--prune` explicitly requests
  retirement, which preserves schema metadata and history while refusing new
  assertions. Hard deletion and excision are separate operations.
- Idents match exactly. Rename inference is forbidden. Value-type changes are
  not performed in place; the planner prescribes add, explicitly convert,
  cut over, and retire.
- Attribute schema is represented by datoms using engine-installed schema
  vocabulary. Creation installs a genesis schema record at `t = 0`; later
  changes are ordinary transactions. Immutable `Db` values derive their
  `Schema` and idents at each basis, including time views, and peers learn
  changes through ordinary tx reports. Creation metadata remains a recoverable
  schema snapshot/cache, not the authority.
- Index and uniqueness changes distinguish requested state from ready-at-basis
  state. Queries and validation never assume a backfill that has not completed.
- Long validation, reindex, and rewrite steps use resumable, idempotent job
  semantics compatible with the operator peer service. The CLI retains an
  in-process fallback; the data plane does not depend on the operator service.

## Consequences

- A reviewed plan names both semantic risk and actual installed-data impact;
  applying a stale or different plan is impossible by construction.
- Additive changes stay cheap, while index builds and rewrites are observable
  work rather than hidden transaction latency.
- Schema history follows Corium's ordinary time model. A peer at basis `t`
  validates and interprets data with the schema at `t`, and live peers converge
  without reconnecting.
- Existing databases need a deterministic compatibility upgrade that emits a
  `t = 0` genesis schema record from their metadata while preserving their
  basis and attribute entity ids. This is foundational work before the first
  applied update.
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
