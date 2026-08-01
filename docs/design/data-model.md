# Data Model: Values, Datoms, Schema

## Datoms

The unit of information is the datom, a 5-tuple:

```rust
pub struct Datom {
    pub e: EntityId,   // u64: 22-bit partition | 42-bit sequence
    pub a: AttrId,     // u64 entity id of the attribute (always :db.part/db partition)
    pub v: Value,      // typed value, see below
    pub tx: TxId,      // u64 entity id in :db.part/tx
    pub added: bool,   // assertion or retraction
}
```

- **Entity ids** are unsigned 64-bit with the partition in the high bits,
  mirroring Datomic: entities in the same partition sort adjacently in EAVT,
  which is the locality knob. Built-in partitions: `:db.part/db` (schema),
  `:db.part/tx` (transaction entities), `:db.part/user` (default). User
  partitions can be created as entities with `:db/ident`.
- **TxIds** are ordinary entity ids in `:db.part/tx`, allocated monotonically.
  `t` (basis) is the sequence portion of the tx id; `t → tx` and `tx → t` are
  cheap bit operations.
- Transaction entities carry `:db/txInstant` (wall-clock, monotonic-corrected)
  and any user-asserted tx metadata.

## Transaction time is data

`:db/txInstant` is not log metadata sitting beside the datoms — it *is* a
datom, asserted on the transaction entity by every commit path:

```
[<tx entity> :db/txInstant #inst"…" <tx entity> true]
```

Consequences, all of them the point of doing it this way:

- It joins like anything else: `[?tx :db/txInstant ?inst]`, and from a datom's
  `tx` position to its commit time in one clause.
- It is AVET-indexed, so resolving a wall clock instant to a basis is an index
  seek, which is what makes the instant-named views in
  [time-model.md](time-model.md) cheap.
- It rides along for free: the log record, the tx-report, every peer's live
  index, and the published snapshot all carry the same datom, so there is no
  second representation of transaction time to keep in sync. (`:db/txInstant`
  datoms are never retracted, so they are live facts and survive into
  current-state snapshots.)
- Transaction entities are therefore ordinary entities with at least one datom
  each. Datom and entity counts include them.

The engine installs the attribute itself (`corium_db::bootstrap`), reserving
`:db.part/db` sequence 50 — the same id Datomic uses. Sequences below the first
user-installable attribute id are reserved for the engine; a user schema that
declares `:db/txInstant` is rejected as a duplicate ident.

Logs written before transaction time became a datom are unaffected: replay
synthesizes the datom from the record's timestamp field, so an old database
gains instant-named views without a rewrite.

## Value model

The engine-internal value type is a compact Rust enum, **not** a cljrs value
(see ADR-0002). v1 value types match core Datomic schema scope:

```rust
pub enum Value {
    Bool(bool),
    Long(i64),
    Double(f64),        // total order via IEEE-754 total-order trick
    BigInt(BigInt),
    BigDec(BigDecimal),
    Instant(i64),       // millis since epoch, UTC
    Uuid(u128),
    Keyword(KwId),      // interned; see below
    Str(Arc<str>),
    Bytes(Arc<[u8]>),
    Ref(EntityId),
}
```

Deferred to post-v1: `:db.type/fulltext` behavior, tuple types, `:db.type/uri`,
`:db.type/symbol` (trivial to add; kept out to hold v1 scope).

A proposed tenth variant, `Sealed`, carries a value encrypted under a
protection class's key: opaque to the engine, hydrated only by a reader holding
that key, and forbidden on indexed, unique, and ref attributes. See
[encryption.md](encryption.md) and
[ADR-0018](../adr/0018-attribute-protection-classes.md).

Keywords are interned per-database in a keyword table (itself stored as
datoms on schema entities where applicable, plus a side table in the index
root for non-ident keywords), so `Value::Keyword` comparisons are integer
comparisons.

## Sortable binary encoding

One encoding serves the index segments, the log, and (wrapped in protobuf
`bytes`) the wire. Requirement: **`memcmp` order on encoded bytes equals
semantic order on values**, so segment trees never decode to compare.

Layout: `[type tag: 1 byte][payload]` with tags ordered by type (cross-type
order is defined by tag). Payloads:

| Type | Encoding |
|---|---|
| Bool | `0x00` / `0x01` |
| Long | big-endian with sign bit flipped |
| Double | IEEE-754 bits; if negative flip all bits, else flip sign bit |
| BigInt | sign byte, big-endian magnitude with length prefix folded into ordering-safe form |
| BigDec | scale-normalized: sign, exponent (order-adjusted), mantissa |
| Instant | as Long |
| Uuid | 16 bytes big-endian |
| Keyword | interned id as Long (order = intern order, stable, not lexical — AVET over keywords is grouping, not lexical sort, same as Datomic) |
| Str | UTF-8 with `0x00` escaped as `0x00 0xFF`, terminated `0x00 0x00` |
| Bytes | same escaping scheme as Str |
| Ref | EntityId big-endian |

Property tests assert `encode(a) < encode(b) ⇔ a < b` for every type and
random pairs, and round-trip fidelity.

Datom keys in segments are the concatenation of the encoded components in
index order (e.g. EAVT: `e ‖ a ‖ v ‖ tx-with-added-bit`), giving pure
`memcmp` trees.

## Schema

Schema is data: attributes are entities in `:db.part/db` described by schema
facts. The creation-time schema is an immutable pre-basis seed rather than a
transaction at `t = 0`. This preserves the time model's empty basis 0. Later
schema changes are ordinary transactions. See
[schema-migrations.md](schema-migrations.md#schema-as-basis-versioned-data).
v1 supports:

- `:db/ident` — keyword identity for any entity (required for attributes).
- `:db/valueType` — one of the value types above.
- `:db/cardinality` — `:db.cardinality/one` | `:db.cardinality/many`.
- `:db/unique` — `:db.unique/identity` (upsert on tempid collision) or
  `:db.unique/value` (conflict error).
- `:db/isComponent` — ref attributes whose targets are retracted with the
  parent (`:db/retractEntity`) and pulled recursively by default.
- `:db/index` — request AVET coverage for this attribute (AVET contains only
  indexed and unique attributes; VAET contains all ref attributes).
- `:db/doc`, `:db/noHistory` (skip history index for high-churn attributes).
- `:db/protection` (proposed) — name a protection class whose key seals this
  attribute's values; mutually exclusive with `:db/index`, `:db/unique`, and
  `:db.type/ref`, since protected datoms cannot be indexed. Alterable, but
  forward-only like every other fact: datoms asserted before the change keep
  the form they were written in ([encryption.md](encryption.md)).

The transactor materializes schema into an immutable in-memory `SchemaCache`
(AttrId → attribute record) rebuilt per basis-t; peers build the same cache
from the same datoms, so validation logic is shared code in `corium-core`.

Schema alteration follows Datomic's rules (additive changes free; a defined
set of legal alterations like adding `:db/index`; no value-type changes). The
plan/apply workflow, impact classes, retirement semantics, and implementation
work needed to make those changes basis-versioned are specified in
[schema-migrations.md](schema-migrations.md).

## Transaction data (input model)

The public transaction format is EDN, converted at the boundary from cljrs
values (or built programmatically in Rust via a builder API):

- List form: `[:db/add e a v]`, `[:db/retract e a v]`, plus built-in and
  user database functions `[:db/cas …]`, `[:db/retractEntity e]`, `[:my/fn …]`.
- Map form: `{:db/id e, :attr v, …}` with nested maps for component/ref
  attributes, expanded to list form.
- **Tempids**: negative numbers or strings; resolved to fresh entity ids in
  the requested partition, with unification through `:db.unique/identity`
  attributes (upsert).
- **Lookup refs**: `[attr v]` where `attr` is unique, usable anywhere an
  entity id is expected.
- **Transaction metadata**: the reserved tempid `"datomic.tx"` (spelled as in
  Datomic so ported transaction data works unchanged) and the keyword
  `:db/current-tx` both name the transaction entity being built, so
  `[:db/add "datomic.tx" :audit/actor "alice"]` records who and why beside the
  engine's when. The reserved tempid allocates nothing and never upserts
  through a `:db.unique/identity` attribute — metadata stays on the
  transaction.
- **Supplying `:db/txInstant`**: asserting it against the transaction entity
  dates the commit explicitly, which is how a backfilled import keeps its
  original timestamps. The transactor stamps `max(now, last + 1)` when the
  data does not, and rejects a supplied instant that does not advance the
  clock — instants must stay monotone for `as-of` by instant to mean anything.

Expansion, tempid resolution, and validation live in `corium-tx` and are pure
functions of `(db, tx-data)` — the transactor applies them, and the
deterministic simulator and unit tests call them directly.
