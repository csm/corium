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

Schema is data: attributes are entities in `:db.part/db` described by datoms,
installed through ordinary transactions. v1 supports:

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

The transactor materializes schema into an immutable in-memory `SchemaCache`
(AttrId → attribute record) rebuilt per basis-t; peers build the same cache
from the same datoms, so validation logic is shared code in `corium-core`.

Schema alteration follows Datomic's rules (additive changes free; a defined
set of legal alterations like adding `:db/index`; no value-type changes).

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

Expansion, tempid resolution, and validation live in `corium-tx` and are pure
functions of `(db, tx-data)` — the transactor applies them, and the
deterministic simulator and unit tests call them directly.
