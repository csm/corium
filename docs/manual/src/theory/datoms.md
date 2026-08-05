# Datoms and the fact model

## The datom

The unit of information is the datom. A datom is a five-part fact.

| Part | Name | Content |
|---|---|---|
| `e` | entity | A 64-bit entity id. |
| `a` | attribute | The entity id of an attribute. |
| `v` | value | A typed value. |
| `tx` | transaction | The entity id of the transaction that recorded the fact. |
| `added` | assert or retract | `true` for an assertion, `false` for a retraction. |

A datom is never modified. A new value for a cardinality-one attribute is one
transaction that retracts the old datom and asserts the new one. The old datom
stays in the history indexes.

## Entity ids and partitions

An entity id is a 64-bit number. The high 22 bits hold the partition. The low
42 bits hold a sequence number.

Entities in one partition sort together in the EAVT index. The partition is
therefore the locality control. Three partitions are built in.

| Partition | Holds |
|---|---|
| `:db.part/db` | Schema entities, such as attributes. |
| `:db.part/tx` | Transaction entities. |
| `:db.part/user` | Application entities, by default. |

> **Not implemented.** User-defined partitions are described in the design
> documents. The engine has three partitions and no way to add a fourth. All
> application entities land in `:db.part/user`.

## Transaction numbers

A transaction id is an entity id in `:db.part/tx`. The basis, written `t`, is
the sequence part of that id. Conversion between `t` and `tx` is a bit
operation.

`t` increases by one for each committed transaction. Every value of `t` up to
the current basis names a real transaction.

## Transaction time is data

Every commit asserts `:db/txInstant` on its own transaction entity. The commit
time is a datom, not log metadata. Three consequences matter to an operator.

- The commit time joins like any other fact. The clause
  `[?tx :db/txInstant ?inst]` binds the time of a transaction.
- The attribute is AVET-indexed, so a wall-clock time resolves to a basis with
  an index seek.
- Transaction entities are ordinary entities. Datom counts and entity counts
  include them.

The transactor stamps `max(now, last + 1)`. This rule keeps commit times
monotone. A transaction can supply its own `:db/txInstant`, which is how an
import keeps original timestamps. The transactor rejects a supplied instant
that does not advance the clock.

## Value types

The engine has nine value types.

| Schema type | Holds |
|---|---|
| `:db.type/boolean` | `true` or `false`. |
| `:db.type/long` | A signed 64-bit integer. |
| `:db.type/double` | A double, totally ordered. |
| `:db.type/instant` | Milliseconds since the Unix epoch, UTC. |
| `:db.type/uuid` | A 128-bit UUID. |
| `:db.type/keyword` | An interned keyword. |
| `:db.type/string` | UTF-8 text. |
| `:db.type/bytes` | A byte array. |
| `:db.type/ref` | An entity reference. |

Keywords are interned per database. A keyword comparison is therefore an
integer comparison.

One binary encoding serves the indexes, the log, and the wire. The encoding is
sortable: the byte order of two encoded values equals the semantic order of the
values. Index segments therefore compare without decoding.

> **Not implemented.** Arbitrary-precision integers and decimals appear in the
> data-model design document, but the engine has no such value type. Store a
> big number as a string, or as a scaled long.

> **Not implemented.** `:db.type/fulltext` behavior, tuple types,
> `:db.type/uri`, and `:db.type/symbol` are out of scope for version 1. See
> [ADR-0009](https://github.com/csm/corium/blob/main/docs/adr/0009-schema-scope.md).

A stored value has one more shape than the nine above. `Sealed` holds a value
encrypted under a protection class key. No schema declares it. It appears when
the writing peer seals a value on a protected attribute, and it sorts after
every plaintext type. See
[attribute protection](../security/protection.md).

## Schema is data

An attribute is an entity in `:db.part/db`, described by datoms. The
[schema chapter](../running/schema.md) covers the attribute properties.

Because schema is data, a schema change is a transaction. `corium db create`
installs the first schema. `corium schema update` compares a file with the
installed schema, and applies the plan you reviewed.

A database also carries a **schema generation**. It is a counter, separate
from the basis. It advances once for each committed transaction that contains
a schema change. The basis says when a change happened. The generation says
whether two database values use the same schema.

## Excision

> **Not implemented.** Excision, which removes historical facts, is out of
> scope for version 1. It is the one operation that breaks immutability. The
> design reserves space for a filter set in the database root, applied at read
> time.
