# corium-core

Core Corium data types: values, datoms, ids, schema, and the sortable byte
encoding shared by every other crate.

## What it does

`corium-core` is the foundation of the workspace. It defines the compact Rust
representation of everything the engine stores and moves around:

- **`Value`** — the scalar value type (keywords, strings, numbers, refs,
  instants, …) plus `TotalF64` for total-ordered floats.
- **`Datom`** `[e a v tx added]` and the four `IndexOrder`s (EAVT, AEVT, AVET,
  VAET) that datoms are sorted into.
- **Ids** — `EntityId`, `AttrId`, `TxId`, `KwId`, and `Partition`/`PartitionId`
  for the partitioned entity-id space.
- **`Keyword`** and the `KeywordInterner` used to map `:db/ident` names to ids.
- **`Schema`** model — `Attribute`, `ValueType`, `Cardinality`, `Unique`.
- **`encoding`** — the order-preserving, tagged byte encoding (`Encodable`,
  `encode_value`) that makes index keys sort correctly and is reused by the
  segment and wire formats.

## Dependencies

- `thiserror` for error types.

It has **no** internal `corium-*` dependencies — every other crate depends on
it, directly or transitively. It is pure library code with no async, network,
or storage dependencies.

## Architecture

This crate is deliberately minimal and allocation-conscious: it holds only the
types and encodings that must be identical everywhere in the system. The
sortable encoding is the load-bearing piece — because index keys are produced
here, the same bytes drive in-memory index ordering (`corium-index`), the
persistent segment format (`corium-store`), and the value bytes carried over
the wire (`corium-protocol`). Keeping it dependency-free keeps the whole engine
testable in the deterministic simulator. See
[`docs/design/data-model.md`](../../docs/design/data-model.md).
