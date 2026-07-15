# ADR-0009: Core Datomic schema scope for v1

**Status:** Accepted (2026-07-15)

## Context

Datomic's attribute system has a well-understood core plus features that pull
in significant machinery: `:db/fulltext` (a text index engine), tuple value
types (composite uniqueness), entity specs/`:db/ensure`.

## Decision

v1 implements the core: attributes as entities; `:db/ident`; value types
bool, long, double, bigint, bigdec, instant, uuid, keyword, string, bytes,
ref; cardinality one/many; `:db/unique` identity (upsert) and value;
`:db/isComponent`; `:db/index` (AVET membership); `:db/noHistory`; `:db/doc`;
partitions; tempids with upsert unification; lookup refs; Datomic's schema
alteration rules. Deferred: fulltext (tantivy, post-v1), tuple types,
`:db/ensure` entity specs, uri/symbol value types.

## Consequences

- v1 schema expresses the overwhelming majority of real Datomic schemas;
  ports needing fulltext or composite uniqueness wait for the backlog items.
- Multi-attribute uniqueness has no v1 primitive; the documented workaround
  is a derived unique attribute maintained by a transaction function.
- The value-type enum and tag space reserve room for the deferred types so
  adding them is non-breaking to the storage format.
