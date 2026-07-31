# Schema management

A schema declares attributes. An attribute has a name, a value type, a
cardinality, and optional properties.

Corium installs the schema when the database is created:

```sh
corium db create people --schema schema.toml
```

The CLI reads TOML when the path ends in `.toml`. Every other extension is
read as EDN.

## The most important rule

> **Partly implemented — the schema is fixed at creation.** `corium db create`
> is the only path that installs attributes. A transaction that carries
> `:db/ident` attribute maps does not install them. A second `db create` with
> the same name prints `:created false` and ignores the schema file.
>
> To add or change an attribute today, create a new database with the full
> schema and copy the data into it. Plan the schema before the first
> production load.

The rest of this chapter describes what the schema file can declare.

## TOML format

The TOML format is an authoring layer over the flat attribute model.

```toml
schema-version = 1

[[entity]]
name = "person"

[entity.attributes]
id      = { type = "uuid", unique = "identity" }
name    = { type = "string", index = true }
age     = "long"
tags    = { type = "keyword", many = true }
address = { type = "ref", component = true }

[[entity]]
name = "organization"

[entity.attributes]
name      = "string"
employees = { type = "ref", cardinality = "many" }
```

The first block declares `:person/id`, `:person/name`, `:person/age`,
`:person/tags`, and `:person/address`. A bare string is shorthand for that
type with cardinality one.

An `[[entity]]` block is an authoring group only. It supplies the keyword
namespace. It does not create an entity type, it does not constrain which
attributes can appear together, and it does not constrain the target of a
reference attribute.

Each group name can appear in at most one `[[entity]]` block.

### Flat attributes

Top-level declarations express ungrouped attributes, or add attributes to a
group without an entity block:

```toml
[[attribute]]
name = "created-at"
type = "instant"
index = true

[[attribute]]
group = "audit"
name = "created-by"
type = "ref"
```

These declare `:created-at` and `:audit/created-by`. Declaring one canonical
attribute through both syntaxes is an error.

### Attribute options

Every detailed declaration requires `type`.

| Option | Values | Default |
|---|---|---|
| `type` | `boolean`, `long`, `double`, `instant`, `uuid`, `keyword`, `string`, `bytes`, `ref` | Required |
| `many` | Boolean cardinality shorthand | `false` |
| `cardinality` | `"one"` or `"many"` | `"one"` |
| `unique` | `"identity"` or `"value"` | Unset |
| `index` | Boolean | `false` |
| `component` | Boolean | `false` |
| `no-history` | Boolean | `false` |

Use only one of `many` and `cardinality` on a declaration. A unique attribute
receives index coverage whether or not `index = true` is present.

### Name rules

Group and attribute names are preserved exactly. They must be valid EDN
keyword components, so that the resulting idents work in queries,
transactions, and console input.

A name cannot start with a digit. It cannot contain whitespace, `/`, `:`, or
EDN delimiter and reader-macro punctuation.

A quoted TOML key carries an EDN-valid name that is not a valid bare TOML key.
Quoting does not bypass the validation.

```toml
[entity.attributes]
"active?" = "boolean"
```

## EDN format

The EDN format is the Datomic-style attribute map. The file holds one vector
of maps, or a sequence of bare maps.

```clojure
[{:db/ident :artist/gid
  :db/valueType :db.type/uuid
  :db/cardinality :db.cardinality/one
  :db/unique :db.unique/identity}
 {:db/ident :artist/name
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/index true}
 {:db/ident :medium/tracks
  :db/valueType :db.type/ref
  :db/cardinality :db.cardinality/many
  :db/isComponent true}]
```

| Key | Values | Default |
|---|---|---|
| `:db/ident` | Keyword. Required. | None |
| `:db/valueType` | `:db.type/` plus `boolean`, `long`, `double`, `instant`, `uuid`, `keyword`, `string`, `bytes`, `ref`. Required. | None |
| `:db/cardinality` | `:db.cardinality/one` or `:db.cardinality/many` | `one` |
| `:db/unique` | `:db.unique/identity` or `:db.unique/value` | Unset |
| `:db/index` | `true` | `false` |
| `:db/isComponent` | `true` | `false` |
| `:db/noHistory` | `true` | `false` |

> **Not implemented.** `:db/doc` has no effect. An EDN schema file can carry
> it, and the engine discards it. The TOML format has no equivalent option,
> and it rejects an unknown option.

> **Not implemented.** Enumerated values as ident entities are not supported.
> Where Datomic models an enumeration as a reference to an ident entity, use a
> plain `:db.type/keyword` value.

## What each property does

**`:db/unique`.** `:db.unique/identity` makes a tempid collision an upsert.
`:db.unique/value` makes a collision an error. Both give the attribute AVET
coverage, so a lookup reference `[:person/email "a@b.c"]` works.

**`:db/index`.** The attribute gets AVET coverage. A query can then seek it by
value or by range. Without it, a value lookup is a bounded scan of AEVT.
Index coverage costs write throughput and storage.

**`:db/isComponent`.** A reference attribute whose targets are retracted with
the parent by `:db/retractEntity`, and pulled recursively by default. Use it
for owned sub-entities, not for shared references.

**`:db/noHistory`.** The attribute is not retained in the history indexes. Use
it for high-churn values whose past does not matter, for example a counter.
The past of that attribute cannot be recovered afterward.

Reference attributes are always covered by VAET. A reverse-reference query
therefore needs no extra declaration.

## Inspecting the installed schema

The console prints the schema:

```text
:schema
:schema person/name
```

The `Schema` panel of [`corium tui`](../surfaces/tui.md) shows the same table,
and it filters with `/`.

SQL exposes the same data as relations:

```sql
SELECT * FROM corium_sys.attributes;
```

## Reserved idents

The engine installs its own attributes first. Attribute entity ids below 100
in the `:db.part/db` partition are reserved.

`:db/txInstant` is installed by the engine at the same id that Datomic uses.
A schema file that declares it is rejected as a duplicate ident.

## Migrating a schema

Until schema alteration lands, use this procedure.

1. Write the new schema file with every attribute, old and new.
2. Create a new database with `corium db create <new-name> --schema <file>`.
3. Read the old database with a client, and transact the data into the new
   one. `tx-range` gives an ordered replay of the source.
4. Compare `corium db stats` on both databases.
5. Point the application at the new database.
6. Delete the old database only after the new one is verified.

Take a [backup](../availability/backup.md) of the old database before step 6.
