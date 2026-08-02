# TOML schemas

Corium accepts hierarchical TOML schema files when `corium db create
--schema` receives a path ending in `.toml`. The format is a concise authoring
layer over Corium's flat attribute model; EDN schema files remain supported.

The same format is the desired input to the proposed `corium schema update`
plan/apply workflow. An update file may be partial: installed attributes absent
from it remain unmanaged unless `--prune` explicitly requests retirement.
`schema-version` versions this file format; it is not a migration sequence.
See [the schema migration design](design/schema-migrations.md).

The current parser accepts only the options documented below. Implementing the
migration design also extends the normalized schema model and this authoring
format with `doc` and, when attribute protection is enabled, `protection`. Those
properties are design commitments rather than accepted fields in the current
binary.

## Grouped attributes

The common form groups attributes under a familiar entity-shaped declaration:

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

The first block declares `person/id`, `person/name`, `person/age`,
`person/tags`, and `person/address`. A string is shorthand for an attribute
with that type and cardinality one.

An `entity` is only an authoring group. It supplies the keyword namespace but
does not create a persisted entity type, constrain which attributes may
coexist, or constrain the targets of reference attributes.

An entity block may omit its attributes table and be populated by flat
declarations. Each entity group name may appear in at most one `[[entity]]`
block.

## Flat attributes

Top-level declarations express ungrouped attributes or add attributes to a
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

These declare `created-at` and `audit/created-by`, respectively. Declaring the
same canonical attribute through grouped and flat syntax is an error.

## Attribute options

Every detailed declaration requires `type`. Supported values are `boolean`,
`long`, `double`, `instant`, `uuid`, `keyword`, `string`, `bytes`, and `ref`.

The remaining options are:

| Option | Values | Default |
|---|---|---|
| `many` | Boolean cardinality shorthand | `false` |
| `cardinality` | `"one"` or `"many"` | `"one"` |
| `unique` | `"identity"` or `"value"` | unset |
| `index` | Boolean | `false` |
| `component` | Boolean | `false` |
| `no-history` | Boolean | `false` |
| `doc` | String documentation (`:db/doc`) | unset |

Use only one of `many` and `cardinality` on a declaration. Unique attributes
receive index coverage whether or not `index = true` is present.

Group and attribute names are preserved exactly and must also be valid EDN
keyword components so the resulting idents remain usable in queries,
transactions, and console input. Names cannot start with a digit or contain
whitespace, `/`, `:`, or EDN delimiter and reader-macro punctuation. TOML
quoted keys support EDN-valid names that are not valid bare TOML keys; quoting
does not bypass this validation:

```toml
[entity.attributes]
"active?" = "boolean"
```

## Creating a database

```sh
corium db create people --schema schema.toml
```

The CLI selects TOML for `.toml` paths. Other extensions retain the existing
EDN behavior, including a single vector of attribute maps or a sequence of
bare attribute maps.

## Updating an existing database

`corium schema update` compares the same file with the schema installed in a
database. It is read-only by default and is documented in
[Operations](operations.md#schema-updates); the model behind it is
[schema migrations](design/schema-migrations.md).

```sh
corium schema update people --schema schema.toml
```
