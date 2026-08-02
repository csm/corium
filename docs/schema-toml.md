# TOML schemas

Corium accepts hierarchical TOML schema files when `corium db create
--schema` receives a path ending in `.toml`. The format is a concise authoring
layer over Corium's flat attribute model; EDN schema files remain supported.

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
| `protection` | A `[protect.<name>]` class, as `"protect/<name>"` | unset |

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

## Protection classes

A protection class names the key that seals values on the attributes assigned
to it. The class never holds key material: the database records only the key
*identity*, and each process resolves it through its own keyring, so who may
read a protected attribute is a question about key distribution rather than
about Corium (see
[docs/design/encryption.md](design/encryption.md) and
[ADR-0018](adr/0018-attribute-protection-classes.md)).

```toml
[protect.pii]
key = "file:/etc/corium/pii.key"
padding = 64
on-missing-key = "redact"

[[entity]]
name = "person"

[entity.attributes]
name = "string"
ssn = { type = "string", protection = "protect/pii" }
```

A section `[protect.<name>]` declares the class `:protect/<name>`. Its options:

| Option | Values | Default |
|---|---|---|
| `key` | Key identity, e.g. `"file:/etc/corium/pii.key"` | required |
| `algorithm` | `"aes-256-gcm-siv"` | `"aes-256-gcm-siv"` |
| `scope` | `"attribute"` or `"entity"` | `"attribute"` |
| `padding` | Bytes to round plaintext up to, at least 16 | unset |
| `on-missing-key` | `"redact"`, `"hide"`, or `"error"` | `"redact"` |
| `legacy-plaintext` | `"redact"` or `"pass-through"` | `"redact"` |
| `epoch` | Key epoch new values seal under | `1` |

`scope` chooses what the sealing determinism leaks. Under `"attribute"` a
reader without the key can tell that two entities share a value on that
attribute; `"entity"` also binds the entity, so it leaks only that one
entity's value repeated over time. Entity scope is declarable but not yet
sealable — a writing peer refuses it rather than binding the wrong subject.

`padding` rounds plaintext up to a multiple of that many bytes before sealing,
which costs storage and removes the length side channel for short, guessable
values.

`on-missing-key` decides what a reader who cannot open a value gets:
`"redact"` binds it in redacted form (structure visible, value not), `"hide"`
drops the datom out of scans entirely, and `"error"` fails the read. Under all
three, an unopenable value never satisfies a constant or a predicate.

Who may read a class is a question about key distribution, not about Corium:
give the key identity to the processes that should hydrate it, and to no
others. One caveat while the per-principal key policy is unimplemented: a
**peer server** hydrates every request with its own keyring, so any client it
serves reads every class it holds. See
[docs/operations.md](operations.md#attribute-protection) before pointing
less-trusted clients at a key-holding peer server.

Protection cannot be combined with `index`, `unique`, or `type = "ref"`:
ciphertext order is not value order, so a protected attribute can never appear
in the value-ordered indexes, and the schema rejects the combination rather
than surprising a range query later. When an application genuinely needs
indexed lookup on a protected field, it adds a second, unprotected attribute
holding a keyed hash of the value and accepts that leak explicitly.

## Creating a database

```sh
corium db create people --schema schema.toml
```

The CLI selects TOML for `.toml` paths. Other extensions retain the existing
EDN behavior, including a single vector of attribute maps or a sequence of
bare attribute maps.
