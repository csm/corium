# Schema management

A schema declares attributes. An attribute has a name, a value type, a
cardinality, and optional properties.

The schema is data. Corium keeps it in the same log as every other fact. The
schema of a database at basis `t` is therefore a question you can ask.

Two commands install attributes.

| Command | Purpose |
|---|---|
| `corium db create --schema <file>` | Install the first schema with the database. |
| `corium schema update <db> --schema <file>` | Compare a file with the installed schema, and apply the plan you reviewed. |

Both commands read the same file. The CLI reads TOML when the path ends in
`.toml`. Every other extension is read as EDN.

```sh
corium db create people --schema schema.toml
corium schema update people --schema schema.toml
```

The rest of this chapter describes the file format, the properties, and the
update procedure.

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
namespace. It does not create an entity type. It does not constrain which
attributes can appear together, and it does not constrain the target of a
reference attribute.

Each group name can appear in at most one `[[entity]]` block.

`schema-version` is the version of the file format. It is not a migration
number, and Corium does not compare it between runs.

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
| `doc` | Documentation string | Unset |
| `protection` | A declared class, as `"protect/<name>"` | Unset |

Use only one of `many` and `cardinality` on a declaration. A unique attribute
receives index coverage whether or not `index = true` is present.

A `[protect.<name>]` section declares a protection class. Read
[attribute protection](../security/protection.md) for the class options and
for what protection costs.

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
  :db/index true
  :db/doc "Credited name of the artist."}
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
| `:db/doc` | String | Unset |
| `:db/protection` | A declared class, for example `:protect/pii` | Unset |

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

**`:db/doc`.** Free documentation text. It has no effect on queries.

**`:db/protection`.** Values on the attribute are sealed by the writing peer
under the key of the named class. Read
[attribute protection](../security/protection.md).

Reference attributes are always covered by VAET. A reverse-reference query
therefore needs no extra declaration.

Protection cannot be combined with `:db/index`, `:db/unique`, or
`:db.type/ref`. Ciphertext order is not value order, so a protected attribute
can never appear in a value-ordered index.

## Enumerated values

An entity outside the `:db.part/db` partition can carry `:db/ident` as an
ordinary name. An ordinary transaction writes it, and a keyword in a reference
position resolves through it.

```clojure
[{:db/ident :status/active}
 {:db/ident :status/retired}]
```

A `:db.type/ref` attribute can then name `:status/active` as its value. Where
this shape is not needed, a plain `:db.type/keyword` value is simpler.

## Updating an installed schema

`corium schema update` compares a schema file with the schema installed in a
database. It prints the changes, their cost, and their meaning.

```sh
corium schema update people --schema schema.toml
```

**The command is read-only without `--apply`.** It plans against one immutable
database value, so every count in the plan is measured at one basis and one
schema.

### The plan

Each difference is one property-level change with an execution class.

| Class | Meaning | Examples |
|---|---|---|
| `additive` | No existing fact is inspected or rewritten. | A new attribute. Cardinality one to many. |
| `validate-reindex` | Existing facts stay valid, but a bounded scan, a constraint validation, or an index rebuild is needed. | Add `index` or `unique`. Change the uniqueness mode. Toggle `isComponent`. Retire an attribute. |
| `rewrite` | Current facts must change first. | Collapse cardinality where an entity holds several values. |
| `destructive` | Information or historical meaning is lost. | Change the value type of an attribute in place. |

Risk is reported beside the class, and the two are independent. An AVET
backfill is expensive and semantically harmless. A metadata-only
`isComponent` change is cheap and changes the meaning of every live reference.

A plan carries a digest. A later schema change, or a failed safety condition,
invalidates the digest. Ordinary data writes do not.

```text
database: people  basis: 418  schema-generation: 3
desired:  sha256:709f947a…
plan:     sha256:ad5cb3c4…

ADDITIVE
  + :person/email string cardinality-one
  + :person/tags keyword cardinality-many

VALIDATE-REINDEX
  ~ :person/address component false -> true
      live refs: 8109
      [ack: component-enable] existing references acquire cascade retract and pull semantics
      note: existing facts are not rewritten; future pull and retract-entity semantics change

UNMANAGED
    :legacy/import-id    use --prune to retire

3 change(s) planned. Nothing was written.
To apply: corium schema update people --schema <file> --apply --plan sha256:ad5cb3c4… --allow validate-reindex --ack component-enable
```

The last line is the exact invocation to run. Copy it, and add the path of
the schema file.

### Acknowledgement codes

A change whose *meaning* changes carries a stable code. Pass the code back
with `--ack`.

| Code | What you accept |
|---|---|
| `component-enable` | Existing references acquire cascade retract and pull semantics. |
| `component-disable` | Existing references lose those semantics. |
| `unique-mode-change` | Upsert and conflict behavior changes for future writes. |
| `no-history-enable` | History stops being recorded from this transaction onward. |
| `no-history-disable` | History resumes. The interval already omitted cannot be reconstructed. |
| `retire-live-attribute` | New assertions are refused while existing facts stay readable. |
| `protection-forward-only` | Protection changes are forward-only and cannot re-seal existing facts. |

Allowing an execution class says which work can run. An acknowledgement says
that you understood what the change means. The two are separate on purpose.

### Partial files and retirement

A file manages the declarations that it contains. An installed attribute that
the file does not name is reported as `unmanaged`, and the command leaves it
alone. `--prune` turns every unmanaged attribute into a retirement request.

Retirement is not deletion. A retired attribute keeps its ident, its metadata,
and its history. New assertions are refused. Retractions stay legal, which is
what makes retirement a usable step when an application moves to a replacement
attribute.

A retirement step prints the current datoms, the current entities, and the
recorded history datoms. It needs `--ack retire-live-attribute` only when the
attribute still holds live facts. Retracting those facts is separate work.

Idents are matched exactly. A removed ident and an added ident are two
changes, never an inferred rename. An incorrect rename aliases two meanings
permanently.

Engine attributes such as `:db/txInstant` are never managed by a file. A file
that declares one is rejected with an explicit error.

### Applying a plan

Run the same command again with `--apply`, the digest that the plan printed,
and every allowance and acknowledgement that the plan asked for.

```sh
corium schema update people --schema schema.toml \
  --apply --plan sha256:ad5cb3c4… \
  --allow validate-reindex --ack component-enable
```

The transactor recomputes the plan under its writer queue. If the digest does
not match, the transactor refuses the apply and changes nothing. Ordinary
writes between the review and the apply are safe. Only a schema change or a
failed condition invalidates the plan.

A successful apply prints the basis and the new schema generation:

```text
Applied 3 change(s) to people at basis 419 (schema-generation 4).
  + :person/email
  + :person/tags
```

An apply that has already landed succeeds and prints `No changes`. Installing
a change is what invalidates the digest that described it, so the command
re-plans, finds nothing to do, and writes nothing. The command is therefore
safe in a pipeline.

Applying needs the `alter-schema` authority. That authority is separate from
`transact`, so an application writer cannot broaden its own vocabulary. Read
[authorization](../security/authorization.md).

### Flags

| Flag | Effect |
|---|---|
| `--schema <path>` | The desired schema file. Required. |
| `--prune` | Retire the installed attributes that the file omits. Part of the digest. |
| `--json` | Print the versioned machine contract instead of the human report. |
| `--detailed-exit-code` | Exit 0 for no change and 2 for changes planned. |
| `--apply` | Apply the plan. Requires `--plan`. |
| `--plan <digest>` | The digest that the read-only plan printed. |
| `--allow <class>` | Permit an execution class above `additive`. Repeatable. |
| `--ack <code>` | Acknowledge a semantic change. Repeatable. |

A successful plan exits 0 whether or not it finds changes, so `&&` chains keep
working. `--detailed-exit-code` changes that to 0 for no change and 2 for
changes planned.

A failure exits 1. With `--json` the failure carries a stable code:
`parse-error`, `connect-error`, `plan-error`, `plan-mismatch`,
`allow-required`, `ack-required`, `blocked-change`, or `apply-failed`.

**Scripts must read `--json`.** The human report is not a contract.

### The audit trail

Every applied schema transaction records the requester, both digests, the
observed basis, the tool version, the execution classes, and the
acknowledgements. The transaction entity carries them under
`:db.schemaUpdate/*`.

These are ordinary queryable attributes, so the schema history of a database
is a Datalog query. An ordinary transaction cannot write them, so a
transaction cannot claim that it was a schema update.

### What an update cannot do

> **Partly implemented.** Every `rewrite` change is reported as blocked, and
> `--allow rewrite` does not enable it. Resolving cardinality conflicts,
> copying values to a replacement attribute, and sweeping current facts are
> jobs that do not exist yet. Do that work through ordinary transactions
> first, then plan again.

A blocked change stops the whole apply. Resolve it in the database, or remove
it from the file, and then apply the rest.

A value-type change in place is `destructive` and can never run. The plan
prints a replacement-attribute recipe instead. Follow it by hand.

1. Add a new attribute with the wanted type.
2. Convert the current values, and assert them under the new attribute.
3. Compare the counts, and record the values that no conversion accepted.
4. Move the application reads and writes to the new ident.
5. Retire the old attribute with `--prune`.

An explicit rename is not specified yet. Excision, which erases historical
facts, is separate work with its own approval path. `schema update` never
hard-deletes anything.

> **Not implemented.** A schema update points an attribute at an installed
> protection class. It cannot install a class. Decide the classes before you
> create the database.

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
A schema file that declares it is rejected.

An update allocates a new attribute id above the highest durable id. Database
creation keeps its positional allocation, so a database built from one file is
reproducible.
