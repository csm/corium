# Authorization

Servers authorize every request permit-all by default. `--authz-db <name>`
switches them to a relationship policy that is stored in an ordinary Corium
database.

The model is relationship-based, in the style of Google Zanzibar and OpenFGA.
A decision is a bounded, cycle-safe walk over relationship tuples.

The policy database is an ordinary database. Backup, restore, fork, `as-of`,
and the log API all work on it.

## Bootstrap

Bootstrap is two steps, in this order.

**Step 1.** Against a transactor started **without** `--authz-db`:

```sh
corium authz init --admin alice --provider oidc
corium authz grant 'group:eng#member' writer database:music
corium authz grant bob member group:eng
corium authz check bob transact --database music
```

`authz init` creates the database `corium_authz`, installs the reserved
schema, installs the default permissions, and grants the first administrator
`owner` on `catalog:*` and on `database:*`.

**Step 2.** Restart the surfaces with enforcement on:

```sh
corium transactor  --data-dir /srv/corium --authz-db corium_authz
corium peer-server --db music --authz-db corium_authz
```

## The default administrator

`authz init` defaults its administrator to `operator`, pinned to the provider
`static-token`. That is the identity a token client presents, so the CLI keeps
working after enforcement is on.

| Flag | Default | Effect |
|---|---|---|
| `--db <name>` | `corium_authz` | Policy database name. |
| `--admin <id>` | `operator` | Subject id of the first administrator. |
| `--provider <name>` | `static-token` | Provider that must vouch for the administrator. `any` accepts every provider. |
| `--no-admin` | Off | Install schema and permissions only. Grant nobody anything. |

## Subjects, relations, and objects

A tuple says `subject relation object`.

```sh
corium authz grant alice writer database:music
corium authz revoke alice writer database:music
```

| Position | Forms |
|---|---|
| Subject | `user:alice`, `group:eng`, `role:ops`, or the userset `group:eng#member`. A bare name reads as `user:<name>`. |
| Relation | A name, for example `owner`, `writer`, `viewer`, `member`, `parent`. |
| Object | `database:music`, `tenant:acme`, `catalog:*`, `database:*`. |

Relation names are data, not built-in values. The permissions decide which
relation satisfies which action.

## Actions

Fifteen actions exist. Each belongs to one class.

| Class | Actions |
|---|---|
| Read | `query`, `pull`, `datoms`, `tx-range`, `subscribe`, `inspect`, `list-databases` |
| Write | `transact` |
| Admin | `create-database`, `delete-database`, `fork-database`, `garbage-collect`, `manage-index`, `manage-keys`, `alter-schema` |

`alter-schema` is deliberately separate from `transact`. An application writer
that can add facts must not be able to broaden the vocabulary that it writes
them under. The action is admin-class and database-scoped, so the default
permissions give it to a database owner only. Read
[schema management](../running/schema.md#applying-a-plan).

The default permissions that `authz init` installs bind classes to relations.

| Object type | Class | Relations that satisfy it |
|---|---|---|
| `database` | read | `viewer`, `writer`, `owner` |
| `database` | write | `writer`, `owner` |
| `database` | admin | `owner` |
| `catalog` | read | `viewer`, `owner` |
| `catalog` | write | `owner` |
| `catalog` | admin | `owner` |

A permission entity can also name one action instead of a class, and `*`
matches any action or any object type.

## Testing a decision

```sh
corium authz check bob transact --database music
```

The command runs the same evaluator that a server runs, and it prints the
matched path. Use it before and after a change.

| Flag | Effect |
|---|---|
| `--database <name>` | Target database. Omit it for catalog-wide actions. |
| `--provider <name>` | Provider that vouched for the subject. Defaults to `oidc`. |
| `--role <name>` | A role that the credentials of the principal assert. Repeatable. |
| `--claim <key>=<value>` | A claim that the principal carries. Repeatable. |

## Status

```sh
corium authz status
```

The command prints the compiled basis, `:authz-t`, and the entity counts.

## Operating notes

**Fail closed.** A surface that cannot read or compile the policy denies every
request. It does not refuse to start. It logs the remedy and recovers on its
own when the database appears, so an ordering mistake is not fatal.

**Changes propagate without a restart.** Each server watches the policy
database and recompiles off the request path. A grant takes effect in
milliseconds.

**Every decision is logged with its basis.** The tracing target is
`corium_authz::audit`. Denials log at `info`. Grants log at `debug`.

**`--authz-fresh-writes`** makes write and admin actions re-read the policy
before they decide. The cost is one snapshot read per such request. Reads keep
using the pinned snapshot.

**`--authz-max-depth <n>`** bounds the relation hops of one check. The default
is 8.

**`--authz-break-glass-role <role>`** admits a role while the policy is
*unreadable*. It never overrides a deny.

## Recovering from a lockout

A policy that denies everyone cannot be repaired through the policy.

1. Stop the transactor.
2. Start it again without `--authz-db`.
3. Fix the tuples with `corium authz grant`.
4. Stop it, and start it again with `--authz-db`.

Break-glass does not help here, because the policy is readable. It denies.

## Interaction with authentication

`--authz-db` conflicts with `--serve-open`, because that flag authorizes
requests whose identity was never established.

An anonymous caller is still admitted in permissive mode. With an authorizer
in place, that caller arrives as `user:anonymous`. Public read with
authenticated write is therefore a policy question, not a flag.

## Views: attribute and key filtering

A **view** narrows what a successful read returns. A **binding** attaches a
view to one relation on one object.

```clojure
;; Hide every attribute outside the allowlist.
{:authz.view/name "support"
 :authz.view/filter-type "attribute-allowlist"
 :authz.view/attribute [":person/name" ":person/city"]}

{:authz.binding/relation "support"
 :authz.binding/object "database:people"
 :authz.binding/view "support"}
```

| Attribute | Meaning |
|---|---|
| `:authz.view/name` | Name a binding refers to. Unique. |
| `:authz.view/filter-type` | `attribute-allowlist` or `attribute-denylist`. Optional. |
| `:authz.view/attribute` | Attribute idents the filter names. Repeatable. |
| `:authz.view/key` | Protection class key ids the view permits. Repeatable. |
| `:authz.binding/relation` | Relation the view attaches to. |
| `:authz.binding/object` | Object the view attaches to. `type:*` is allowed. |
| `:authz.binding/view` | Name of the view to apply. |
| `:authz.binding/unfiltered` | Marks the relation as granting full attribute visibility. |

A view can name attributes, key ids, or both. `:authz.view/filter-type` is
needed only when the view names attributes.

### Hidden means hidden

A datom on a hidden attribute is dropped inside the scan. It never binds, it
never joins, and it never satisfies a predicate.

Every path that reaches an index directly is closed with it. A pull omits the
key. SQL reports `NULL` and refuses to push a predicate down that column.
`get-else` falls to its default. `missing?` reports missing. A lookup
reference does not resolve. Reverse-reference traversal returns nothing.

Each of those is an existence test over the attribute that the view
withholds.

### Combining views

When several relations succeed, their views **intersect**. Holding one more
relation can never reveal more than holding it alone.

`:authz.binding/unfiltered` is the escape. Use it for a relation such as
`owner` that must see everything.

> **CAUTION: `:authz.binding/unfiltered` grants attributes and no keys.** Keys
> are named by key id, and that binding names none. A relation that must read
> protected values names those key ids on a view.

### Key grants

`:authz.view/key` names the protection class keys that a principal can use.
Read [attribute protection](protection.md), which states the key policy modes
and the strict default.

### Surfaces that cannot filter refuse

A principal whose view hides attributes cannot write on any surface. The peer
server refuses `Transact` and `Subscribe`. The transactor refuses a write. SQL
refuses DML.

Transaction data is opaque bytes by the time authorization runs, and
`Subscribe` proxies the stream of the transactor, so neither can honor a
filter. All four ask the same question, so one principal under one policy gets
one answer whichever surface it reached.

A view that names every attribute takes nothing away, so it is not treated as
hiding. A view that restricts only keys is refused nowhere.

## Protecting the policy database

The policy database is governed by the policy that it holds. The `database:*`
ownership of the administrator is what keeps `corium authz grant` working.

Back it up like any other database. Read
[backup and restore](../availability/backup.md).
