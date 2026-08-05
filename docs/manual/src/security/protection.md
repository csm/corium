# Attribute protection

[Encryption at rest](encryption.md) protects the medium. Attribute protection
protects facts from readers.

Values on a protected attribute are sealed by the **writing peer** under the
key of a protection class. Only a process whose keyring resolves that key sees
them in the clear. The transactor does not. A peer without the key does not.
An operator with storage credentials does not.

## Declaring a class

A protection class names a key. It never holds key material. Declare classes
in the schema file that creates the database.

```toml
[protect.pii]
key = "file:/etc/corium/pii.key"
padding = 64
on-missing-key = "redact"

[[entity]]
name = "person"

[entity.attributes]
name = "string"
ssn  = { type = "string", protection = "protect/pii" }
```

A `[protect.<name>]` section declares the class `:protect/<name>`.

| Option | Values | Default |
|---|---|---|
| `key` | Key identity, for example `"file:/etc/corium/pii.key"` | Required |
| `algorithm` | `"aes-256-gcm-siv"` | `"aes-256-gcm-siv"` |
| `scope` | `"attribute"` or `"entity"` | `"attribute"` |
| `padding` | Bytes to round the plaintext up to. At least 16. | Unset |
| `on-missing-key` | `"redact"`, `"hide"`, or `"error"` | `"redact"` |
| `legacy-plaintext` | `"redact"` or `"pass-through"` | `"redact"` |
| `epoch` | Key epoch that new values seal under | `1` |

The EDN form declares the same class as an entity with `:db.protect/*`
attributes, and points an attribute at it with `:db/protection`.

> **Not implemented.** A schema update points an attribute at an installed
> class. It cannot install a class. Decide the classes before you create the
> database.

> **Partly implemented.** `scope = "entity"` parses and stores, and a writing
> peer refuses to seal under it. Use the default attribute scope today.

## What each option costs

**`scope`** decides what the sealing determinism leaks. Sealing must be
deterministic, because the transactor compares values as bytes and holds no
key. Under `"attribute"` a reader without the key can tell that two entities
share a value on that attribute. Under `"entity"` the seal also binds the
entity, so it leaks only the repeated value of that one entity.

**`padding`** rounds the plaintext up to a multiple of that many bytes before
sealing. It costs storage and removes the length side channel for short,
guessable values.

**`on-missing-key`** decides what a reader who cannot open a value gets.

| Policy | Result |
|---|---|
| `redact` | The value binds in redacted form. EDN prints `#corium/redacted`. SQL prints `NULL`. |
| `hide` | The datom is dropped from every scan. The entity leaves the join. |
| `error` | The read fails. |

Under all three, an unopenable value never satisfies a constant and never
satisfies a predicate. It binds, it disappears, or it raises. It never matches
by accident.

**`legacy-plaintext`** decides the same question for a value that was written
before the attribute became protected.

## Protection is not free

A protected attribute cannot also carry `:db/index`, `:db/unique`, or
`:db.type/ref`. Ciphertext order is not value order, so a protected value can
never appear in a value-ordered index. The schema rejects the combination
rather than surprising a range query later.

An attribute that has **ever** been protected can never gain `:db/index` or
`:db/unique`, even after it is unprotected again.

Where an application needs an indexed lookup on a protected field, add a
second unprotected attribute that holds a keyed hash of the value. That leak
is then explicit.

## Which processes need class keys

`--storage-key` is the keyring of the process. It resolves key-encryption keys
and protection class keys alike. Give a class key to the processes that must
read the class, and to no others.

```sh
corium peer-server --db people \
  --storage-key file:/etc/corium/storage.key \
  --storage-key file:/etc/corium/pii.key
```

A process with no class keys is a fully working peer. It commits, it indexes,
it syncs, and it answers every query that touches no protected value
identically. Protected values come back redacted, hidden, or refused, as the
class policy says.

`corium console`, `corium sql`, and `corium tui` take no key flag. They print
protected values in redacted form.

## Serving many principals from one process

A peer server and a PostgreSQL wire server serve many principals from one
process. Which of the keys of that process a request can use is a policy
question.

Name the key ids on a view, and bind the view to the relation that can read
them:

```clojure
{:authz.view/name "pii-reader" :authz.view/key ["file:/etc/corium/pii.key"]}
{:authz.binding/relation "hr" :authz.binding/object "database:people"
 :authz.binding/view "pii-reader"}
```

A guarded server defaults to the **strict** key policy. A principal whose
decision names no key id hydrates nothing.

| Mode | A decision that names no key id | Default when |
|---|---|---|
| `strict` | Grants no class key. | Authentication is configured. |
| `server-wide` | Grants the whole keyring of the process. | Authentication is off. |

`--key-policy strict` and `--key-policy server-wide` override the default on
`peer-server` and on `postgres-server`.

Three rules matter.

- Key grants combine across successful paths by intersection. One more
  relation can never widen a key set.
- Granting a key id that the process does not hold does nothing. Policy
  narrows the keyring of the process. It never extends it.
- `:authz.binding/unfiltered` grants full **attribute** visibility and **no**
  keys. Keys are named by key id, and that binding names none. A relation that
  must read protected values names them.

> **CAUTION: This is authorization, not cryptography.** A key-holding server
> still holds the plaintext and is choosing not to disclose it. A compromised
> or misconfigured server defeats it.
>
> For a genuinely less-trusted deployment, run the server with no class keys.
> Let each entitled application embed a peer with its own keyring, so the keys
> stay in the process that owns them.

> **Not implemented.** Seal-through mode, in which the server forwards sealed
> values and the thin client opens them itself, does not exist. A key set is
> always resolved in the server.

## Upgrading a guarded server that holds keys

Turning authorization on changes what a key-holding server discloses. Under
the derived `strict` default, a principal whose policy names no key id stops
seeing protected plaintext and starts seeing redactions.

That is the safe direction, and it is loud. Plan the change.

1. List the relations that must read each class.
2. Create one view per class, naming its key ids on `:authz.view/key`.
3. Bind each view to its relation with `:authz.binding/*`.
4. Restart the server with `--authz-db`.

To keep the old behavior while you write those grants, pass
`--key-policy server-wide` deliberately.

## Changing protection

Protection is **not** fixed at database creation.
[`corium schema update`](../running/schema.md#updating-an-installed-schema)
can protect an attribute, unprotect it, or move it to another class.

The change is forward-only. Values written from that basis onward take the new
form. Every value already stored keeps the form it had.

- Protecting an attribute does not seal the plaintext already stored.
- Unprotecting an attribute does not open the ciphertext already stored.

The plan reports both consequences and requires
`--ack protection-forward-only`. It also reports that lookup references and
value-ordered reads through the attribute stop working from that basis on.

> **Not implemented.** Sweeping the current values into the new form is a
> rewrite job that does not exist. There is no `corium keys protect`,
> `corium keys unprotect`, or `corium keys audit`. Class key rotation and
> crypto-shredding commands are also absent.

## What a protected value looks like on the wire

A sealed value needs thin-client protocol version 3. An older client never
receives one. Values reach a thin client as boundary EDN, which renders an
unopened value as `#corium/redacted`, a tagged element that every EDN reader
parses.
