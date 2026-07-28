# Encryption at Rest and Attribute Protection

Status: **specified, not implemented.** This document proposes two independent
layers — envelope encryption of every durable artifact
([ADR-0017](../adr/0017-encryption-at-rest.md)) and per-attribute protection
classes keyed by separate data keys
([ADR-0018](../adr/0018-attribute-protection-classes.md)) — and fixes the
formats, schema, APIs, and operational surface they need. Nothing here changes
the peer's public interface: a peer still connects, reads storage itself, holds
an immutable `Db`, and runs queries locally. What changes is that some values
arrive **sealed**, and a peer hydrates exactly the ones whose class key it
holds.

## The two layers

```
                        plaintext datoms
                              │
   ┌──────────────────────────┼───────────────────────────┐
   │ layer 2: attribute protection (per-class keys)       │
   │   values on protected attributes are sealed HERE,    │
   │   in the writing peer, before tx-data leaves it      │
   └──────────────────────────┼───────────────────────────┘
                              │ datoms whose v may be Sealed{class, epoch, ct}
                      transactor pipeline
                      log · index job · peers
                              │
   ┌──────────────────────────┼───────────────────────────┐
   │ layer 1: storage encryption (one DEK per database)   │
   │   log records, index blobs, backups, cached segments │
   └──────────────────────────┼───────────────────────────┘
                              │
                     disk / object store
```

Layer 1 protects the **medium**: a stolen disk, a readable bucket, a copied
backup file. Everyone inside the deployment sees plaintext once the bytes are
decrypted, which is the property that makes it cheap and invisible.

Layer 2 protects **facts from readers**: a peer, a peer server, an operator, or
the transactor itself may hold the whole database and still be unable to read
salaries. It is the layer the "multiple encryption keys" requirement is about,
and the only one that survives a compromised process inside the deployment.

They compose without knowing about each other: a sealed value is opaque bytes by
the time layer 1 sees it, so layer 1 encrypts and compresses it like anything
else, and layer 2 never has to care where the datom is eventually written.

## Threat model

| Adversary | Layer 1 alone | Layer 1 + protected attribute |
|---|---|---|
| Stolen disk, snapshot, or object-store bucket | protected | protected |
| Copied backup archive | protected | protected |
| Operator with storage credentials | protected | protected |
| Compromised **peer** process without the class key | plaintext | protected |
| Compromised **transactor** | plaintext | protected (cannot read; cannot forge) |
| Thin client / SQL session without the class key | plaintext | protected |
| Query-authorized insider (`Allow` from the authorizer) | plaintext | protected unless granted the key |
| Network interception | TLS | TLS + sealed |

Explicit non-goals. None of this hides:

- **Structure.** Which entities exist, which attributes they carry, how many
  datoms each has, which transaction asserted what and when. A keyless reader
  sees the entire skeleton; only values on protected attributes are opaque.
- **Volume and timing.** Database size, transaction rate, commit instants,
  index publication cadence.
- **Value length.** Ciphertext length reveals plaintext length within the AEAD
  expansion, unless the class enables padding (below).
- **Access patterns.** Which segments a peer fetches, which entities a query
  touches.
- **Availability.** A transactor without the class key can still drop,
  duplicate, or refuse writes; layer 2 buys confidentiality and per-fact
  authenticity, not liveness.
- **Key-holder misbehaviour.** A reader granted a class key can read and copy
  every value in that class. Encryption bounds *who can*, not *what they do
  next*.

## Keys and keyrings

Four kinds of key material, in one hierarchy:

| Key | Scope | Lives | Held by |
|---|---|---|---|
| **KEK** (key-encryption key) | deployment or database | KMS / HSM / operator file; never in Corium | nothing in-process; used to unwrap |
| **Storage DEK** | one database, per epoch | wrapped, in the `keys:<db>` root record | transactor, GC, backup, any peer reading storage directly |
| **Class key** | one protection class, per epoch | resolved by key id from the reader's keyring; **never stored in Corium** | only processes granted that class |
| **Derived subkeys** | per blob / per record / per fact | derived, never stored | whoever holds the parent DEK |

The asymmetry is the point. Storage DEKs are stored wrapped, because a restore
must be able to bootstrap itself from the archive plus KMS access. Class keys
are *not* stored at all: the database records only a key **id**, and a process
gets material by resolving that id through its own keyring. Corium therefore
never decides who may hydrate an attribute — the KMS or keyring grant does. That
separation is deliberate; see [Keys versus authorization](#keys-versus-authorization).

### `corium-crypt`

A new pure-library crate below everything else, next to `corium-core` in the
dependency order (no tokio in the primitives; the keyring trait is async
because KMS calls are):

```rust
/// Opaque, zeroized key material.
pub struct SecretKey(/* [u8; 32], Zeroizing */);

/// A key identity as written in a root record or a class entity.
/// Rendered as a URI: `file:/etc/corium/pii.key`, `env:CORIUM_PII_KEY`,
/// `awskms:arn:aws:kms:…`, `gcpkms:projects/…`, `vault:transit/keys/pii`.
pub struct KeyId(String);

#[async_trait]
pub trait Keyring: Send + Sync {
    /// Material for a specific epoch; used to read existing data.
    async fn key(&self, id: &KeyId, epoch: u32) -> Result<SecretKey, KeyError>;
    /// The epoch new writes use.
    async fn current_epoch(&self, id: &KeyId) -> Result<u32, KeyError>;
    /// Wrap/unwrap for stored DEKs (KMS-backed rings do this remotely).
    async fn wrap(&self, id: &KeyId, epoch: u32, dek: &SecretKey) -> Result<Vec<u8>, KeyError>;
    async fn unwrap(&self, id: &KeyId, epoch: u32, wrapped: &[u8]) -> Result<SecretKey, KeyError>;
    /// Which key ids this process can resolve at all — the hydration set.
    fn key_ids(&self) -> &[KeyId];
}
```

Shipped implementations, mirroring how `TokenVerifier`/OIDC already stage a
seam and its concrete backend: `StaticKeyring` (files, environment variables,
in-memory test keys) in the base crate; `KmsKeyring` over a small `KmsClient`
trait behind feature flags (`aws-kms`, `gcp-kms`, `vault`). `CompositeKeyring`
tries a list in order so one process can take its storage key from a file and
its class keys from KMS.

Primitives:

- **AEAD:** AES-256-GCM for blobs and log records (unique derived nonce per
  object); **AES-256-GCM-SIV** (RFC 8452, zero nonce, full context in the AAD)
  for sealed values, because value sealing must be deterministic — see
  [Fact identity](#fact-identity-and-why-sealing-is-deterministic). The
  algorithm is a stored field everywhere it is used, so a future
  XChaCha20-Poly1305 or FIPS-mode backend is a value, not a rewrite.
- **Derivation:** BLAKE3 in keyed mode as an HKDF-equivalent, already a
  workspace dependency.
- **Hygiene:** `zeroize` on all material, no key in `Debug`, no key in metrics
  or traces, `KeyError` never quotes bytes. Operators should disable core dumps
  on processes holding class keys; the docs say so and the CLI logs a warning
  when it detects `RLIMIT_CORE` unset.

## Layer 1 — storage encryption

One DEK per database per epoch, wrapped under the deployment KEK. Enabled by
`--storage-key <key-uri>` at database creation; a database created without it
stays unencrypted forever unless migrated by backup/restore.

### Index blobs

Today a blob id is the BLAKE3 hash of the plaintext bytes and the caller
computes it before `put`. Under encryption the id becomes **the hash of the
stored (encrypted) object**:

```
object := header ‖ ciphertext ‖ tag
header := magic "CORIUMB1" ‖ alg:u8 ‖ epoch:u32 ‖ plaintext-len:u64
nonce  := BLAKE3_keyed(dek, "corium/blob-nonce" ‖ header ‖ blake3(plaintext))[..12]
AAD    := header
id     := blake3(object)
```

Deriving the nonce from the plaintext digest makes encryption **deterministic
for a given (epoch, content)**, which is what preserves every property the
segment design depends on:

- `put` stays idempotent, and re-publishing an unchanged leaf produces the same
  id, so structural sharing and incremental publication are untouched.
- The store still verifies integrity by re-hashing the bytes it holds — with no
  key. GC, backup, and the segment cache keep working without ever decrypting.
- Mixed-epoch trees are legal: the epoch is per object, and a carried-over leaf
  keeps its old id and old epoch until something rewrites it.

What is lost: cross-database blob deduplication (identical content in two
databases with different DEKs is two objects). That is a fair price and, for a
multi-tenant deployment, an improvement.

API shape: encryption is a **decorator** — `EncryptedBlobStore<S: BlobStore>` —
so `mark_and_sweep`, `index_blob_children`, backup, and every reader see
plaintext and stay unchanged. The one signature adjustment is that the store,
not the caller, now computes the id: `put_content(bytes) -> BlobId` alongside
the existing `put(&id, bytes)` (which a plaintext store keeps and an encrypted
store rejects for non-matching ids).

**Placement in the read stack matters:**

```
peer read → EncryptedBlobStore → SegmentCache → FsStore/S3/Postgres/Turso
```

The decorator sits *above* the cache, so the SSD tier holds ciphertext and its
digest check is unchanged. This supersedes
[peer-segment-cache.md](peer-segment-cache.md)'s current reliance on host
filesystem encryption for cached data.

### Transaction log

The frame header (length, checksum flag) and the CRC32C stay cleartext, so
range scans, recovery truncation, and the existing framing machinery are
untouched. The record **payload** is encrypted:

```
nonce := BLAKE3_keyed(dek, "corium/log-nonce" ‖ log-version:u64 ‖ t:u64)[..12]
AAD   := db-lineage-id ‖ log-version ‖ t ‖ epoch
```

Binding `(log-version, t)` means a record cannot be replayed at another basis or
moved between the per-lease-version log files that M7 fencing relies on. The
CRC32C still covers framing (cheap corruption detection during scans); the AEAD
tag is the authenticity check.

### Roots and the key manifest

Root records stay **cleartext**. They hold no user data — database name, basis
numbers, blob ids, lease state — and `RootStore::compare_and_set` compares
bytes, which deterministic encryption would only complicate. What a root leaks
is listed under non-goals above.

A new root record per database, `keys:<db>`, is the key manifest:

```
KeyManifest {
  format-version,
  kek: KeyId,
  storage-keys: [ { epoch, wrapped-dek, alg, created-at, state } ],   // active | retiring | retired
  classes:      [ { class-entity-id, key-id, current-epoch } ],       // ids only, never material
}
```

`DbRoot` gains a `key-manifest-version` field (storage format 4) so a reader
detects an encrypted database before it tries to parse a blob, and fails with
"database is encrypted; no storage key configured" instead of a decode error.

The class table in the manifest is a **cache of what the schema already says**
(class entities live in `:db.part/db`, below) so that a process can discover
which key ids it needs before it can read any datoms. It is advisory; the schema
is authoritative.

### Backups

Backup format version 2:

- Header gains `Content encryption: u32` and `Key manifest: bytes`.
- `BLOB` frames carry stored objects **verbatim** — no decrypt/re-encrypt — so
  backup remains a byte copy and needs no storage key to *copy*, only to walk
  manifests.
- `CKPT` transaction records stay log-framed, hence still encrypted.

Restoring into a deployment that can unwrap the archive's KEK yields a working
database. Restoring where the KEK is unavailable fails cleanly at open.
Restoring *without the class keys* yields a fully functional database whose
protected attributes are permanently redacted — which is exactly what you want
when shipping production data to a staging environment.

### Rotation

| Rotation | Cost | Trigger |
|---|---|---|
| **KEK** | re-wrap each DEK in the manifest; no data touched | `corium keys rewrap --kek <uri>` |
| **Storage DEK** | new epoch; new writes use it; old objects stay readable under the retained epoch | `corium keys rotate --storage` |
| **Class key** | forward-only, see [Class key rotation](#class-key-rotation-and-crypto-shredding) | `corium keys rotate --class :protect/pii` |

A storage epoch retires only when no live object carries it. The manifest tracks
a per-epoch live-object count maintained by the same mark pass GC already runs,
and `corium keys status` prints it; a forced full index rebuild is the way to
drain an epoch deliberately.

## Layer 2 — attribute protection classes

### Schema

Two additions, both ordinary data:

```clojure
;; A protection class: an entity in :db.part/db naming a key, not holding one.
{:db/id                     "pii"
 :db.protect/ident          :protect/pii
 :db.protect/key            "awskms:arn:aws:kms:us-west-2:…:key/2f1c…"
 :db.protect/algorithm      :db.protect.alg/aes-256-gcm-siv
 :db.protect/scope          :db.protect.scope/attribute   ; or /entity
 :db.protect/padding        64                            ; optional, bytes
 :db.protect/on-missing-key :db.protect.missing/redact    ; or /hide, /error
 :db.protect/legacy-plaintext :db.protect.legacy/redact}  ; or /pass-through

;; An attribute in that class.
{:db/ident       :person/ssn
 :db/valueType   :db.type/string
 :db/cardinality :db.cardinality/one
 :db/protection  :protect/pii}
```

`docs/schema-toml.md` gains the same field under an attribute's table
(`protection = "protect/pii"`) and a `[protect.<name>]` section for classes.

Rules the transactor enforces, all without holding any key:

1. `:db/protection` may not be combined with `:db/index`, `:db/unique`, or
   `:db/valueType :db.type/ref` — this is the "protected datoms cannot be
   indexed" rule, made checkable at schema-install time.
2. `:db/protection` may not be asserted on schema attributes, `:db/ident`,
   `:db/txInstant`, or any attribute in the reserved id range.
3. `:db/protection` may be asserted, retracted, or changed on a populated
   attribute, and takes effect **forward only** — see
   [Changing protection](#changing-protection). An attribute that has ever been
   protected may never afterwards gain `:db/index` or `:db/unique`.
4. **Assertions** must use the form the attribute has *now*: sealed under the
   current class and current epoch if it is protected, plaintext if it is not.
   A client that does not understand protection cannot accidentally write
   plaintext into a protected attribute.
5. **Retractions**, and the old-value position of `:db/cas`, may use any form
   the attribute has ever had — plaintext, or sealed under any class and epoch
   in its protection timeline. A fact can only be retracted by naming the bytes
   it was asserted as, and those bytes do not change when the schema does.
6. A sealed value's cleartext header must name a class in the attribute's
   timeline and a known epoch, and its declared value type must match
   `:db/valueType`.

Rules 4 and 5 make the schema cache carry a **protection timeline** per
attribute — the `(t, class)` pairs its `:db/protection` datoms already record —
rather than a single current class. The form required at basis `t` is what the
timeline says at `t`; the forms *accepted* for a retraction are every entry in
it.

### The sealed value

A new `Value` variant and a new tag in the sortable encoding (`0xA0`, above
`REF`'s `0x90`; a protected attribute never mixes sealed and plaintext values,
so cross-type ordering is a formality):

```rust
pub struct Sealed {
    pub class: EntityId,   // the class entity — self-describing across fork/restore
    pub epoch: u32,
    pub vtype: ValueType,  // cleartext: needed for redaction rendering and validation
    pub body: Arc<[u8]>,   // deterministic AEAD ciphertext ‖ 16-byte tag
}
pub enum Value { /* … */ Sealed(Sealed) }
```

Encoded as `0xA0 ‖ class ‖ epoch ‖ vtype ‖ len ‖ body`, self-delimiting like
every other value encoding, so `Datom::key`, `key_components`, and
`Datom::from_key` need no structural change.

Sealing:

```
context := class-key-id ‖ epoch ‖ attr-id [‖ entity-id if scope = entity]
AAD     := "corium/seal-v1" ‖ context ‖ vtype
body    := AES-256-GCM-SIV(key, nonce = 0, aad = AAD, plaintext = encode_value(v))
```

Type-specific rules:

- **Keyword** values seal their *text*, not their interned id, so a protected
  keyword vocabulary never enters the keyword table (where it would be readable
  in the clear).
- **Ref** values cannot be protected (rule 1): they belong in VAET, and the
  target id would leak the edge anyway.
- **Padding**, when a class sets it, rounds the plaintext up to a multiple of
  `:db.protect/padding` bytes before sealing, with the true length recovered
  from the decoded value. It costs storage and removes the length side channel
  for short, guessable values.

### Fact identity, and why sealing is deterministic

The index key *is* the datom. A retraction cancels an assertion by sharing the
`(e, a, v)` byte prefix, and the current-value fold keeps at most one entry per
prefix. Randomized encryption would give the same fact two different byte
representations, and retraction, cardinality-one supersession, cardinality-many
deduplication, and `:db/cas` would all break — on a transactor that has no key
and therefore cannot compare plaintext.

So sealing is deterministic: identical plaintext in the same context produces
identical bytes. Everything the transactor does with values it does bytewise,
exactly as today, and it keeps working with no key at all.

The cost is equality leakage, and the class's **scope** chooses how much:

| Scope | Context | A keyless reader can tell… | Write cost |
|---|---|---|---|
| `:db.protect.scope/attribute` (default) | key, epoch, attribute | …that two entities share a value on that attribute, and can count distinct values (frequency analysis; for a low-cardinality attribute such as a status enum, that is close to plaintext) | none |
| `:db.protect.scope/entity` | key, epoch, attribute, **entity** | …only that one entity's value repeated over time | an id-reservation round trip and a basis fence |

Entity scope also strengthens integrity: the AAD binds the entity, so a
compromised transactor cannot move a ciphertext from one subject to another.
Under attribute scope it can. For adversarial-transactor deployments that, more
than the equality leak, is the reason to pay for entity scope.

**Entity scope and tempids.** Sealing needs the entity id, and a tempid does not
have one until the transactor resolves it. The writing peer therefore resolves
first:

1. Tempids that would upsert through a `:db.unique/identity` attribute are
   resolved locally against the peer's own `Db` (unique attributes are never
   protected, so this lookup is exact and local).
2. Remaining tempids get ids from a new `Catalog.ReserveEntityIds(partition, n)`
   RPC — a bump of the same monotonic allocator the transactor already owns.
   Unused reservations simply leave gaps, which the allocator already tolerates.
3. The transaction is submitted with concrete ids and `expected_basis_t` (the
   protocol-v2 fence, already implemented) set to the basis the local resolution
   was computed against. A concurrent write that would have changed the upsert
   outcome rejects the transaction and the peer retries.

The fence is what makes this safe rather than racy: without it, a concurrent
upsert could bind the transaction to a different entity than the one the values
were sealed for, and the mismatch would surface much later as a decryption
failure.

### What "cannot be indexed" means in practice

| Index | Protected datoms |
|---|---|
| EAVT | present — entity access and pull must work |
| AEVT | present — attribute scans must work |
| AVET | **absent** — `:db/index`/`:db/unique` are rejected at schema install |
| VAET | **absent** — refs cannot be protected |

Consequences the planner and executor must implement:

- A pattern with a bound value position on a protected attribute is served by an
  AEVT scan comparing sealed bytes; it never selects AVET, and the planner's
  "never full-scan when `a` is bound" rule still holds.
- Bound-value matching works only under attribute scope, and only for a caller
  holding the key (it must seal the constant to compare). Under entity scope the
  scan hydrates candidates and compares plaintext. Either way the cost is a scan
  of the attribute, and `explain` says so.
- `index-range` and range predicates over a protected attribute are **errors**,
  not silent nonsense — sealed order is byte order, which is not value order.
- `PlannerStats` records protected attributes' datom counts but contributes no
  distinct-value estimate; the planner treats a bound protected value position
  as unbound for selectivity. (Distinct *ciphertext* counts under attribute
  scope would be a real estimate and a real leak; the estimate is dropped
  rather than used.)
- `min`/`max`/`sum`/`avg` over an unhydrated sealed value raise
  `QueryError::Protected`; `count`, `count-distinct`, and grouping work.

**Buying lookup back: the blind-index recipe.** When an application genuinely
needs indexed lookup or uniqueness on a protected field, it stores a second,
*unprotected* attribute holding a keyed hash:

```clojure
{:db/ident :person/email       :db/valueType :db.type/string :db/protection :protect/pii}
{:db/ident :person/email-hmac  :db/valueType :db.type/bytes  :db/unique :db.unique/identity}
```

The writing peer computes `email-hmac = BLAKE3_keyed(blind-index-key, email)`
and transacts both. Lookup and uniqueness are then ordinary AVET work, at the
documented price: the blind index is deterministic, indexed, unprotected data,
and it leaks equality by construction. Keeping it a recipe rather than an engine
feature keeps that price visible. (An engine-level `:db.protect/blind-index`
that derives and maintains the second attribute automatically is a plausible
follow-up; it would not change any of the above.)

### Changing protection

Protecting an attribute, unprotecting it, or moving it to another class are all
legal alterations, and all of them are **forward-only**: a datom keeps the form
it was asserted in, forever. Nothing rewrites the log, and nothing re-encrypts
history, because that is the one thing an immutable database does not do.

That is the consistent answer, and it is also the surprising one — "I protected
the attribute" reads as "the data is now protected", and cryptographically it is
not. The design's job is to make the gap visible, bounded, and closable rather
than to pretend the flip did more than it did. Three mechanisms do that, in
increasing order of strength and cost.

#### What each transition does

| Alteration | Datoms before `t` | Datoms after `t` |
|---|---|---|
| **Protect** (assert `:db/protection`) | plaintext, in the log, the indexes, every peer's memory, and every existing backup | sealed under the new class |
| **Unprotect** (retract it) | sealed; still need the class key to read, forever | plaintext |
| **Re-classify** (change the class) | sealed under the old class; readable by holders of the *old* key | sealed under the new class |
| **Rotate the class key** | sealed under the old epoch | sealed under the new epoch |

A sealed value carries its class and epoch in its cleartext header, so a mixed
attribute needs no schema archaeology to read: each datom says what it needs.
That is why re-classification works at all, and why the timeline in schema rule 5
is a validation device rather than a decoding one.

The one hard prohibition is re-indexing. An attribute that has ever held sealed
datoms may never gain `:db/index` or `:db/unique`, because AVET would then mix
value-ordered plaintext with byte-ordered ciphertext: range scans would return
silently wrong answers and uniqueness would silently not be enforced. Excluding
the sealed datoms instead would make AVET *incomplete*, which is worse — a
lookup ref would miss them. Symmetrically, protecting an attribute that is
currently `:db/index` or `:db/unique` requires retracting those in the same
transaction; the next publication stops emitting AVET entries for it, and
**lookup refs through that attribute stop working**, which is usually the part
that breaks application code.

#### 1. Legacy plaintext is redacted on read, by default

A reader that cannot hydrate an attribute's sealed values does not get its
legacy plaintext either. `:db.protect/legacy-plaintext` defaults to `redact`:
a plaintext datom on an attribute that is *currently* protected is treated
exactly like a sealed value the reader has no key for, following the class's
`on-missing-key` policy. `pass-through` restores the literal behaviour for
deployments that want it.

This is policy, not cryptography — the same strength as a `ViewFilter`, enforced
by the serving process, defeated by anyone who can read segments directly. But
it is immediate, free, and it means the *ordinary* consequence of protecting an
attribute is that keyless readers stop seeing its values, which is what the
operator expected in the first place. Key holders are unaffected: they see
plaintext for old datoms and hydrated values for new ones, uniformly.

Deliberate wrinkle: this policy is evaluated against the **current** schema, not
the schema of the time view being read. An `as-of` before the protection basis
would otherwise hand back the plaintext by time-travelling under the policy.
Schema-as-data says the old view had no protection; confidentiality says the
answer is no. Confidentiality wins, and the exception is documented here
because it is the one place where a read does not see the schema of its own
basis.

#### 2. The sweep makes the current value cryptographically protected

Redaction does not help against a reader with storage access, and protection
alone does not seal a value that nobody has re-asserted since. `corium keys
protect <attr> --class <c> --sweep` performs the alteration and then, from a
key-holding peer, walks AEVT and re-asserts every current value in sealed form
(retracting the plaintext datom and asserting its sealed twin, chunked into
bounded transactions, resumable, and idempotent — a value already sealed under
the current class is skipped).

After a sweep the *current* database value holds no plaintext for that
attribute. History still does, and `as-of` before the sweep still yields it,
subject to the redaction policy above. A `:db/noHistory` attribute is the happy
exception: nothing retains the superseded plaintext in any index, so a sweep
leaves it only in the log — which is the closest thing to retroactive protection
that does not involve rebuilding or shredding.

Cardinality-many needs care in both directions: across an unswept transition an
entity can hold both a plaintext and a sealed copy of the same value — they are
distinct datoms with distinct keys, so a key holder sees a duplicate. The sweep
removes it; without the sweep it is a real artifact of a mid-life change and
worth stating plainly. Cardinality-one cannot exhibit this, since supersession
keys on `(e, a)`.

#### 3. Retroactivity, when it is actually required

Only two things make a protection change retroactive, and both are heavy:

- **Rebuild.** Restore or fork through a key-holding peer that seals on the way
  in, producing a database whose log never contained the plaintext. This is the
  only option that also clears history, existing backups (they are new
  artifacts), and the published index roots. It yields a new database identity.
- **Re-classify, then shred.** Sweep the current values into a new class, then
  destroy the old class key. The old ciphertext stays where it is and becomes
  permanently unreadable — including in history, in every backup, and on every
  peer. This is the composition that makes crypto-shredding worth having:
  key destruction is what turns a forward-only change into a retroactive one,
  and it works *because* the past is immutable rather than in spite of it.

Shredding is class-granular, so it only expresses "this attribute's history"
when the attribute owns its class. The operational rule that follows: **give an
attribute its own class whenever you might want to re-classify or shred it
independently.** Classes are cheap; entanglement is not.

The mirror-image hazard is unprotecting: once an attribute has sealed datoms,
its class key must be retained for as long as that history matters. Shredding
after unprotecting destroys the old values while the attribute reads as
unprotected in the schema, which is the most confusing possible state to arrive
at by accident.

#### Making the gap measurable

A change this consequential should not be silent, and its residue should be a
number rather than a worry:

- The alteration is rejected unless the transaction acknowledges it —
  `[:db/add "datomic.tx" :db.protect/acknowledge-forward-only true]`, which the
  CLI sets for `corium keys protect` and which a hand-written transaction must
  set deliberately. The point is not ceremony; it is that the acknowledgement
  is recorded on the transaction entity, so the schema history says who
  accepted the semantics and when.
- `corium keys audit <attr>` reports the exposure directly: the protection
  basis `t`, how many *current* values are still plaintext, how many historical
  plaintext datoms exist below `t`, and how many published index roots and
  backup archives still contain them. A sweep drives the first to zero; only a
  rebuild or a shred drives the rest there.
- `corium keys status` prints each attribute's protection timeline, so
  "protected since" is answerable without querying the schema history by hand.

### Write path

Sealing happens in the **writing peer**, in `corium-peer`'s transact path,
before tx-data is encoded onto the wire — the peer already holds the schema, so
it knows every attribute's class:

1. Expand map/list forms far enough to see `(a, v)` pairs (`corium-tx`'s
   expansion is a pure function and already reusable here).
2. For each attribute protected *at the peer's current basis*, resolve the
   class, get key material from the keyring, seal, and substitute. Retractions
   are the exception: a retraction carries the bytes of the fact being
   retracted, which the peer takes from its own `Db` rather than re-sealing, so
   it names the right form even when that form predates the current class (or
   predates protection entirely).
3. Missing key for a class the transaction writes → refuse the transaction with
   `PeerError::MissingKey(class)`. Writing is never partial.

Because sealing reads the peer's schema and the transactor validates against
its own, a protection change committed between the two rejects the transaction
rather than storing a value in the stale form — the same staleness the
`expected_basis_t` fence exists for, and the peer retries against the new
schema.

The transactor validates, orders, logs, and indexes sealed values without ever
resolving a key. Specifically it still performs: tempid resolution, cardinality
enforcement, retraction pairing, `:db/cas` (a bytewise compare of two sealed
values — which works precisely because sealing is deterministic), and every
uniqueness check (which protected attributes never have).

The exception is **database functions**. A `:db/fn` running on the transactor
receives sealed values and cannot branch on their plaintext. That is a real
limitation of keeping keys out of the transactor, and the answer is to do that
work in the peer, guarded by `expected_basis_t`, rather than to hand the
transactor keys. `:db/cas` on a protected attribute is the one comparison that
does still work.

### Read path and hydration

Values stay sealed inside the engine — in segments, in the live index, in join
keys — and hydration is a property of the **read**, not of the `Db` value. That
matters because one peer server serves many principals with different key sets
from the same `Db`.

- `corium-peer`: `ConnectConfig::with_keyring(Arc<dyn Keyring>)`. A `Db`
  obtained from that connection carries the connection's keyring by default, so
  the embedded-peer API is unchanged for an application that just wants its
  values.
- `corium-query`: `ExecOptions` gains a `hydrator: Option<Arc<Hydrator>>`.
  Hydration is applied **at scan output** — a datom leaving an index scan on a
  protected attribute is hydrated if the key is available — so predicates,
  functions, aggregates, sorting, and pull all see plaintext with no further
  changes. A bounded per-connection plaintext cache keyed by ciphertext digest
  keeps repeated scans of the same values off the AEAD path.
- The **query cache** keys on a fingerprint of the hydration key set alongside
  the query and basis, for the same reason the deferred `ViewFilter` work must:
  a result computed with a key must never be served to a caller without it.

When a value cannot be hydrated, the class's `:db.protect/on-missing-key`
decides, and a request may narrow (never widen) it:

| Policy | Behaviour |
|---|---|
| `redact` (default) | The value binds as `Value::Sealed` and renders as `#corium/redacted {:class :protect/pii :type :db.type/string}` in EDN, `NULL` in SQL, the sealed tag on the wire. Structure is visible; the value is not. |
| `hide` | The datom is filtered out of scan results entirely, so `[?e :person/ssn ?s]` binds nothing and the entity drops out of that join — the same shape as an attribute denylist `ViewFilter`. |
| `error` | The read fails with `QueryError::Protected(class)`. For deployments that would rather see a loud failure than a quiet hole. |

The same three policies cover legacy plaintext — a datom asserted before the
attribute was protected, when `:db.protect/legacy-plaintext` is `redact` (see
[Changing protection](#changing-protection)). Both checks happen at the same
place in the scan, against the same key set: the question "can this reader have
this attribute's values?" is asked once, and its answer does not depend on which
side of a schema change the datom fell on.

Under every policy, an unhydratable value never satisfies a value-position
constant and never satisfies a predicate. It binds, or it disappears, or it
raises — it never matches by accident.

### Peer server, thin clients, SQL

- **Peer server holding keys.** Hydrates per request. The key set for a request
  comes from a `KeyPolicy: Principal → [KeyId]`, which is where this meets
  authorization: the ReBAC policy can name key ids the same way it names view
  filters. Plaintext then travels to the thin client over TLS.
- **Peer server in seal-through mode** (`--seal-through`). Returns sealed values
  and lets the thin client hydrate with its own keyring — end-to-end protection
  for languages using the thin protocol, at the cost of client-side key
  distribution. This requires the sealed tag in the wire codec and a
  thin-client protocol version bump (v3); a v2 client that would receive a
  sealed value gets `FAILED_PRECONDITION` rather than bytes it cannot name.
- **SQL / pgwire.** A protected column keeps its declared type and reports
  `NULL` when unhydrated. The planner refuses pushdown of any predicate over a
  protected column, and rewrites `=` to a sealed-bytes comparison when the
  session holds the key and the class is attribute-scoped. `corium sql` prints
  `<redacted>`. Session key sets come from the same `KeyPolicy` as the peer
  server.

### Class key rotation and crypto-shredding

Rotation is **forward-only**. New assertions seal under the new epoch; existing
datoms keep the old one, because the database is immutable and history is not
rewritten. A reader needs every epoch it wants to read, and
`corium keys status --class :protect/pii` prints the epochs in use with datom
counts per epoch.

An **epoch pins the whole crypto parameter set** — key material, algorithm,
scope, and padding — because all four are baked into existing ciphertext and its
AAD. Changing any of them on a class therefore mints a new epoch and is
forward-only in exactly the way a key rotation is; a class whose scope changes
from attribute to entity protects new values more strongly and cannot
retroactively protect old ones. The class's *read* policies —
`:db.protect/on-missing-key` and `:db.protect/legacy-plaintext` — are not crypto
parameters, so they change freely and apply immediately to every epoch.

The corollary is a genuinely useful primitive: **destroying a class key makes
its ciphertext unrecoverable, including in history, in every backup, and on
every peer.** For an immutable database that is the practical answer to
"delete this data" — excision by key destruction rather than by rewriting the
past. It is class-granular, which is coarse: shredding `:protect/pii` shreds it
for everyone.

Per-subject erasure needs per-subject keys. The shape is a
`:db.protect.scope/entity` class whose `:db.protect/key` names a key
*namespace*, with the keyring resolving `<namespace>/<entity-id>` to material
held in a key table (a small Corium database, or Vault) where deleting one row
destroys one subject's key. That is a real design, with real costs — a key
lookup per subject on the read path, and a key store that is now a durability
dependency — and it is deliberately **out of scope for v1**, sketched here so
the class model does not have to change to accommodate it later.

## Keys versus authorization

Corium now has two ways to keep a reader from seeing a fact, and they are not
redundant:

| | `ViewFilter` (authz) | Protection class (keys) |
|---|---|---|
| Enforced by | the serving process | mathematics |
| Survives a compromised peer? | no | yes |
| Survives a stolen backup? | no | yes |
| Granularity | attribute, entity, predicate | attribute (class) |
| Changed by | a policy transaction, effective immediately | key distribution, and only forward |
| Cost | a filter on the read path | sealing, scan-only access, no indexing |

Use policy for "who should" and keys for "who can". The natural deployment
combines them: the ReBAC policy denies the query outright, *and* the key was
never distributed, so a policy bug does not become a disclosure. `KeyPolicy`
lets one policy database express both, but they remain independently enforced —
a peer that ignores the policy still cannot decrypt.

This also gives the deferred `AllowFiltered` work a cheaper first customer:
attribute-level redaction driven by an absent key is enforceable in the scan
today, with no executor predicate.

## Operating it

```sh
# Storage encryption, at database creation.
corium db create people --schema schema.toml --storage-key awskms:arn:…:key/2f1c…

# A protection class and an attribute in it (ordinary schema, ordinary transaction).
corium keys class add :protect/pii --key awskms:arn:…:key/9ab3… --scope entity

# Protect an existing attribute: alter the schema, then seal the current values.
corium keys protect :person/ssn --class :protect/pii --sweep
corium keys audit   :person/ssn          # plaintext still current / in history / in backups
corium keys unprotect :person/legacy-note  # forward-only; old values stay sealed

# Who can read what.
corium keys status                       # timelines, epochs, live-object counts, key ids
corium keys rotate --class :protect/pii  # forward-only; new writes use the new epoch
corium keys rewrap --kek awskms:arn:…    # KEK rotation; no data rewritten
corium keys shred :protect/pii           # destroys material; irreversible; audited

# Processes declare which keys they hold.
corium transactor  --data-dir … --storage-key file:/etc/corium/storage.key
corium peer-server --db people --storage-key file:… --key :protect/pii=awskms:…
corium peer-server --db people --storage-key file:… --seal-through
```

Environment overrides `CORIUM_STORAGE_KEY`, `CORIUM_KEYRING`, and
`CORIUM_KEYS` follow the existing `CORIUM_*` conventions.

The long-running ones — the sweep, an epoch drain, a rewrap across a large
database, and the audit that must precede a shred — are hour-scale jobs that
need a process outliving the shell that started them, and the sweep additionally
needs to *hold a class key while it runs*. Those are duties of the proposed
[operator peer service](operator-service.md): `corium keys protect --sweep`
submits a job and follows it, key custody for the job is an explicit opt-in
grant recorded with it, and `shred` requires a fresh plan plus a second
approver. Without a service configured the commands still run in-process, which
is fine for a small database and is exactly what you do not want for a migration
measured in hours.

Failure modes, all of which must be distinguishable in logs and metrics:

| Condition | Behaviour |
|---|---|
| Encrypted database, no storage key | refuse to open, name the manifest's key id |
| Storage key resolves but epoch missing | refuse to open; that epoch's data is unreadable |
| Class key missing | reads follow `on-missing-key`; writes to that class refuse |
| Class key wrong (unwrap succeeds, AEAD fails) | `QueryError::Protected` with the class and epoch, never a decode error |
| KMS unreachable | cached material keeps serving; new epochs fail; `corium_keys_unavailable` gauge set |
| Assertion in the wrong form for the attribute's current state | transaction rejected at validation |
| Retraction naming a form the attribute never had | transaction rejected at validation |
| Protection altered without the acknowledgement | transaction rejected, with the remedy in the message |
| Legacy plaintext read by a keyless reader | follows `legacy-plaintext` (default `redact`); counted in `corium_legacy_plaintext_reads_total` |

Metrics: `corium_seal_ops_total{op,class}`, `corium_seal_errors_total{kind}`,
`corium_hydrate_cache_{hits,misses}_total`, `corium_blob_decrypt_seconds`,
`corium_key_epoch{key_id}`, `corium_keys_unavailable`. Key ids appear as labels;
key material never does, and the audit sink already used by `corium-authz`
records key grants, rotations, and shreds.

## Performance

- **Layer 1.** AES-256-GCM with AES-NI runs at multiple GB/s per core, well
  under the zstd cost already paid on the same bytes. Order is
  encode → compress → encrypt, so compression ratios are unchanged for
  unprotected data. Expect single-digit percent on index publication and log
  append; the acceptance bar is <5% on the M3 benchmark suite.
- **Layer 2.** One AEAD pass per protected value on write, one per hydrated
  value on read. GCM-SIV is two passes over the plaintext, which for
  short values is dominated by per-call overhead — hence the per-connection
  hydration cache and hydrate-at-scan-output rather than hydrate-per-tuple.
- **The real cost is the missing index.** A query that filters on a protected
  attribute scans that attribute instead of seeking AVET. That is inherent, it
  is the point of the constraint, and the fix when it hurts is the blind-index
  recipe, chosen explicitly.
- Sealed values are incompressible, so protected-heavy leaves compress worse and
  segment counts rise; padding makes that worse still. Capacity planning should
  assume protected attributes cost their plaintext length plus 16 bytes of tag
  plus header, with no compression benefit.

## Compatibility and migration

- **Storage format 4** (`DbRoot.key-manifest-version`), **backup format 2**,
  **thin-client protocol v3** (sealed value tag). Each is additive and each is
  version-checked the way the existing formats are; an older reader meeting a
  newer artifact gets the existing upgrade error, not a parse failure.
- An existing unencrypted database keeps working untouched, forever. Turning on
  storage encryption for one is a backup/restore into a new database created
  with `--storage-key`.
- Adding a protection class to an existing schema is additive, and so is
  protecting a populated attribute — forward only, per
  [Changing protection](#changing-protection). The alteration that is *not*
  available is re-indexing an attribute that has ever been protected, and the
  one that breaks callers is protecting an attribute currently used in lookup
  refs, whose `:db/unique` must be retracted at the same time.
- `Value::Sealed` is a new variant on an enum matched exhaustively in roughly
  thirty places across `corium-query`, `corium-sql`, `corium-pgwire`,
  `corium-cljrs`, `corium-ffi`, and `corium-cli`. That mechanical blast radius
  is the single largest implementation cost of layer 2, and it is deliberate:
  making every consumer confront the variant is how "this value might be
  unreadable" stops being forgettable.

## Implementation plan

1. **`corium-crypt`.** Primitives, `KeyId`/`SecretKey`/`Keyring`,
   `StaticKeyring`, deterministic sealing, derivation, zeroization. Pure
   library, property-tested in isolation.
2. **Layer 1.** `EncryptedBlobStore` decorator and the id change; log record
   payload encryption; `keys:<db>` manifest and storage format 4; cache
   placement; backup format 2; `--storage-key` on every process; `corium keys
   init|status|rotate|rewrap`. Deliverable acceptance: a byte scan of a
   populated data directory and blob store finds no sentinel plaintext, and a
   full backup/restore round-trips across a DEK rotation.
3. **Layer 2 core.** `Value::Sealed`, the `0xA0` encoding, class entities, the
   per-attribute protection timeline in the schema cache, schema validation
   rules 1–6, peer-side sealing, transactor keyless validation, EAVT/AEVT-only
   membership, planner rules.
4. **Layer 2 read path.** Hydration in `ExecOptions`, redaction policies
   (including legacy plaintext), query cache keying, pull/entity/datoms
   surfaces, `corium-cljrs` rendering, console output.
5. **Protection changes.** Removing `:db/index`/`:db/unique` as a supported
   alteration (the prerequisite for protecting an indexed attribute), the
   forward-only transitions and their acknowledgement, `corium keys
   protect|unprotect|audit`, and the resumable, chunked sweep.
6. **Entity scope.** `ReserveEntityIds`, local upsert pre-resolution, the
   `expected_basis_t` fence, and its retry loop.
7. **Surfaces.** Peer server `KeyPolicy` and `--seal-through`, thin-client
   protocol v3, SQL/pgwire redaction and pushdown rules, schema-TOML fields.
8. **Operations.** Rotation, shredding, metrics, audit events, and the
   operations-guide runbook.

Acceptance tests, beyond unit coverage:

- **Property:** seal/unseal round-trip per value type; determinism (same context
  and plaintext ⇒ identical bytes); AAD binding (a ciphertext moved to another
  attribute — or, under entity scope, another entity — fails to open); encoded
  order stability; a retraction of a sealed fact shares the asserted key's
  `key_components` prefix.
- **Keyless parity:** a transactor with no class keys commits, indexes,
  publishes, GCs, and backs up a protected database; a peer with no class keys
  serves the full conformance corpus with redacted values and identical results
  on every query that does not touch a protected value.
- **Key-holder equivalence:** the conformance corpus run against a schema whose
  attributes are all protected, on a peer holding every key, returns results
  identical to the unprotected run — except for the queries the constraint
  rules make illegal, which must fail with the documented errors.
- **Rotation and shredding:** mixed-epoch trees read correctly; a retired
  storage epoch is drained by a rebuild; after a class shred the database stays
  healthy and reads of that class fail with the documented error.
- **Protection transitions:** an attribute protected mid-life keeps its old
  plaintext readable to key holders and redacted to everyone else; a retraction
  of a pre-protection fact still cancels it; a re-classified attribute is
  readable only by a holder of *both* keys; a swept attribute has no plaintext
  current value while `as-of` before the sweep still does; and re-indexing an
  ever-protected attribute is rejected.
- **No plaintext at rest:** the sentinel scan of step 2, extended to the log,
  the SSD cache directory, and a backup archive.
- **Fuzz:** the sealed-value decoder and the encrypted blob/log headers, which
  are the new attacker-reachable parsers.

## Open questions

- **Hydration granularity.** Hydrate-at-scan-output is simple and makes the rest
  of the engine oblivious, but it decrypts values that a query only counts or
  joins away. Lazy hydration on first plaintext use is strictly better and
  strictly more invasive; the plaintext cache is the hedge until measurement
  says otherwise.
- **`:db/fn` and protected values.** Keeping keys out of the transactor means
  database functions cannot branch on protected plaintext. An opt-in "transactor
  holds this class's key" mode would restore it and give up the strongest
  property in the threat model. Not proposed; recorded because deployments will
  ask.
- **Per-subject keys.** Sketched above and deliberately deferred. The open part
  is the key store's durability and availability contract, not the crypto.
- **Sweep cost.** The sweep rewrites every current value of an attribute —
  proportional to entity count, not to the change — and competes with ordinary
  writes for the transactor. *Where* it runs is now answered: the
  [operator peer service](operator-service.md) hosts it as a resumable job. What
  it should do about throttling, and whether a very large attribute wants
  several workers rather than one, is not.
- **Redaction of legacy plaintext under `as-of`.** Evaluating the policy against
  the current schema rather than the view's is the safe choice and the one
  inconsistency with schema-as-data in the whole design. A future `:db/protection`
  that participates in time views properly would need reads to consult two
  schemas and explain which one answered.
- **Cross-database class keys.** A class key id is stable across fork and
  restore by design, so two databases can share a class and hydrate each
  other's values. Whether that is a feature (tenant-wide keys) or a footgun
  (unintended reach after a fork) probably wants an explicit
  `:db.protect/lineage` marker.
- **Blind index as an engine feature.** Automating the second attribute would
  make the common case correct by default; it also embeds a deterministic,
  indexed hash of protected data in the schema, which is exactly the thing this
  design otherwise refuses to do implicitly.
- **Key distribution.** Out of scope here and genuinely hard: this design
  assumes a keyring exists and grants are made elsewhere. A deployment with
  many classes and many peers will want tooling that Corium does not yet have.
