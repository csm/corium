# ADR-0018: Attribute-level protection with per-class keys

**Status:** Accepted (2026-07-28); implemented for protection declared at
database creation (2026-07-31) — schema alteration, entity scope, and the
operational surface remain, see the status header in the design. Design in
[`docs/design/encryption.md`](../design/encryption.md). Builds on
[ADR-0017](0017-encryption-at-rest.md) (storage encryption) and extends the
visibility model of [ADR-0012](0012-optional-authn-authz.md) /
[ADR-0014](0014-self-hosted-rebac-authz.md).

## Context

ADR-0017 protects the medium. It does nothing about a reader inside the
deployment: a peer holds the whole database in memory, a peer server serves many
tenants from one `Db`, and the transactor sees every fact it commits. The
`ViewFilter` seam ADR-0012 defined is the policy answer to that, and it has two
limits — it is not implemented on any read path yet, and, more fundamentally, it
is enforced *by the process that already has the plaintext*. A peer that ignores
policy, or is compromised, discloses everything it holds.

What is wanted instead is a cryptographic floor: some attributes should be
unreadable to a reader who was not granted a key, no matter what that reader
does — while the peer interface stays exactly as it is, peers keep reading
storage themselves and running queries locally, and everything that does not
depend on those values keeps working for everyone.

Two engine constraints shape the design. First, **the index key is the datom**:
a retraction cancels an assertion by sharing its `(e, a, v)` byte prefix, and
the current-value fold keeps one entry per prefix — so a fact's byte
representation must be stable, on a transactor that will not hold a key.
Second, **protected datoms cannot be indexed**: ciphertext order is not value
order, so AVET and VAET cannot contain them and no value-ordered access can be
offered over them.

## Decision

Attributes may declare a **protection class**; values on those attributes are
sealed with that class's key by the writing peer, before the transaction leaves
it, and are hydrated only by readers whose keyring resolves the class key.

- **Classes are schema data.** A class entity names a key *id*, an algorithm, a
  scope, an optional padding, and a missing-key policy. It never holds key
  material: the database records the id, and each process resolves material
  through its own keyring, so Corium never decides who may hydrate.
- **Sealing is deterministic** (AES-256-GCM-SIV, context in the AAD). Identical
  plaintext in the same context yields identical bytes, which is what preserves
  retraction pairing, cardinality-one supersession, cardinality-many
  deduplication, and `:db/cas` on a transactor with no key. The transactor keeps
  doing everything it does today, bytewise.
- **Scope chooses what determinism leaks.** `scope/attribute` (default) binds
  the context to the attribute, so a keyless reader can tell that two entities
  share a value. `scope/entity` also binds the entity: no cross-entity equality,
  and the AAD authenticates the subject, so a compromised transactor cannot move
  a ciphertext between entities. Entity scope costs a `ReserveEntityIds` round
  trip and an `expected_basis_t` fence, because sealing must know the entity id
  before tempid resolution would have assigned one.
- **The constraint is enforced at schema-install time.** `:db/protection` is
  rejected together with `:db/index`, `:db/unique`, and `:db.type/ref`, so
  "protected datoms cannot be indexed" is a validation rule rather than a
  runtime surprise. Protected datoms live in EAVT and AEVT only, and the planner
  treats a bound protected value position as unbound for selectivity, refuses
  range access, and never selects AVET.
- **Protection changes are legal and forward-only.** Protecting, unprotecting,
  and re-classifying a populated attribute all take effect from the basis they
  are transacted at; a datom keeps the form it was asserted in, because
  rewriting the past is the one thing an immutable database does not do. A
  sealed value carries its class and epoch inline, so a mixed attribute decodes
  without schema archaeology, while the schema cache carries a per-attribute
  protection *timeline* so validation can require the current form for
  assertions and accept any historical form for retractions and `:db/cas` old
  values. An attribute that has ever been protected may never afterwards gain
  `:db/index` or `:db/unique`, because a mixed AVET would silently misorder
  ranges and silently stop enforcing uniqueness.
- **The non-retroactivity gap is closed in three graded steps,** not papered
  over: legacy plaintext on a now-protected attribute is redacted on read by
  default (policy, immediate, free); a sweep re-asserts current values in sealed
  form (cryptographic, current view only); and a rebuild — or a re-classify
  followed by destroying the old class key — is what makes the change reach
  history. `corium keys audit` reports how much plaintext remains and where, and
  the alteration must acknowledge its own semantics on the transaction entity.
- **Keys stay out of the transactor.** It validates the sealed header against
  the schema, orders, logs, indexes, publishes, GCs, and backs up — all without
  a class key, and without the ability to forge a protected fact.
- **Hydration is a property of the read, not of the `Db`.** Values stay sealed
  in segments, in the live index, and in join keys; `ExecOptions` carries the
  hydrator, so one peer server serves principals with different key sets from
  one immutable value. The query cache keys on the key set.
- **A missing key redacts, hides, or errors,** per class and narrowable per
  request. Under every policy an unhydratable value never satisfies a constant
  or a predicate.
- **Rotation is forward-only, and destroying a class key is the erasure
  primitive.** History is immutable, so old datoms keep their epoch; shredding
  the key makes that ciphertext unrecoverable everywhere, including in backups.

## Consequences

- A peer, a peer server, an operator, or the transactor can hold the entire
  database and still not read a protected attribute. That is the property no
  policy-enforced filter can offer, and it is what makes running a peer in a
  less-trusted environment — or shipping a production restore to staging —
  reasonable.
- The peer interface is unchanged. A keyless peer connects, bootstraps from
  storage, maintains its live index from tx-reports, and answers every query
  that does not depend on a protected value with identical results. Protected
  values come back redacted; the entity, the attribute, and the transaction are
  all still there.
- Protected attributes lose indexed access, uniqueness, reverse-ref traversal,
  range queries, and value-ordered aggregates. Filtering one means scanning it.
  When an application genuinely needs indexed lookup, it adds a separate,
  unprotected blind-index attribute and accepts that leak explicitly — a
  documented recipe rather than a hidden engine behaviour.
- Determinism is a real, permanent leak, and the default scope leaks equality
  across entities. For low-cardinality attributes that approaches plaintext
  under frequency analysis. The alternative — randomized sealing — breaks fact
  identity on a keyless transactor, so the honest move is to make the leak a
  documented, per-class choice with a stronger option next to it.
- Database functions on the transactor cannot branch on protected plaintext.
  That work moves to the peer under a basis fence. `:db/cas` still works,
  because it is a bytewise comparison.
- Encryption and authorization become two independent controls over the same
  data: policy for "who should", keys for "who can". A policy bug no longer
  becomes a disclosure, and the deferred `AllowFiltered` work gains a cheap
  first customer — attribute redaction driven by an absent key, enforceable in
  the scan with no executor predicate.
- `Value` gains a variant matched exhaustively across roughly thirty sites in
  six crates, and the thin-client protocol gains a tag (v3). The blast radius is
  the cost of making every consumer confront "this value might be unreadable"
  rather than discovering it at runtime.
- Erasure is class-granular. Per-subject shredding needs per-subject keys and a
  key store that becomes a durability dependency; the class model is shaped to
  accept it later, and v1 does not attempt it.
- "I protected the attribute" does not mean "the data is protected", and no
  amount of documentation fully removes that surprise. The mitigations make the
  residue measurable and closable rather than invisible, and they make the good
  path one command (`corium keys protect --sweep`) — but an operator who
  protects an attribute and stops there has changed what ordinary readers see
  and *not* what a reader with storage access can recover. That is the honest
  cost of forward-only semantics, and it is preferred to the alternatives:
  rejecting the alteration outright pushes every deployment into a manual
  attribute-renaming migration, and silently rewriting history would break
  immutability, the log's authority, and every existing backup and peer.
- Key retention becomes a lifecycle obligation in both directions. An
  unprotected attribute may still need its old class key forever, and shredding
  a class after unprotecting destroys values whose schema now says they are not
  protected — the most confusing state the model can reach, and one only
  reachable deliberately.
