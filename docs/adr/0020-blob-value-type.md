# ADR-0020: A blob value type, stored in the blob store and fetched lazily

**Status:** Proposed (2026-07-29); design in
[`docs/design/data-model.md`](../design/data-model.md) (value and encoding),
[`docs/design/indexes-and-storage.md`](../design/indexes-and-storage.md)
(payload layout, payload root, GC and expunge), and
[`docs/design/protocol.md`](../design/protocol.md) (transfer). Extends the
value scope fixed by [ADR-0009](0009-schema-scope.md) and builds on the
content-addressed store of [ADR-0003](0003-immutable-segments.md).

## Context

Datomic has no large-object story, and neither do we. `:db.type/bytes` puts the
payload *in the datom*: the value is embedded in the key of every covering
index the attribute appears in, uploaded in whatever leaf chunk it lands in,
written into the transaction log, held resident in every peer's `Db` for the
life of the database, and shipped to every subscriber in the tx-report. A
one-megabyte value costs all of that on every peer, forever, whether or not any
query ever reads it. The practical answer today is to keep the bytes in some
other system and store a URL — which puts the payload outside the database's
transactions, time model, backups, and access control, and makes the two
systems' notions of what exists disagree the first time a write half-fails.

The missing piece is already here. Corium's storage layer *is* a
content-addressed blob store ([ADR-0003](0003-immutable-segments.md)): index
chunks, manifests, and backups all ride on `BlobStore`, with a read-through
size-bounded `SegmentCache` in front of it, an encryption decorator
([ADR-0017](0017-encryption-at-rest.md)) around it, and mark-and-sweep GC and
backup already walking its reachability graph. Peers hold credentials to it
directly and read index segments from it without involving the transactor. A
large value wants exactly that treatment: content-addressed, immutable, shared
by hash, cached where it is used, fetched only when someone asks.

What that requires from the data model is a value whose *identity* is small
enough to live in an index key while its *content* lives somewhere else, and
which is still an ordinary fact — asserted, retracted, joined, pulled, and
time-traveled like any other.

Three engine constraints shape the rest of the design. First, **peers, not the
transactor, hold storage credentials**: routing payload bytes through the
single writer would make it the bottleneck the peer architecture exists to
avoid. Second, **`corium_tx::prepare` is a pure function of `(db, tx-data)`**,
which the transactor, the simulator, and the unit tests all call directly, so
no upload may happen inside it. Third, **published index snapshots contain live
facts only**, while history lives in a log that is never truncated — so a datom
that has been retracted is still a valid historical fact with no pointer to it
in any index.

## Decision

Add `:db.type/blob`: a value that carries the content hash and length of a
payload in the blob store, whose bytes are fetched lazily and explicitly.

- **The datom carries a reference, never the bytes.** `Value::Blob` holds a
  32-byte content id and a length, fixed-width in the sortable encoding, so a
  blob value costs the same in an index key whether it names four kilobytes or
  four gigabytes. Nothing about the log, the segment format, the tx-report, or a
  peer's resident set changes shape.
- **The content id is a digest of the plaintext, not of the stored object.**
  Everywhere else in Corium a blob id is the digest of whatever is stored, which
  under encryption is ciphertext. That cannot work here: a payload reference
  lives in a datom in a log that is never truncated, so it can never be
  rewritten, while a stored-object id moves whenever a storage re-key
  re-encrypts the object under a new epoch. Index blobs survive re-keying only
  because publication rewrites their roots, and a blob datom has no such step.
  Plaintext addressing keeps the reference valid across re-keys and makes blob
  equality genuine content equality rather than an artifact of the epoch a
  payload happened to be written in. The indirection lives in the payload root,
  which maps content id → current stored object id, so re-keying republishes a
  mapping instead of orphaning history.
- **A payload is a manifest naming content-defined chunks**, the same structure
  a published index already uses, so large values stream and read by range,
  near-identical payloads share chunks, and the reachability walk that GC and
  backup share extends to payloads by teaching one function a second magic
  rather than by adding a second walk.
- **The writing peer uploads.** Chunks first, then the manifest, then the
  transaction naming the reference — the ordering rule the segment publisher
  already obeys. Bytes never cross the transactor, which keeps validating,
  ordering, logging, indexing, and publishing exactly as it does today, over a
  reference. Thin clients, which hold no storage credentials, hand bytes to
  their peer server over a streaming RPC and it uploads on their behalf.
- **Reads are lazy and explicit.** Query results, pull, the entity API, and
  tx-reports carry the reference. Hydration is a separate call on the
  connection, served through the existing segment cache — a query that selects
  a blob attribute transfers nothing until someone asks for the bytes.
- **A blob attribute may not be indexed, unique, or `:db/noHistory`,** enforced
  at schema-install time the way [ADR-0018](0018-attribute-protection-classes.md)
  enforces its own exclusions. Digest order is not content order, so AVET over
  blob values would offer ranges that silently mean nothing; and history is what
  keeps a payload's pointer alive, so an attribute allowed to discard its old
  datoms could strand payloads that nothing names any more.
- **A committed payload is live forever.** History is complete, so a retracted
  blob datom remains a valid historical value and its bytes must remain
  fetchable. Reachability is recorded in a **payload root**: a map from the
  content id of every payload object ever committed to the stored object id
  currently holding it, published by the indexing pass beside the four index
  roots, chunked and shared like an index so a publish costs only the new
  entries. GC marks from it and backup copies from it, both unchanged. Uploads
  whose transaction never committed are unreachable and are swept by the
  retention window that already exists — which is why that window must exceed
  the longest plausible upload-to-commit latency.
- **Expunge is the erasure primitive.** The one operation that removes payload
  bytes is explicit, operator-driven, and irreversible: it deletes the chunks
  no surviving payload shares and drops the entry from the payload map. The
  datom is left exactly where it is — history stays honest that the fact was
  asserted — and a read of an expunged reference returns a defined `Expunged`
  outcome rather than a corrupt-store error, the same shape as ADR-0018's
  missing-key policy. It marks across every database in the store, as GC
  already does, so a fork or a clone that still names a payload keeps it alive.
  It runs as an approved job under
  [ADR-0019](0019-operator-peer-service.md) and is recorded as data.
- **Confidentiality is storage encryption, not attribute protection.** Payload
  bytes are covered by [ADR-0017](0017-encryption-at-rest.md) like every other
  durable artifact. Sealing a blob value under a protection class would hide
  *which* payload a datom names, not the payload — a distinction worth stating
  because the opposite is the natural assumption.

## Consequences

- Large values become ordinary facts. A payload is transactional, immutable,
  time-traveled, backed up, encrypted at rest, and reachable through the same
  authorization surfaces as everything else, instead of living in a second
  system whose contents can disagree with the database's.
- Cost tracks use rather than existence. A peer that never reads a blob
  attribute pays 41 encoded bytes per datom for it (a tag, a 32-byte digest,
  and a length), whatever the payload weighs; a peer that reads one pays for the
  chunks it touched, cached on the same LRU and disk tier as index segments.
  This is the property `:db.type/bytes` cannot offer at any size.
- Writes scale with peers, not with the transactor. A bulk import of large
  payloads uploads in parallel from as many peers as are running and asks the
  single writer only to order fixed-width references. The cost is that a peer needs
  storage write credentials — today it only needs read — and that the
  write path now has two failure domains, so a partial upload is possible and
  shows up as an orphaned blob rather than as a broken fact.
- The `:db/noHistory` prohibition is a real restriction on exactly the
  attributes most likely to want it: high-churn large values are precisely
  where an operator would reach for it. The honest answer is that they should
  reach for expunge instead, which removes the bytes deliberately and leaves an
  auditable record, rather than for a schema flag that would drop pointers
  silently. The alternative considered and rejected was refcounting payloads
  from live datoms, which would in principle let a noHistory blob attribute
  reclaim: it buys a narrow case at the price of a second reachability regime —
  durable, transactional refcounts that have to stay correct across retraction
  and re-assertion, restart, fork, and clone — running beside a mark-and-sweep
  that is already correct. Silent reclamation is also precisely what expunge
  exists to make explicit.
- Retracting a blob datom frees nothing. Storage grows with everything ever
  written, and the only way down is expunge. That is the same bargain
  immutability already makes for datoms, but datoms are small and payloads are
  not, so it is the first place where "never forget" has a bill an operator
  will actually notice. `corium blob` reports the size of the payload set so
  the bill is visible before it is a surprise.
- Expunge cannot be undone, and its reach is narrower than the word suggests.
  It does not touch backups taken before it — a restore of an older archive
  brings the bytes back, which is either the safety net or the hole in the
  erasure story depending on why the expunge happened — and it does not touch a
  fork or a clone that still names the payload, because the blob store is one
  shared namespace and the mark unions every database in it. Erasure is
  therefore a per-store obligation: expunge everywhere the payload is named,
  including archives, or it is not erased.
- Deduplication is content-addressed and therefore automatic and invisible:
  transacting the same payload twice stores it once and yields the same
  reference, so two entities can share bytes and an expunge driven by one of
  them must not delete what the other still names. That is why expunge sweeps
  from the surviving set rather than deleting a manifest's chunks directly.
- **Dedup and per-subject erasure are in genuine tension, and this design picks
  dedup.** Because identity is content, two subjects who upload identical bytes
  — the same stock image, the same template PDF, the same standard form — get
  one object, and expunging it for one subject expunges it for all of them.
  That is exactly backwards for the case blob storage most often has to serve, a
  deletion obligation over user-submitted content. An application that needs
  per-subject erasure has to make its payloads per-subject distinct before
  upload (encrypt or salt them per subject) and give up dedup deliberately,
  which is the same shape as ADR-0018's admission that per-subject shredding
  needs per-subject keys. Naming it here is better than discovering it during an
  audit.
- `DbRoot` gains a trailing field at storage format 5, after
  [ADR-0017](0017-encryption-at-rest.md)'s `key-manifest-version` at format 4.
  Older binaries reject the newer root rather than misreading it, which is what
  stops a pre-blob transactor from running GC against a database whose payloads
  it cannot see. Both proposals extend the same line-ordered record by
  appending, so the landing order fixes the layout: a format-5 root carries the
  key-manifest line and then the payload-root line. Databases with no blob
  attributes never grow a payload root and are unaffected.
- `Value` gains a variant matched exhaustively across roughly thirty sites in a
  dozen crates — `corium-core` (value, encoding, schema), `corium-forms`
  (`schemaform.rs`, `toml_schema.rs`), `corium-protocol` (`codec.rs`),
  `corium-query` (`boundary.rs`, `builtins.rs`), `corium-sql` (`catalog.rs`,
  `mutation.rs`), `corium-pgwire` (`types.rs`), `corium-client`
  (`remote.rs`), `corium-transactor` (`backend.rs`), `corium-cli`, plus the
  cljrs, FFI, wasm, and Python surfaces — and the wire codec gains a tag, which
  costs a thin-client protocol version. ADR-0018 anticipates the same bump for
  sealed values; whichever lands first takes the number. As there, the blast
  radius is the point: every consumer is made to confront "this value is a
  handle, not the content" at compile time rather than at runtime.
- SQL sees the reference, and paying for that properly means teaching the SQL
  stack a type shape it does not have. A blob column projects as a struct of
  content id and length (`List<Struct>` when cardinality-many); a scan never
  fetches bytes, and a hydrating function is future work. `corium-sql` today
  maps every value type to an Arrow scalar or `List<scalar>` and has no struct
  case in `catalog.rs`, `SqlType`, or `SqlValue`, and `corium-pgwire` routes an
  unrecognized Arrow type through `SqlType::Other` to `OID_TEXT` — so shipping
  the type without a composite OID and a binary/text codec in `types.rs` would
  quietly show JDBC clients an opaque text column. The pgwire codec lands with
  the type, not after it. A SQL user who expects `SELECT payload` to return
  content gets a handle instead, which is the same surprise the Rust and cljrs
  APIs deliver, deliberately.
- Streaming is real on the wire and in the store but not yet in the trait:
  `BlobStore::put`/`get` are whole-buffer, so a chunk is the unit that must fit
  in memory (single-digit megabytes) while a payload need not. Range reads over
  a manifest are what make that true, and they are also what a future streaming
  trait would be implemented in terms of.
