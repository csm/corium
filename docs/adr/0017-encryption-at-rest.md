# ADR-0017: Envelope encryption at rest for every durable artifact

**Status:** Proposed (2026-07-28); design in
[`docs/design/encryption.md`](../design/encryption.md). Companion to
[ADR-0018](0018-attribute-protection-classes.md), which adds per-attribute keys
above this layer.

## Context

Every durable artifact Corium writes is currently plaintext: index blobs in the
object store, transaction log records, root records, backup archives, and the
peer's SSD segment cache. [ADR-0003](0003-immutable-segments.md) makes those
blobs immutable and content-addressed, and
[docs/design/protocol.md](../design/protocol.md) states the operating
assumption plainly — "the blob store is assumed private to the deployment".

That assumption does not survive the deployments Corium already supports. An S3
bucket, a Postgres blob table, a backup file copied to a laptop, and an SSD
cache directory on a shared host all have failure modes where the medium is
readable by someone who was never granted database access. Authentication and
authorization ([ADR-0012](0012-optional-authn-authz.md),
[ADR-0014](0014-self-hosted-rebac-authz.md)) guard the *request* path and do
nothing for any of them.

The constraint that shapes the answer is content addressing. A blob's id is its
BLAKE3 digest; publication reuses unchanged leaves by id; GC marks and sweeps by
id; backups copy by id; the cache verifies by id. Encryption must not disturb
any of that, and must not require a key to perform the operations that today
require none.

## Decision

Encrypt every durable artifact under a per-database data key, wrapped by a
key-encryption key held in a KMS or an operator file, with a new
`corium-crypt` crate owning the primitives and a `Keyring` trait owning key
resolution.

- **Encryption is a decorator, not a rewrite.** `EncryptedBlobStore<S>` wraps
  any `BlobStore`, so `mark_and_sweep`, manifest walking, backup, and every
  reader keep operating on plaintext and stay unchanged.
- **The blob id becomes the digest of the stored (encrypted) object.**
  Encryption is deterministic for a given (epoch, content) — the nonce is
  derived from the plaintext digest — so `put` stays idempotent, unchanged
  leaves still produce identical objects and are still shared structurally, and
  the store still verifies integrity by re-hashing what it holds, with no key.
  Cross-database deduplication is given up; within a database nothing changes.
- **The decorator sits above the segment cache.** The SSD tier holds ciphertext
  and keeps its existing digest check, superseding
  [peer-segment-cache.md](../design/peer-segment-cache.md)'s reliance on host
  filesystem encryption.
- **Log frames stay legible, payloads do not.** Frame length and CRC32C remain
  cleartext so scanning, range reads, and recovery truncation are untouched; the
  record payload is encrypted with a nonce derived from `(log-version, t)` and
  an AAD binding both, so no record can be replayed at another basis or moved
  between the per-lease-version files M7 fencing depends on.
- **Root records stay cleartext,** because they hold no user data and
  `RootStore::compare_and_set` compares bytes. A new `keys:<db>` root record
  holds the wrapped DEKs, and `DbRoot` gains a manifest version (storage
  format 4) so an encrypted database announces itself before a reader tries to
  parse a blob.
- **Backups copy ciphertext verbatim** (format 2, carrying the key manifest), so
  backup remains a byte copy and an archive is restorable given KMS access and
  nothing else.
- **Rotation is layered.** KEK rotation re-wraps DEKs and touches no data; DEK
  rotation opens a new epoch that new writes use, with old epochs retained and
  drained by ordinary re-indexing rather than a rewrite.

## Consequences

- A stolen disk, bucket, snapshot, backup file, or cache directory yields
  nothing without KMS access. That is the whole of what this layer buys, and it
  is the property most deployments are actually missing.
- It buys nothing against a compromised process inside the deployment: any
  peer or transactor that can read the database can read it in the clear. That
  gap is exactly what ADR-0018 addresses, and keeping the two layers separate
  keeps this one cheap enough to leave on.
- Content addressing survives intact, but the identity of a blob is now the
  identity of its *ciphertext*. Two databases no longer share objects, and
  re-encrypting a leaf under a new epoch gives it a new id — which is correct
  (it is new content) and makes epoch draining observable in the mark pass.
- Encryption is per database, so per-tenant databases get key isolation for
  free, and `corium keys` operates one database at a time.
- Every process that reads storage directly now needs a key: transactor, GC,
  backup, and storage-aware peers. Thin clients and peer-server callers do not.
  Misconfiguration therefore fails at open, loudly, rather than at first read.
- Three format versions move (storage 4, backup 2, plus the manifest record).
  Each is additive and version-checked; existing unencrypted databases keep
  working untouched, and the migration to an encrypted one is a backup/restore.
- Deterministic content encryption reveals repeated object ids within one
  database and storage-key epoch. It does not create a plaintext-guess oracle
  for a storage-only adversary: recomputing an object requires the DEK or an
  encryption oracle, and different databases and epochs use different DEKs.
  Randomized encryption would hide equality but would cost idempotent `put`,
  structural sharing, and keyless integrity verification, which is far too much
  to pay.
