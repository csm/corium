# Glossary

**Attribute.** An entity in `:db.part/db` that declares a name, a value type,
a cardinality, and optional properties. See
[schema management](../running/schema.md).

**AEVT.** The covering index sorted by attribute, entity, value, transaction.
It holds all current datoms.

**AVET.** The covering index sorted by attribute, value, entity, transaction.
It holds the datoms of indexed and unique attributes only.

**Basis.** The transaction number, written `t`, that a database value covers.

**BLAKE3.** The hash function that addresses blobs.

**Blob store.** The immutable, content-addressed half of the storage service.

**Cardinality.** Whether an attribute holds one value or many per entity.

**Chunk.** A content-defined run of a sorted key stream. A published leaf is
exactly one chunk.

**Covering index.** An index that holds whole datoms, so an answer needs no
second lookup.

**Database root.** The record in the root store that names a database: its
basis, its index roots, its log root, and its write lease.

**Database value.** An immutable snapshot of a database at one basis.

**Datom.** One fact: entity, attribute, value, transaction, and assert or
retract.

**EAVT.** The covering index sorted by entity, attribute, value, transaction.
It holds all current datoms.

**Entity id.** A 64-bit number: a 22-bit partition and a 42-bit sequence.

**Epoch (storage key).** One generation of the per-database data key. New
writes seal under the newest open epoch.

**Fence.** The mechanism that stops a deposed writer. Every root write is a
compare-and-set on the record that holds the lease.

**Fork.** A new database that duplicates an existing one at a basis, and then
diverges. See [forking a database](../availability/fork.md).

**Fuel.** A budget on the datoms a query touches, or on the work a database
function does.

**Group commit.** Committing concurrent transactions as one batch under one
durability boundary, while each keeps its own `t` and its own acknowledgement.

**Ident.** A keyword that names an entity, above all an attribute.

**Index basis.** The transaction number that the published index trees cover.

**Index lag.** The difference between the basis and the index basis.

**KEK.** Key-encryption key. It wraps the per-database data key. Corium never
stores it.

**Lease.** The right to write one database. It lives in the database root and
is renewed by compare-and-set.

**Lease version.** The generation of a lease. It names the log file, or the
log key prefix, that its holder appends to.

**Lookup reference.** A `[attribute value]` pair on a unique attribute, usable
anywhere an entity id is expected.

**Partition.** The high bits of an entity id. Entities of one partition sort
together in EAVT.

**Peer.** A library, or a process, that holds a database value and queries it
locally.

**Peer server.** A peer hosted as a standalone process for thin clients.

**Principal.** The identity of a request, produced by an identity provider.

**ReBAC.** Relationship-based access control, the authorization model that
`--authz-db` enables.

**Root store.** The small, mutable, strongly consistent half of the storage
service. It is updated only by compare-and-set.

**Segment.** An immutable, content-addressed node of an index tree.

**Standby.** A transactor that polls a lease held elsewhere and takes over
when it lapses.

**Tempid.** A transaction-local placeholder for an entity id, resolved at
commit. A collision on a `:db.unique/identity` attribute becomes an upsert.

**Transaction report.** The record that a commit broadcasts to peers: the
basis before, the basis after, the datoms, and the tempid map.

**Transactor.** The single writer for a database.

**Upsert.** Unifying a tempid with an existing entity through a
`:db.unique/identity` attribute.

**VAET.** The covering index sorted by value, attribute, entity, transaction.
It holds reference-typed datoms.

**`t`.** The sequence part of a transaction id, and the name of a basis.

**`:db/txInstant`.** The commit time of a transaction, asserted as a datom on
the transaction entity.
