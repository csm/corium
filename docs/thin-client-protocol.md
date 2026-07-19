# Thin-client protocol specification (v1)

This document is the interoperability contract for clients that do not embed
`corium-peer`. The canonical service schema is
[`corium.proto`](../crates/corium-protocol/proto/corium.proto); protocol v1 is
served by `PeerServerService` over gRPC/HTTP2.

## Compatibility and authentication

Every transact/subscribe request sends `protocol_version = 1`. A mismatch is
`FAILED_PRECONDITION`, never silent downgrade. Deployments use TLS and may
require `authorization: Bearer <token>` metadata. Standard gRPC status codes
are used: malformed EDN/query input is `INVALID_ARGUMENT`, an unknown
database/entity is `NOT_FOUND`, budget exhaustion is `INVALID_ARGUMENT`, and
upstream loss is `UNAVAILABLE`.

## Composite value encoding

All protobuf `bytes` value fields contain one Corium composite item. Integers
use unsigned LEB128 varints; signed integers use zig-zag then LEB128. Counts
precede container contents. Multi-byte fixed scalars are big-endian.

| Tag | Meaning | Payload |
|---:|---|---|
| `00` | nil | none |
| `10` | boolean | one byte, `00` or `01` |
| `20` | long | zig-zag varint |
| `30` | double | sortable IEEE-754 bits, 8 bytes |
| `40` | instant | Unix milliseconds, zig-zag varint |
| `50` | UUID | 16 bytes |
| `61` | keyword | interned UTF-8 name |
| `71` | string | interned UTF-8 text |
| `81` | bytes | length varint + bytes |
| `90` | entity ref | unsigned varint |
| `a0`/`a1`/`a3` | list/vector/set | count + items |
| `a2` | map | pair count + alternating key/value items |
| `a4` | tagged literal | interned tag + item |
| `a5` | symbol | interned UTF-8 text |

An interned string starts with varint `0`, then byte-length and new UTF-8
bytes. This defines the next 1-based table slot. Later occurrences encode
that non-zero slot directly. The table is per top-level message. Implementors
can validate their codec against
[`codec.rs` tests](../crates/corium-protocol/tests/codec.rs).

## Database views and calls

`DbViewSpec` names the database plus at most one of `as_of`, `since`, or
`history`. No selector means current. Query database views bind positionally
to `$`, `$2`, and so on; `args` is a composite vector for remaining `:in`
bindings. `fuel = 0` requests the server default, otherwise the server clamps
it to its configured ceiling.

Query relations/collections stream chunks whose `rows` decode to vectors and
must be concatenated. Tuple/scalar results contain one item in one chunk.
Always inspect `shape`; stop only after `last = true`. Datoms and transaction
ranges use the same chunk/`last` rule. `Transact` provides read-your-writes on
the serving peer before it responds.

`Subscribe.from_basis_t` is exclusive. The first item is a handshake, then
the server backfills every `t > from_basis_t` without gaps and continues with
live reports, index announcements, and heartbeats. The handshake's
`heartbeat_interval_ms` (0 from older servers) is the server's heartbeat
cadence; treat silence for a few multiples of it as a dead upstream and
reconnect. After reconnect, send the last fully applied basis and
deduplicate by transaction number.

## Conformance

The language-neutral behavioral corpus is in
[`tests/conformance`](../tests/conformance/README.md). The gRPC replay harness
is [`conformance_grpc.rs`](../crates/corium-cli/tests/conformance_grpc.rs).
A client is conformant when it produces the same decoded EDN values for that
corpus, honors every result shape and time view, rejects protocol mismatch,
and resumes subscriptions gaplessly.
