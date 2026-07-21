# corium-protocol

The Corium wire protocol: gRPC service definitions (tonic/prost) plus the
composite value encoding carried inside protobuf `bytes` fields.

## What it does

Defines how the transactor, peers, and thin clients talk to each other:

- **gRPC services** for `corium.v1` (Transactor, Catalog, …) generated from
  `proto/` by the build script into the `pb` module.
- **`codec`** — the composite value encoding that packs Corium `Value`s,
  datoms, and schema/tx forms into protobuf `bytes`, reusing `corium-core`'s
  order-preserving encoding rather than mirroring every type in protobuf.
- **`schemaform` / `txforms`** — encode/decode helpers for schema and
  transaction payloads on the wire.
- **`auth`** — bearer-token interceptors and TLS config helpers.
- `PROTOCOL_VERSION` — the version this crate speaks (currently `1`).

## Dependencies

- `corium-core`, `corium-db`, `corium-log`, `corium-query`, `corium-tx` — the
  domain types that get encoded.
- `prost` + `tonic` + `tonic-prost` for protobuf/gRPC; `tokio`/`tokio-stream`
  for streaming RPCs; `thiserror` for errors.
- Build: `protox` + `tonic-prost-build` compile the `.proto` files.

## Architecture

The control plane (transact, subscribe, catalog, lease discovery) is described
in protobuf and served by tonic. Value payloads are **not** re-modeled in
protobuf; they travel as opaque `bytes` carrying Corium's own tagged encoding,
so the wire format and the on-disk segment format share one source of truth and
stay byte-compatible. This crate is the seam between the pure engine and the
async/networked world — it is the lowest layer that pulls in tokio. The
peer-facing surface is stabilized as a public interoperability contract. See
[`docs/design/protocol.md`](../../docs/design/protocol.md) and
[`docs/thin-client-protocol.md`](../../docs/thin-client-protocol.md).
