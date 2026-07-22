//! Wire protocol: gRPC service definitions (tonic/prost) plus the Corium
//! composite value encoding carried in protobuf `bytes` fields
//! (see `docs/design/protocol.md`).

pub mod auth;
pub mod authz;
pub mod codec;

// The EDN schema/transaction boundary conversions live in the pure
// `corium-forms` crate (so they compile on wasm, away from tonic/tokio).
// Re-exported here for back-compatible paths (`corium_protocol::schemaform`,
// `corium_protocol::txforms`).
pub use corium_forms::{schemaform, txforms};

/// Protocol version spoken by this crate.
pub const PROTOCOL_VERSION: u32 = 1;

/// Generated protobuf/tonic bindings for `corium.v1`.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::missing_errors_doc,
    clippy::derive_partial_eq_without_eq
)]
pub mod pb {
    tonic::include_proto!("corium.v1");
}
