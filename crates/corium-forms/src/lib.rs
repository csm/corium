//! Boundary conversion between EDN forms and engine types.
//!
//! Two pure, dependency-light modules bridge the Datomic-dialect EDN used at
//! the wire and console boundaries onto the engine's schema and transaction
//! types:
//!
//! * [`schemaform`] turns `{:db/ident … :db/valueType …}` maps into a
//!   [`corium_core::Schema`] and its [`corium_db::Idents`].
//! * [`txforms`] turns map/list transaction forms into [`corium_tx::TxItem`]s.
//!
//! Both depend only on the pure engine crates (`corium-core`, `corium-db`,
//! `corium-query`, `corium-tx`), so they compile anywhere those do —
//! including `wasm32-unknown-unknown`. `corium-protocol` re-exports them for
//! back-compatible paths (`corium_protocol::schemaform`,
//! `corium_protocol::txforms`).

pub mod schemaform;
pub mod txforms;
