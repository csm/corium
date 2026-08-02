//! Boundary conversion between human-facing forms and engine types.
//!
//! Boundary modules bridge the formats used at wire, file, and console
//! boundaries onto the engine's schema and transaction types:
//!
//! * [`schemaform`] turns `{:db/ident … :db/valueType …}` maps into a
//!   [`corium_core::Schema`] and its [`corium_db::Idents`].
//! * [`desired`] normalizes either syntax into the desired-schema model a
//!   schema update compares against a database, allocating no entity ids.
//! * [`planner`] diffs that model against an immutable database value and
//!   emits the property-level plan described in
//!   `docs/design/schema-migrations.md`.
//! * `toml_schema` (feature `toml`) turns grouped or flat TOML declarations
//!   into one neutral attribute model and equivalent EDN schema forms.
//! * [`txforms`] turns map/list transaction forms into [`corium_tx::TxItem`]s.
//!
//! The EDN modules depend only on the pure engine crates (`corium-core`,
//! `corium-db`, `corium-query`, `corium-tx`), so disabling default features
//! keeps TOML and Serde out of consumers such as `corium-wasm`.
//! `corium-protocol` re-exports the EDN modules for back-compatible paths
//! (`corium_protocol::schemaform`, `corium_protocol::txforms`).

pub mod desired;
pub mod planner;
pub mod schemaform;
#[cfg(feature = "toml")]
pub mod toml_schema;
pub mod txforms;
