//! Clojurust integration for Corium (M5): boundary value conversion, the
//! `corium.api` client namespace, and the sandboxed database-function host
//! (see `docs/design/clojurust-integration.md`, ADR-0002, ADR-0008).
//!
//! Clojurust isolates own per-thread GC heaps, so every cljrs value is
//! confined to the thread that created it. This crate follows two rules
//! throughout: work crossing between engine threads and a cljrs isolate
//! travels as plain boundary EDN, and each sandbox owns a dedicated worker
//! thread that doubles as the watchdog boundary for runaway user code.

pub mod api;
pub mod convert;
pub mod dbfn;
pub mod query;
pub mod sandbox;
