//! The ephemeral, in-memory corium engine driving the browser demo.
//!
//! This module is pure Rust — no `wasm_bindgen`, no async, no I/O — so it
//! compiles and unit-tests on the host toolchain exactly as it runs in the
//! browser. It ties together the pure engine crates:
//!
//! * [`corium_forms::schemaform`] installs an EDN schema into a [`Db`].
//! * [`corium_forms::txforms`] converts EDN transaction forms into items.
//! * [`corium_tx::prepare`] + [`Db::with_transaction`] apply a transaction,
//!   mirroring `corium_transactor::Transactor::transact` minus the durable
//!   log and wall-clock instant (neither is meaningful for a memory-only,
//!   ephemeral database).
//! * [`corium_query`] runs EDN Datalog against the current [`Db`] value.

use std::fmt;

use corium_core::{EntityId, KeywordInterner, Partition};
use corium_db::{Db, FIRST_USER_ID, Idents};
use corium_forms::schemaform::schema_from_edn;
use corium_forms::txforms::tx_items_from_edn;
use corium_query::edn::{self, Edn};
use corium_query::{QInput, q};
use corium_tx::prepare;

/// Any failure surfaced to the console, already rendered to a message.
#[derive(Debug, Clone)]
pub struct EngineError(pub String);

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EngineError {}

fn err(context: &str, e: impl fmt::Display) -> EngineError {
    EngineError(format!("{context}: {e}"))
}

/// An in-memory corium database: a schema, the current immutable [`Db`]
/// value, and the naming/allocation state needed to keep transacting.
pub struct Engine {
    db: Db,
    idents: Idents,
    interner: KeywordInterner,
    next_user: u64,
    tx_count: u64,
}

impl Engine {
    /// Boots an empty database from an EDN schema.
    ///
    /// The schema may be either a single top-level vector of attribute maps
    /// (as in `schema.edn`) or a bare sequence of maps.
    ///
    /// # Errors
    /// Returns [`EngineError`] if the schema text is unreadable or invalid.
    pub fn from_schema_edn(schema_edn: &str) -> Result<Self, EngineError> {
        let forms = edn::read_all(schema_edn).map_err(|e| err("reading schema", e))?;
        let attrs = unwrap_single_vector(&forms);
        let (schema, idents) = schema_from_edn(attrs).map_err(|e| err("installing schema", e))?;
        let interner = KeywordInterner::default();
        let db = Db::new(schema).with_naming(idents.clone(), interner.clone());
        Ok(Self {
            db,
            idents,
            interner,
            next_user: FIRST_USER_ID,
            tx_count: 0,
        })
    }

    /// Applies a stream of transactions: each top-level form in `text` is one
    /// transaction (a vector of ops), applied in order. Returns the number of
    /// transactions applied. Used to seed the bundled dataset.
    ///
    /// # Errors
    /// Returns [`EngineError`] on the first unreadable or rejected transaction.
    pub fn load_edn(&mut self, text: &str) -> Result<usize, EngineError> {
        let txns = edn::read_all(text).map_err(|e| err("reading data", e))?;
        let mut applied = 0;
        for txn in &txns {
            let forms = txn
                .as_seq()
                .ok_or_else(|| EngineError(format!("transaction must be a vector, got {txn}")))?;
            self.apply(forms)?;
            applied += 1;
        }
        Ok(applied)
    }

    /// Applies one transaction given as an EDN vector of ops, returning a
    /// small EDN report `{:basis-t t :tx-datoms n}`.
    ///
    /// # Errors
    /// Returns [`EngineError`] if the text is unreadable or the transaction
    /// is rejected.
    pub fn transact(&mut self, text: &str) -> Result<String, EngineError> {
        let form = edn::read_one(text).map_err(|e| err("reading transaction", e))?;
        let forms = form
            .as_seq()
            .ok_or_else(|| EngineError("transaction must be a vector of ops".into()))?
            .to_vec();
        let datoms = self.apply(&forms)?;
        Ok(Edn::Map(vec![
            (
                Edn::keyword("basis-t"),
                Edn::Long(i64::try_from(self.db.basis_t()).unwrap_or(i64::MAX)),
            ),
            (
                Edn::keyword("tx-datoms"),
                Edn::Long(i64::try_from(datoms).unwrap_or(i64::MAX)),
            ),
        ])
        .to_string())
    }

    /// Runs an EDN Datalog query against the current database value and
    /// returns the result as an EDN string.
    ///
    /// The query's default `$` source is bound to the current database. A
    /// query declaring extra `:in` parameters is not supported here (bind
    /// values inline in the `:where` clauses for the demo).
    ///
    /// # Errors
    /// Returns [`EngineError`] for unreadable, malformed, or failing queries.
    pub fn q(&self, text: &str) -> Result<String, EngineError> {
        let form = edn::read_one(text).map_err(|e| err("reading query", e))?;
        let result = q(&form, &[QInput::Db(&self.db)]).map_err(|e| err("query", e))?;
        Ok(result.to_string())
    }

    /// Current transaction basis (monotonic; increments once per transaction).
    #[must_use]
    pub const fn basis_t(&self) -> u64 {
        self.db.basis_t()
    }

    /// A small EDN summary of the loaded database, for `console.log`.
    #[must_use]
    pub fn stats_edn(&self) -> String {
        let datoms = self.db.datoms().len();
        Edn::Map(vec![
            (
                Edn::keyword("basis-t"),
                Edn::Long(i64::try_from(self.db.basis_t()).unwrap_or(i64::MAX)),
            ),
            (
                Edn::keyword("transactions"),
                Edn::Long(i64::try_from(self.tx_count).unwrap_or(i64::MAX)),
            ),
            (
                Edn::keyword("datoms"),
                Edn::Long(i64::try_from(datoms).unwrap_or(i64::MAX)),
            ),
        ])
        .to_string()
    }

    /// The shared apply path used by both loading and interactive transact.
    /// Returns the number of datoms committed. Mirrors the transactor's
    /// commit sequence: intern new keyword values, reinstall naming so the
    /// new datoms resolve, then `prepare` + `with_transaction`.
    fn apply(&mut self, forms: &[Edn]) -> Result<usize, EngineError> {
        let mut interner = self.interner.clone();
        let items = tx_items_from_edn(&self.db, &mut interner, forms)
            .map_err(|e| err("transaction form", e))?;
        // New keyword names must be visible to the value before its datoms
        // are applied (mirrors `Transactor::update_naming`).
        self.db = self
            .db
            .clone()
            .with_naming(self.idents.clone(), interner.clone());
        let t = self.db.basis_t() + 1;
        let tx_id = EntityId::new(Partition::Tx as u32, t);
        let prepared = prepare(&self.db, items, tx_id, self.next_user)
            .map_err(|e| err("preparing transaction", e))?;
        let count = prepared.datoms.len();
        self.db = self.db.with_transaction(t, &prepared.datoms);
        self.next_user = prepared
            .tempids
            .values()
            .filter(|e| e.partition() == Partition::User as u32)
            .map(|e| e.sequence() + 1)
            .max()
            .unwrap_or(self.next_user)
            .max(self.next_user);
        self.interner = interner;
        self.tx_count += 1;
        Ok(count)
    }
}

/// Schema files wrap their attribute maps in one top-level `[ … ]`; accept
/// either that or a bare sequence of maps.
fn unwrap_single_vector(forms: &[Edn]) -> &[Edn] {
    if let [only] = forms
        && let Some(inner) = only.as_seq()
    {
        return inner;
    }
    forms
}

// A schema/dataset used only by the host-side unit tests below.
#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = include_str!("../assets/schema.edn");
    const DATA: &str = include_str!("../assets/releases-1997.edn");

    fn engine() -> Engine {
        let mut e = Engine::from_schema_edn(SCHEMA).expect("schema boots");
        let n = e.load_edn(DATA).expect("dataset loads");
        assert_eq!(n, 4, "dataset is four transactions");
        e
    }

    #[test]
    fn loads_and_counts_releases() {
        let e = engine();
        let out = e
            .q("[:find (count ?r) :where [?r :release/year 1997]]")
            .expect("query runs");
        // 20 curated 1997 releases.
        assert!(out.contains("20"), "expected 20 releases, got {out}");
    }

    #[test]
    fn joins_release_to_artist() {
        let e = engine();
        let out = e
            .q("[:find ?name :where [?r :release/name \"OK Computer\"] \
                 [?r :release/artists ?a] [?a :artist/name ?name]]")
            .expect("query runs");
        assert!(out.contains("Radiohead"), "got {out}");
    }

    #[test]
    fn pulls_tracks_through_media() {
        let e = engine();
        let out = e
            .q("[:find (count ?t) :where \
                 [?r :release/name \"OK Computer\"] [?r :release/media ?m] \
                 [?m :medium/tracks ?t]]")
            .expect("query runs");
        assert!(out.contains("12"), "OK Computer has 12 tracks, got {out}");
    }

    #[test]
    fn transact_is_visible_to_queries() {
        let mut e = engine();
        let before = e.basis_t();
        let report = e
            .transact(
                "[{:db/id \"demo\" \
                   :artist/gid #uuid \"00000000000000000000000000009001\" \
                   :artist/name \"Demo Artist\" :artist/country :country/US \
                   :artist/startYear 1997}]",
            )
            .expect("transact ok");
        assert!(report.contains(":basis-t"), "report was {report}");
        assert!(e.basis_t() > before, "basis advanced");
        let out = e
            .q("[:find ?y :where [?a :artist/name \"Demo Artist\"] [?a :artist/startYear ?y]]")
            .expect("query runs");
        assert!(out.contains("1997"), "got {out}");
    }
}
