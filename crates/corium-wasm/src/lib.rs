//! WebAssembly bindings for an ephemeral, in-memory corium database.
//!
//! Compiles the pure engine subset (`corium-core`, `corium-db`, `corium-tx`,
//! `corium-query`, `corium-forms`) to `wasm32-unknown-unknown` and exposes a
//! tiny surface to JavaScript. A curated MusicBrainz-shaped dataset of 1997
//! releases is embedded and loaded at construction, so the whole thing is a
//! single self-contained `.wasm` with no network fetch.
//!
//! ```js
//! import init, { Mbrainz } from "./corium_wasm.js";
//! await init();
//! const mb = new Mbrainz();
//! mb.q('[:find ?name :where [?r :release/year 1997] [?r :release/name ?name]]');
//! mb.transact('[{:artist/gid #uuid "…" :artist/name "New Artist"}]');
//! ```
//!
//! Results and reports are returned as EDN strings.

mod engine;

use engine::Engine;
use wasm_bindgen::prelude::*;

/// The `MusicBrainz` schema (mirrors `examples/musicbrainz/schema.edn`).
const SCHEMA: &str = include_str!("../assets/schema.edn");
/// Curated 1997 releases, artists, labels, media, and tracks.
const DATA: &str = include_str!("../assets/releases-1997.edn");

/// An in-browser, memory-only corium database seeded with the 1997 dataset.
#[wasm_bindgen]
pub struct Mbrainz {
    engine: Engine,
}

#[wasm_bindgen]
impl Mbrainz {
    /// Boots the schema and loads the embedded 1997 dataset.
    ///
    /// # Errors
    /// Returns a JS error if the bundled schema or data fail to load (a bug).
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<Mbrainz, JsError> {
        let mut engine = Engine::from_schema_edn(SCHEMA)?;
        engine.load_edn(DATA)?;
        Ok(Mbrainz { engine })
    }

    /// Runs an EDN Datalog query and returns the result as an EDN string.
    ///
    /// # Errors
    /// Returns a JS error for unreadable, malformed, or failing queries.
    pub fn q(&self, query: &str) -> Result<String, JsError> {
        Ok(self.engine.q(query)?)
    }

    /// Applies one transaction (an EDN vector of ops) and returns an EDN
    /// report `{:basis-t t :tx-datoms n}`. The change is immediately visible
    /// to subsequent [`Mbrainz::q`] calls, and lost when the page reloads.
    ///
    /// # Errors
    /// Returns a JS error if the text is unreadable or the transaction is
    /// rejected (schema violation, bad lookup ref, …).
    pub fn transact(&mut self, tx: &str) -> Result<String, JsError> {
        Ok(self.engine.transact(tx)?)
    }

    /// Current transaction basis (increments once per committed transaction).
    #[wasm_bindgen(js_name = basisT)]
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        reason = "basis_t is a small transaction counter; f64 is exact well past \
                  any value this ephemeral demo reaches, and JS numbers are f64"
    )]
    pub fn basis_t(&self) -> f64 {
        self.engine.basis_t() as f64
    }

    /// An EDN summary of the loaded database `{:basis-t … :transactions … :datoms …}`.
    #[must_use]
    pub fn stats(&self) -> String {
        self.engine.stats_edn()
    }
}
