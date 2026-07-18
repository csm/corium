//! User database functions (`:db/fn`, ADR-0008): resolution, sandboxed
//! invocation, and recursive tx-data expansion.
//!
//! A database function is an entity whose `:db/fn` attribute holds the
//! function source (compiled and cached by the sandbox). An invocation form
//! `[:my/fn arg…]` inside transaction data calls the function with the
//! db-in-transaction plus the arguments; the returned tx-data is expanded
//! recursively up to a depth limit. The log records only expanded datoms,
//! so replay never re-runs functions.

use corium_core::{Keyword, Value as CoreValue};
use corium_db::Db;
use corium_query::edn::Edn;
use corium_transactor::node::TxFnExpander;
use thiserror::Error;

use crate::sandbox::{Sandbox, SandboxBudget, SandboxError};

/// Database-function expansion failure; aborts the transaction.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DbFnError {
    /// Sandbox rejection, failure, or budget exhaustion.
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    /// Functions kept returning invocations past the depth limit.
    #[error("db function recursion limit ({0}) exceeded")]
    Recursion(usize),
    /// A function returned something other than tx-data.
    #[error("db function must return a sequence of tx forms, got {0}")]
    BadResult(String),
}

/// Transaction operations handled natively by `corium-tx`.
const NATIVE_OPS: &[&str] = &["db/add", "db/retract", "db/cas", "db/retractEntity"];

/// Expands `[:fn-ident arg…]` invocations through a [`Sandbox`].
pub struct DbFnExpander {
    sandbox: Sandbox,
    budget: SandboxBudget,
    max_depth: usize,
}

impl Default for DbFnExpander {
    fn default() -> Self {
        Self::new(SandboxBudget::default())
    }
}

impl DbFnExpander {
    /// Creates an expander with the given per-invocation budget and the
    /// default recursion depth (16).
    #[must_use]
    pub fn new(budget: SandboxBudget) -> Self {
        Self {
            sandbox: Sandbox::new(),
            budget,
            max_depth: 16,
        }
    }

    /// Overrides the recursion depth limit.
    #[must_use]
    pub const fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = max_depth;
        self
    }

    /// The hosted sandbox (exposed for telemetry).
    #[must_use]
    pub const fn sandbox(&self) -> &Sandbox {
        &self.sandbox
    }

    /// Expands every database-function invocation in `forms` against `db`,
    /// returning tx forms containing only native operations and map forms.
    ///
    /// # Errors
    /// Returns [`DbFnError`] when a function fails, is rejected, exceeds
    /// its budget, or recursion exceeds the depth limit.
    pub fn expand(&self, db: &Db, forms: Vec<Edn>) -> Result<Vec<Edn>, DbFnError> {
        self.expand_at(db, forms, self.max_depth)
    }

    fn expand_at(&self, db: &Db, forms: Vec<Edn>, depth: usize) -> Result<Vec<Edn>, DbFnError> {
        let mut out = Vec::with_capacity(forms.len());
        for form in forms {
            match invocation(db, &form) {
                None => out.push(form),
                Some((source, args)) => {
                    if depth == 0 {
                        return Err(DbFnError::Recursion(self.max_depth));
                    }
                    let result =
                        self.sandbox
                            .invoke(&source, Some(db.clone()), args, self.budget)?;
                    let produced = result
                        .as_seq()
                        .ok_or_else(|| DbFnError::BadResult(result.to_string()))?
                        .to_vec();
                    out.extend(self.expand_at(db, produced, depth - 1)?);
                }
            }
        }
        Ok(out)
    }
}

impl TxFnExpander for DbFnExpander {
    fn expand(&self, db: &Db, forms: Vec<Edn>) -> Result<Vec<Edn>, String> {
        Self::expand(self, db, forms).map_err(|error| error.to_string())
    }
}

/// Recognizes a database-function invocation form, returning its source and
/// argument forms.
fn invocation(db: &Db, form: &Edn) -> Option<(String, Vec<Edn>)> {
    let (Edn::Vector(items) | Edn::List(items)) = form else {
        return None;
    };
    let head = items.first()?.as_keyword()?;
    let full = head.namespace.as_deref().map_or_else(
        || head.name.clone(),
        |namespace| format!("{namespace}/{}", head.name),
    );
    if NATIVE_OPS.contains(&full.as_str()) {
        return None;
    }
    let source = fn_source(db, head)?;
    Some((source, items[1..].to_vec()))
}

/// Looks up the `:db/fn` source for a function ident: first through the
/// schema-installed ident registry, then through a `:db/ident`
/// unique-keyword attribute when the database declares one.
fn fn_source(db: &Db, ident: &Keyword) -> Option<String> {
    let fn_attr = db.idents().entid(&Keyword::parse("db/fn"))?;
    let entity = db.idents().entid(ident).or_else(|| {
        let ident_attr = db.idents().entid(&Keyword::parse("db/ident"))?;
        let interned = db.interner().get(ident)?;
        db.lookup(ident_attr, &CoreValue::Keyword(interned))
    })?;
    match db.values(entity, fn_attr).into_iter().next() {
        Some(CoreValue::Str(source)) => Some(source.to_string()),
        _ => None,
    }
}
