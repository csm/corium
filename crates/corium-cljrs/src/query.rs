//! Sandbox-backed resolution for query predicate/function clauses.
//!
//! Wires the [`corium_query::ExternCall`] seam (resolution step 2 in
//! `docs/design/query-engine.md`) to the database-function sandbox: names
//! registered here evaluate their cljrs source under the same fuel,
//! allocation, and deadline discipline as `:db/fn` code. Registration is
//! explicit — full user-code query fns remain post-v1 backlog.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use corium_core::Value;
use corium_db::Db;
use corium_query::builtins::CallResult;
use corium_query::edn::Edn;
use corium_query::{ExternCall, QueryError, boundary};

use crate::sandbox::{Sandbox, SandboxBudget};

/// A registry of sandbox-executed query functions.
pub struct QueryFns {
    sandbox: Arc<Sandbox>,
    budget: SandboxBudget,
    sources: RwLock<HashMap<String, Arc<str>>>,
}

impl QueryFns {
    /// Creates an empty registry backed by its own sandbox.
    #[must_use]
    pub fn new(budget: SandboxBudget) -> Arc<Self> {
        Arc::new(Self {
            sandbox: Arc::new(Sandbox::new()),
            budget,
            sources: RwLock::new(HashMap::new()),
        })
    }

    /// Registers (or replaces) the cljrs source for a call clause name.
    pub fn register(&self, name: impl Into<String>, source: impl Into<Arc<str>>) {
        self.sources
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.into(), source.into());
    }

    /// Builds the extern resolver for one execution against `db` (the
    /// database supplies keyword naming for boundary conversion).
    #[must_use]
    pub fn extern_call(self: &Arc<Self>, db: &Db) -> ExternCall {
        let this = Arc::clone(self);
        let db = db.clone();
        Arc::new(move |name, values| {
            let source = this
                .sources
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(name)?
                .clone();
            let args: Vec<Edn> = values
                .iter()
                .map(|value| boundary::value_to_edn(&db, value))
                .collect();
            let outcome = this
                .sandbox
                .invoke(&source, None, args, this.budget)
                .map_err(|error| QueryError::Unsupported(format!("{name}: {error}")))
                .and_then(|result| edn_call_result(&db, &result));
            Some(outcome)
        })
    }
}

/// Maps a sandbox result onto the call-clause result shapes.
fn edn_call_result(db: &Db, form: &Edn) -> Result<CallResult, QueryError> {
    match form {
        Edn::Bool(truth) => Ok(CallResult::Test(*truth)),
        Edn::Vector(rows) if rows.iter().all(|row| matches!(row, Edn::Vector(_))) => {
            let rel = rows
                .iter()
                .map(|row| {
                    row.as_seq()
                        .expect("vector rows")
                        .iter()
                        .map(|item| scalar(db, item))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CallResult::Rel(rel))
        }
        Edn::Vector(items) | Edn::List(items) | Edn::Set(items) => Ok(CallResult::Coll(
            items
                .iter()
                .map(|item| scalar(db, item))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        other => Ok(CallResult::Scalar(scalar(db, other)?)),
    }
}

fn scalar(db: &Db, form: &Edn) -> Result<Value, QueryError> {
    boundary::edn_to_value(Some(db), form)
        .ok_or_else(|| QueryError::Type(format!("query fn returned non-scalar {form}")))
}
