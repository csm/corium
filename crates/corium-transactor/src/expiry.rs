//! Expiry, compensation, and branch retention: how a saga ends when nobody
//! ends it (ADR-0023).
//!
//! Three duties live here, and they are one duty seen at three distances.
//!
//! **Expiry is mandatory**, because an open saga pins the parent segments its
//! branch is rooted at and holds entity-id blocks the allocator has written
//! off. A deadline the owner extends is the whole liveness story, so something
//! has to act when the extensions stop. That something is a sweep — an
//! operator-service job in the fullness of ADR-0019, and until then (and
//! afterwards, as the fallback that keeps the data plane free of any
//! dependence on that service) a background duty of the transactor hosting the
//! database.
//!
//! **A saga is live only where its branch lives.** Registry datoms travel with
//! backup, restore, `db fork`, and replication; branches do not. So a database
//! can hold `:open` entries whose branches were never its own, and the rule
//! that covers every such case is to expire them when this node starts hosting
//! the database. `:expired` already means "the system, not a decision, ended
//! this", which is exactly what happened.
//!
//! **Retention is the grace period afterwards.** A finished saga's branch is
//! not deleted with the flip: a committed one is the step-grain audit annex
//! the design points auditors at, and an expired one is what a returning owner
//! salvages from. Both are kept for a window — the node's policy, or the
//! saga's own `:db.saga/retain-for` — measured from the transition that
//! finished it, after which the sweep discards the branch.
//!
//! What this module holds is the part of that with no I/O in it: turning a
//! registry entry into the compensating transaction it registered, and the
//! report a pass produces. The passes themselves are
//! [`crate::node::TransactorNode`] methods, because expiring a saga is a
//! transaction and discarding a branch is a store operation.

use std::sync::Arc;

use corium_core::{Keyword, Value};
use corium_db::saga::SagaEntry;
use corium_db::{Db, bootstrap};
use corium_forms::txforms::tx_data_forms;
use corium_query::edn::{Edn, read_all};

use crate::node::TxFnExpander;

/// The EDN spelling of a token naming the database in scope under `name`.
///
/// This is how a second database reaches a `:db/fn`: the token a function
/// receives as its `db` argument is ordinary pure data — a map under
/// `:corium.db/*` keys — so one naming another database in scope rides in the
/// invocation form as an argument beside any other. `basis_t` is what the
/// function reads back from `corium.api/basis-t`; the value it actually
/// queries is whatever the expander holds under `name`, so a stale basis here
/// cannot widen what is readable. [`crate::txfn`] parses it, and its tests
/// hold the two halves to the same shape.
#[must_use]
pub fn named_db_token(name: &str, basis_t: u64) -> Edn {
    Edn::Map(vec![
        (
            Edn::keyword("corium.db/basis-t"),
            Edn::Long(i64::try_from(basis_t).unwrap_or(i64::MAX)),
        ),
        (Edn::keyword("corium.db/of"), Edn::Str(name.to_owned())),
    ])
}

/// The name the branch is in scope under while a compensation function runs.
///
/// A `:db/fn` compensation is invoked with the parent's current value as its
/// `db` argument and this as a second: `(fn [db branch] …)`. The name is
/// fixed rather than configurable because it is part of the calling
/// convention a registered function is written against.
pub const BRANCH_SCOPE: &str = "branch";

/// What a saga's registered compensation comes to at the moment it is needed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Compensation {
    /// Nothing to apply: none was registered, or the caller replaced the
    /// registered one with nothing.
    None,
    /// Transaction forms to apply atomically with the flip.
    Forms(Vec<Edn>),
    /// A registered compensation deliberately not applied, and why.
    ///
    /// This is not a failure. The one case is a branchless expiry: a restored
    /// or forked database shares its timeline's past with the original but not
    /// its future, so applying the same failure record on both sides of the
    /// divergence would double every externally visible consequence it stands
    /// for. The reason is recorded in `:db.saga/on-abort-error`.
    Skipped(String),
}

impl Compensation {
    /// The forms to apply, empty when there are none.
    #[must_use]
    pub fn forms(&self) -> &[Edn] {
        match self {
            Self::Forms(forms) => forms,
            Self::None | Self::Skipped(_) => &[],
        }
    }

    /// Why a registered compensation was not applied, if it was not.
    #[must_use]
    pub fn skipped(&self) -> Option<&str> {
        match self {
            Self::Skipped(reason) => Some(reason),
            Self::None | Self::Forms(_) => None,
        }
    }
}

/// What the caller wants done about the saga's registered compensation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Intent {
    /// Apply whatever the saga registered at open, if anything.
    Registered,
    /// Apply these forms instead of whatever it registered.
    ///
    /// This is `abort --compensate`: the owner is present and authoring the
    /// failure record by hand, so an empty vector means "abort with nothing",
    /// not "abort with what was registered".
    Replace(Vec<Edn>),
    /// Apply nothing, recording `reason` if something was registered.
    ///
    /// The branchless expiry of a restored or forked parent: the database
    /// shares its timeline's past with the original but not its future, so
    /// applying the same failure record on both sides of the divergence would
    /// double every externally visible consequence it stands for.
    Skip(&'static str),
}

/// Resolves `intent` against what `entry` registered into the forms to apply
/// now.
///
/// # Errors
/// As [`compose`], for the [`Intent::Registered`] case that consults it.
pub fn resolve(
    intent: Intent,
    entry: &SagaEntry,
    parent: &Db,
    branch: Option<&Db>,
    expander: Option<&Arc<dyn TxFnExpander>>,
) -> Result<Compensation, String> {
    match intent {
        Intent::Registered => compose(entry, parent, branch, expander),
        Intent::Replace(forms) => Ok(Compensation::Forms(forms)),
        Intent::Skip(reason) => Ok(if registers_compensation(entry) {
            Compensation::Skipped(reason.to_owned())
        } else {
            Compensation::None
        }),
    }
}

/// Resolves the compensation `entry` registered into the forms to apply now.
///
/// `branch` is the saga's branch value, absent when the branch is gone — a
/// restored or forked parent, or one whose retention window already closed.
/// A static `:db.saga/on-abort-tx` needs neither the branch nor a function
/// runtime and is applied either way; a `:db.saga/on-abort-fn` needs both,
/// because the whole reason to register a function rather than static data is
/// to author a failure record *about* what the branch did.
///
/// # Errors
/// Returns why the compensation could not be composed: unreadable EDN, both
/// forms registered at once, a function this build cannot run, or a function
/// that failed. The caller decides what that means — an explicit abort fails
/// with it, while an expiry records it and expires anyway, because liveness
/// outranks a compensation nobody is present to fix.
pub fn compose(
    entry: &SagaEntry,
    parent: &Db,
    branch: Option<&Db>,
    expander: Option<&Arc<dyn TxFnExpander>>,
) -> Result<Compensation, String> {
    match (&entry.on_abort_tx, entry.on_abort_fn) {
        (Some(_), Some(_)) => Err(
            "saga registers both :db.saga/on-abort-tx and :db.saga/on-abort-fn; \
             a compensation is one or the other"
                .to_owned(),
        ),
        (Some(tx_data), None) => read_all(tx_data)
            .map(|read| Compensation::Forms(tx_data_forms(&read)))
            .map_err(|error| format!(":db.saga/on-abort-tx is not EDN: {error}")),
        (None, Some(db_fn)) => {
            let Some(branch) = branch else {
                return Ok(Compensation::Skipped(
                    "the branch is gone, so :db.saga/on-abort-fn was not invoked".to_owned(),
                ));
            };
            let expander = expander.ok_or_else(|| {
                ":db.saga/on-abort-fn needs a database-function runtime, and this \
                 transactor has none"
                    .to_owned()
            })?;
            let ident = fn_ident(parent, db_fn).ok_or_else(|| {
                format!(
                    ":db.saga/on-abort-fn entity {} has no :db/ident, so there is no \
                     name to invoke it by",
                    db_fn.raw()
                )
            })?;
            let form = Edn::Vector(vec![
                Edn::Keyword(ident),
                named_db_token(BRANCH_SCOPE, branch.basis_t()),
            ]);
            expander
                .expand_with(parent, &[(BRANCH_SCOPE, branch)], vec![form])
                .map(Compensation::Forms)
                .map_err(|error| format!(":db.saga/on-abort-fn failed: {error}"))
        }
        (None, None) => Ok(Compensation::None),
    }
}

/// The keyword a `:db/fn` entity answers to, so it can be named in tx data.
///
/// Schema idents first, then a user `:db/ident` attribute — the same two
/// places [`crate::txfn`] looks when it resolves an invocation, in the same
/// order, so a function that can be called from ordinary transaction data can
/// be called as a compensation and vice versa.
fn fn_ident(db: &Db, entity: corium_core::EntityId) -> Option<Keyword> {
    if let Some(keyword) = db.idents().ident(entity) {
        return Some(keyword.clone());
    }
    let ident_attr = db.idents().entid(&Keyword::parse("db/ident"))?;
    match db.values(entity, ident_attr).first() {
        Some(Value::Keyword(interned)) => db.interner().resolve(*interned),
        _ => None,
    }
}

/// Whether `entry` registered a compensation at all.
#[must_use]
pub fn registers_compensation(entry: &SagaEntry) -> bool {
    entry.on_abort_tx.is_some() || entry.on_abort_fn.is_some()
}

/// Transaction items recording why a system-time compensation did not land.
#[must_use]
pub fn on_abort_error_item(entry: &SagaEntry, reason: &str) -> corium_tx::TxItem {
    corium_tx::TxItem::Op(corium_tx::TxOp::Add(
        corium_tx::EntityRef::Id(entry.entity),
        bootstrap::SAGA_ON_ABORT_ERROR,
        Value::Str(reason.into()),
    ))
}

/// What one sweep pass did to one database's registry.
///
/// Every field is a saga id, so a caller can say which sagas changed rather
/// than only how many — which is what an operator reading a log line, or a
/// test asserting the sweep did its job, actually needs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SweepReport {
    /// Sagas expired because their deadline had passed.
    pub expired: Vec<u128>,
    /// Sagas expired because their branch is not this database's.
    pub branchless: Vec<u128>,
    /// Branches discarded at the end of their retention window.
    pub discarded: Vec<u128>,
    /// Sagas the pass could not finish, and why.
    ///
    /// A sweep never stops at the first failure: an abandoned saga the sweep
    /// cannot end is a leak, and the other overdue sagas are not to blame
    /// for it.
    pub failures: Vec<(u128, String)>,
}

impl SweepReport {
    /// Whether the pass changed anything at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.expired.is_empty()
            && self.branchless.is_empty()
            && self.discarded.is_empty()
            && self.failures.is_empty()
    }
}
