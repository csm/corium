//! Saga branches: the overlay databases a saga's steps run against
//! (ADR-0023).
//!
//! A branch is the parent's published state as of the saga's opening basis
//! `t₀` plus a log of its own, hosted in the parent's transactor process. It
//! is deliberately *a database* rather than a new kind of thing: it has a
//! [`DbState`] like any other, so a step is an ordinary `Transact`, a tier-2
//! reader is an ordinary `Subscribe`, and time views, Datalog, Pull, and SQL
//! come along without a line of new read-path code. What this module adds is
//! the three things a branch does not share with a catalogued database.
//!
//! **Its name says whose it is.** A branch is hosted under
//! `<parent>.saga.<id>`, which is not a name [`crate::node`] will create a
//! database under — user database names are alphanumeric — so a branch can
//! never be confused for one, is never listed by `db list`, and is never
//! taken over as a standalone database by a standby node.
//!
//! **It borrows the parent's lease and keys.** Branch commits are fenced by
//! the parent's write lease, because a branch is not independently owned: the
//! node that owns the parent hosts its branches, and a node that loses the
//! parent has no business acking a step. Its blobs and log records are sealed
//! under the parent's data key, since a branch shares the parent's segments
//! by construction and lives in the same trust domain.
//!
//! **Its steps obey the saga's declaration.** The registry entry — leased id
//! blocks, reservation set — is read from the parent at step time and applied
//! by [`corium_tx::branch`]. Reading it per batch rather than caching it at
//! open is what lets an unsealed saga widen its reservation set with an
//! ordinary parent transaction while its branch is running.
//!
//! Branches are opened on demand rather than at the instant the saga opens.
//! Creation is a deterministic function of the registry entry, so "open the
//! branch if it isn't hosted" is both the ordinary path and the crash
//! recovery path, and no window exists in which a saga is open but its branch
//! is unreachable for want of a step that never ran.

use std::sync::Arc;

use corium_db::saga::{self, SagaStatus};
use corium_tx::branch::BranchRules;

use crate::node::{DbState, NodeError};

pub use corium_db::saga::{branch_name, is_branch_name, parse_branch_name};

/// What a hosted [`DbState`] needs to know to be a branch.
pub struct Branch {
    /// The database this branch overlays, whose lease fences its commits and
    /// whose registry states its rules.
    pub(crate) parent: Arc<DbState>,
    /// The saga this branch belongs to.
    pub(crate) saga: u128,
    /// The parent basis the branch is rooted at.
    pub(crate) basis_t: u64,
}

impl Branch {
    /// The saga this branch belongs to.
    #[must_use]
    pub const fn saga(&self) -> u128 {
        self.saga
    }

    /// The parent basis this branch is rooted at.
    #[must_use]
    pub const fn basis_t(&self) -> u64 {
        self.basis_t
    }

    /// The database this branch overlays.
    #[must_use]
    pub fn parent_name(&self) -> &str {
        self.parent.name()
    }

    /// The rules a step must obey right now, read from the parent's registry.
    ///
    /// Steps stop the moment the saga does. A branch outlives its saga for as
    /// long as retention keeps it readable, and a step landing in a branch
    /// whose saga has already committed would be novelty with nowhere to
    /// merge — so the refusal is here, where the parent's registry is the
    /// only authority worth asking.
    ///
    /// # Errors
    /// Returns [`NodeError::BadRequest`] when the saga is gone from the
    /// registry or is no longer open.
    pub fn step_rules(&self) -> Result<BranchRules, NodeError> {
        let db = self.parent.db();
        let entry = saga::entry(&db, self.saga).ok_or_else(|| {
            NodeError::BadRequest(format!(
                "saga {:032x} is not in {}'s registry",
                self.saga,
                self.parent.name()
            ))
        })?;
        match entry.status {
            Some(SagaStatus::Open) => Ok(BranchRules::of(&entry)),
            Some(status) => Err(NodeError::BadRequest(format!(
                "saga {:032x} is {status}; its branch no longer accepts steps",
                self.saga
            ))),
            None => Err(NodeError::BadRequest(format!(
                "saga {:032x} has no status; its branch does not accept steps",
                self.saga
            ))),
        }
    }
}
