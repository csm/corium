//! Serving the authorization database from this transactor node.
//!
//! The transactor already holds every database it leads, so the authz database
//! is just another one of them: the policy source hands out a snapshot of the
//! local `DbState` with no network hop, and the transactor's own basis watch is
//! the change signal that triggers recompilation. This is also the surface
//! where write admission is decided, so authorizing a transaction against the
//! node's own authz snapshot makes admission consistent at the serialization
//! point.

use std::sync::Arc;

use corium_authz::source::{PolicySource, SourceError};
use corium_db::Db;

use crate::node::TransactorNode;

/// A [`PolicySource`] reading the authz database this node hosts.
pub struct NodePolicySource {
    node: Arc<TransactorNode>,
    db: String,
}

impl NodePolicySource {
    /// Reads policy from the database `db` on `node`.
    pub fn new(node: Arc<TransactorNode>, db: impl Into<String>) -> Self {
        Self {
            node,
            db: db.into(),
        }
    }
}

#[tonic::async_trait]
impl PolicySource for NodePolicySource {
    fn name(&self) -> &str {
        &self.db
    }

    async fn snapshot(&self) -> Result<Db, SourceError> {
        match self.node.db_state(&self.db).await {
            Ok(state) => Ok(state.db()),
            // Distinguish "no such database" — a misconfiguration an operator
            // must see — from a transient read failure.
            Err(crate::node::NodeError::UnknownDb(name)) => Err(SourceError::Unavailable(name)),
            Err(error) => Err(SourceError::Failed {
                db: self.db.clone(),
                reason: error.to_string(),
            }),
        }
    }

    async fn changed(&self, basis_t: u64) {
        let Ok(state) = self.node.db_state(&self.db).await else {
            tokio::time::sleep(corium_authz::source::DEFAULT_POLL_INTERVAL).await;
            return;
        };
        let mut basis = state.basis_watch();
        // `wait_for` returns immediately when the basis has already moved past
        // the compiled snapshot, so no policy change can be missed between the
        // compile and the subscribe.
        let _ = basis.wait_for(|current| *current > basis_t).await;
    }
}
