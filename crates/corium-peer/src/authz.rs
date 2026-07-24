//! Serving the authorization database to a peer server.
//!
//! A peer server hosts one application database, so the authz database reaches
//! it the same way its own data does: over a second [`Connection`], kept in
//! sync by the peer's subscription. Policy checks then read a local, immutable
//! database value — no per-request round trip to the transactor — and the
//! subscription's tx-report broadcast is the change signal that triggers
//! recompilation.

use std::sync::Arc;

use corium_authz::source::{PolicySource, SourceError};
use corium_db::Db;

use crate::Connection;

/// A [`PolicySource`] reading the authz database over a peer connection.
pub struct ConnectionPolicySource {
    connection: Arc<Connection>,
}

impl ConnectionPolicySource {
    /// Reads policy from the database `connection` is synced to.
    #[must_use]
    pub fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }
}

#[tonic::async_trait]
impl PolicySource for ConnectionPolicySource {
    fn name(&self) -> &str {
        self.connection.db_name()
    }

    async fn snapshot(&self) -> Result<Db, SourceError> {
        Ok(self.connection.db())
    }

    async fn changed(&self, basis_t: u64) {
        // Subscribe before re-reading the basis: a report committed between the
        // two is still delivered, so a policy change is never missed.
        let mut reports = self.connection.tx_reports();
        if self.connection.basis_t() > basis_t {
            return;
        }
        loop {
            match reports.recv().await {
                Ok(report) if report.t > basis_t => return,
                Ok(_) => {}
                // Lagged: the peer applied more than the broadcast buffer held,
                // so the basis has certainly moved.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => return,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tokio::time::sleep(corium_authz::source::DEFAULT_POLL_INTERVAL).await;
                    return;
                }
            }
        }
    }
}
