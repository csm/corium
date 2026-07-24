//! Where a compiled policy comes from: a snapshot of the authz database.
//!
//! The trait is deliberately thin — one method returning a [`Db`] value — so
//! every surface can supply the snapshot it already holds: the transactor
//! reads its own `DbState`, a peer server reads the database value its
//! connection keeps in sync, and tests hand over a value they built in
//! memory. Nothing here fetches over the network on the request path; a
//! snapshot is a cheap clone of an immutable value.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use corium_db::Db;

/// How long a source with no change notification waits between polls.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Failure to obtain a policy snapshot.
#[derive(Clone, Debug, thiserror::Error)]
pub enum SourceError {
    /// The configured authz database does not exist on this surface.
    #[error("authorization database {0:?} is not available")]
    Unavailable(String),
    /// The snapshot could not be read.
    #[error("cannot read authorization database {db:?}: {reason}")]
    Failed {
        /// Database name.
        db: String,
        /// Underlying failure.
        reason: String,
    },
}

/// Supplies snapshots of the authorization database.
#[tonic::async_trait]
pub trait PolicySource: Send + Sync + 'static {
    /// Name of the authz database, for errors and audit lines.
    fn name(&self) -> &str;

    /// The current snapshot.
    ///
    /// # Errors
    /// Returns [`SourceError`] when the database is missing or unreadable;
    /// the authorizer fails closed on it.
    async fn snapshot(&self) -> Result<Db, SourceError>;

    /// Resolves when the source may have advanced past `basis_t`.
    ///
    /// The default polls. Sources with a change signal (a transactor's basis
    /// watch, a peer's tx-report broadcast) override this so a policy change
    /// propagates immediately instead of within one poll interval.
    async fn changed(&self, basis_t: u64) {
        let _ = basis_t;
        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

/// A source over a database value held in memory: the embedded case, and what
/// tests and `corium authz check --policy` use.
pub struct MemoryPolicySource {
    name: String,
    db: RwLock<Db>,
}

impl MemoryPolicySource {
    /// Wraps `db` under the name `name`.
    pub fn new(name: impl Into<String>, db: Db) -> Self {
        Self {
            name: name.into(),
            db: RwLock::new(db),
        }
    }

    /// Replaces the snapshot, as a transaction against the authz database
    /// would.
    pub fn set(&self, db: Db) {
        *self
            .db
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = db;
    }

    /// Wraps this source for use with [`crate::SystemDbAuthorizer`].
    #[must_use]
    pub fn shared(self) -> Arc<dyn PolicySource> {
        Arc::new(self)
    }
}

#[tonic::async_trait]
impl PolicySource for MemoryPolicySource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn snapshot(&self) -> Result<Db, SourceError> {
        Ok(self
            .db
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone())
    }
}
