//! Embedded single-writer transaction pipeline and index publisher, plus the
//! networked transactor process (lease, gRPC services, indexing job).

pub mod backend;
pub mod backup;
pub mod lease;
pub mod metrics;
pub mod node;
pub mod server;

pub use backend::{LogBackend, NodeStore, StoreSpec};

use corium_core::{EntityId, IndexOrder, KeywordInterner, Partition, Schema};
use corium_db::{Db, FIRST_USER_ID, Idents};
use corium_index::Segment;
use corium_log::{LogError, TransactionLog, TxRecord};
use corium_store::{BlobStore, RootStore, StoreError};
use corium_tx::{PreparedTx, TxError, TxItem, prepare};
use std::{
    sync::{Arc, Mutex, mpsc},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

/// Result delivered after a transaction is durable and visible.
#[derive(Clone, Debug)]
pub struct TxReport {
    /// Database before the transaction.
    pub db_before: Db,
    /// Database including the transaction.
    pub db_after: Db,
    /// Prepared transaction and tempid map.
    pub tx: PreparedTx,
    /// Commit timestamp.
    pub tx_instant: i64,
}

/// Pipeline errors.
#[derive(Debug, Error)]
pub enum TransactError {
    /// Transaction rejected before durability.
    #[error(transparent)]
    Tx(#[from] TxError),
    /// Durable log failed.
    #[error(transparent)]
    Log(#[from] LogError),
    /// Index/root store failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// System clock predates the Unix epoch.
    #[error("system clock is before Unix epoch")]
    Clock,
    /// An index-building worker failed before returning its result.
    #[error("index task failed: {0}")]
    IndexTask(String),
    /// A newer lease version owns the database root; this writer is deposed.
    #[error("deposed: database root is owned by lease version {published}")]
    Deposed {
        /// Lease version found on the published root.
        published: u64,
    },
}

struct State {
    db: Db,
    next_user: u64,
    last_instant: i64,
    subscribers: Vec<mpsc::Sender<TxReport>>,
}

/// A serialized, in-process transactor. The log append is the commit point.
pub struct EmbeddedTransactor {
    log: Arc<dyn TransactionLog>,
    state: Mutex<State>,
}
impl EmbeddedTransactor {
    /// Recovers a transactor by replaying the durable log exactly once.
    ///
    /// # Errors
    /// Returns an error when the durable log cannot be replayed.
    pub fn recover(schema: Schema, log: Arc<dyn TransactionLog>) -> Result<Self, TransactError> {
        Self::recover_from(Db::new(schema), log)
    }

    /// Recovers from an empty base database value (schema plus naming) by
    /// replaying the durable log exactly once.
    ///
    /// # Errors
    /// Returns an error when the durable log cannot be replayed.
    pub fn recover_from(base: Db, log: Arc<dyn TransactionLog>) -> Result<Self, TransactError> {
        let mut db = base;
        let mut last_instant = i64::MIN;
        for record in log.replay()? {
            db = db.with_transaction(record.t, &record.datoms);
            last_instant = last_instant.max(record.tx_instant);
        }
        // Allocation must resume past every id that ever appeared in the log,
        // not just ids with current datoms; otherwise a fully retracted
        // entity's id would be reused after a restart.
        let next_user = db
            .recorded_datoms()
            .iter()
            .filter(|d| d.e.partition() == Partition::User as u32)
            .map(|d| d.e.sequence() + 1)
            .max()
            .unwrap_or(FIRST_USER_ID);
        Ok(Self {
            log,
            state: Mutex::new(State {
                db,
                next_user,
                last_instant,
                subscribers: Vec::new(),
            }),
        })
    }
    /// Returns the current immutable database value.
    #[must_use]
    pub fn db(&self) -> Db {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .db
            .clone()
    }
    /// Subscribes to reports for transactions committed after this call.
    pub fn subscribe(&self) -> mpsc::Receiver<TxReport> {
        let (tx, rx) = mpsc::channel();
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .subscribers
            .push(tx);
        rx
    }
    /// Validates, durably appends, applies, and reports a transaction.
    ///
    /// # Errors
    /// Returns an error for rejected transaction data, clock failure, or when
    /// the durable append fails. No report is sent on error.
    pub fn transact(
        &self,
        items: impl IntoIterator<Item = TxItem>,
    ) -> Result<TxReport, TransactError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = state.db.clone();
        let t = before.basis_t() + 1;
        let tx_id = EntityId::new(Partition::Tx as u32, t);
        let prepared = prepare(&before, items, tx_id, state.next_user)?;
        let millis = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| TransactError::Clock)?
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        let tx_instant = millis.max(state.last_instant.saturating_add(1));
        self.log.append(&TxRecord {
            t,
            tx_instant,
            datoms: prepared.datoms.clone(),
        })?;
        state.db = before.with_transaction(t, &prepared.datoms);
        state.last_instant = tx_instant;
        state.next_user = prepared
            .tempids
            .values()
            .filter(|e| e.partition() == Partition::User as u32)
            .map(|e| e.sequence() + 1)
            .max()
            .unwrap_or(state.next_user)
            .max(state.next_user);
        let report = TxReport {
            db_before: before,
            db_after: state.db.clone(),
            tx: prepared,
            tx_instant,
        };
        state
            .subscribers
            .retain(|subscriber| subscriber.send(report.clone()).is_ok());
        Ok(report)
    }
    /// Replaces the ident/keyword naming attached to the current database
    /// value (used when the boundary interns new keywords).
    pub fn update_naming(&self, idents: Idents, interner: KeywordInterner) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.db = state.db.clone().with_naming(idents, interner);
    }

    /// Builds a consistent snapshot of all four indexes and publishes their blob ids.
    ///
    /// Blobs are uploaded before the root CAS. Transactions may continue while the
    /// immutable snapshot is encoded; a later run indexes any remaining log tail.
    ///
    /// Publication is fenced by `lease_version` and monotone in
    /// `index_basis_t`: a root already published under a newer lease version
    /// deposes this writer ([`TransactError::Deposed`]); a root at an equal
    /// or newer basis (or one that wins a concurrent CAS race) leaves this
    /// snapshot's blobs for garbage collection. The freshly built root is
    /// returned when it, or a newer basis, is installed.
    ///
    /// # Errors
    /// Returns an error if a blob upload, root read, or fenced publication fails.
    pub async fn publish_indexes(
        &self,
        store: &(impl BlobStore + RootStore),
        root_name: &str,
        lease_version: u64,
    ) -> Result<DbRoot, TransactError> {
        let snapshot = self.db();
        let datoms = snapshot.datoms();
        let segments = tokio::task::spawn_blocking(move || {
            [
                IndexOrder::Eavt,
                IndexOrder::Aevt,
                IndexOrder::Avet,
                IndexOrder::Vaet,
            ]
            .into_iter()
            .map(|order| {
                let segment = Segment::build(order, datoms.clone());
                let mut bytes = Vec::new();
                for (key, _) in segment.entries() {
                    bytes.extend_from_slice(&(key.len() as u64).to_be_bytes());
                    bytes.extend_from_slice(key);
                }
                bytes
            })
            .collect::<Vec<_>>()
        })
        .await
        .map_err(|error| TransactError::IndexTask(error.to_string()))?;
        let mut ids = Vec::new();
        for bytes in segments {
            ids.push(store.put(&bytes).await?);
        }
        let root = DbRoot {
            format_version: corium_store::FORMAT_VERSION,
            lease_version,
            owner: String::new(),
            lease_expires_unix_ms: 0,
            owner_endpoint: String::new(),
            index_basis_t: snapshot.basis_t(),
            roots: Some([
                ids[0].clone(),
                ids[1].clone(),
                ids[2].clone(),
                ids[3].clone(),
            ]),
        };
        publish_root(store, root_name, &root).await?;
        Ok(root)
    }
}

/// Publishes `root` under the fencing rules described on
/// [`EmbeddedTransactor::publish_indexes`].
///
/// # Errors
/// Returns [`TransactError::Deposed`] when a newer lease version owns the
/// root, or a store error when the CAS cannot be completed.
pub async fn publish_root(
    store: &dyn RootStore,
    root_name: &str,
    root: &DbRoot,
) -> Result<(), TransactError> {
    loop {
        let previous = store.get_root(root_name).await?;
        let stored = previous.as_deref().and_then(DbRoot::decode);
        let mut next = root.clone();
        if let Some(stored) = stored {
            if stored.lease_version > root.lease_version {
                return Err(TransactError::Deposed {
                    published: stored.lease_version,
                });
            }
            if stored.lease_version == root.lease_version
                && stored.index_basis_t >= root.index_basis_t
            {
                return Ok(());
            }
            // The stored record carries the live lease fields (renewals CAS
            // the same key); publication must not clobber them.
            if stored.lease_version == root.lease_version {
                next.owner = stored.owner;
                next.lease_expires_unix_ms = stored.lease_expires_unix_ms;
                next.owner_endpoint = stored.owner_endpoint;
            }
        }
        match store
            .cas_root(root_name, previous.as_deref(), &next.encode())
            .await
        {
            Ok(()) => return Ok(()),
            Err(StoreError::CasFailed { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

pub use corium_store::{DbRoot, db_root_name};
