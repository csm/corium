//! Embedded single-writer transaction pipeline and index publisher.

use corium_core::{EntityId, IndexOrder, Partition, Schema};
use corium_db::{Db, FIRST_USER_ID};
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
        let mut db = Db::new(schema);
        let mut last_instant = i64::MIN;
        for record in log.replay()? {
            db = db.with_transaction(record.t, &record.datoms);
            last_instant = last_instant.max(record.tx_instant);
        }
        let next_user = db
            .datoms()
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
    /// Builds a consistent snapshot of all four indexes and publishes their blob ids.
    ///
    /// Blobs are uploaded before the root CAS. Transactions may continue while the
    /// immutable snapshot is encoded; a later run indexes any remaining log tail.
    ///
    /// # Errors
    /// Returns an error if a blob upload, root read, or fenced publication fails.
    pub fn publish_indexes(
        &self,
        store: &(impl BlobStore + RootStore),
    ) -> Result<PublishedRoot, TransactError> {
        let snapshot = self.db();
        let datoms = snapshot.datoms();
        let mut ids = Vec::new();
        for order in [
            IndexOrder::Eavt,
            IndexOrder::Aevt,
            IndexOrder::Avet,
            IndexOrder::Vaet,
        ] {
            let segment = Segment::build(order, datoms.clone());
            let mut bytes = Vec::new();
            for (key, _) in segment.entries() {
                bytes.extend_from_slice(&(key.len() as u64).to_be_bytes());
                bytes.extend_from_slice(key);
            }
            ids.push(store.put(&bytes)?);
        }
        let root = PublishedRoot {
            index_basis_t: snapshot.basis_t(),
            roots: [
                ids[0].clone(),
                ids[1].clone(),
                ids[2].clone(),
                ids[3].clone(),
            ],
        };
        let encoded = root.encode();
        let previous = store.get_root("db")?;
        store.cas_root("db", previous.as_deref(), &encoded)?;
        Ok(root)
    }
}

/// Published durable index root metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedRoot {
    /// Highest indexed transaction.
    pub index_basis_t: u64,
    /// EAVT, AEVT, AVET, and VAET blob ids.
    pub roots: [corium_store::BlobId; 4],
}
impl PublishedRoot {
    fn encode(&self) -> Vec<u8> {
        format!(
            "{}\n{}\n{}\n{}\n{}\n",
            self.index_basis_t, self.roots[0], self.roots[1], self.roots[2], self.roots[3]
        )
        .into_bytes()
    }
}
