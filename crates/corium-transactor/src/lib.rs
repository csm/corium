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
    /// A synchronous caller raced an asynchronous transaction in progress.
    #[error("an asynchronous transaction is already in progress")]
    AsyncTransactionPending,
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
    async_pending: bool,
}

struct AsyncPending<'a> {
    state: &'a Mutex<State>,
    active: bool,
}

impl Drop for AsyncPending<'_> {
    fn drop(&mut self) {
        if self.active {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .async_pending = false;
        }
    }
}

/// The next free user-partition entity id given a run of datoms and a floor,
/// so allocation never revisits an id any of them used.
fn next_user_id<'a>(datoms: impl Iterator<Item = &'a corium_core::Datom>, floor: u64) -> u64 {
    datoms
        .filter(|d| d.e.partition() == Partition::User as u32)
        .map(|d| d.e.sequence() + 1)
        .fold(floor, u64::max)
}

/// A serialized, in-process transactor. The log append is the commit point.
pub struct EmbeddedTransactor {
    log: Arc<dyn TransactionLog>,
    state: Mutex<State>,
    async_commit: tokio::sync::Mutex<()>,
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
        let next_user = next_user_id(db.recorded_datoms(), FIRST_USER_ID);
        Ok(Self {
            log,
            state: Mutex::new(State {
                db,
                next_user,
                last_instant,
                subscribers: Vec::new(),
                async_pending: false,
            }),
            async_commit: tokio::sync::Mutex::new(()),
        })
    }

    /// Recovers a transactor through the log's asynchronous storage path.
    ///
    /// # Errors
    /// Returns an error when the durable log cannot be replayed.
    pub async fn recover_from_async(
        base: Db,
        log: Arc<dyn TransactionLog>,
    ) -> Result<Self, TransactError> {
        let records = log.replay_async().await?;
        Ok(Self::recover_from_records(base, log, records))
    }

    fn recover_from_records(
        mut db: Db,
        log: Arc<dyn TransactionLog>,
        records: Vec<TxRecord>,
    ) -> Self {
        let mut last_instant = i64::MIN;
        for record in records {
            db = db.with_transaction(record.t, &record.datoms);
            last_instant = last_instant.max(record.tx_instant);
        }
        let next_user = next_user_id(db.recorded_datoms(), FIRST_USER_ID);
        Self {
            log,
            state: Mutex::new(State {
                db,
                next_user,
                last_instant,
                subscribers: Vec::new(),
                async_pending: false,
            }),
            async_commit: tokio::sync::Mutex::new(()),
        }
    }

    /// Recovers from a published current-state snapshot plus the log tail,
    /// replaying only transactions after the snapshot's basis instead of the
    /// whole history — so open and restart cost scale with the tail, not the
    /// database's age.
    ///
    /// `snapshot` is the current value at `snapshot.basis_t()` (typically
    /// [`Db::from_current_snapshot`] materialized from the published EAVT
    /// index). `next_entity_id` and `last_tx_instant` are the allocator and
    /// transaction-time high-water marks recorded in the [`DbRoot`] at
    /// publication (`DbRoot::next_entity_id` / `DbRoot::last_tx_instant`);
    /// they carry the state a current-facts snapshot cannot: entities fully
    /// retracted before the snapshot (whose ids must not be reused) and the
    /// last commit's instant (for `:db/txInstant` monotonicity when the tail
    /// is empty). Both are combined by `max` with whatever the replayed tail
    /// reveals, so an over-estimate is safe and a stale hint can only make
    /// allocation more conservative.
    ///
    /// The caller is responsible for opening `log` at the same lease version
    /// it recovered the snapshot under, exactly as [`recover_from`] requires.
    ///
    /// # Errors
    /// Returns an error when the log tail cannot be replayed.
    ///
    /// [`recover_from`]: Self::recover_from
    pub fn recover_from_snapshot(
        snapshot: Db,
        next_entity_id: u64,
        last_tx_instant: i64,
        log: Arc<dyn TransactionLog>,
    ) -> Result<Self, TransactError> {
        let mut db = snapshot;
        let index_basis = db.basis_t();
        let mut last_instant = last_tx_instant;
        // The snapshot's live datoms are already covered by the persisted
        // `next_entity_id`; only the tail can introduce ids past it.
        let mut next_user = next_entity_id.max(FIRST_USER_ID);
        for record in log.tx_range(index_basis + 1, None)? {
            db = db.with_transaction(record.t, &record.datoms);
            last_instant = last_instant.max(record.tx_instant);
            next_user = next_user.max(next_user_id(record.datoms.iter(), next_user));
        }
        Ok(Self {
            log,
            state: Mutex::new(State {
                db,
                next_user,
                last_instant,
                subscribers: Vec::new(),
                async_pending: false,
            }),
            async_commit: tokio::sync::Mutex::new(()),
        })
    }

    /// Recovers from a published snapshot plus an asynchronously read log
    /// tail.
    ///
    /// # Errors
    /// Returns an error when the log tail cannot be replayed.
    pub async fn recover_from_snapshot_async(
        snapshot: Db,
        next_entity_id: u64,
        last_tx_instant: i64,
        log: Arc<dyn TransactionLog>,
    ) -> Result<Self, TransactError> {
        let index_basis = snapshot.basis_t();
        let records = log.tx_range_async(index_basis + 1, None).await?;
        let mut db = snapshot;
        let mut last_instant = last_tx_instant;
        let mut next_user = next_entity_id.max(FIRST_USER_ID);
        for record in records {
            db = db.with_transaction(record.t, &record.datoms);
            last_instant = last_instant.max(record.tx_instant);
            next_user = next_user.max(next_user_id(record.datoms.iter(), next_user));
        }
        Ok(Self {
            log,
            state: Mutex::new(State {
                db,
                next_user,
                last_instant,
                subscribers: Vec::new(),
                async_pending: false,
            }),
            async_commit: tokio::sync::Mutex::new(()),
        })
    }

    /// Captures a consistent recovery snapshot: the current database value
    /// with the allocator and transaction-time high-water marks that a
    /// snapshot-only recovery would otherwise lose, all read under one lock.
    fn recovery_snapshot(&self) -> (Db, u64, i64) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.db.clone(), state.next_user, state.last_instant)
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
        if state.async_pending {
            return Err(TransactError::AsyncTransactionPending);
        }
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

    /// Validates under a short state lock, awaits durability without holding
    /// that lock, then atomically publishes the durable transaction in memory.
    /// Async calls are serialized here so standalone callers have the same
    /// single-writer guarantee as node-hosted callers.
    ///
    /// # Errors
    /// Returns an error for rejected transaction data, clock failure, or when
    /// the durable append fails. No report is sent on error.
    pub async fn transact_async(
        &self,
        items: impl IntoIterator<Item = TxItem>,
    ) -> Result<TxReport, TransactError> {
        let _commit = self.async_commit.lock().await;
        let (before, prepared, t, tx_instant) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.async_pending {
                return Err(TransactError::AsyncTransactionPending);
            }
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
            state.async_pending = true;
            (before, prepared, t, tx_instant)
        };
        let record = TxRecord {
            t,
            tx_instant,
            datoms: prepared.datoms.clone(),
        };
        let mut pending = AsyncPending {
            state: &self.state,
            active: true,
        };
        self.log.append_async(&record).await?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert_eq!(state.db.basis_t() + 1, t);
        // Apply to the live value so a naming-only update that ran while the
        // append was pending is preserved; transaction-bearing state cannot
        // change while `async_pending` is set.
        state.db = state.db.clone().with_transaction(t, &prepared.datoms);
        state.last_instant = tx_instant;
        state.next_user = prepared
            .tempids
            .values()
            .filter(|e| e.partition() == Partition::User as u32)
            .map(|e| e.sequence() + 1)
            .max()
            .unwrap_or(state.next_user)
            .max(state.next_user);
        state.async_pending = false;
        pending.active = false;
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
    /// Each index is chunked into content-defined leaf blobs under a
    /// manifest blob ([`corium_store::chunk_segment_keys`]), and only
    /// chunks absent from the store are uploaded — consecutive publications
    /// share every unchanged chunk, so a small change re-uploads a few
    /// chunks instead of the whole index.
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
        let (snapshot, next_entity_id, last_tx_instant) = self.recovery_snapshot();
        let datoms = snapshot.datoms();
        let chunked = tokio::task::spawn_blocking(move || {
            [
                IndexOrder::Eavt,
                IndexOrder::Aevt,
                IndexOrder::Avet,
                IndexOrder::Vaet,
            ]
            .into_iter()
            .map(|order| {
                let segment = Segment::build(order, datoms.clone());
                corium_store::chunk_segment_keys(segment.entries().map(|(key, _)| key.as_slice()))
            })
            .collect::<Vec<_>>()
        })
        .await
        .map_err(|error| TransactError::IndexTask(error.to_string()))?;
        let mut ids = Vec::new();
        for chunks in chunked {
            let mut children = Vec::new();
            for chunk in chunks {
                children.push(store.put_if_absent(&chunk).await?);
            }
            let manifest = corium_store::encode_index_manifest(&children);
            ids.push(store.put_if_absent(&manifest).await?);
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
            // Recovery hints for opening from this root without full replay.
            next_entity_id,
            last_tx_instant,
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
