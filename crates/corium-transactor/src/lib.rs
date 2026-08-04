//! Embedded single-writer transaction pipeline and index publisher, plus the
//! networked transactor process (lease, gRPC services, indexing job).

pub mod authz;
pub mod backend;
pub mod backup;
pub mod keys;
pub mod lease;
pub mod metrics;
pub mod node;
pub mod server;
#[cfg(feature = "cljrs")]
pub mod txfn;

pub use backend::{
    LogBackend, NodeStore, StorageConnectionError, StorageInfoConfig, StoreSpec,
    register_static_storage_backends,
};
#[cfg(feature = "s3")]
pub use backend::{S3ReadOnlyConfig, S3ReadOnlyCredentials};

use corium_core::{EntityId, IndexOrder, KeywordInterner, Partition, Schema};
use corium_db::{Db, FIRST_USER_ID, Idents};
use corium_index::{Leaf, LeafId, Segment};
use corium_log::{LogError, TransactionLog, TxRecord};
use corium_store::{BlobId, BlobStore, RootStore, StoreError};
use corium_tx::{PreparedTx, TxError, TxItem, prepare};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, mpsc},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

/// The covering indexes a database publishes, in the slot order [`DbRoot`]
/// stores their blob ids in.
const ORDERS: [IndexOrder; 4] = [
    IndexOrder::Eavt,
    IndexOrder::Aevt,
    IndexOrder::Avet,
    IndexOrder::Vaet,
];

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

/// Settles a transaction's `:db/txInstant` and materializes it as a datom.
///
/// Transaction data may assert its own instant against the transaction entity
/// (`[:db/add "datomic.tx" :db/txInstant …]` — how an import dates backfilled
/// transactions); otherwise the transactor stamps `max(now, last + 1)`. Either
/// way the instant ends up in the committed datom set, so the log, the
/// tx-report, every peer's live index, and the published snapshot all carry
/// one representation of transaction time.
///
/// # Errors
/// Returns [`TxError::TxInstantNotMonotonic`] when supplied data would move the
/// transaction clock backwards.
fn seal_tx_instant(
    datoms: &mut Vec<corium_core::Datom>,
    t: u64,
    now_ms: i64,
    last_instant: i64,
) -> Result<i64, TxError> {
    match corium_db::bootstrap::asserted_instant(t, datoms) {
        Some(supplied) if supplied <= last_instant => Err(TxError::TxInstantNotMonotonic {
            supplied,
            last: last_instant,
        }),
        Some(supplied) => Ok(supplied),
        None => {
            let instant = now_ms.max(last_instant.saturating_add(1));
            datoms.push(corium_db::bootstrap::tx_instant_datom(t, instant));
            Ok(instant)
        }
    }
}

fn effective_tx_instant(record: &TxRecord) -> i64 {
    corium_db::bootstrap::asserted_instant(record.t, &record.datoms).unwrap_or(record.tx_instant)
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

/// A transaction prepared against a [`BatchCursor`] but not yet durable,
/// carrying its log record and the pieces needed to build a [`TxReport`] once
/// the batch is durable.
pub struct Prepared {
    /// The record to make durable in the log.
    pub record: TxRecord,
    db_before: Db,
    db_after: Db,
    tx: PreparedTx,
    tx_instant: i64,
}

impl Prepared {
    /// The database value this transaction produces, before it is durable.
    ///
    /// The schema-update path reads the derived schema and ident registry
    /// from here, because those have to be published to the metadata root
    /// *before* the datoms that name them are appended.
    #[must_use]
    pub const fn db_after(&self) -> &Db {
        &self.db_after
    }
}

/// A cursor for preparing a group-commit batch against an evolving in-memory
/// value without touching the live one. Each [`Self::prepare`] validates
/// against the effects of the earlier transactions in the batch (uniqueness,
/// cardinality-one retraction, CAS), so a batch commits with exactly the
/// semantics of committing the transactions one at a time. The caller makes
/// the batch's records durable, then publishes the cursor with
/// [`EmbeddedTransactor::install_batch`]; a batch that never installs leaves
/// the live value untouched.
pub struct BatchCursor {
    db: Db,
    next_user: u64,
    last_instant: i64,
}

impl BatchCursor {
    /// The value including every transaction prepared into this batch so far,
    /// for expanding and converting the next transaction against it.
    #[must_use]
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Prepares one transaction against the cursor, advancing the cursor's
    /// in-memory value by it. `now_ms` is the wall clock in Unix milliseconds;
    /// `:db/txInstant` stays monotone via `max(now, last + 1)`. On error the
    /// cursor is unchanged, so a rejected transaction leaves the rest of the
    /// batch unaffected.
    ///
    /// # Errors
    /// Returns [`TxError`] when the transaction fails resolution or validation.
    pub fn prepare(
        &mut self,
        items: impl IntoIterator<Item = TxItem>,
        now_ms: i64,
    ) -> Result<Prepared, TxError> {
        let before = self.db.clone();
        let t = before.basis_t() + 1;
        let tx_id = EntityId::new(Partition::Tx as u32, t);
        let mut prepared = prepare(&before, items, tx_id, self.next_user)?;
        let tx_instant = seal_tx_instant(&mut prepared.datoms, t, now_ms, self.last_instant)?;
        let after = before
            .clone()
            .with_transaction_at(t, tx_instant, &prepared.datoms);
        self.next_user = prepared
            .tempids
            .values()
            .filter(|e| e.partition() == Partition::User as u32)
            .map(|e| e.sequence() + 1)
            .max()
            .unwrap_or(self.next_user)
            .max(self.next_user);
        self.last_instant = tx_instant;
        self.db = after.clone();
        Ok(Prepared {
            record: TxRecord {
                t,
                tx_instant,
                datoms: prepared.datoms.clone(),
            },
            db_before: before,
            db_after: after,
            tx: prepared,
            tx_instant,
        })
    }

    /// Attaches an updated keyword interner to the cursor's value.
    ///
    /// A schema transaction stores keyword *values* — `:db.type/string`, the
    /// attribute's own ident — and the schema derived from those datoms is
    /// only as good as the interner that resolves them. Minting the names into
    /// a private interner and then applying the datoms against a value that
    /// still holds the older one would silently drop every property whose
    /// value is a keyword the older interner never saw.
    pub fn intern_naming(&mut self, interner: KeywordInterner) {
        let idents = self.db.idents().clone();
        self.db = self.db.clone().with_naming(idents, interner);
    }

    /// Prepares a transaction from datoms that are already resolved.
    ///
    /// The schema-update path builds its datoms from a reviewed plan: every
    /// one names a concrete attribute entity and carries a literal value, so
    /// there is no tempid to allocate, no lookup ref to resolve, and no
    /// uniqueness or cardinality rule of an *ordinary* transaction to apply.
    /// Running them through [`Self::prepare`] would not merely be redundant —
    /// it would refuse them, because `corium-tx` deliberately rejects the
    /// schema vocabulary as ordinary data.
    ///
    /// The preconditions this bypasses are checked before the caller compiles
    /// the plan, under the same `commit` lock this runs beneath.
    ///
    /// # Errors
    /// Returns [`TxError`] when the transaction's `:db/txInstant` would not
    /// advance the transaction clock.
    pub fn prepare_datoms(
        &mut self,
        mut datoms: Vec<corium_core::Datom>,
        now_ms: i64,
    ) -> Result<Prepared, TxError> {
        let before = self.db.clone();
        let t = before.basis_t() + 1;
        let tx_instant = seal_tx_instant(&mut datoms, t, now_ms, self.last_instant)?;
        let after = before.clone().with_transaction_at(t, tx_instant, &datoms);
        self.last_instant = tx_instant;
        self.db = after.clone();
        Ok(Prepared {
            record: TxRecord {
                t,
                tx_instant,
                datoms: datoms.clone(),
            },
            db_before: before,
            db_after: after,
            tx: PreparedTx {
                datoms,
                tempids: std::collections::BTreeMap::new(),
            },
            tx_instant,
        })
    }
}

/// The snapshot this transactor last installed at the published root, kept so
/// the next indexing pass folds the log tail into it instead of rebuilding
/// every covering index from scratch.
struct PublishedIndexes {
    /// Lease version and index basis the published root carries; both are
    /// checked against that root before the pass trusts anything below.
    lease_version: u64,
    basis_t: u64,
    /// Segments in [`ORDERS`] slot order, whose leaves are the chunks named
    /// by `manifests`.
    segments: [Segment; 4],
    /// Manifest blob id per index, matched against the stored root to prove
    /// a live root still references the chunks this pass carries over — so
    /// garbage collection cannot have swept one out from under it.
    manifests: [BlobId; 4],
    /// The chunk each leaf was published as. A leaf carried over from this
    /// snapshot is already in the store under this id, so the next pass
    /// neither re-encodes nor re-uploads it.
    chunks: [HashMap<LeafId, BlobId>; 4],
}

impl PublishedIndexes {
    /// Whether `root` is still exactly the root this snapshot published.
    fn is_current(&self, root: Option<&DbRoot>, lease_version: u64) -> bool {
        root.is_some_and(|root| {
            self.lease_version == lease_version
                && root.lease_version == lease_version
                && root.index_basis_t == self.basis_t
                && root.roots.as_ref() == Some(&self.manifests)
        })
    }
}

/// One index's chunk after a pass has decided what to do with it.
enum ChunkPlan {
    /// A leaf carried over from the last publication; already stored under
    /// this id, so the pass neither encodes nor uploads it.
    Published(BlobId),
    /// A rebuilt leaf, encoded and awaiting upload.
    Rebuilt(Vec<u8>),
}

/// A serialized, in-process transactor. The log append is the commit point.
pub struct EmbeddedTransactor {
    log: Arc<dyn TransactionLog>,
    state: Mutex<State>,
    async_commit: tokio::sync::Mutex<()>,
    published: Mutex<Option<PublishedIndexes>>,
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
            let tx_instant = effective_tx_instant(&record);
            db = db.with_transaction_at(record.t, record.tx_instant, &record.datoms);
            last_instant = last_instant.max(tx_instant);
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
            published: Mutex::new(None),
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
            let tx_instant = effective_tx_instant(&record);
            db = db.with_transaction_at(record.t, record.tx_instant, &record.datoms);
            last_instant = last_instant.max(tx_instant);
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
            published: Mutex::new(None),
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
            let tx_instant = effective_tx_instant(&record);
            db = db.with_transaction_at(record.t, record.tx_instant, &record.datoms);
            last_instant = last_instant.max(tx_instant);
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
            published: Mutex::new(None),
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
            let tx_instant = effective_tx_instant(&record);
            db = db.with_transaction_at(record.t, record.tx_instant, &record.datoms);
            last_instant = last_instant.max(tx_instant);
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
            published: Mutex::new(None),
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
        let mut prepared = prepare(&before, items, tx_id, state.next_user)?;
        let millis = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| TransactError::Clock)?
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        let tx_instant = seal_tx_instant(&mut prepared.datoms, t, millis, state.last_instant)?;
        self.log.append(&TxRecord {
            t,
            tx_instant,
            datoms: prepared.datoms.clone(),
        })?;
        state.db = before.with_transaction_at(t, tx_instant, &prepared.datoms);
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
            let mut prepared = prepare(&before, items, tx_id, state.next_user)?;
            let millis = i64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| TransactError::Clock)?
                    .as_millis(),
            )
            .unwrap_or(i64::MAX);
            let tx_instant = seal_tx_instant(&mut prepared.datoms, t, millis, state.last_instant)?;
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
        state.db = state
            .db
            .clone()
            .with_transaction_at(t, tx_instant, &prepared.datoms);
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
    /// Snapshots the live value and allocation watermarks for preparing a
    /// group-commit batch. The returned [`BatchCursor`] evolves privately;
    /// [`Self::install_batch`] publishes it after the batch is durable.
    #[must_use]
    pub fn batch_cursor(&self) -> BatchCursor {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        BatchCursor {
            db: state.db.clone(),
            next_user: state.next_user,
            last_instant: state.last_instant,
        }
    }

    /// Publishes a durably-committed batch: installs the cursor's value as the
    /// live value, advances the allocation and transaction-time watermarks,
    /// fans the reports out to subscribers, and returns them in batch order.
    /// The caller must have made every record in `prepared` durable and
    /// verified ownership first. The cursor was snapshotted from, and advanced
    /// past, the live value under the same single-writer serialization, so
    /// installing it cannot lose a concurrent commit.
    pub fn install_batch(&self, cursor: BatchCursor, prepared: Vec<Prepared>) -> Vec<TxReport> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.db = cursor.db;
        state.next_user = state.next_user.max(cursor.next_user);
        state.last_instant = state.last_instant.max(cursor.last_instant);
        let reports: Vec<TxReport> = prepared
            .into_iter()
            .map(|p| TxReport {
                db_before: p.db_before,
                db_after: p.db_after,
                tx: p.tx,
                tx_instant: p.tx_instant,
            })
            .collect();
        for report in &reports {
            state
                .subscribers
                .retain(|subscriber| subscriber.send(report.clone()).is_ok());
        }
        reports
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

    /// Publishes a consistent snapshot of all four covering indexes.
    ///
    /// The pass folds the log tail since the last publication into the
    /// segments that publication produced ([`corium_index::Segment::apply`]),
    /// so its cost tracks the tail rather than the size of the database: a
    /// segment's leaf is exactly one published chunk, and a leaf the tail did
    /// not touch is carried over by handle and keeps the blob id it already
    /// has. Only rebuilt leaves are encoded, hashed, and uploaded, under a
    /// manifest blob naming every chunk in key order.
    ///
    /// A carried-over chunk is published by id without being re-uploaded, and
    /// nothing but the root that names it keeps it from being swept. So a pass
    /// carries chunks over only while it can prove that root is live: it
    /// requires the published root to be the one this transactor last
    /// installed, *and* pins that index state through the CAS, which installs
    /// only if the root never changed in between. A takeover, a concurrent
    /// publisher, or a root rolled back therefore costs a rebuild rather than
    /// a manifest naming chunks the sweep is free to delete.
    ///
    /// Rebuilding is always available and always correct — it is also what the
    /// first publication of a process does — and it reuses no chunk id, so it
    /// cannot be raced this way. It re-encodes every chunk, but reproduces the
    /// boundaries the previous publication cut, so it re-uploads only what
    /// genuinely changed.
    ///
    /// Blobs are uploaded before the root CAS. Transactions may continue while
    /// the immutable snapshot is encoded; a later run indexes any remaining
    /// log tail.
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
        let previous = self
            .published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let PassOutcome::Published(root) = self
            .publish_pass(store, root_name, lease_version, previous)
            .await?
        {
            return Ok(root);
        }
        // The root's index state moved under a pass that was carrying chunks
        // over from the last publication, so nothing was installed. Rebuilding
        // reuses no chunk id, so the retry has nothing left to be raced for.
        match self
            .publish_pass(store, root_name, lease_version, None)
            .await?
        {
            PassOutcome::Published(root) => Ok(root),
            PassOutcome::Raced => {
                unreachable!("a pass that carries nothing over publishes unconditionally")
            }
        }
    }

    /// One publication attempt, optionally folding the tail into `previous`
    /// rather than rebuilding.
    async fn publish_pass(
        &self,
        store: &(impl BlobStore + RootStore),
        root_name: &str,
        lease_version: u64,
        previous: Option<PublishedIndexes>,
    ) -> Result<PassOutcome, TransactError> {
        let stored = store
            .get_root(root_name)
            .await?
            .as_deref()
            .and_then(DbRoot::decode);
        if let Some(published) = stored.as_ref().map(|root| root.lease_version)
            && published > lease_version
        {
            return Err(TransactError::Deposed { published });
        }
        let (snapshot, next_entity_id, last_tx_instant) = self.recovery_snapshot();
        let basis_t = snapshot.basis_t();
        let previous = previous.filter(|previous| {
            previous.is_current(stored.as_ref(), lease_version) && previous.basis_t <= basis_t
        });
        // Carrying a chunk over publishes its blob id without re-uploading it,
        // and nothing but the root that names it keeps it from being swept.
        // Pinning the index state this pass validated makes the root CAS the
        // fence for those blobs too: it installs only if the root never
        // changed in between, so the carried chunks stayed referenced by the
        // live root from the check right through to the write.
        let pinned = previous
            .as_ref()
            .map(|_| RootIndexState::of(stored.as_ref()));

        let (segments, planned) =
            tokio::task::spawn_blocking(move || plan_indexes(&snapshot, previous.as_ref()))
                .await
                .map_err(|error| TransactError::IndexTask(error.to_string()))?;

        let mut manifests = Vec::with_capacity(ORDERS.len());
        let mut chunk_ids = Vec::with_capacity(ORDERS.len());
        for chunks in &planned {
            let mut children = Vec::with_capacity(chunks.len());
            for chunk in chunks {
                children.push(match chunk {
                    ChunkPlan::Published(id) => id.clone(),
                    ChunkPlan::Rebuilt(bytes) => store.put_if_absent(bytes).await?,
                });
            }
            let manifest = corium_store::encode_index_manifest(&children);
            manifests.push(store.put_if_absent(&manifest).await?);
            chunk_ids.push(children);
        }
        let manifests: [BlobId; 4] = manifests
            .try_into()
            .unwrap_or_else(|_| unreachable!("one manifest per covering index"));
        let root = DbRoot {
            format_version: corium_store::FORMAT_VERSION,
            lease_version,
            owner: String::new(),
            lease_expires_unix_ms: 0,
            owner_endpoint: String::new(),
            index_basis_t: basis_t,
            roots: Some(manifests.clone()),
            // Recovery hints for opening from this root without full replay.
            next_entity_id,
            last_tx_instant,
            // Owned by the key manifest, not by publication;
            // `publish_root_pinned` carries the stored value forward.
            key_manifest_version: 0,
        };
        let outcome = publish_root_pinned(store, root_name, &root, pinned.as_ref()).await?;
        if outcome == RootPublication::Raced {
            return Ok(PassOutcome::Raced);
        }
        let next = PublishedIndexes {
            lease_version,
            basis_t,
            chunks: chunk_ids_by_leaf(&segments, chunk_ids),
            segments,
            manifests,
        };
        // Keep the pass's segments only when the published root is the one
        // that describes them — either because this CAS installed it, or
        // because a publication at this basis had already stored the same
        // manifests (an indexing run over an unchanged basis). Anything else
        // leaves these chunks unreferenced, so the next pass must rebuild
        // rather than assume they survived a sweep.
        if outcome == RootPublication::Installed || next.is_current(stored.as_ref(), lease_version)
        {
            *self
                .published
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(next);
        }
        Ok(PassOutcome::Published(root))
    }
}

/// What one publication attempt produced.
enum PassOutcome {
    /// The attempt published this root (installed, or superseded by an equal
    /// or newer basis).
    Published(DbRoot),
    /// The attempt carried chunks over from an earlier publication and the
    /// root stopped vouching for them before the CAS, so nothing was
    /// installed and the caller must retry from a rebuild.
    Raced,
}

/// The published index state a publication pins the stored root to.
///
/// Only the index roots and their basis: the lease fields of the same record
/// change under an ordinary renewal, which neither drops a chunk nor makes a
/// carried blob id unsafe.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RootIndexState {
    index_basis_t: u64,
    roots: Option<[BlobId; 4]>,
}

impl RootIndexState {
    fn of(root: Option<&DbRoot>) -> Self {
        Self {
            index_basis_t: root.map_or(0, |root| root.index_basis_t),
            roots: root.and_then(|root| root.roots.clone()),
        }
    }
}

/// Builds the four covering-index segments for `snapshot` and decides, leaf by
/// leaf, which chunks the pass has to encode.
///
/// With a usable previous publication this folds only the tail since its basis
/// into each segment; without one it rebuilds each segment from the database's
/// own covering index, which is already in key order — so even the fallback
/// path never sorts. Either way the datoms are read in place and kept only as
/// the key they encode to, so a pass never copies the facts it indexes.
fn plan_indexes(
    snapshot: &Db,
    previous: Option<&PublishedIndexes>,
) -> ([Segment; 4], [Vec<ChunkPlan>; 4]) {
    let mut segments: Vec<Segment> = Vec::with_capacity(ORDERS.len());
    let mut planned: Vec<Vec<ChunkPlan>> = Vec::with_capacity(ORDERS.len());
    for (slot, order) in ORDERS.into_iter().enumerate() {
        let segment = match previous {
            Some(previous) => previous.segments[slot].apply_ref(
                order,
                snapshot
                    .recorded_since(previous.basis_t)
                    .filter(|datom| corium_db::covered(snapshot.schema(), order, datom)),
            ),
            None => Segment::from_sorted_ref(order, snapshot.datoms_at(order)),
        };
        let stored = previous.map(|previous| &previous.chunks[slot]);
        planned.push(
            segment
                .leaves()
                .map(|leaf| plan_chunk(leaf, stored))
                .collect(),
        );
        segments.push(segment);
    }
    (
        segments
            .try_into()
            .unwrap_or_else(|_| unreachable!("one segment per covering index")),
        planned
            .try_into()
            .unwrap_or_else(|_| unreachable!("one chunk plan per covering index")),
    )
}

/// Reuses the blob id a leaf was last published under, or encodes the leaf
/// when the pass rebuilt it.
fn plan_chunk(leaf: &Leaf, stored: Option<&HashMap<LeafId, BlobId>>) -> ChunkPlan {
    match stored.and_then(|stored| stored.get(&leaf.id())) {
        Some(id) => ChunkPlan::Published(id.clone()),
        None => ChunkPlan::Rebuilt(corium_store::encode_segment_chunk(
            leaf.keys().iter().map(Vec::as_slice),
        )),
    }
}

/// Indexes each segment's published chunk ids by leaf, so the next pass can
/// recognize a carried-over leaf as a chunk it has already stored.
fn chunk_ids_by_leaf(
    segments: &[Segment; 4],
    chunk_ids: Vec<Vec<BlobId>>,
) -> [HashMap<LeafId, BlobId>; 4] {
    let mut per_index = chunk_ids.into_iter();
    std::array::from_fn(|slot| {
        segments[slot]
            .leaves()
            .map(Leaf::id)
            .zip(per_index.next().unwrap_or_default())
            .collect()
    })
}

/// Whether a fenced root publication installed the root it was given.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootPublication {
    /// The published root is now this one.
    Installed,
    /// A root at an equal or newer basis was already published, so this one
    /// was not installed and the blobs behind it are left for garbage
    /// collection.
    Superseded,
    /// The publication pinned the index state it was replacing and that state
    /// changed, so this root was not installed. Only a caller that supplies a
    /// pin can see this; [`publish_root`] never returns it.
    Raced,
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
) -> Result<RootPublication, TransactError> {
    publish_root_pinned(store, root_name, root, None).await
}

/// Publishes `root`, optionally only while the stored record still carries
/// `pinned` as its index state.
///
/// A publication that names blobs it did not upload — an indexing pass
/// carrying chunks over from the last one — has to pin: those blobs are kept
/// alive only by the root that references them, so installing over a root
/// that had already dropped them would leave the live root pointing at chunks
/// the sweep is free to delete. The pin is re-checked against the record read
/// immediately before each CAS attempt, and the CAS is conditional on exactly
/// those bytes, so there is no window between the check and the write.
///
/// The pin covers the index roots and their basis, not the lease fields of
/// the same record: an ordinary renewal rewrites those, and losing a pass to
/// one would be a needless rebuild.
async fn publish_root_pinned(
    store: &dyn RootStore,
    root_name: &str,
    root: &DbRoot,
    pinned: Option<&RootIndexState>,
) -> Result<RootPublication, TransactError> {
    loop {
        let previous = store.get_root(root_name).await?;
        let stored = previous.as_deref().and_then(DbRoot::decode);
        if let Some(pinned) = pinned
            && RootIndexState::of(stored.as_ref()) != *pinned
        {
            return Ok(RootPublication::Raced);
        }
        let mut next = root.clone();
        if let Some(stored) = stored {
            // The key manifest changes independently of the lease and of
            // publication, so its generation always comes from the stored
            // record rather than from the root being installed.
            next.key_manifest_version = stored.key_manifest_version;
            if stored.lease_version > root.lease_version {
                return Err(TransactError::Deposed {
                    published: stored.lease_version,
                });
            }
            if stored.lease_version == root.lease_version
                && stored.index_basis_t >= root.index_basis_t
            {
                return Ok(RootPublication::Superseded);
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
            Ok(()) => return Ok(RootPublication::Installed),
            Err(StoreError::CasFailed { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

pub use corium_store::{DbRoot, db_root_name};
