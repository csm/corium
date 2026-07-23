//! The transactor as a process: multi-database state, durable naming,
//! lease acquisition/renewal, background indexing, tx-report fan-out, and
//! high-availability standby takeover.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use corium_core::{Datom, IndexOrder, KeywordInterner, Schema};
use corium_db::{Db, Idents};
use corium_log::{LogError, TransactionLog, TxRecord};
use corium_protocol::codec::{self, CodecError};
use corium_protocol::pb;
use corium_protocol::schemaform::{SchemaFormError, schema_from_edn};
use corium_protocol::txforms::{TxFormError, tx_items_from_edn};
use corium_query::edn::Edn;
use corium_store::{
    BlobId, BlobStore, RootStore, StoreError, decode_index_manifest, decode_segment_keys,
    is_index_manifest, mark_and_sweep_retained, meta_root_name,
};
use thiserror::Error;
use tokio::sync::{broadcast, oneshot, watch};
use tracing::Instrument;

use crate::backend::{LogBackend, NodeStore, StoreSpec};
use crate::lease::{self, Lease, LeaseError};
use crate::metrics::Metrics;
use crate::{DbRoot, EmbeddedTransactor, Prepared, TransactError, db_root_name};

/// Expands user database-function invocations in boundary EDN transaction
/// forms before native conversion. The built-in implementation is
/// [`crate::txfn::DbFnExpander`] on the bounded `cljrs-tx` runtime (feature
/// `cljrs`, on by default, ADR-0008); embedders may inject their own.
pub trait TxFnExpander: Send + Sync {
    /// Rewrites `forms` with every `[:my/fn arg…]` invocation replaced by
    /// the function's returned tx-data (recursively).
    ///
    /// # Errors
    /// Returns a display message when a function is missing, rejected by
    /// the sandbox, fails, or exceeds its budget; the transaction aborts.
    fn expand(&self, db: &Db, forms: Vec<Edn>) -> Result<Vec<Edn>, String>;
}

/// Node process configuration.
#[derive(Clone)]
pub struct NodeConfig {
    /// Storage-service backend for blobs and roots (`mem`, `fs`, or Turso).
    pub store: StoreSpec,
    /// Data directory holding the filesystem blob/root store (for the `fs`
    /// backend) and the transaction logs (for every non-`mem` backend).
    pub data_dir: PathBuf,
    /// Stable owner identity for lease records.
    pub owner: String,
    /// Lease time-to-live in milliseconds.
    pub lease_ttl_ms: i64,
    /// How long to wait for a held lease to expire before giving up.
    pub lease_wait_ms: i64,
    /// High-availability mode: when another owner holds a database's lease,
    /// stand by and take over on expiry instead of failing startup, and on
    /// depose return to standby instead of shutting the process down.
    pub ha: bool,
    /// Client endpoint advertised in the lease for peer lease-holder
    /// rediscovery (e.g. `http://transactor-a:4334`).
    pub advertise: Option<String>,
    /// Interval between background index publications.
    pub index_interval: Duration,
    /// Minimum wait before the next index publication, as a multiple of the
    /// previous publication's duration. Publications currently rewrite every
    /// index in full, so this stretches the effective interval as the
    /// database grows, bounding the share of time and storage bandwidth
    /// spent republishing to at most `1/(1+n)`; 0 disables the backoff.
    pub index_backoff: u32,
    /// Pending log-tail growth (recorded datoms) below which a due
    /// publication is deferred, so trickle writes coalesce instead of
    /// rewriting every index; 0 publishes any pending work.
    pub index_tail_threshold: u64,
    /// Longest a pending below-threshold tail may defer publication.
    pub index_tail_deadline: Duration,
    /// Interval between heartbeats on subscription streams.
    pub heartbeat_interval: Duration,
    /// Interval between scheduled garbage-collection duties; `None` disables it.
    pub gc_interval: Option<Duration>,
    /// Minimum age of an unreachable blob before scheduled/manual online GC.
    pub gc_retention: Duration,
    /// Most transactions grouped into one commit batch (group commit). A batch
    /// commits under one durable append and one ownership fence, so a larger
    /// cap raises peak write throughput under high concurrency at the cost of a
    /// larger log object per batch; `1` effectively disables batching. Ignored
    /// once [`Self::max_commit_batch_bytes`] is reached first.
    pub max_commit_batch: usize,
    /// Byte budget for one commit batch: it stops accepting more transactions
    /// once their combined encoded size reaches this, bounding the per-batch
    /// log object even when transactions are large. At least one transaction
    /// always commits, so a single oversized transaction is not blocked.
    pub max_commit_batch_bytes: usize,
    /// Optional database-function expander (`:db/fn` support).
    pub tx_fn_expander: Option<Arc<dyn TxFnExpander>>,
}

impl std::fmt::Debug for NodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeConfig")
            .field("store", &self.store)
            .field("data_dir", &self.data_dir)
            .field("owner", &self.owner)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .field("lease_wait_ms", &self.lease_wait_ms)
            .field("ha", &self.ha)
            .field("advertise", &self.advertise)
            .field("index_interval", &self.index_interval)
            .field("index_backoff", &self.index_backoff)
            .field("index_tail_threshold", &self.index_tail_threshold)
            .field("index_tail_deadline", &self.index_tail_deadline)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("gc_interval", &self.gc_interval)
            .field("gc_retention", &self.gc_retention)
            .field("max_commit_batch", &self.max_commit_batch)
            .field("max_commit_batch_bytes", &self.max_commit_batch_bytes)
            .field("tx_fn_expander", &self.tx_fn_expander.is_some())
            .finish()
    }
}

impl NodeConfig {
    /// Sensible defaults for a data directory.
    #[must_use]
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            store: StoreSpec::Fs,
            data_dir,
            owner: format!(
                "transactor-{}",
                std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into())
            ),
            lease_ttl_ms: 5_000,
            lease_wait_ms: 15_000,
            ha: false,
            advertise: None,
            index_interval: Duration::from_secs(5),
            index_backoff: 4,
            index_tail_threshold: 0,
            index_tail_deadline: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(10),
            gc_interval: Some(Duration::from_secs(60 * 60)),
            gc_retention: Duration::from_secs(72 * 60 * 60),
            max_commit_batch: 256,
            max_commit_batch_bytes: 4 * 1024 * 1024,
            #[cfg(feature = "cljrs")]
            tx_fn_expander: Some(Arc::new(crate::txfn::DbFnExpander::default())),
            #[cfg(not(feature = "cljrs"))]
            tx_fn_expander: None,
        }
    }
}

/// Pacing policy for one database's background indexing job.
///
/// A publication is due when the adaptive floor has elapsed — the base
/// interval stretched by a multiple of the previous publication's duration,
/// which bounds the indexing duty cycle as full republication gets slower —
/// and the pending log tail is either large enough to be worth rewriting
/// every index or old enough that deferring it further would leave cold
/// readers and backups too far behind.
///
/// Every database starts from the node's [`NodeConfig`] pacing fields; the
/// catalog `SetIndexPolicy` RPC (or
/// [`TransactorNode::set_index_policy`]) overrides it at runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexPolicy {
    /// Base interval between publications ([`NodeConfig::index_interval`]).
    pub interval: Duration,
    /// Duty-cycle multiplier on the previous publication's duration
    /// ([`NodeConfig::index_backoff`]).
    pub backoff: u32,
    /// Pending-datom count below which a due publication is deferred
    /// ([`NodeConfig::index_tail_threshold`]).
    pub tail_threshold: u64,
    /// Longest a below-threshold tail may defer publication
    /// ([`NodeConfig::index_tail_deadline`]).
    pub tail_deadline: Duration,
}

/// Partial [`IndexPolicy`] override; `None` fields are left unchanged.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexPolicyUpdate {
    /// New base interval, when set.
    pub interval: Option<Duration>,
    /// New duty-cycle multiplier, when set.
    pub backoff: Option<u32>,
    /// New pending-datom threshold, when set.
    pub tail_threshold: Option<u64>,
    /// New deferral deadline, when set.
    pub tail_deadline: Option<Duration>,
}

impl IndexPolicy {
    fn from_config(config: &NodeConfig) -> Self {
        Self {
            interval: config.index_interval,
            backoff: config.index_backoff,
            tail_threshold: config.index_tail_threshold,
            tail_deadline: config.index_tail_deadline,
        }
    }

    fn apply(&mut self, update: IndexPolicyUpdate) {
        if let Some(interval) = update.interval {
            self.interval = interval;
        }
        if let Some(backoff) = update.backoff {
            self.backoff = backoff;
        }
        if let Some(tail_threshold) = update.tail_threshold {
            self.tail_threshold = tail_threshold;
        }
        if let Some(tail_deadline) = update.tail_deadline {
            self.tail_deadline = tail_deadline;
        }
    }

    /// Decides whether pending work should publish now. `since_publish` is
    /// the time since the last publication finished (or the job started),
    /// `last_duration` how long it took (zero before the first), and
    /// `pending` the recorded datoms appended since it — `None` until a
    /// publication in this process establishes a baseline, which publishes
    /// at base pacing (covers restarting with an unindexed backlog).
    fn due(&self, since_publish: Duration, last_duration: Duration, pending: Option<u64>) -> bool {
        let floor = self
            .interval
            .max(last_duration.saturating_mul(self.backoff));
        if since_publish < floor {
            return false;
        }
        match pending {
            Some(pending) if pending < self.tail_threshold => since_publish >= self.tail_deadline,
            _ => true,
        }
    }
}

/// Node operation failure.
#[derive(Debug, Error)]
pub enum NodeError {
    /// Named database does not exist.
    #[error("unknown database {0:?}")]
    UnknownDb(String),
    /// Database name is not storable.
    #[error("invalid database name {0:?}")]
    InvalidName(String),
    /// Database root uses a storage format newer than this binary.
    #[error("storage format {found} is newer than supported format {supported}")]
    UnsupportedFormat {
        /// Version found in the root.
        found: u32,
        /// Newest version understood by this binary.
        supported: u32,
    },
    /// This node no longer holds the write lease.
    #[error("deposed: write lease for {0:?} is held elsewhere")]
    Deposed(String),
    /// This node is a warm standby for the database; the lease holder
    /// serves it.
    #[error("standby for {db:?}: lease held by {owner} at {endpoint:?}")]
    Standby {
        /// Database name.
        db: String,
        /// Current lease owner id (empty when unknown).
        owner: String,
        /// Owner's advertised client endpoint (empty when unadvertised).
        endpoint: String,
    },
    /// Payload failed to decode.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// Transaction forms failed to convert.
    #[error(transparent)]
    TxForm(#[from] TxFormError),
    /// Schema forms failed to convert.
    #[error(transparent)]
    SchemaForm(#[from] SchemaFormError),
    /// Transaction pipeline failure.
    #[error(transparent)]
    Transact(#[from] TransactError),
    /// Store failure.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Log failure.
    #[error(transparent)]
    Log(#[from] LogError),
    /// Lease failure.
    #[error(transparent)]
    Lease(#[from] LeaseError),
    /// Malformed request.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// A group-commit batch aborted after preparation (durable append,
    /// ownership fence, or metadata publish failed); every batched caller
    /// receives this so it retries. Carries the originating error's text
    /// because the underlying store/log errors are not cloneable.
    #[error("group commit aborted: {0}")]
    GroupCommit(String),
}

struct Naming {
    schema: Schema,
    idents: Idents,
    interner: KeywordInterner,
}

/// One caller's queued transaction, awaiting a group-commit flush. The
/// leader that flushes the queue answers `resp`.
struct CommitRequest {
    forms: Vec<Edn>,
    resp: oneshot::Sender<Result<pb::TransactResponse, NodeError>>,
}

/// Per-database state hosted by a node.
pub struct DbState {
    name: String,
    transactor: EmbeddedTransactor,
    log: Arc<dyn TransactionLog>,
    naming: Mutex<Naming>,
    /// Held by the batch leader while it flushes the pending queue; also taken
    /// by lease renewal so a renewal never interleaves with a commit's
    /// ownership checks.
    commit: tokio::sync::Mutex<()>,
    /// Transactions queued for the next group-commit flush.
    pending: Mutex<VecDeque<CommitRequest>>,
    broadcast: broadcast::Sender<pb::subscribe_item::Item>,
    basis: watch::Sender<u64>,
    index_basis: AtomicU64,
    index_policy: Mutex<IndexPolicy>,
    held_lease: Mutex<Lease>,
    deposed: AtomicBool,
}

impl DbState {
    /// Database name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Current database value.
    #[must_use]
    pub fn db(&self) -> Db {
        self.transactor.db()
    }

    /// Watch channel following the commit basis.
    #[must_use]
    pub fn basis_watch(&self) -> watch::Receiver<u64> {
        self.basis.subscribe()
    }

    /// Subscribes to live stream items (reports, index announcements,
    /// heartbeats).
    #[must_use]
    pub fn stream_items(&self) -> broadcast::Receiver<pb::subscribe_item::Item> {
        self.broadcast.subscribe()
    }

    /// Basis of the newest published index root.
    #[must_use]
    pub fn index_basis(&self) -> u64 {
        self.index_basis.load(Ordering::Acquire)
    }

    /// The indexing pacing policy currently in effect for this database.
    #[must_use]
    pub fn index_policy(&self) -> IndexPolicy {
        *self
            .index_policy
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Currently held lease record.
    #[must_use]
    pub fn lease(&self) -> Lease {
        self.held_lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Encoded schema/ident handshake payload plus a consistent basis and
    /// interner snapshot for backfill encoding.
    #[must_use]
    pub fn handshake_snapshot(&self) -> (Vec<u8>, KeywordInterner) {
        let naming = self
            .naming
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            codec::encode_schema(&naming.schema, &naming.idents),
            naming.interner.clone(),
        )
    }

    /// Reads committed records in `[start, end)` from the durable log.
    ///
    /// # Errors
    /// Returns an error when the log cannot be read.
    pub async fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, NodeError> {
        Ok(self.log.tx_range_async(start, end).await?)
    }

    /// Verifies this node still owns the write lease (identity check on
    /// the root record; expiry changes from renewals do not matter).
    async fn check_lease(&self, store: &dyn RootStore) -> Result<Lease, NodeError> {
        if self.deposed.load(Ordering::Acquire) {
            return Err(NodeError::Deposed(self.name.clone()));
        }
        let held = self.lease();
        match lease::verify(store, &self.name, &held).await {
            Ok(()) => Ok(held),
            Err(LeaseError::Lost) => {
                self.deposed.store(true, Ordering::Release);
                Err(NodeError::Deposed(self.name.clone()))
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// A running transactor node hosting every database under one data directory.
pub struct TransactorNode {
    config: NodeConfig,
    store: Arc<NodeStore>,
    log_backend: LogBackend,
    dbs: std::sync::RwLock<HashMap<String, Arc<DbState>>>,
    /// Databases this node is standing by for (HA mode): the lease is held
    /// elsewhere and the standby poller attempts takeover on expiry.
    standby: std::sync::RwLock<BTreeSet<String>>,
    gc_lock: tokio::sync::Mutex<()>,
    /// Serializes forks: two forks to the same target must not interleave
    /// appends into one target log.
    fork_lock: tokio::sync::Mutex<()>,
    metrics: Metrics,
    shutdown: watch::Sender<Option<String>>,
}

fn now_unix_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// The error a group-commit batch hands every one of its callers when it
/// aborts after preparation. `Deposed` is preserved structurally so callers
/// fail over; other store/log errors (not cloneable) surface as
/// [`NodeError::GroupCommit`], which carries the text and maps to the same
/// retriable status the single-transaction path returned.
fn batch_abort_error(name: &str, error: &NodeError) -> NodeError {
    match error {
        NodeError::Deposed(_) => NodeError::Deposed(name.to_owned()),
        other => NodeError::GroupCommit(other.to_string()),
    }
}

fn valid_db_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

impl TransactorNode {
    /// Opens a node over `config.data_dir`, recovering every database found
    /// there (acquiring its lease, waiting out held leases up to the
    /// configured bound).
    ///
    /// # Errors
    /// Returns an error when the store cannot be opened or a database cannot
    /// be recovered.
    pub async fn open(config: NodeConfig) -> Result<Arc<Self>, NodeError> {
        let store = Arc::new(NodeStore::open(&config.store, &config.data_dir).await?);
        let log_backend = LogBackend::for_spec(&config.store, &config.data_dir, Arc::clone(&store));
        let node = Arc::new(Self {
            config,
            store,
            log_backend,
            dbs: std::sync::RwLock::new(HashMap::new()),
            standby: std::sync::RwLock::new(BTreeSet::new()),
            gc_lock: tokio::sync::Mutex::new(()),
            fork_lock: tokio::sync::Mutex::new(()),
            metrics: Metrics::default(),
            shutdown: watch::channel(None).0,
        });
        let names: Vec<String> = node
            .store
            .list_roots("meta:")
            .await?
            .into_iter()
            .filter_map(|root| root.strip_prefix("meta:").map(str::to_owned))
            .collect();
        for name in names {
            match node.open_db(&name).await {
                Ok(state) => {
                    node.dbs
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(name, state);
                }
                Err(NodeError::Lease(LeaseError::Held { owner, .. })) if node.config.ha => {
                    tracing::info!(db = %name, %owner, "standing by; lease held elsewhere");
                    node.standby
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(name);
                }
                Err(error) => return Err(error),
            }
        }
        node.spawn_standby_poller();
        node.spawn_scheduled_gc();
        Ok(node)
    }

    /// The node's storage-service backend (blobs + roots).
    #[must_use]
    pub fn store(&self) -> &Arc<NodeStore> {
        &self.store
    }

    /// Node configuration.
    #[must_use]
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Process observability counters.
    #[must_use]
    pub const fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    fn spawn_scheduled_gc(self: &Arc<Self>) {
        let Some(interval) = self.config.gc_interval else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // Embedded callers may construct an empty catalog before they
            // enter a runtime. Process wiring opens nodes inside Tokio.
            return;
        };
        let node = Arc::clone(self);
        runtime.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // `interval` ticks immediately; scheduled duties should wait a full interval.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(error) = node.gc_deleted().await {
                    tracing::warn!(%error, "scheduled garbage collection failed");
                }
            }
        });
    }

    /// Watch channel that reports a shutdown reason when the node deposes.
    #[must_use]
    pub fn shutdown_watch(&self) -> watch::Receiver<Option<String>> {
        self.shutdown.subscribe()
    }

    /// Deposes a hosted database. In HA mode the database returns to
    /// standby (the poller re-attempts takeover); otherwise the whole
    /// process shuts down and a supervisor restart re-acquires or waits.
    fn depose(&self, state: &DbState, reason: &str) {
        state.deposed.store(true, Ordering::Release);
        if self.config.ha {
            tracing::warn!(db = %state.name, reason, "deposed; returning to standby");
            self.dbs
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&state.name);
            self.standby
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(state.name.clone());
        } else {
            let _ = self
                .shutdown
                .send(Some(format!("database {:?}: {reason}", state.name)));
        }
    }

    fn advertised(&self) -> &str {
        self.config.advertise.as_deref().unwrap_or("")
    }

    /// Acquires the lease for `name`. In HA mode a held lease surfaces
    /// immediately (the caller stands by); otherwise startup waits it out
    /// up to the configured bound.
    async fn acquire_lease(&self, name: &str) -> Result<Lease, NodeError> {
        let deadline = now_unix_ms() + self.config.lease_wait_ms;
        loop {
            match lease::acquire(
                self.store.as_ref(),
                name,
                &self.config.owner,
                self.advertised(),
                self.config.lease_ttl_ms,
                now_unix_ms(),
            )
            .await
            {
                Ok(held) => return Ok(held),
                Err(LeaseError::Held { .. }) if !self.config.ha && now_unix_ms() < deadline => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn open_db(self: &Arc<Self>, name: &str) -> Result<Arc<DbState>, NodeError> {
        let meta = self
            .store
            .get_root(&meta_root_name(name))
            .await?
            .ok_or_else(|| NodeError::UnknownDb(name.to_owned()))?;
        let (schema, idents, interner) = codec::decode_metadata(&meta)?;
        let root_name = db_root_name(name);
        let current = self
            .store
            .get_root(&root_name)
            .await?
            .as_deref()
            .and_then(DbRoot::decode);
        if let Some(root) = &current
            && root.format_version > corium_store::FORMAT_VERSION
        {
            return Err(NodeError::UnsupportedFormat {
                found: root.format_version,
                supported: corium_store::FORMAT_VERSION,
            });
        }
        // Acquisition rewrites the root record under our lease version, so
        // it doubles as the fence bump: a deposed writer's pending root CAS
        // now has stale expected bytes and must fail. It also preserves the
        // published snapshot's recovery hints, so the root we re-read below
        // carries everything index-root recovery needs.
        let held = self.acquire_lease(name).await?;
        // The log tail replay below happens strictly after the fence, so it
        // observes every record a previous owner could ever have acked.
        let log = self.log_backend.open(name, held.version).await?;
        let post_fence = self
            .store
            .get_root(&root_name)
            .await?
            .as_deref()
            .and_then(DbRoot::decode);
        let transactor = self
            .recover_transactor(name, &schema, &idents, &interner, post_fence.as_ref(), &log)
            .await?;
        let basis_t = transactor.db().basis_t();
        let index_basis = post_fence.map_or(0, |root| root.index_basis_t);
        let state = Arc::new(DbState {
            name: name.to_owned(),
            transactor,
            log,
            naming: Mutex::new(Naming {
                schema,
                idents,
                interner,
            }),
            commit: tokio::sync::Mutex::new(()),
            pending: Mutex::new(VecDeque::new()),
            broadcast: broadcast::channel(1024).0,
            basis: watch::channel(basis_t).0,
            index_basis: AtomicU64::new(index_basis),
            index_policy: Mutex::new(IndexPolicy::from_config(&self.config)),
            held_lease: Mutex::new(held),
            deposed: AtomicBool::new(false),
        });
        self.spawn_maintenance(&state);
        Ok(state)
    }

    /// Builds the recovered transactor for `open_db`.
    ///
    /// When the post-fence root publishes a current snapshot with recovery
    /// hints, recovers from the index root plus the log tail — open time
    /// proportional to the tail, not the whole history. Any missing hint
    /// (a pre-recovery root, or a bare fence bump with no snapshot) or a
    /// failure materializing the snapshot falls back to full-log replay,
    /// which is always correct because the log is the source of truth.
    async fn recover_transactor(
        &self,
        name: &str,
        schema: &Schema,
        idents: &Idents,
        interner: &KeywordInterner,
        root: Option<&DbRoot>,
        log: &Arc<dyn TransactionLog>,
    ) -> Result<EmbeddedTransactor, NodeError> {
        // `next_entity_id == 0` is the "no hint" sentinel (see DbRoot); it and
        // an absent snapshot both rule out the tail-only path.
        if let Some(root) = root
            && let Some(roots) = &root.roots
            && root.next_entity_id != 0
        {
            match self
                .load_current_snapshot(
                    root,
                    &roots[IndexOrder::Eavt as usize],
                    schema,
                    idents,
                    interner,
                )
                .await
            {
                Ok(snapshot) => {
                    return Ok(EmbeddedTransactor::recover_from_snapshot_async(
                        snapshot,
                        root.next_entity_id,
                        root.last_tx_instant,
                        Arc::clone(log),
                    )
                    .await?);
                }
                Err(error) => {
                    tracing::warn!(
                        db = %name,
                        %error,
                        "index-root recovery failed; falling back to full-log replay"
                    );
                }
            }
        }
        let base = Db::new(schema.clone()).with_naming(idents.clone(), interner.clone());
        Ok(EmbeddedTransactor::recover_from_async(base, Arc::clone(log)).await?)
    }

    /// Materializes the current database value at a published index root from
    /// its EAVT snapshot — the transactor-side counterpart of the peer's
    /// bootstrap (`corium-peer`'s `load_current_snapshot`). Only current
    /// facts are reconstructed; the log tail carries everything since.
    async fn load_current_snapshot(
        &self,
        root: &DbRoot,
        eavt: &BlobId,
        schema: &Schema,
        idents: &Idents,
        interner: &KeywordInterner,
    ) -> Result<Db, StoreError> {
        let datoms = self
            .load_index_keys(eavt)
            .await?
            .into_iter()
            .map(|key| Datom::from_key(IndexOrder::Eavt, &key))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Io(std::io::Error::other(error.to_string())))?;
        Ok(Db::from_current_snapshot(
            root.index_basis_t,
            schema.clone(),
            idents.clone(),
            interner.clone(),
            datoms,
        ))
    }

    /// Reads one covering index's sorted key stream from the blob store: a
    /// format-3 manifest's chunks in order, or a pre-format-3 flat blob.
    async fn load_index_keys(&self, id: &BlobId) -> Result<Vec<Vec<u8>>, StoreError> {
        let blob = self
            .store
            .get(id)
            .await?
            .ok_or_else(|| StoreError::MissingBlob(id.clone()))?;
        if !is_index_manifest(&blob) {
            return decode_segment_keys(&blob);
        }
        let mut keys = Vec::new();
        for child in decode_index_manifest(&blob)? {
            let chunk = self
                .store
                .get(&child)
                .await?
                .ok_or_else(|| StoreError::MissingBlob(child.clone()))?;
            keys.extend(decode_segment_keys(&chunk)?);
        }
        Ok(keys)
    }

    /// HA standby duty: at the lease-renewal cadence, rediscover databases
    /// (including ones created on the active after this process started)
    /// and attempt takeover of any whose lease has lapsed. Takeover is
    /// ordinary startup — acquire (which fences), replay the log tail,
    /// serve — per the crash-only design.
    fn spawn_standby_poller(self: &Arc<Self>) {
        if !self.config.ha {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let ttl = self.config.lease_ttl_ms;
        let poll_every = Duration::from_millis(u64::try_from(ttl / 3).unwrap_or(1).max(50));
        let node = Arc::clone(self);
        runtime.spawn(async move {
            let mut ticker = tokio::time::interval(poll_every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if let Err(error) = node.standby_scan().await {
                    tracing::warn!(%error, "standby scan failed");
                }
            }
        });
    }

    /// One standby pass: refresh the standby set from the catalog and try
    /// to take over lapsed leases.
    async fn standby_scan(self: &Arc<Self>) -> Result<(), NodeError> {
        let names: Vec<String> = self
            .store
            .list_roots("meta:")
            .await?
            .into_iter()
            .filter_map(|root| root.strip_prefix("meta:").map(str::to_owned))
            .collect();
        {
            let mut standby = self
                .standby
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            standby.retain(|name| names.contains(name));
        }
        for name in names {
            if self
                .dbs
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&name)
            {
                continue;
            }
            match self.open_db(&name).await {
                Ok(state) => {
                    tracing::info!(db = %name, owner = %self.config.owner, "standby took over write lease");
                    self.standby
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&name);
                    self.dbs
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(name, state);
                }
                Err(NodeError::Lease(LeaseError::Held { .. })) => {
                    self.standby
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(name);
                }
                Err(error) => {
                    tracing::warn!(db = %name, %error, "standby takeover attempt failed");
                }
            }
        }
        Ok(())
    }

    fn spawn_maintenance(self: &Arc<Self>, state: &Arc<DbState>) {
        let ttl = self.config.lease_ttl_ms;
        let renew_every = Duration::from_millis(u64::try_from(ttl / 3).unwrap_or(1).max(50));
        // Lease renewal.
        let node = Arc::clone(self);
        let db = Arc::clone(state);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(renew_every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if db.deposed.load(Ordering::Acquire) {
                    return;
                }
                // Serialize the root update and local held-lease update with
                // transaction lease checks so they cannot observe different
                // renewal generations and falsely depose this node.
                let _commit = db.commit.lock().await;
                let held = db.lease();
                let name = db.name.clone();
                let renewed =
                    lease::renew(node.store.as_ref(), &name, &held, ttl, now_unix_ms()).await;
                match renewed {
                    Ok(renewed) => {
                        *db.held_lease
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = renewed;
                    }
                    Err(LeaseError::Lost) => {
                        node.depose(&db, "write lease lost");
                        return;
                    }
                    Err(_) => {}
                }
            }
        });
        self.spawn_indexing(state);
        // Heartbeats.
        let node = Arc::clone(self);
        let db = Arc::clone(state);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(node.config.heartbeat_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if db.deposed.load(Ordering::Acquire) {
                    return;
                }
                let _ = db
                    .broadcast
                    .send(pb::subscribe_item::Item::Heartbeat(pb::Heartbeat {
                        basis_t: db.db().basis_t(),
                    }));
            }
        });
    }

    /// Spawns the background indexing job, paced by the database's
    /// [`IndexPolicy`] (re-read every iteration so runtime overrides apply
    /// within a poll interval).
    fn spawn_indexing(self: &Arc<Self>, state: &Arc<DbState>) {
        // How often the job re-checks work and policy when the configured
        // interval is longer; runtime policy changes and pending work are
        // never noticed later than this.
        const POLICY_POLL: Duration = Duration::from_secs(1);
        let node = Arc::clone(self);
        let db = Arc::clone(state);
        tokio::spawn(async move {
            let mut published_at = Instant::now();
            let mut last_duration = Duration::ZERO;
            let mut published_len: Option<u64> = None;
            loop {
                let policy = db.index_policy();
                tokio::time::sleep(policy.interval.min(POLICY_POLL)).await;
                if db.deposed.load(Ordering::Acquire) {
                    return;
                }
                let snapshot = db.db();
                if snapshot.basis_t() <= db.index_basis() {
                    continue;
                }
                let recorded_len = u64::try_from(snapshot.recorded_len()).unwrap_or(u64::MAX);
                let pending = published_len.map(|len| recorded_len.saturating_sub(len));
                if !policy.due(published_at.elapsed(), last_duration, pending) {
                    continue;
                }
                match node.publish_db_indexes(&db).await {
                    Ok((_, duration)) => {
                        last_duration = duration;
                        // publish_db_indexes snapshots after this loop did,
                        // so the covered length is at least recorded_len; the
                        // underestimate only makes the next tail look bigger.
                        published_len = Some(recorded_len);
                    }
                    Err(NodeError::Deposed(_)) => return,
                    Err(_) => {}
                }
                published_at = Instant::now();
            }
        });
    }

    /// Publishes `db`'s covering indexes now, returning the published index
    /// basis and how long the publication took. Serialized with garbage
    /// collection; deposes the database when the root is fenced by a newer
    /// lease.
    async fn publish_db_indexes(&self, db: &Arc<DbState>) -> Result<(u64, Duration), NodeError> {
        let _gc = self.gc_lock.lock().await;
        let version = db.lease().version;
        let root_name = db_root_name(&db.name);
        let started = Instant::now();
        let published = db
            .transactor
            .publish_indexes(self.store.as_ref(), &root_name, version)
            .await;
        let duration = started.elapsed();
        self.metrics.record_index(duration);
        match published {
            Ok(root) => {
                tracing::debug!(db = %db.name, index_basis_t = root.index_basis_t, "published indexes");
                db.index_basis.store(root.index_basis_t, Ordering::Release);
                let _ = db
                    .broadcast
                    .send(pb::subscribe_item::Item::IndexBasis(pb::IndexBasis {
                        index_basis_t: root.index_basis_t,
                    }));
                Ok((root.index_basis_t, duration))
            }
            Err(TransactError::Deposed { .. }) => {
                self.depose(db, "database root fenced by a newer lease");
                Err(NodeError::Deposed(db.name.clone()))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Publishes indexes for `name` immediately, bypassing the pacing
    /// policy (the catalog `RequestIndex` RPC). Returns the resulting index
    /// basis; when the published indexes already cover every committed
    /// transaction, returns the current index basis without publishing.
    ///
    /// # Errors
    /// Returns [`NodeError`] when the database is unknown, this node is
    /// deposed or standing by, or publication fails.
    pub async fn request_index(&self, name: &str) -> Result<u64, NodeError> {
        let state = self.db_state(name).await?;
        if state.db().basis_t() <= state.index_basis() {
            return Ok(state.index_basis());
        }
        self.publish_db_indexes(&state)
            .await
            .map(|(index_basis_t, _)| index_basis_t)
    }

    /// Applies per-database indexing-policy overrides at runtime, returning
    /// the policy now in effect. An empty update reads the current policy.
    ///
    /// # Errors
    /// Returns [`NodeError`] when the database is unknown or served
    /// elsewhere.
    pub async fn set_index_policy(
        &self,
        name: &str,
        update: IndexPolicyUpdate,
    ) -> Result<IndexPolicy, NodeError> {
        let state = self.db_state(name).await?;
        let mut policy = state
            .index_policy
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        policy.apply(update);
        Ok(*policy)
    }

    /// Looks up a hosted database.
    ///
    /// # Errors
    /// Returns [`NodeError::Standby`] when this HA node is standing by for
    /// the database, [`NodeError::UnknownDb`] when absent.
    pub async fn db_state(&self, name: &str) -> Result<Arc<DbState>, NodeError> {
        if let Some(state) = self
            .dbs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
        {
            return Ok(state);
        }
        if self.config.ha
            && self
                .standby
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(name)
        {
            let root = self
                .store
                .get_root(&db_root_name(name))
                .await?
                .as_deref()
                .and_then(DbRoot::decode);
            return Err(NodeError::Standby {
                db: name.to_owned(),
                owner: root.as_ref().map(|r| r.owner.clone()).unwrap_or_default(),
                endpoint: root.map(|r| r.owner_endpoint).unwrap_or_default(),
            });
        }
        Err(NodeError::UnknownDb(name.to_owned()))
    }

    /// Databases this node currently stands by for (HA mode).
    #[must_use]
    pub fn standby_dbs(&self) -> Vec<String> {
        self.standby
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }

    /// Creates a database with the supplied EDN schema forms; returns
    /// `false` when it already exists.
    ///
    /// # Errors
    /// Returns an error for invalid names/schema or store failures.
    pub async fn create_db(
        self: &Arc<Self>,
        name: &str,
        schema_edn: &[u8],
    ) -> Result<bool, NodeError> {
        if !valid_db_name(name) {
            return Err(NodeError::InvalidName(name.to_owned()));
        }
        if self
            .dbs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(name)
        {
            return Ok(false);
        }
        let forms = match codec::decode_edn(schema_edn)? {
            Edn::Vector(items) | Edn::List(items) => items,
            Edn::Nil => Vec::new(),
            other => {
                return Err(NodeError::BadRequest(format!(
                    "schema must be a vector of attribute maps, got {other}"
                )));
            }
        };
        let (schema, idents) = schema_from_edn(&forms)?;
        let meta = codec::encode_metadata(&schema, &idents, &KeywordInterner::default());
        match self
            .store
            .cas_root(&meta_root_name(name), None, &meta)
            .await
        {
            Ok(()) => {}
            Err(StoreError::CasFailed { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        let state = self.open_db(name).await?;
        self.dbs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_owned(), state);
        Ok(true)
    }

    /// Forks `source` into a new database `target` whose state duplicates
    /// the source as of transaction `as_of_t` (`0` forks at the current
    /// basis). Only the log prefix is copied; the target replays it and
    /// publishes its own indexes, while blob segments dedupe by content
    /// address. Returns the fork's basis, or `None` when `target` already
    /// exists.
    ///
    /// # Errors
    /// Returns an error for an invalid target name, an unknown source, an
    /// `as_of_t` ahead of the source's basis, or store/log failures.
    pub async fn fork_db(
        self: &Arc<Self>,
        source: &str,
        target: &str,
        as_of_t: u64,
    ) -> Result<Option<u64>, NodeError> {
        if !valid_db_name(target) {
            return Err(NodeError::InvalidName(target.to_owned()));
        }
        if source == target {
            return Err(NodeError::BadRequest(
                "fork target must differ from the source".into(),
            ));
        }
        let state = self.db_state(source).await?;
        let basis = state.db().basis_t();
        let t = if as_of_t == 0 { basis } else { as_of_t };
        if t > basis {
            return Err(NodeError::BadRequest(format!(
                "as-of t {t} is ahead of {source:?} basis {basis}"
            )));
        }
        let _guard = self.fork_lock.lock().await;
        if self
            .dbs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(target)
            || self
                .store
                .get_root(&meta_root_name(target))
                .await?
                .is_some()
            || self.log_backend.exists(target).await
        {
            return Ok(None);
        }
        // Capture the records before the metadata: meta is made durable
        // before any record that references it, so a meta read afterwards is
        // always a sufficient decode dictionary for the captured prefix.
        // Transaction numbers are contiguous from 1, so the prefix through
        // `t` is exactly the source's state at that basis.
        let records = state.log.tx_range_async(0, Some(t + 1)).await?;
        let meta = self
            .store
            .get_root(&meta_root_name(source))
            .await?
            .ok_or_else(|| NodeError::UnknownDb(source.to_owned()))?;
        // Write the log under version 0 so it sorts beneath the
        // lease-versioned file the target's first open creates, and publish
        // meta last — it is the catalog entry, so a crash mid-fork never
        // catalogs a target without its log.
        let log = self.log_backend.open(target, 0).await?;
        for record in &records {
            log.append_async(record).await?;
        }
        drop(log);
        match self
            .store
            .cas_root(&meta_root_name(target), None, &meta)
            .await
        {
            Ok(()) => {}
            Err(StoreError::CasFailed { .. }) => {
                // Another node claimed the name first; discard our log copy.
                self.log_backend.delete_all(target).await?;
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        }
        let state = self.open_db(target).await?;
        self.dbs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(target.to_owned(), state);
        Ok(Some(t))
    }

    /// Deletes a database: unhosts it, releases its lease, and removes its
    /// roots and log. Blobs remain until [`Self::gc_deleted`].
    ///
    /// # Errors
    /// Returns an error when roots or the log cannot be removed.
    pub async fn delete_db(&self, name: &str) -> Result<bool, NodeError> {
        let Some(state) = self
            .dbs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name)
        else {
            return Ok(false);
        };
        state.deposed.store(true, Ordering::Release);
        self.standby
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
        self.store.delete_root(&db_root_name(name)).await?;
        self.store.delete_root(&meta_root_name(name)).await?;
        self.log_backend.delete_all(name).await?;
        Ok(true)
    }

    /// Lists hosted databases.
    #[must_use]
    pub fn list_dbs(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .dbs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    /// Sweeps blobs unreachable from any live database root (including
    /// everything left behind by deleted databases and superseded indexes).
    ///
    /// # Errors
    /// Returns an error when the store cannot be enumerated or swept.
    pub async fn gc_deleted(&self) -> Result<u64, NodeError> {
        self.gc_deleted_with_retention(self.config.gc_retention)
            .await
    }

    /// Sweeps unreachable blobs older than the caller-supplied retention.
    ///
    /// # Errors
    /// Returns an error when the store cannot be enumerated or swept.
    pub async fn gc_deleted_with_retention(&self, retention: Duration) -> Result<u64, NodeError> {
        let _gc = self.gc_lock.lock().await;
        let mut live = Vec::new();
        for root_name in self.store.list_roots("db:").await? {
            if let Some(root) = self
                .store
                .get_root(&root_name)
                .await?
                .as_deref()
                .and_then(DbRoot::decode)
                && let Some(roots) = root.roots
            {
                live.extend(roots);
            }
        }
        let report = mark_and_sweep_retained(
            self.store.as_ref(),
            live,
            |_, bytes| corium_store::index_blob_children(bytes),
            retention,
            SystemTime::now(),
        )
        .await?;
        self.metrics
            .record_gc(report.swept as u64, report.retained as u64);
        tracing::info!(
            marked = report.marked,
            swept = report.swept,
            retained = report.retained,
            "garbage collection completed"
        );
        Ok(report.swept as u64)
    }

    /// Validates, appends, applies, and reports one transaction supplied as
    /// composite-encoded EDN transaction forms.
    ///
    /// # Errors
    /// Returns [`NodeError`] for decode/validation failures, lease loss, or
    /// storage failures.
    pub async fn transact(
        &self,
        name: &str,
        tx_data: &[u8],
    ) -> Result<pb::TransactResponse, NodeError> {
        let started = Instant::now();
        let result = self
            .transact_inner(name, tx_data)
            .instrument(tracing::info_span!("transact", db = name))
            .await;
        self.metrics.record_tx(started.elapsed(), result.is_ok());
        if let Err(error) = &result {
            tracing::warn!(%error, "transaction failed");
        }
        result
    }

    async fn transact_inner(
        &self,
        name: &str,
        tx_data: &[u8],
    ) -> Result<pb::TransactResponse, NodeError> {
        let state = self.db_state(name).await?;
        let decoded = codec::decode_edn(tx_data)?;
        let forms = decoded
            .as_seq()
            .ok_or_else(|| NodeError::BadRequest("tx-data must be a vector".into()))?
            .to_vec();
        // Enqueue for the next group-commit flush, then contend to lead one.
        // Whichever caller holds `commit` drains the queue and commits the
        // whole run under one durable append and one ownership fence, then
        // answers every queued caller — so batching is invisible to clients:
        // each transaction keeps its own `t`, report, and ack.
        let (resp_tx, mut resp_rx) = oneshot::channel();
        state
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(CommitRequest {
                forms,
                resp: resp_tx,
            });
        let queued = self.metrics.queue_waiter();
        loop {
            let commit = state.commit.lock().await;
            // A prior leader may already have committed this request.
            match resp_rx.try_recv() {
                Ok(result) => {
                    drop(commit);
                    drop(queued);
                    return result;
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => {
                    drop(commit);
                    drop(queued);
                    return Err(NodeError::GroupCommit("commit response dropped".into()));
                }
            }
            // Lead a flush of the pending queue (which contains this request).
            self.flush_commit_batch(&state).await;
            drop(commit);
            match resp_rx.try_recv() {
                Ok(result) => {
                    drop(queued);
                    return result;
                }
                // A naming change ends a batch before this request; the
                // remainder was requeued, so loop and lead the next flush.
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => {
                    drop(queued);
                    return Err(NodeError::GroupCommit("commit response dropped".into()));
                }
            }
        }
    }

    /// Group-commit flush, run by the batch leader while it holds
    /// `state.commit`: drains the pending queue, prepares the run against a
    /// staging value (so each transaction still validates against its
    /// predecessors), makes the whole run durable with one batched append and
    /// one post-append ownership fence, then installs it and answers every
    /// caller. A transaction that interns new keywords ends the batch — so a
    /// later transaction never depends on names not yet durable — and the
    /// unprepared remainder is requeued for the next flush.
    #[allow(clippy::too_many_lines)]
    async fn flush_commit_batch(&self, state: &Arc<DbState>) {
        let max_count = self.config.max_commit_batch.max(1);
        let max_bytes = self.config.max_commit_batch_bytes;

        let mut batch: VecDeque<CommitRequest> = {
            let mut pending = state
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *pending)
        };
        if batch.is_empty() {
            return;
        }
        // No pre-append ownership check on the common path: the post-append
        // fence below is the safety-critical one, and skipping the pre-check
        // removes a lease round trip per batch. A deposed leader still prepares
        // and appends (harmlessly, under its old lease version, which the
        // successor's cutoff discards), then the fence refuses to acknowledge.
        // The one exception is a batch that interns new keywords, which
        // publishes the unfenced metadata root — that path re-checks ownership
        // before writing it, below.
        let now_ms = now_unix_ms();
        let mut cursor = state.transactor.batch_cursor();
        let mut resps: Vec<oneshot::Sender<Result<pb::TransactResponse, NodeError>>> = Vec::new();
        let mut prepared: Vec<Prepared> = Vec::new();
        let mut batch_bytes: usize = 0;
        let mut measure = Vec::new();
        let mut naming_changed = false;
        while let Some(request) = batch.pop_front() {
            // Expand `:db/fn` against the staging value, so each transaction
            // sees the earlier ones in the batch. The expander blocks up to
            // its budget, so it runs off the async workers.
            let forms = if let Some(expander) = &self.config.tx_fn_expander {
                let expander = Arc::clone(expander);
                let db = cursor.db().clone();
                let forms = request.forms;
                match tokio::task::spawn_blocking(move || expander.expand(&db, forms)).await {
                    Ok(Ok(forms)) => forms,
                    Ok(Err(message)) => {
                        let _ = request.resp.send(Err(NodeError::BadRequest(message)));
                        continue;
                    }
                    Err(error) => {
                        let _ = request.resp.send(Err(NodeError::BadRequest(format!(
                            "expander task failed: {error}"
                        ))));
                        continue;
                    }
                }
            } else {
                request.forms
            };
            // Convert forms, interning new keyword values into the shared
            // naming, against the staging value.
            let (items, this_changed) = {
                let mut naming = state
                    .naming
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let before = naming.interner.len();
                match tx_items_from_edn(cursor.db(), &mut naming.interner, &forms) {
                    Ok(items) => (items, naming.interner.len() > before),
                    Err(error) => {
                        drop(naming);
                        let _ = request.resp.send(Err(error.into()));
                        continue;
                    }
                }
            };
            match cursor.prepare(items, now_ms) {
                Ok(prep) => {
                    measure.clear();
                    let _ = corium_log::append_framed_record(&mut measure, &prep.record);
                    batch_bytes += measure.len();
                    resps.push(request.resp);
                    prepared.push(prep);
                }
                Err(error) => {
                    let _ = request.resp.send(Err(NodeError::Transact(error.into())));
                    continue;
                }
            }
            if this_changed {
                naming_changed = true;
                break;
            }
            // Cap the batch by transaction count or accumulated encoded size.
            // The transaction that crosses the byte budget is already included,
            // so at least one — even a single oversized transaction — commits.
            if prepared.len() >= max_count || batch_bytes >= max_bytes {
                break;
            }
        }
        // Requeue the unprepared remainder at the front of the queue, in order.
        if !batch.is_empty() {
            let mut pending = state
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while let Some(request) = batch.pop_back() {
                pending.push_front(request);
            }
        }
        if prepared.is_empty() {
            return;
        }
        // Snapshot the interner for response encoding; capture the idents only
        // when naming changed, to carry into `update_naming` after install.
        let interner = {
            let naming = state
                .naming
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            naming.interner.clone()
        };
        let changed_idents = if naming_changed {
            // Publishing new keyword names writes the metadata root, which is
            // not lease-fenced, so verify ownership before writing it. This is
            // the one lease check the common (no-new-keyword) path skips.
            if let Err(error) = state.check_lease(self.store.as_ref()).await {
                if matches!(error, NodeError::Deposed(_)) {
                    self.depose(state, "write lease lost before metadata publish");
                }
                for resp in resps {
                    let _ = resp.send(Err(batch_abort_error(&state.name, &error)));
                }
                return;
            }
            let (idents, schema) = {
                let naming = state
                    .naming
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (naming.idents.clone(), naming.schema.clone())
            };
            // New keyword names must be durable before the datoms that
            // reference them; recovery decodes the log against this meta.
            let meta = codec::encode_metadata(&schema, &idents, &interner);
            loop {
                let cas = match self.store.get_root(&meta_root_name(&state.name)).await {
                    Ok(current) => {
                        self.store
                            .cas_root(&meta_root_name(&state.name), current.as_deref(), &meta)
                            .await
                    }
                    Err(error) => Err(error),
                };
                match cas {
                    Ok(()) => break,
                    Err(StoreError::CasFailed { .. }) => {}
                    Err(error) => {
                        let error = NodeError::Store(error);
                        for resp in resps {
                            let _ = resp.send(Err(batch_abort_error(&state.name, &error)));
                        }
                        return;
                    }
                }
            }
            Some(idents)
        } else {
            None
        };
        // One durable append for the whole batch — the commit point.
        let records: Vec<TxRecord> = prepared.iter().map(|prep| prep.record.clone()).collect();
        if let Err(error) = state.log.append_batch_async(&records).await {
            let error = NodeError::Log(error);
            for resp in resps {
                let _ = resp.send(Err(batch_abort_error(&state.name, &error)));
            }
            return;
        }
        // Install in memory now — while still holding `commit`, before the
        // fence — so the live value stays in lock-step with the durable log
        // regardless of the fence outcome (exactly as the single-transaction
        // path applied before its fence). Installing advances the value and
        // notifies in-process subscribers; it does not acknowledge callers.
        let reports = state.transactor.install_batch(cursor, prepared);
        if let Some(idents) = changed_idents {
            state.transactor.update_naming(idents, interner.clone());
        }
        // Post-append fence gates only the acknowledgement and the peer
        // stream: ack a batch only if ownership was intact after it became
        // durable. A takeover that raced the append replayed the log *after*
        // rewriting the root record, so a batch we ack is provably in the
        // successor's replay; one we refuse is discarded by the successor's
        // cutoff — and because the whole batch is one atomic object, the cutoff
        // keeps all or none of it. One fence covers the batch (see
        // log-and-transactor.md).
        if let Err(error) = state.check_lease(self.store.as_ref()).await {
            if matches!(error, NodeError::Deposed(_)) {
                self.depose(state, "write lease lost after durable append");
            }
            for resp in resps {
                let _ = resp.send(Err(batch_abort_error(&state.name, &error)));
            }
            return;
        }
        let mut last_t = 0;
        for (resp, report) in resps.into_iter().zip(reports) {
            let t = report.db_after.basis_t();
            last_t = last_t.max(t);
            let datoms = match codec::encode_datoms(&report.tx.datoms, &interner) {
                Ok(datoms) => datoms,
                Err(error) => {
                    let _ = resp.send(Err(NodeError::Codec(error)));
                    continue;
                }
            };
            let tempids = codec::encode_edn(&Edn::Map(
                report
                    .tx
                    .tempids
                    .iter()
                    .map(|(tempid, eid)| {
                        (
                            Edn::Str(tempid.clone()),
                            Edn::Long(i64::try_from(eid.raw()).unwrap_or(i64::MAX)),
                        )
                    })
                    .collect(),
            ));
            let _ = state
                .broadcast
                .send(pb::subscribe_item::Item::Report(pb::TxReport {
                    t,
                    tx_instant: report.tx_instant,
                    datoms: datoms.clone(),
                }));
            let _ = resp.send(Ok(pb::TransactResponse {
                basis_before: report.db_before.basis_t(),
                basis_t: t,
                tx_instant: report.tx_instant,
                tempids,
                tx_data: datoms,
            }));
        }
        if last_t > 0 {
            let _ = state.basis.send(last_t);
        }
    }

    /// Current status for a database.
    ///
    /// # Errors
    /// Returns [`NodeError::UnknownDb`] when absent.
    pub async fn status(&self, name: &str) -> Result<pb::StatusResponse, NodeError> {
        let state = self.db_state(name).await?;
        let db = state.db();
        let counts = db.stats();
        let held = state.lease();
        let metrics = self.metrics.snapshot();
        Ok(pb::StatusResponse {
            basis_t: db.basis_t(),
            index_basis_t: state.index_basis(),
            lease_owner: held.owner,
            lease_version: held.version,
            lease_expires_unix_ms: held.expires_unix_ms,
            datom_count: counts.datoms as u64,
            entity_count: counts.entities as u64,
            attribute_count: counts.attributes as u64,
            transaction_count: metrics.tx_total,
            transaction_failure_count: metrics.tx_failed,
            transaction_queue_depth: metrics.queue_depth,
            index_lag: db.basis_t().saturating_sub(state.index_basis()),
            indexing_runs: metrics.index_runs,
            gc_runs: metrics.gc_runs,
            gc_swept_blobs: metrics.gc_swept,
            lease_owner_endpoint: held.endpoint,
        })
    }

    /// Fixes the current transaction basis and returns the connection details
    /// an administrative client needs to replay the underlying storage log
    /// independently.
    ///
    /// # Errors
    /// Returns [`NodeError::UnknownDb`] when absent, or a bad-request error
    /// when local connection details cannot be represented on the wire.
    pub async fn backup_info(&self, name: &str) -> Result<pb::GetBackupInfoResponse, NodeError> {
        let state = self.db_state(name).await?;
        // Serialize with the tiny commit critical section so the checkpoint
        // cannot observe a batch after its durable append but before its
        // ownership fence and acknowledgement decision.
        let _commit = state.commit.lock().await;
        state.check_lease(self.store.as_ref()).await?;
        let basis_t = state.db().basis_t();
        let storage = self
            .config
            .store
            .connection_info(&self.config.data_dir)
            .map_err(NodeError::BadRequest)?;
        Ok(pb::GetBackupInfoResponse {
            basis_t,
            storage: Some(storage),
        })
    }

    /// Releases every held write lease (graceful shutdown): the record is
    /// expired in place so a standby's next poll takes over immediately
    /// instead of waiting out the TTL. Hosted databases stop accepting
    /// work first, so nothing commits after its lease is gone.
    pub async fn release_leases(&self) {
        let states: Vec<Arc<DbState>> = self
            .dbs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
            .map(|(_, state)| state)
            .collect();
        for state in states {
            state.deposed.store(true, Ordering::Release);
            if let Err(error) =
                lease::release(self.store.as_ref(), &state.name, &state.lease()).await
            {
                tracing::warn!(db = %state.name, %error, "lease release failed at shutdown");
            }
        }
    }

    /// Waits until the database basis reaches `t`, returning the basis seen.
    ///
    /// # Errors
    /// Returns [`NodeError::UnknownDb`] when absent.
    pub async fn sync(&self, name: &str, t: u64) -> Result<u64, NodeError> {
        let state = self.db_state(name).await?;
        let mut basis = state.basis_watch();
        let target = if t == 0 { *basis.borrow() } else { t };
        loop {
            let current = *basis.borrow();
            if current >= target {
                return Ok(current);
            }
            if basis.changed().await.is_err() {
                return Ok(*basis.borrow());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IndexPolicy;
    use std::time::Duration;

    fn pacing(interval_ms: u64, backoff: u32, threshold: u64, deadline_ms: u64) -> IndexPolicy {
        IndexPolicy {
            interval: Duration::from_millis(interval_ms),
            backoff,
            tail_threshold: threshold,
            tail_deadline: Duration::from_millis(deadline_ms),
        }
    }

    #[test]
    fn base_interval_gates_publication() {
        let pacing = pacing(100, 4, 0, 60_000);
        assert!(!pacing.due(Duration::from_millis(99), Duration::ZERO, None));
        assert!(pacing.due(Duration::from_millis(100), Duration::ZERO, None));
    }

    #[test]
    fn backoff_stretches_the_floor_past_the_interval() {
        let pacing = pacing(100, 4, 0, 60_000);
        let last = Duration::from_millis(300);
        assert!(!pacing.due(Duration::from_millis(1_199), last, Some(10)));
        assert!(pacing.due(Duration::from_millis(1_200), last, Some(10)));
    }

    #[test]
    fn zero_backoff_keeps_the_base_interval() {
        let pacing = pacing(100, 0, 0, 60_000);
        assert!(pacing.due(
            Duration::from_millis(100),
            Duration::from_secs(30),
            Some(10)
        ));
    }

    #[test]
    fn fast_publications_leave_the_interval_untouched() {
        let pacing = pacing(5_000, 4, 0, 60_000);
        assert!(pacing.due(Duration::from_secs(5), Duration::from_millis(3), Some(1)));
    }

    #[test]
    fn small_tail_defers_until_the_deadline() {
        let pacing = pacing(100, 4, 1_000, 60_000);
        assert!(!pacing.due(Duration::from_secs(30), Duration::ZERO, Some(999)));
        assert!(pacing.due(Duration::from_secs(60), Duration::ZERO, Some(999)));
    }

    #[test]
    fn tail_at_threshold_publishes_at_base_pacing() {
        let pacing = pacing(100, 4, 1_000, 60_000);
        assert!(pacing.due(Duration::from_millis(100), Duration::ZERO, Some(1_000)));
    }

    #[test]
    fn unknown_tail_publishes_at_base_pacing() {
        let pacing = pacing(100, 4, 1_000, 60_000);
        assert!(pacing.due(Duration::from_millis(100), Duration::ZERO, None));
    }

    #[test]
    fn deadline_never_overrides_the_backoff_floor() {
        let pacing = pacing(100, 4, 1_000, 200);
        let last = Duration::from_millis(300);
        assert!(!pacing.due(Duration::from_millis(400), last, Some(1)));
        assert!(pacing.due(Duration::from_millis(1_200), last, Some(1)));
    }
}
