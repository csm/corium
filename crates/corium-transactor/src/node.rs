//! The transactor as a process: multi-database state, durable naming,
//! lease acquisition/renewal, background indexing, tx-report fan-out, and
//! high-availability standby takeover.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use corium_core::{Datom, IndexOrder, KeywordInterner, Partition, Schema};
use corium_crypt::{KeyId, Keyring};
use corium_db::{Db, Idents};
use corium_log::{LogError, RootedLog, TransactionLog, TxRecord};
use corium_protocol::codec::{self, CodecError};
use corium_protocol::pb;
use corium_protocol::schemaform::{SchemaFormError, schema_from_edn};
use corium_protocol::txforms::{TxFormError, tx_items_from_edn};
use corium_query::edn::Edn;
use corium_store::{
    BlobId, BlobStore, KeyManifest, RootStore, StoreError, decode_index_manifest,
    decode_segment_keys, is_index_manifest, keys_root_name, load_key_manifest, mark_reachable,
    meta_root_name, publish_key_manifest, sweep_unmarked,
};
use thiserror::Error;
use tokio::sync::{broadcast, oneshot, watch};
use tracing::Instrument;

use crate::backend::{LogBackend, NodeStore, StorageInfoConfig, StoreSpec, open_node_store};
use crate::branch::{Branch, branch_name, is_branch_name, parse_branch_name};
use crate::keys::{DbCrypto, DbStore, KeyWiringError, reload_db_crypto, resolve_db_crypto};
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
    /// Separately provisioned read-only service credentials advertised by
    /// `GetStorageInfo`. Local backends do not use this setting.
    pub storage_info: StorageInfoConfig,
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
    /// Keys this process can resolve, for databases encrypted at rest.
    ///
    /// A node without one serves unencrypted databases exactly as before and
    /// refuses to open an encrypted one, naming the key its manifest wants.
    pub keyring: Option<Arc<dyn Keyring>>,
}

impl std::fmt::Debug for NodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeConfig")
            .field("store", &self.store)
            .field("storage_info", &self.storage_info)
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
            .field("keyring", &self.keyring.is_some())
            .finish()
    }
}

impl NodeConfig {
    /// Sensible defaults for a data directory.
    #[must_use]
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            store: StoreSpec::filesystem(),
            storage_info: StorageInfoConfig::default(),
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
            keyring: None,
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

/// What `corium keys status` reports for one database.
///
/// The two alarm flags are about *this node*, not the stored manifest: they
/// answer "is the process doing the encrypting actually using the keys the
/// manifest names?", which the manifest alone cannot say.
#[derive(Debug)]
pub struct KeyStatus {
    /// The stored manifest; `None` for an unencrypted database.
    pub manifest: Option<KeyManifest>,
    /// Current transaction basis, which closes the active epoch's nonce span.
    pub basis_t: u64,
    /// A manifest change could not be loaded, but the keys in hand still
    /// serve and still write under the active epoch.
    pub keys_unavailable: bool,
    /// This node writes under an epoch the manifest has closed, so writes are
    /// refused until a reload succeeds.
    pub keys_fenced: bool,
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
    /// Storage-encryption keys could not be resolved.
    #[error(transparent)]
    Keys(#[from] KeyWiringError),
    /// The key manifest opened a storage-key epoch this node cannot load, so
    /// it would otherwise keep sealing records under a closed one.
    #[error(
        "database {db:?} writes under storage-key epoch {writing}, but the key manifest          has opened epoch {active} and this node cannot load it;          fix --storage-key so the new epoch resolves"
    )]
    KeysFenced {
        /// Database refusing writes.
        db: String,
        /// Epoch the manifest says new writes belong to.
        active: u32,
        /// Epoch this node still holds.
        writing: u32,
    },
    /// Malformed request.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// A conditional transaction was prepared from a stale database value.
    #[error("basis changed: expected {expected}, current basis is {actual}")]
    BasisMismatch {
        /// Basis supplied by the caller.
        expected: u64,
        /// Basis current when the request reached the commit queue.
        actual: u64,
    },
    /// A group-commit batch aborted after preparation (durable append,
    /// ownership fence, or metadata publish failed); every batched caller
    /// receives this so it retries. Carries the originating error's text
    /// because the underlying store/log errors are not cloneable.
    #[error("group commit aborted: {0}")]
    GroupCommit(String),
    /// The plan submitted for apply is not the plan this database produces
    /// now, so the operator would be applying something they did not read.
    #[error(
        "plan {submitted} is stale: the current plan for this schema file is {current}; \
         review the new plan and re-run"
    )]
    StalePlan {
        /// Digest the caller submitted.
        submitted: String,
        /// Digest the transactor computed under its commit queue.
        current: String,
    },
    /// A precondition of the plan no longer holds, or the caller did not
    /// supply the authority the plan needs. Nothing was changed.
    #[error("schema update blocked: {0}")]
    BlockedPlan(String),
    /// A desired schema that cannot be planned or compiled at all.
    #[error("schema update rejected: {0}")]
    SchemaUpdate(String),
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
    expected_basis_t: Option<u64>,
    resp: oneshot::Sender<Result<pb::TransactResponse, NodeError>>,
}

/// Per-database state hosted by a node.
pub struct DbState {
    name: String,
    transactor: EmbeddedTransactor,
    log: Arc<dyn TransactionLog>,
    /// This database's view of the node's storage service, plus its log
    /// cipher. A storage-key rotation swaps the blob decorator here and
    /// installs the new snapshot into the (shared) cipher the open log holds.
    crypto: std::sync::RwLock<Arc<DbCrypto>>,
    /// Generation of the key manifest this state was resolved from, mirroring
    /// `DbRoot::key_manifest_version`. A root carrying a different one means
    /// another process rotated or re-wrapped, and these keys are stale.
    key_manifest_version: AtomicU64,
    /// A manifest change could not be loaded: the keyring cannot resolve the
    /// KEK it now names, or the KMS holding it is unreachable. The already
    /// unwrapped snapshot keeps serving, because a re-wrap leaves the data
    /// keys themselves unchanged and a KMS outage should not take the write
    /// path down. Observable rather than fatal.
    keys_unavailable: AtomicBool,
    /// The manifest's active epoch is *not* the one this node writes under,
    /// and the keys to adopt it could not be loaded. Unlike the above this is
    /// unsafe to continue through: every record sealed from here is drawn
    /// under a key the manifest considers closed, and the log-record nonce
    /// budget — which is measured as the span of `t` between epochs — stops
    /// counting them. Writes refuse until a reload succeeds.
    keys_fenced: AtomicBool,
    /// The epoch the manifest opened when [`Self::keys_fenced`] was raised, so
    /// the refusal names both sides of the mismatch.
    fenced_active_epoch: AtomicU32,
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
    /// Present when this state is a saga's branch rather than a database
    /// (ADR-0023): its steps obey the saga's declaration, and its commits are
    /// fenced by the parent's lease.
    branch: Option<Branch>,
}

impl DbState {
    /// Database name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The store this database's blobs are read and written through:
    /// encrypting for an encrypted database, the bare backend otherwise.
    #[must_use]
    pub fn store(&self) -> Arc<DbStore> {
        Arc::clone(
            &self
                .crypto
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .store,
        )
    }

    /// Clears both key alarms after a successful reload, reporting whether
    /// this database had been raising one.
    fn clear_key_alarms(&self) -> bool {
        // Both swaps must run, so this is a bitwise `|`, not a short-circuit.
        self.keys_unavailable.swap(false, Ordering::AcqRel)
            | self.keys_fenced.swap(false, Ordering::AcqRel)
    }

    /// Whether either key alarm is currently raised.
    fn key_alarm_raised(&self) -> bool {
        self.keys_unavailable.load(Ordering::Acquire) || self.keys_fenced.load(Ordering::Acquire)
    }

    /// The error writes refuse with while the keys are fenced, if they are.
    ///
    /// A branch seals its records under the parent's keys, so it is the
    /// parent's alarm that must stop it writing.
    fn keys_fenced_error(&self) -> Option<NodeError> {
        if let Some(branch) = &self.branch {
            return branch.parent.keys_fenced_error();
        }
        if !self.keys_fenced.load(Ordering::Acquire) {
            return None;
        }
        let crypto = self.crypto();
        Some(NodeError::KeysFenced {
            db: self.name.clone(),
            active: self.fenced_active_epoch.load(Ordering::Acquire),
            writing: crypto.store.storage_epoch().unwrap_or_default(),
        })
    }

    fn crypto(&self) -> Arc<DbCrypto> {
        Arc::clone(
            &self
                .crypto
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Whether this database's durable artifacts are encrypted.
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        self.store().storage_epoch().is_some()
    }

    /// Whether a key-manifest change could not be loaded. The keys in hand
    /// still serve; see [`Self::keys_fenced`] for the case that does not.
    #[must_use]
    pub fn keys_unavailable(&self) -> bool {
        self.keys_unavailable.load(Ordering::Acquire)
    }

    /// Whether this node is writing under a storage-key epoch the manifest has
    /// closed. Writes refuse while this holds.
    #[must_use]
    pub fn keys_fenced(&self) -> bool {
        self.keys_fenced.load(Ordering::Acquire)
    }

    /// Current database value.
    #[must_use]
    pub fn db(&self) -> Db {
        self.transactor.db()
    }

    /// The saga this state is a branch of, when it is one.
    #[must_use]
    pub const fn branch(&self) -> Option<&Branch> {
        self.branch.as_ref()
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

    /// The schema, ident registry, and interner this database currently
    /// names its data by.
    #[must_use]
    pub fn naming_snapshot(&self) -> (Schema, Idents, KeywordInterner) {
        let naming = self
            .naming
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            naming.schema.clone(),
            naming.idents.clone(),
            naming.interner.clone(),
        )
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
    /// A branch answers from two logs (ADR-0023). Its own log holds the
    /// steps, numbered from `t₀ + 1`; everything at or below `t₀` is the
    /// parent's history, which the branch shares rather than copies. Splicing
    /// them here is what makes a branch a whole database to every reader that
    /// reads a log: the concatenation is contiguous and in `t` order, so a
    /// peer subscribing to a branch folds the parent's prefix and the
    /// branch's novelty into one value, and `as-of t` for `t ≤ t₀` answers
    /// exactly what the parent answers.
    ///
    /// # Errors
    /// Returns an error when the log cannot be read.
    pub async fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, NodeError> {
        let Some(branch) = &self.branch else {
            return Ok(self.log.tx_range_async(start, end).await?);
        };
        let split = branch.basis_t() + 1;
        let mut records = Vec::new();
        if start < split {
            let prefix_end = end.map_or(split, |end| end.min(split));
            records.extend(
                branch
                    .parent
                    .log
                    .tx_range_async(start, Some(prefix_end))
                    .await?,
            );
        }
        let tail_start = start.max(split);
        if end.is_none_or(|end| end > tail_start) {
            records.extend(self.log.tx_range_async(tail_start, end).await?);
        }
        Ok(records)
    }

    /// Verifies this node still owns the write lease (identity check on
    /// the root record; expiry changes from renewals do not matter).
    ///
    /// A branch is fenced by its parent's lease. A branch has no lease of its
    /// own because it has no independent existence: the node that owns the
    /// parent hosts its branches, and one that has lost the parent must not
    /// acknowledge a step against them either.
    async fn check_lease(&self, store: &dyn RootStore) -> Result<Lease, NodeError> {
        match &self.branch {
            Some(branch) => Box::pin(branch.parent.check_lease(store)).await,
            None => self.check_own_lease(store).await,
        }
    }

    async fn check_own_lease(&self, store: &dyn RootStore) -> Result<Lease, NodeError> {
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
    /// Saga branches hosted beside their parents (ADR-0023). They are kept
    /// apart from `dbs` because they are not databases: nothing lists them,
    /// nothing stands by for them, and nothing may create one by name.
    branches: std::sync::RwLock<HashMap<String, Arc<DbState>>>,
    /// Serializes branch opening, so two callers racing the first step of a
    /// saga do not build two overlays over one branch log.
    branch_lock: tokio::sync::Mutex<()>,
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

/// Beats the subscription stream of `state` every `interval`.
///
/// A subscriber treats silence as a dead transactor and fails over, so every
/// hosted state needs this — a saga branch as much as a database, even though
/// a branch has no lease to renew and no indexes to publish, which is the
/// rest of what maintenance does.
fn spawn_heartbeat(state: &Arc<DbState>, interval: Duration) {
    let db = Arc::clone(state);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
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
        #[cfg(feature = "s3")]
        if config.store.kind() == "s3" && config.storage_info.s3.is_none() {
            tracing::warn!(
                "S3 read-only credentials are not configured; GetStorageInfo will reject \
                 peer bootstrap and direct-storage backup requests"
            );
        }
        config.storage_info.initialize().await;
        let store = open_node_store(&config.store, &config.data_dir).await?;
        let log_backend = LogBackend::for_spec(&config.store, &config.data_dir, Arc::clone(&store));
        let node = Arc::new(Self {
            config,
            store,
            log_backend,
            dbs: std::sync::RwLock::new(HashMap::new()),
            standby: std::sync::RwLock::new(BTreeSet::new()),
            gc_lock: tokio::sync::Mutex::new(()),
            fork_lock: tokio::sync::Mutex::new(()),
            branches: std::sync::RwLock::new(HashMap::new()),
            branch_lock: tokio::sync::Mutex::new(()),
            metrics: Metrics::default(),
            shutdown: watch::channel(None).0,
        });
        let names: Vec<String> = node
            .store
            .list_roots("meta:")
            .await?
            .into_iter()
            .filter_map(|root| root.strip_prefix("meta:").map(str::to_owned))
            // Saga branches carry metadata roots of their own, and are not
            // databases: they are opened beside the parent that owns them.
            .filter(|name| !is_branch_name(name))
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
        // Keys are resolved before the lease is taken: an encrypted database
        // this process cannot open should fail loudly without first fencing
        // out whoever can.
        let manifest = load_key_manifest(self.store.as_ref(), name).await?;
        let crypto = resolve_db_crypto(
            name,
            &self.store,
            manifest.as_ref(),
            self.config.keyring.as_ref(),
        )
        .await?;
        // Acquisition rewrites the root record under our lease version, so
        // it doubles as the fence bump: a deposed writer's pending root CAS
        // now has stale expected bytes and must fail. It also preserves the
        // published snapshot's recovery hints, so the root we re-read below
        // carries everything index-root recovery needs.
        let held = self.acquire_lease(name).await?;
        // The log tail replay below happens strictly after the fence, so it
        // observes every record a previous owner could ever have acked.
        let log = self
            .log_backend
            .open(name, held.version, crypto.cipher.clone())
            .await?;
        let post_fence = self
            .store
            .get_root(&root_name)
            .await?
            .as_deref()
            .and_then(DbRoot::decode);
        let transactor = self
            .recover_transactor(
                name,
                &schema,
                &idents,
                &interner,
                post_fence.as_ref(),
                &log,
                crypto.store.as_ref(),
            )
            .await?;
        let basis_t = transactor.db().basis_t();
        let key_manifest_version = post_fence
            .as_ref()
            .map_or(0, |root| root.key_manifest_version);
        let index_basis = post_fence.map_or(0, |root| root.index_basis_t);
        let state = Arc::new(DbState {
            name: name.to_owned(),
            transactor,
            log,
            crypto: std::sync::RwLock::new(Arc::new(crypto)),
            key_manifest_version: AtomicU64::new(key_manifest_version),
            keys_unavailable: AtomicBool::new(false),
            keys_fenced: AtomicBool::new(false),
            fenced_active_epoch: AtomicU32::new(0),
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
            branch: None,
        });
        self.spawn_maintenance(&state);
        Ok(state)
    }

    /// Builds the recovered transactor for `open_db`.
    ///
    /// When the post-fence root publishes current and history snapshots with
    /// recovery hints, recovers from those roots plus the log tail without
    /// replaying the log prefix. Any missing history root or hint (an older
    /// root, or a bare fence bump with no snapshot), or a failure
    /// materializing the snapshot, falls back to full-log replay, which is
    /// always correct because the log is the source of truth.
    #[allow(clippy::too_many_arguments)]
    async fn recover_transactor(
        &self,
        name: &str,
        schema: &Schema,
        idents: &Idents,
        interner: &KeywordInterner,
        root: Option<&DbRoot>,
        log: &Arc<dyn TransactionLog>,
        store: &DbStore,
    ) -> Result<EmbeddedTransactor, NodeError> {
        // `next_entity_id == 0` is the "no hint" sentinel (see DbRoot); it and
        // an absent snapshot both rule out the tail-only path.
        if let Some(root) = root
            && let Some(roots) = &root.roots
            && let Some(history_roots) = &root.history_roots
            && root.next_entity_id != 0
        {
            match Self::load_history_snapshot(
                store,
                root,
                &roots[IndexOrder::Eavt as usize],
                &history_roots[IndexOrder::Eavt as usize],
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

    /// Materializes a complete database value at a published index root from
    /// its current and history EAVT snapshots — the transactor-side
    /// counterpart of the peer bootstrap. The log tail carries everything
    /// since this shared basis.
    async fn load_history_snapshot(
        store: &DbStore,
        root: &DbRoot,
        current_eavt: &BlobId,
        history_eavt: &BlobId,
        schema: &Schema,
        idents: &Idents,
        interner: &KeywordInterner,
    ) -> Result<Db, StoreError> {
        let current = Self::load_index_keys(store, current_eavt)
            .await?
            .into_iter()
            .map(|key| Datom::from_key(IndexOrder::Eavt, &key))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Io(std::io::Error::other(error.to_string())))?;
        let history = Self::load_index_keys(store, history_eavt)
            .await?
            .into_iter()
            .map(|key| Datom::from_key(IndexOrder::Eavt, &key))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Io(std::io::Error::other(error.to_string())))?;
        Ok(Db::from_history_snapshot_with_next_user(
            root.index_basis_t,
            root.next_entity_id,
            schema.clone(),
            idents.clone(),
            interner.clone(),
            history,
            current,
        ))
    }

    /// Reads one covering index's sorted key stream from the blob store: a
    /// format-3 manifest's chunks in order, or a pre-format-3 flat blob.
    async fn load_index_keys(store: &DbStore, id: &BlobId) -> Result<Vec<Vec<u8>>, StoreError> {
        let blob = store
            .get(id)
            .await?
            .ok_or_else(|| StoreError::MissingBlob(id.clone()))?;
        if !is_index_manifest(&blob) {
            return decode_segment_keys(&blob);
        }
        let mut keys = Vec::new();
        for child in decode_index_manifest(&blob)? {
            let chunk = store
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
            .filter(|name| !is_branch_name(name))
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
                // The renewal already re-read the root, so this is where a
                // manifest change made elsewhere is cheapest to notice.
                if let Err(error) = node.refresh_keys_if_stale(&db).await {
                    tracing::warn!(db = %name, %error, "cannot reload storage keys");
                }
            }
        });
        self.spawn_indexing(state);
        spawn_heartbeat(state, self.config.heartbeat_interval);
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
            .publish_indexes(db.store().as_ref(), &root_name, version)
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

    /// Applies a reviewed schema plan under the single-writer commit queue.
    ///
    /// The caller submits the *desired schema*, not the plan. This method
    /// recomputes the plan against the schema installed right now and refuses
    /// unless its digest is the one the caller reviewed, so the digest is a
    /// precondition rather than an opaque token. Every safety-critical check —
    /// blocked steps, execution-class allowances, acknowledgement codes — is
    /// re-run here, beneath `commit`, where no concurrent write can invalidate
    /// it between the check and the append.
    ///
    /// Data-basis drift is deliberately tolerated. A plan is invalidated by a
    /// schema change or a failed precondition, not by an unrelated write, so a
    /// busy database can still add an attribute.
    ///
    /// Re-applying an already installed schema is a no-op that reports
    /// `changed: false`, which is what makes the command safe in a pipeline.
    ///
    /// # Errors
    /// Returns [`NodeError::StalePlan`] when the plan digest no longer
    /// matches, [`NodeError::BlockedPlan`] when a precondition or authority is
    /// missing, and the usual lease, store, and log failures otherwise.
    #[allow(clippy::too_many_lines)]
    pub async fn alter_schema(
        &self,
        request: &pb::AlterSchemaRequest,
        requester: &str,
    ) -> Result<pb::AlterSchemaResponse, NodeError> {
        use corium_core::migration::{AckCode, ExecutionClass};
        use corium_forms::apply::{AuditRecord, audit_datoms, compile};
        use corium_forms::desired::DesiredSchema;
        use corium_forms::planner::{PlanOptions, installed_schema, plan_against};

        let state = self.db_state(&request.db).await?;
        // Schema migration has its own plan/apply lifecycle against the
        // parent, and a branch's novelty is validated against the parent's
        // schema at merge; a branch that could migrate would be planning
        // against a database nobody else can see (ADR-0023).
        if state.branch().is_some() {
            return Err(NodeError::BadRequest(format!(
                "{} is a saga branch; schema changes belong to the parent database",
                request.db
            )));
        }
        let forms = match codec::decode_edn(&request.desired_schema)? {
            Edn::Vector(forms) | Edn::List(forms) => forms,
            other => {
                return Err(NodeError::BadRequest(format!(
                    "desired schema must be a vector of attribute maps, got {other}"
                )));
            }
        };
        let desired = DesiredSchema::from_edn(&forms)
            .map_err(|error| NodeError::SchemaUpdate(error.to_string()))?;
        let allowed: BTreeSet<ExecutionClass> = request
            .allow
            .iter()
            .map(|name| {
                ExecutionClass::parse(name).ok_or_else(|| {
                    NodeError::BadRequest(format!("unknown execution class {name:?}"))
                })
            })
            .collect::<Result<_, _>>()?;
        let acknowledged: BTreeSet<AckCode> = request
            .ack
            .iter()
            .map(|code| {
                AckCode::parse(code)
                    .ok_or_else(|| NodeError::BadRequest(format!("unknown change code {code:?}")))
            })
            .collect::<Result<_, _>>()?;

        // Everything below runs beneath the writer queue. Ordinary commits
        // serialize against it, so the schema the plan is verified against is
        // the schema the transaction is appended onto.
        let _commit = state.commit.lock().await;
        if let Some(error) = state.keys_fenced_error() {
            return Err(error);
        }

        let mut cursor = state.transactor.batch_cursor();
        let db = cursor.db().clone();
        let installed = installed_schema(&db);
        let options = PlanOptions::new(request.db.clone()).with_prune(request.prune);
        let planned = plan_against(&desired, &db, &installed, &options)
            .map_err(|error| NodeError::SchemaUpdate(error.to_string()))?;

        let unchanged = |db: &Db, steps: u32| pb::AlterSchemaResponse {
            basis_t: db.basis_t(),
            schema_generation: db.schema_generation(),
            changed: false,
            installed_idents: Vec::new(),
            steps,
        };
        // Nothing to do is not a stale plan. Applying a change is exactly what
        // invalidates the digest that described it, so a re-run finds the
        // database already matching and must say so rather than refuse.
        if !planned.has_changes() {
            return Ok(unchanged(&db, 0));
        }

        // The digest first: a stale plan must never be partially validated.
        let current_digest = planned.digest();
        if request.plan_digest != current_digest {
            return Err(NodeError::StalePlan {
                submitted: request.plan_digest.clone(),
                current: current_digest,
            });
        }
        if !request.installed_fingerprint.is_empty()
            && request.installed_fingerprint != planned.installed_fingerprint
        {
            return Err(NodeError::StalePlan {
                submitted: request.plan_digest.clone(),
                current: current_digest,
            });
        }
        if let Some(step) = planned.blocked_steps().next() {
            return Err(NodeError::BlockedPlan(format!(
                "{} {}: {}",
                step.ident,
                step.summary,
                step.blocked.map_or_else(
                    || format!("{} changes cannot be executed", step.class),
                    |blocked| blocked.message().to_owned()
                )
            )));
        }
        if let Some(class) = planned
            .required_allowances()
            .into_iter()
            .find(|class| !allowed.contains(class))
        {
            return Err(NodeError::BlockedPlan(format!(
                "this plan requires --allow {class}"
            )));
        }
        if let Some(ack) = planned
            .required_acks()
            .into_iter()
            .find(|ack| !acknowledged.contains(ack))
        {
            return Err(NodeError::BlockedPlan(format!(
                "this plan requires --ack {ack}"
            )));
        }

        // Compile against a private copy of the interner: nothing is published
        // until the transaction is durable, so a failed compile leaves no
        // half-minted name behind.
        let mut interner = {
            let naming = state
                .naming
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            naming.interner.clone()
        };
        let tx = corium_core::EntityId::new(corium_core::Partition::Tx as u32, db.basis_t() + 1);
        let transaction = compile(&planned, &desired, &db, &installed, &mut interner, tx)
            .map_err(|error| NodeError::SchemaUpdate(error.to_string()))?;
        if transaction.is_empty() {
            return Ok(unchanged(&db, 0));
        }
        let steps = u32::try_from(planned.steps.len()).unwrap_or(u32::MAX);
        let installed_idents: Vec<String> = transaction
            .installed
            .iter()
            .map(|(ident, _)| ident.clone())
            .collect();

        let mut datoms = transaction.datoms;
        datoms.extend(audit_datoms(
            &planned,
            &AuditRecord {
                requester: requester.to_owned(),
                tool: request.tool.clone(),
            },
            tx,
        ));
        // The schema is derived from these datoms, and their keyword values
        // name keywords just minted, so the value they are applied against has
        // to carry the interner that knows them.
        cursor.intern_naming(interner.clone());
        let prepared = cursor
            .prepare_datoms(datoms, now_unix_ms())
            .map_err(|error| NodeError::Transact(error.into()))?;

        // Naming must be durable before the datoms that reference it: recovery
        // decodes the log against the metadata root, and this transaction's
        // values name keywords the root does not carry yet. Publishing it is
        // an unfenced write, so ownership is re-checked first — the same rule
        // the keyword-interning commit path follows.
        let (new_schema, new_idents) = {
            let after = prepared.db_after();
            (after.schema().clone(), after.idents().clone())
        };

        if let Err(error) = state.check_lease(self.store.as_ref()).await {
            if matches!(error, NodeError::Deposed(_)) {
                self.depose(&state, "write lease lost before schema metadata publish");
            }
            return Err(error);
        }
        let meta = codec::encode_metadata(&new_schema, &new_idents, &interner);
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
                Err(error) => return Err(NodeError::Store(error)),
            }
        }

        let record = prepared.record.clone();
        state.log.append_batch_async(&[record]).await?;
        let reports = state.transactor.install_batch(cursor, vec![prepared]);
        state
            .transactor
            .update_naming(new_idents.clone(), interner.clone());
        {
            let mut naming = state
                .naming
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            naming.schema = new_schema;
            naming.idents = new_idents;
            naming.interner = interner.clone();
        }

        // Post-append fence gates the acknowledgement and the peer stream, as
        // it does for an ordinary commit: a schema change is acked only if
        // ownership was intact after it became durable.
        if let Err(error) = state.check_lease(self.store.as_ref()).await {
            if matches!(error, NodeError::Deposed(_)) {
                self.depose(&state, "write lease lost after durable schema append");
            }
            return Err(error);
        }

        let mut basis_t = db.basis_t();
        let mut schema_generation = db.schema_generation();
        for report in reports {
            basis_t = report.db_after.basis_t();
            schema_generation = report.db_after.schema_generation();
            let encoded = codec::encode_datoms(&report.tx.datoms, &interner)?;
            let _ = state
                .broadcast
                .send(pb::subscribe_item::Item::Report(pb::TxReport {
                    t: basis_t,
                    tx_instant: report.tx_instant,
                    datoms: encoded,
                }));
        }
        let _ = state.basis.send(basis_t);
        Ok(pb::AlterSchemaResponse {
            basis_t,
            schema_generation,
            changed: true,
            installed_idents,
            steps,
        })
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
        // A saga branch is named for the saga it hosts, and is opened on
        // demand: whoever asks for it first — a step, a tier-2 reader — is
        // what builds the overlay (ADR-0023).
        if let Some((parent, saga)) = parse_branch_name(name) {
            return Box::pin(self.saga_branch(parent, saga)).await;
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

    /// Opens (creating on first use) the branch hosting saga `saga` of
    /// database `parent` — ADR-0023's overlay construction.
    ///
    /// The branch is the parent's value as of the saga's opening basis `t₀`
    /// with the branch's own log replayed on top, and its allocator points at
    /// the entity-id blocks the parent leased the saga. Everything it needs
    /// is durable registry data plus two logs, so this is equally the ordinary
    /// path (a saga's first step) and the recovery path (a step after the
    /// node restarted): opening is a deterministic function of the registry
    /// entry, and doing it twice yields the same branch.
    ///
    /// # Errors
    /// Returns [`NodeError::BadRequest`] when the saga is unknown, is not
    /// open, or its registry entry is missing the basis or the id blocks a
    /// branch cannot be built without; otherwise store or log failures.
    pub async fn saga_branch(&self, parent: &str, saga: u128) -> Result<Arc<DbState>, NodeError> {
        let name = branch_name(parent, saga);
        if let Some(state) = self
            .branches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&name)
            .cloned()
        {
            return Ok(state);
        }
        let _guard = self.branch_lock.lock().await;
        if let Some(state) = self
            .branches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&name)
            .cloned()
        {
            return Ok(state);
        }
        let state = self.open_branch(parent, saga, &name).await?;
        spawn_heartbeat(&state, self.config.heartbeat_interval);
        self.branches
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name, Arc::clone(&state));
        Ok(state)
    }

    async fn open_branch(
        &self,
        parent: &str,
        saga: u128,
        name: &str,
    ) -> Result<Arc<DbState>, NodeError> {
        let parent_state = self.db_state(parent).await?;
        let entry = corium_db::saga::entry(&parent_state.db(), saga).ok_or_else(|| {
            NodeError::BadRequest(format!("no saga {saga:032x} in {parent}'s registry"))
        })?;
        if !entry.is_open() {
            return Err(NodeError::BadRequest(format!(
                "saga {saga:032x} is {}; it has no live branch",
                entry
                    .status
                    .as_ref()
                    .map_or_else(|| "statusless".to_owned(), ToString::to_string)
            )));
        }
        let basis_t = u64::try_from(entry.basis_t.unwrap_or_default())
            .map_err(|_| NodeError::BadRequest(format!("saga {saga:032x} has no opening basis")))?;
        // Without a block the branch could not allocate a single entity, and
        // steps would fail one by one instead of the branch failing to open.
        let floor = entry
            .grants
            .iter()
            .filter(|grant| grant.partition == Some(i64::from(Partition::User as u32)))
            .filter_map(|grant| grant.start)
            .filter_map(|start| u64::try_from(start).ok())
            .min()
            .ok_or_else(|| {
                NodeError::BadRequest(format!(
                    "saga {saga:032x} holds no entity-id grants; its branch cannot allocate"
                ))
            })?;
        // A branch's naming is the parent's, copied when the branch is first
        // opened and durable from then on: a step may mint keyword names the
        // parent has never seen, and the branch's own log records cannot be
        // decoded without them.
        let stored_meta = self.store.get_root(&meta_root_name(name)).await?;
        let (schema, idents, interner) = if let Some(meta) = stored_meta {
            codec::decode_metadata(&meta)?
        } else {
            let (schema, idents, interner) = parent_state.naming_snapshot();
            let meta = codec::encode_metadata(&schema, &idents, &interner);
            self.store
                .cas_root(&meta_root_name(name), None, &meta)
                .await?;
            (schema, idents, interner)
        };
        let base = self
            .branch_base(&parent_state, basis_t, &schema, &idents, &interner)
            .await?;
        // The branch shares the parent's data key: same trust domain, and it
        // shares the parent's segments by construction.
        let crypto = parent_state.crypto();
        let held = parent_state.lease();
        // The branch's own log is an ordinary log numbered from one; rooting
        // it at `t₀` is what makes its first step `t₀ + 1` without copying a
        // byte of the parent's prefix.
        let log: Arc<dyn TransactionLog> = Arc::new(RootedLog::new(
            self.log_backend
                .open(name, held.version, crypto.cipher.clone())
                .await?,
            basis_t,
        ));
        let transactor = EmbeddedTransactor::recover_from_async(base, Arc::clone(&log)).await?;
        transactor.raise_allocation_floor(floor);
        let basis = transactor.db().basis_t();
        Ok(Arc::new(DbState {
            name: name.to_owned(),
            transactor,
            log,
            crypto: std::sync::RwLock::new(crypto),
            key_manifest_version: AtomicU64::new(
                parent_state.key_manifest_version.load(Ordering::Acquire),
            ),
            keys_unavailable: AtomicBool::new(false),
            keys_fenced: AtomicBool::new(false),
            fenced_active_epoch: AtomicU32::new(0),
            naming: Mutex::new(Naming {
                schema,
                idents,
                interner,
            }),
            commit: tokio::sync::Mutex::new(()),
            pending: Mutex::new(VecDeque::new()),
            broadcast: broadcast::channel(1024).0,
            basis: watch::channel(basis).0,
            index_basis: AtomicU64::new(0),
            index_policy: Mutex::new(IndexPolicy::from_config(&self.config)),
            held_lease: Mutex::new(held),
            deposed: AtomicBool::new(false),
            branch: Some(Branch {
                parent: parent_state,
                saga,
                basis_t,
            }),
        }))
    }

    /// Materializes the parent's value as of `basis_t`: the branch's base.
    ///
    /// This is the three-layer read of the design, and never more: the newest
    /// published parent root whose index basis is at or below `t₀`, plus the
    /// parent log records closing the gap to `t₀`. Both layers are frozen —
    /// the branch's base never moves — so what the branch adds on top is only
    /// ever its own log.
    ///
    /// Any missing or unreadable snapshot falls back to replaying the
    /// parent's log prefix, which is always correct because the log is the
    /// source of truth; it is also the honest answer once garbage collection
    /// has swept a `t₀`-era segment the published root no longer names.
    async fn branch_base(
        &self,
        parent: &Arc<DbState>,
        basis_t: u64,
        schema: &Schema,
        idents: &Idents,
        interner: &KeywordInterner,
    ) -> Result<Db, NodeError> {
        let root = self
            .store
            .get_root(&db_root_name(parent.name()))
            .await?
            .as_deref()
            .and_then(DbRoot::decode);
        let snapshot = match &root {
            Some(root)
                if root.index_basis_t <= basis_t
                    && root.next_entity_id != 0
                    && root.roots.is_some()
                    && root.history_roots.is_some() =>
            {
                let roots = root.roots.as_ref().expect("checked above");
                let history_roots = root.history_roots.as_ref().expect("checked above");
                match Self::load_history_snapshot(
                    parent.store().as_ref(),
                    root,
                    &roots[IndexOrder::Eavt as usize],
                    &history_roots[IndexOrder::Eavt as usize],
                    schema,
                    idents,
                    interner,
                )
                .await
                {
                    Ok(snapshot) => Some(snapshot),
                    Err(error) => {
                        tracing::warn!(
                            db = %parent.name(),
                            %error,
                            "branch base could not use the published root; replaying the log prefix"
                        );
                        None
                    }
                }
            }
            _ => None,
        };
        let mut db = snapshot.unwrap_or_else(|| {
            Db::new(schema.clone()).with_naming(idents.clone(), interner.clone())
        });
        let records = parent
            .log
            .tx_range_async(db.basis_t() + 1, Some(basis_t + 1))
            .await?;
        for record in records {
            db = db.with_transaction_at(record.t, record.tx_instant, &record.datoms);
        }
        Ok(db)
    }

    /// Stops hosting a saga's branch and removes its durable state.
    ///
    /// Discarding is what abort, expiry, and the end of a retention window
    /// each come to; it is idempotent, and reports whether the branch was
    /// there to discard.
    ///
    /// # Errors
    /// Returns an error when the branch's log or metadata cannot be removed.
    pub async fn discard_branch(&self, parent: &str, saga: u128) -> Result<bool, NodeError> {
        let name = branch_name(parent, saga);
        let _guard = self.branch_lock.lock().await;
        let hosted = self
            .branches
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&name);
        if let Some(state) = &hosted {
            state.deposed.store(true, Ordering::Release);
        }
        let had_log = self.log_backend.exists(&name).await;
        self.log_backend.delete_all(&name).await?;
        self.store.delete_root(&meta_root_name(&name)).await?;
        Ok(hosted.is_some() || had_log)
    }

    /// The branches this node currently hosts, by name.
    #[must_use]
    pub fn hosted_branches(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .branches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
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
    /// `storage_key` names the key-encryption key the database's data keys are
    /// wrapped under, enabling encryption at rest. It is fixed here and
    /// forever: a database created without one stays unencrypted, and turning
    /// encryption on later is a backup and restore into a new database.
    ///
    /// # Errors
    /// Returns an error for invalid names/schema, an unresolvable storage key,
    /// or store failures.
    pub async fn create_db(
        self: &Arc<Self>,
        name: &str,
        schema_edn: &[u8],
        storage_key: Option<KeyId>,
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
        // The manifest is written before the catalog entry: a crash between
        // them leaves an unreferenced key record, whereas the other order
        // would leave a catalogued database whose first write went out in the
        // clear.
        let encrypted = storage_key.is_some();
        if let Some(kek) = storage_key {
            self.create_key_manifest(name, kek).await?;
        }
        match self
            .store
            .cas_root(&meta_root_name(name), None, &meta)
            .await
        {
            Ok(()) => {}
            Err(StoreError::CasFailed { .. }) => {
                // Another node catalogued the name first; take our keys back
                // out so its manifest is the only one.
                if encrypted {
                    self.store.delete_root(&keys_root_name(name)).await?;
                }
                return Ok(false);
            }
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
        // A fork of an encrypted database is encrypted too, under its own
        // fresh data key: the log records below are re-sealed on the way in,
        // so the target shares no key material and no ciphertext with its
        // source, and its own KEK grant can be revoked independently.
        let target_crypto = match load_key_manifest(self.store.as_ref(), source).await? {
            Some(source_manifest) => {
                let manifest = self
                    .create_key_manifest(target, source_manifest.kek)
                    .await?;
                Some(
                    resolve_db_crypto(
                        target,
                        &self.store,
                        Some(&manifest),
                        self.config.keyring.as_ref(),
                    )
                    .await?,
                )
            }
            None => None,
        };
        let log = self
            .log_backend
            .open(
                target,
                0,
                target_crypto
                    .as_ref()
                    .and_then(|crypto| crypto.cipher.clone()),
            )
            .await?;
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
                // Another node claimed the name first; discard our log copy
                // and the keys we minted for it.
                self.log_backend.delete_all(target).await?;
                if target_crypto.is_some() {
                    self.store.delete_root(&keys_root_name(target)).await?;
                }
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
        // A branch has no life apart from its parent: deleting the database
        // deletes the overlays hosted on it, log and metadata alike.
        let branches: Vec<String> = self
            .branches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .filter(|hosted| parse_branch_name(hosted).is_some_and(|(parent, _)| parent == name))
            .cloned()
            .collect();
        for branch in branches {
            if let Some((parent, saga)) = parse_branch_name(&branch) {
                self.discard_branch(parent, saga).await?;
            }
        }
        self.store.delete_root(&db_root_name(name)).await?;
        self.store.delete_root(&meta_root_name(name)).await?;
        // The manifest goes with the database: leaving it would block
        // recreating the name, and its wrapped keys protect nothing once the
        // objects they encrypted are swept.
        self.store.delete_root(&keys_root_name(name)).await?;
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

    /// The store this node marks `db`'s reachable blobs through.
    ///
    /// A hosted database already holds one; a database this node only stands
    /// by for is resolved from its manifest, because garbage collection is a
    /// node-wide duty that must not skip a database merely because another
    /// process holds its lease.
    async fn gc_store(&self, db: &str) -> Result<Arc<DbStore>, NodeError> {
        if let Some(state) = self
            .dbs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(db)
            .cloned()
        {
            return Ok(state.store());
        }
        let manifest = load_key_manifest(self.store.as_ref(), db).await?;
        Ok(resolve_db_crypto(
            db,
            &self.store,
            manifest.as_ref(),
            self.config.keyring.as_ref(),
        )
        .await?
        .store)
    }

    /// Mints a database's first storage key and publishes its manifest.
    async fn create_key_manifest(&self, db: &str, kek: KeyId) -> Result<KeyManifest, NodeError> {
        let keyring = self.keyring()?;
        let manifest = KeyManifest::create(keyring.as_ref(), kek, now_unix_ms()).await?;
        match publish_key_manifest(self.store.as_ref(), db, None, &manifest).await {
            Ok(()) => Ok(manifest),
            Err(StoreError::CasFailed { .. }) => Err(NodeError::BadRequest(format!(
                "database {db:?} already has a key manifest; \
                 remove the stale {} root before recreating it",
                keys_root_name(db)
            ))),
            Err(error) => Err(error.into()),
        }
    }

    fn keyring(&self) -> Result<&Arc<dyn Keyring>, NodeError> {
        self.config.keyring.as_ref().ok_or_else(|| {
            NodeError::BadRequest(
                "this transactor holds no storage keys; start it with --storage-key".into(),
            )
        })
    }

    /// Reads a database's key manifest, current basis, and whether this node
    /// is actually operating on the keys the manifest names.
    ///
    /// # Errors
    /// Returns [`NodeError`] when the database is unknown or the manifest
    /// cannot be read.
    pub async fn key_status(&self, name: &str) -> Result<KeyStatus, NodeError> {
        let state = self.db_state(name).await?;
        Ok(KeyStatus {
            manifest: load_key_manifest(self.store.as_ref(), name).await?,
            basis_t: state.db().basis_t(),
            keys_unavailable: state.keys_unavailable(),
            keys_fenced: state.keys_fenced(),
        })
    }

    /// Opens a new storage-key epoch that new writes use immediately.
    ///
    /// Nothing already stored is rewritten: old epochs stay readable and drain
    /// through ordinary re-indexing. The rotation runs under the database's
    /// commit lock, so the basis it records as the new epoch's opening is one
    /// no concurrent transaction can move, and no record is sealed between the
    /// manifest write and the cipher swap.
    ///
    /// # Errors
    /// Returns [`NodeError`] when the database is unknown or unencrypted, when
    /// the manifest changed under the rotation, or when the new key cannot be
    /// wrapped.
    pub async fn rotate_storage_key(&self, name: &str) -> Result<u32, NodeError> {
        let state = self.db_state(name).await?;
        let keyring = Arc::clone(self.keyring()?);
        let _commit = state.commit.lock().await;
        let previous = self.require_key_manifest(name).await?;
        let mut manifest = previous.clone();
        let epoch = manifest
            .rotate_storage_key(keyring.as_ref(), now_unix_ms(), state.db().basis_t())
            .await?;
        self.install_key_manifest(&state, Some(&previous), &manifest)
            .await?;
        tracing::info!(db = %name, epoch, "opened a new storage-key epoch");
        Ok(epoch)
    }

    /// Re-wraps every storage key under `kek`, rewriting no data.
    ///
    /// # Errors
    /// Returns [`NodeError`] when the database is unknown or unencrypted, when
    /// either KEK cannot be resolved, or when the manifest changed underneath.
    pub async fn rewrap_keys(&self, name: &str, kek: KeyId) -> Result<(), NodeError> {
        let state = self.db_state(name).await?;
        let keyring = Arc::clone(self.keyring()?);
        let _commit = state.commit.lock().await;
        let previous = self.require_key_manifest(name).await?;
        let mut manifest = previous.clone();
        manifest.rewrap(keyring.as_ref(), kek.clone()).await?;
        self.install_key_manifest(&state, Some(&previous), &manifest)
            .await?;
        tracing::info!(db = %name, %kek, "re-wrapped storage keys under a new KEK");
        Ok(())
    }

    async fn require_key_manifest(&self, name: &str) -> Result<KeyManifest, NodeError> {
        load_key_manifest(self.store.as_ref(), name)
            .await?
            .filter(|manifest| !manifest.storage_keys.is_empty())
            .ok_or_else(|| {
                NodeError::BadRequest(format!(
                    "database {name:?} is not encrypted; encryption is fixed at creation \
                     (corium db create --storage-key)"
                ))
            })
    }

    /// Publishes a changed manifest, bumps the root's generation counter so
    /// other processes notice, and adopts the new keys here.
    ///
    /// The manifest is the durable record and goes first; the generation bump
    /// and the local swap follow. A crash between them leaves a manifest whose
    /// generation no root announces, which the next open resolves correctly
    /// because open reads the manifest itself.
    async fn install_key_manifest(
        &self,
        state: &Arc<DbState>,
        previous: Option<&KeyManifest>,
        manifest: &KeyManifest,
    ) -> Result<(), NodeError> {
        publish_key_manifest(self.store.as_ref(), &state.name, previous, manifest).await?;
        let version = self.bump_key_manifest_version(&state.name).await?;
        let crypto = state.crypto();
        let store =
            reload_db_crypto(&state.name, &crypto, manifest, self.config.keyring.as_ref()).await?;
        *state
            .crypto
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(DbCrypto {
            store,
            cipher: crypto.cipher.clone(),
        });
        state.key_manifest_version.store(version, Ordering::Release);
        // A rotation performed here supersedes whatever alarm an earlier
        // failed reload raised: these keys came from this manifest.
        if state.clear_key_alarms() {
            self.metrics.record_keys_available();
        }
        Ok(())
    }

    /// Increments `DbRoot::key_manifest_version`, the generation counter a
    /// running process watches to learn its key snapshot is stale.
    ///
    /// An index publication may install a new root between the read and the
    /// write, so the compare-and-set is retried against the newer root rather
    /// than failing the rotation whose manifest is already durable. The bump
    /// is a pure increment on whatever root is current, so re-reading loses
    /// nothing.
    async fn bump_key_manifest_version(&self, name: &str) -> Result<u64, NodeError> {
        const ATTEMPTS: usize = 5;
        let root_name = db_root_name(name);
        for attempt in 1..=ATTEMPTS {
            let stored = self.store.get_root(&root_name).await?;
            let mut root = stored
                .as_deref()
                .and_then(DbRoot::decode)
                .ok_or_else(|| NodeError::UnknownDb(name.to_owned()))?;
            root.key_manifest_version = root.key_manifest_version.saturating_add(1);
            match self
                .store
                .cas_root(&root_name, stored.as_deref(), &root.encode())
                .await
            {
                Ok(()) => return Ok(root.key_manifest_version),
                Err(StoreError::CasFailed { .. }) if attempt < ATTEMPTS => {}
                Err(error) => return Err(error.into()),
            }
        }
        unreachable!("the final attempt returns its result")
    }

    /// Reloads `db`'s keys when another process changed the manifest.
    ///
    /// Called from the maintenance loop, so a rotation or re-wrap performed
    /// elsewhere — by an operator against a standby, or by the other half of
    /// an HA pair — is picked up without a restart.
    ///
    /// A failure here is not automatically fatal, and which failure it is
    /// decides that. Re-wrapping leaves the data keys themselves untouched, so
    /// a node that cannot resolve the new KEK still holds correct material and
    /// still writes under the epoch the manifest calls active; refusing its
    /// writes would turn a KMS outage into an outage. A *rotation* it cannot
    /// load is different in kind: every record it seals from then on is drawn
    /// under a key the manifest has closed, and the nonce budget — measured as
    /// the span of `t` between epochs — silently stops counting them. The
    /// epoch comparison that separates the two needs no key at all.
    async fn refresh_keys_if_stale(&self, state: &Arc<DbState>) -> Result<(), NodeError> {
        let Some(root) = self
            .store
            .get_root(&db_root_name(&state.name))
            .await?
            .as_deref()
            .and_then(DbRoot::decode)
        else {
            return Ok(());
        };
        if root.key_manifest_version == state.key_manifest_version.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(manifest) = load_key_manifest(self.store.as_ref(), &state.name).await? else {
            return Ok(());
        };
        let crypto = state.crypto();
        match reload_db_crypto(
            &state.name,
            &crypto,
            &manifest,
            self.config.keyring.as_ref(),
        )
        .await
        {
            Ok(store) => {
                *state
                    .crypto
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(DbCrypto {
                    store,
                    cipher: crypto.cipher.clone(),
                });
                state
                    .key_manifest_version
                    .store(root.key_manifest_version, Ordering::Release);
                if state.clear_key_alarms() {
                    self.metrics.record_keys_available();
                }
                tracing::info!(
                    db = %state.name,
                    key_manifest_version = root.key_manifest_version,
                    "reloaded storage keys after a manifest change"
                );
                Ok(())
            }
            Err(error) => {
                self.raise_key_alarm(state, &crypto, &manifest, &error);
                Ok(())
            }
        }
    }

    /// Records a failed key reload, escalating only when this node would
    /// otherwise keep writing under an epoch the manifest has closed.
    ///
    /// The maintenance loop calls this every renewal tick while the condition
    /// lasts, so both the log lines and the metric are edge-triggered: an
    /// operator sees one line naming the problem, not one per tick.
    fn raise_key_alarm(
        &self,
        state: &Arc<DbState>,
        crypto: &DbCrypto,
        manifest: &KeyManifest,
        error: &KeyWiringError,
    ) {
        let writing = crypto.store.storage_epoch();
        let active = manifest.active_storage_epoch();
        let was_alarmed = state.key_alarm_raised();
        let fenced =
            matches!((active, writing), (Some(active), Some(writing)) if active != writing);
        if fenced {
            let active = active.unwrap_or_default();
            state.fenced_active_epoch.store(active, Ordering::Release);
            if !state.keys_fenced.swap(true, Ordering::AcqRel) {
                tracing::error!(
                    db = %state.name,
                    active_epoch = active,
                    writing_epoch = writing.unwrap_or_default(),
                    %error,
                    "storage keys are fenced: the manifest opened an epoch this node cannot \
                     load, so writes are refused until --storage-key resolves it"
                );
            }
        } else if !state.keys_unavailable.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                db = %state.name,
                %error,
                "cannot load the changed key manifest; continuing on the keys already held \
                 (they still open this database, and the epoch new writes use is unchanged)"
            );
        }
        if !was_alarmed {
            self.metrics.record_keys_unavailable();
        }
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
        // Marking reads blob *content* to find references, so each database's
        // roots are walked through that database's own store — an encrypted
        // one's manifests are ciphertext to everyone else. Sweeping is keyless:
        // it lists, stats, and deletes by id, so one marked set covers the
        // whole shared backend.
        let mut marked = std::collections::HashSet::new();
        for root_name in self.store.list_roots("db:").await? {
            let Some(root) = self
                .store
                .get_root(&root_name)
                .await?
                .as_deref()
                .and_then(DbRoot::decode)
            else {
                continue;
            };
            let Some(roots) = root.roots else {
                continue;
            };
            let db = root_name.strip_prefix("db:").unwrap_or(&root_name);
            let store = self.gc_store(db).await?;
            mark_reachable(
                store.as_ref(),
                roots,
                |_, bytes| corium_store::index_blob_children(bytes),
                &mut marked,
            )
            .await?;
            if let Some(history_roots) = root.history_roots {
                mark_reachable(
                    store.as_ref(),
                    history_roots,
                    |_, bytes| corium_store::index_blob_children(bytes),
                    &mut marked,
                )
                .await?;
            }
        }
        let report =
            sweep_unmarked(self.store.as_ref(), &marked, retention, SystemTime::now()).await?;
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
        self.transact_at(name, tx_data, None).await
    }

    /// Applies one transaction only if `expected_basis_t` is still current.
    ///
    /// An absent expectation preserves the ordinary Corium transaction
    /// behavior. Conditional callers receive [`NodeError::BasisMismatch`]
    /// before preparation or durability when another transaction won first.
    ///
    /// # Errors
    /// Returns [`NodeError`] for a stale basis, decode/validation failures,
    /// lease loss, or storage failures.
    pub async fn transact_at(
        &self,
        name: &str,
        tx_data: &[u8],
        expected_basis_t: Option<u64>,
    ) -> Result<pb::TransactResponse, NodeError> {
        let started = Instant::now();
        let result = self
            .transact_inner(name, tx_data, expected_basis_t)
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
        expected_basis_t: Option<u64>,
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
                expected_basis_t,
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
        // Refuse before preparing anything when this node's storage keys are
        // fenced. Unlike the ownership fence below, this is a local flag with
        // no round trip, and it has to come first: a record sealed under a
        // closed epoch is durable and uncounted the moment it is appended.
        if let Some(error) = state.keys_fenced_error() {
            for request in batch {
                let _ = request
                    .resp
                    .send(Err(batch_abort_error(&state.name, &error)));
            }
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
        // A branch's steps are bound by what its saga declared, read from
        // the parent's registry now rather than cached when the branch
        // opened: an unsealed saga may widen its reservation set with an
        // ordinary parent transaction while the branch runs (ADR-0023).
        let rules = match state.branch.as_ref().map(Branch::step_rules) {
            Some(Ok(rules)) => Some(rules),
            Some(Err(error)) => {
                for request in batch {
                    let _ = request
                        .resp
                        .send(Err(NodeError::BadRequest(error.to_string())));
                }
                return;
            }
            None => None,
        };
        let now_ms = now_unix_ms();
        let mut cursor = state.transactor.batch_cursor();
        let mut resps: Vec<oneshot::Sender<Result<pb::TransactResponse, NodeError>>> = Vec::new();
        let mut prepared: Vec<Prepared> = Vec::new();
        let mut batch_bytes: usize = 0;
        let mut measure = Vec::new();
        let mut naming_changed = false;
        while let Some(request) = batch.pop_front() {
            if let Some(expected) = request.expected_basis_t {
                let actual = cursor.db().basis_t();
                if actual != expected {
                    let _ = request
                        .resp
                        .send(Err(NodeError::BasisMismatch { expected, actual }));
                    continue;
                }
            }
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
            let (converted, minted) = {
                let mut naming = state
                    .naming
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let before = naming.interner.len();
                let converted = tx_items_from_edn(cursor.db(), &mut naming.interner, &forms);
                let minted = (naming.interner.len() > before).then(|| naming.interner.clone());
                (converted, minted)
            };
            let items = match converted {
                Ok(items) => items,
                Err(error) => {
                    let _ = request.resp.send(Err(error.into()));
                    continue;
                }
            };
            // Validation reads keyword *values* — a saga's status, say —
            // through the interner of the value being prepared against, so a
            // transaction that mints a keyword has to be prepared against a
            // value that already knows it. This is the same reason the
            // schema-update path interns before `prepare_datoms`.
            let this_changed = minted.is_some();
            if let Some(interner) = minted {
                cursor.intern_naming(interner);
            }
            let prepare = match &rules {
                Some(rules) => cursor.prepare_step(items, now_ms, rules),
                None => cursor.prepare(items, now_ms),
            };
            match prepare {
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
    pub async fn backup_info(&self, name: &str) -> Result<pb::GetStorageInfoResponse, NodeError> {
        let state = self.db_state(name).await?;
        // Serialize with the tiny commit critical section so the checkpoint
        // cannot observe a batch after its durable append but before its
        // ownership fence and acknowledgement decision.
        let basis_t = {
            let _commit = state.commit.lock().await;
            state.check_lease(self.store.as_ref()).await?;
            state.db().basis_t()
        };
        // Credential generation may call AWS STS. Do that after fixing the
        // checkpoint and releasing the commit lock so a slow identity service
        // never stalls transactions.
        let storage = self
            .config
            .store
            .connection_info(&self.config.data_dir, &self.config.storage_info)
            .await
            .map_err(NodeError::BadRequest)?;
        Ok(pb::GetStorageInfoResponse {
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
