//! The transactor as a process: multi-database state, durable naming,
//! lease acquisition/renewal, background indexing, tx-report fan-out, and
//! high-availability standby takeover.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use corium_core::{KeywordInterner, Schema};
use corium_db::{Db, Idents};
use corium_log::{LogError, TransactionLog, TxRecord, VersionedLog};
use corium_protocol::codec::{self, CodecError};
use corium_protocol::pb;
use corium_protocol::schemaform::{SchemaFormError, schema_from_edn};
use corium_protocol::txforms::{TxFormError, tx_items_from_edn};
use corium_query::edn::Edn;
use corium_store::{FsStore, RootStore, StoreError, mark_and_sweep_retained};
use thiserror::Error;
use tokio::sync::{broadcast, watch};
use tracing::Instrument;

use crate::lease::{self, Lease, LeaseError};
use crate::metrics::Metrics;
use crate::{DbRoot, EmbeddedTransactor, TransactError, db_root_name};

/// Expands user database-function invocations in boundary EDN transaction
/// forms before native conversion. Implemented by `corium-cljrs` (the
/// sandboxed Clojurust host, ADR-0008) and injected by the process wiring;
/// the transactor itself stays free of cljrs dependencies.
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
    /// Data directory holding the blob/root store and transaction logs.
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
    /// Interval between heartbeats on subscription streams.
    pub heartbeat_interval: Duration,
    /// Interval between scheduled garbage-collection duties; `None` disables it.
    pub gc_interval: Option<Duration>,
    /// Minimum age of an unreachable blob before scheduled/manual online GC.
    pub gc_retention: Duration,
    /// Optional database-function expander (`:db/fn` support).
    pub tx_fn_expander: Option<Arc<dyn TxFnExpander>>,
}

impl std::fmt::Debug for NodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeConfig")
            .field("data_dir", &self.data_dir)
            .field("owner", &self.owner)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .field("lease_wait_ms", &self.lease_wait_ms)
            .field("ha", &self.ha)
            .field("advertise", &self.advertise)
            .field("index_interval", &self.index_interval)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("gc_interval", &self.gc_interval)
            .field("gc_retention", &self.gc_retention)
            .field("tx_fn_expander", &self.tx_fn_expander.is_some())
            .finish()
    }
}

impl NodeConfig {
    /// Sensible defaults for a data directory.
    #[must_use]
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
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
            heartbeat_interval: Duration::from_secs(10),
            gc_interval: Some(Duration::from_secs(60 * 60)),
            gc_retention: Duration::from_secs(72 * 60 * 60),
            tx_fn_expander: None,
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
}

struct Naming {
    schema: Schema,
    idents: Idents,
    interner: KeywordInterner,
}

/// Per-database state hosted by a node.
pub struct DbState {
    name: String,
    transactor: EmbeddedTransactor,
    log: Arc<VersionedLog>,
    naming: Mutex<Naming>,
    commit: tokio::sync::Mutex<()>,
    broadcast: broadcast::Sender<pb::subscribe_item::Item>,
    basis: watch::Sender<u64>,
    index_basis: AtomicU64,
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
    pub fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, NodeError> {
        Ok(self.log.tx_range(start, end)?)
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
    store: Arc<FsStore>,
    dbs: std::sync::RwLock<HashMap<String, Arc<DbState>>>,
    /// Databases this node is standing by for (HA mode): the lease is held
    /// elsewhere and the standby poller attempts takeover on expiry.
    standby: std::sync::RwLock<BTreeSet<String>>,
    gc_lock: tokio::sync::Mutex<()>,
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

fn valid_db_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn meta_root_name(db: &str) -> String {
    format!("meta:{db}")
}

fn encode_meta(schema: &Schema, idents: &Idents, interner: &KeywordInterner) -> Vec<u8> {
    let schema_bytes = codec::encode_schema(schema, idents);
    let naming_bytes = codec::encode_naming(interner);
    let mut out = Vec::with_capacity(8 + schema_bytes.len() + naming_bytes.len());
    out.extend_from_slice(&u32::try_from(schema_bytes.len()).unwrap_or(0).to_be_bytes());
    out.extend_from_slice(&schema_bytes);
    out.extend_from_slice(&u32::try_from(naming_bytes.len()).unwrap_or(0).to_be_bytes());
    out.extend_from_slice(&naming_bytes);
    out
}

fn decode_meta(bytes: &[u8]) -> Result<(Schema, Idents, KeywordInterner), NodeError> {
    let take = |input: &mut &[u8]| -> Result<Vec<u8>, NodeError> {
        let len_bytes = input
            .get(..4)
            .ok_or(NodeError::Codec(CodecError::Truncated))?;
        let len = usize::try_from(u32::from_be_bytes(len_bytes.try_into().unwrap_or_default()))
            .map_err(|_| NodeError::Codec(CodecError::Length))?;
        let payload = input
            .get(4..4 + len)
            .ok_or(NodeError::Codec(CodecError::Truncated))?
            .to_vec();
        *input = &input[4 + len..];
        Ok(payload)
    };
    let mut input = bytes;
    let schema_bytes = take(&mut input)?;
    let naming_bytes = take(&mut input)?;
    let (schema, idents) = codec::decode_schema(&schema_bytes)?;
    let interner = codec::decode_naming(&naming_bytes)?;
    Ok((schema, idents, interner))
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
        let store = Arc::new(FsStore::open(config.data_dir.join("store"))?);
        let node = Arc::new(Self {
            config,
            store,
            dbs: std::sync::RwLock::new(HashMap::new()),
            standby: std::sync::RwLock::new(BTreeSet::new()),
            gc_lock: tokio::sync::Mutex::new(()),
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

    /// The node's data-directory store.
    #[must_use]
    pub fn store(&self) -> &Arc<FsStore> {
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
        let (schema, idents, interner) = decode_meta(&meta)?;
        let root_name = db_root_name(name);
        let current = self
            .store
            .get_root(&root_name)
            .await?
            .as_deref()
            .and_then(DbRoot::decode);
        if let Some(root) = &current {
            if root.format_version > corium_store::FORMAT_VERSION {
                return Err(NodeError::UnsupportedFormat {
                    found: root.format_version,
                    supported: corium_store::FORMAT_VERSION,
                });
            }
        }
        // Acquisition rewrites the root record under our lease version, so
        // it doubles as the fence bump: a deposed writer's pending root CAS
        // now has stale expected bytes and must fail.
        let held = self.acquire_lease(name).await?;
        // The log tail replay below happens strictly after the fence, so it
        // observes every record a previous owner could ever have acked.
        let log = Arc::new(VersionedLog::open(
            self.config.data_dir.join("logs"),
            name,
            held.version,
        )?);
        let base = Db::new(schema.clone()).with_naming(idents.clone(), interner.clone());
        let transactor = EmbeddedTransactor::recover_from(base, Arc::clone(&log) as _)?;
        let basis_t = transactor.db().basis_t();
        let index_basis = self
            .store
            .get_root(&root_name)
            .await?
            .as_deref()
            .and_then(DbRoot::decode)
            .map_or(0, |root| root.index_basis_t);
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
            broadcast: broadcast::channel(1024).0,
            basis: watch::channel(basis_t).0,
            index_basis: AtomicU64::new(index_basis),
            held_lease: Mutex::new(held),
            deposed: AtomicBool::new(false),
        });
        self.spawn_maintenance(&state);
        Ok(state)
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
        // Background indexing.
        let node = Arc::clone(self);
        let db = Arc::clone(state);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(node.config.index_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if db.deposed.load(Ordering::Acquire) {
                    return;
                }
                if db.db().basis_t() <= db.index_basis() {
                    continue;
                }
                let _gc = node.gc_lock.lock().await;
                let version = db.lease().version;
                let root_name = db_root_name(&db.name);
                let started = Instant::now();
                let published = db
                    .transactor
                    .publish_indexes(node.store.as_ref(), &root_name, version)
                    .await;
                node.metrics.record_index(started.elapsed());
                match published {
                    Ok(root) => {
                        tracing::debug!(db = %db.name, index_basis_t = root.index_basis_t, "published indexes");
                        db.index_basis.store(root.index_basis_t, Ordering::Release);
                        let _ = db.broadcast.send(pb::subscribe_item::Item::IndexBasis(
                            pb::IndexBasis {
                                index_basis_t: root.index_basis_t,
                            },
                        ));
                    }
                    Err(TransactError::Deposed { .. }) => {
                        node.depose(&db, "database root fenced by a newer lease");
                        return;
                    }
                    Err(_) => {}
                }
            }
        });
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
        let meta = encode_meta(&schema, &idents, &KeywordInterner::default());
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
        VersionedLog::delete_all(self.config.data_dir.join("logs"), name)?;
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
            {
                if let Some(roots) = root.roots {
                    live.extend(roots);
                }
            }
        }
        let report = mark_and_sweep_retained(
            self.store.as_ref(),
            live,
            |_, _| Ok(Vec::new()),
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
        let queued = self.metrics.queue_waiter();
        let commit = state.commit.lock().await;
        drop(queued);
        let _commit = commit;
        state.check_lease(self.store.as_ref()).await?;
        // Expand user database-function invocations against the
        // db-in-transaction (the value under the commit lock) before native
        // conversion. The expander blocks up to its budget deadline, so it
        // runs off the async workers.
        let forms = if let Some(expander) = &self.config.tx_fn_expander {
            let expander = Arc::clone(expander);
            let db = state.transactor.db();
            tokio::task::spawn_blocking(move || expander.expand(&db, forms))
                .await
                .map_err(|error| NodeError::BadRequest(format!("expander task failed: {error}")))?
                .map_err(NodeError::BadRequest)?
        } else {
            forms
        };
        // Convert forms, interning any new keyword values.
        let (items, naming_changed, idents, interner, schema) = {
            let mut naming = state
                .naming
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let db = state.transactor.db();
            let before = naming.interner.len();
            let items = tx_items_from_edn(&db, &mut naming.interner, &forms)?;
            (
                items,
                naming.interner.len() > before,
                naming.idents.clone(),
                naming.interner.clone(),
                naming.schema.clone(),
            )
        };
        if naming_changed {
            // New keyword names must be durable before the datoms that
            // reference them; recovery decodes the log against this meta.
            let meta = encode_meta(&schema, &idents, &interner);
            loop {
                let current = self.store.get_root(&meta_root_name(name)).await?;
                match self
                    .store
                    .cas_root(&meta_root_name(name), current.as_deref(), &meta)
                    .await
                {
                    Ok(()) => break,
                    Err(StoreError::CasFailed { .. }) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            state.transactor.update_naming(idents, interner.clone());
        }
        let worker = Arc::clone(&state);
        let report = tokio::task::spawn_blocking(move || worker.transactor.transact(items))
            .await
            .map_err(|error| NodeError::BadRequest(format!("transact task failed: {error}")))??;
        // Post-append fence: acknowledge only if ownership was intact after
        // the record became durable. A takeover that raced the append will
        // have replayed the log *after* rewriting the root record, so a
        // record we acked here is provably in the successor's replay, and a
        // record we refuse here lands in our version's log file where the
        // successor's cutoff discards it (see log-and-transactor.md).
        if let Err(error) = state.check_lease(self.store.as_ref()).await {
            if matches!(error, NodeError::Deposed(_)) {
                self.depose(&state, "write lease lost after durable append");
            }
            // Either way the transaction is not acknowledged; a transient
            // store failure here is ambiguous to the caller, exactly like a
            // crash between append and reply.
            return Err(error);
        }
        let t = report.db_after.basis_t();
        let datoms = codec::encode_datoms(&report.tx.datoms, &interner)?;
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
        let _ = state.basis.send(t);
        Ok(pb::TransactResponse {
            basis_before: report.db_before.basis_t(),
            basis_t: t,
            tx_instant: report.tx_instant,
            tempids,
            tx_data: datoms,
        })
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
