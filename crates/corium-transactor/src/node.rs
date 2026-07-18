//! The transactor as a process: multi-database state, durable naming,
//! lease acquisition/renewal, background indexing, and tx-report fan-out.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use corium_core::{KeywordInterner, Schema};
use corium_db::{Db, Idents};
use corium_log::{FileLog, LogError, TransactionLog, TxRecord};
use corium_protocol::codec::{self, CodecError};
use corium_protocol::pb;
use corium_protocol::schemaform::{SchemaFormError, schema_from_edn};
use corium_protocol::txforms::{TxFormError, tx_items_from_edn};
use corium_query::edn::Edn;
use corium_store::{FsStore, RootStore, StoreError, mark_and_sweep};
use thiserror::Error;
use tokio::sync::{broadcast, watch};

use crate::lease::{self, Lease, LeaseError};
use crate::{DbRoot, EmbeddedTransactor, TransactError, db_root_name, publish_root};

/// Node process configuration.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Data directory holding the blob/root store and transaction logs.
    pub data_dir: PathBuf,
    /// Stable owner identity for lease records.
    pub owner: String,
    /// Lease time-to-live in milliseconds.
    pub lease_ttl_ms: i64,
    /// How long to wait for a held lease to expire before giving up.
    pub lease_wait_ms: i64,
    /// Interval between background index publications.
    pub index_interval: Duration,
    /// Interval between heartbeats on subscription streams.
    pub heartbeat_interval: Duration,
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
            index_interval: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(10),
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
    /// This node no longer holds the write lease.
    #[error("deposed: write lease for {0:?} is held elsewhere")]
    Deposed(String),
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
    log: Arc<FileLog>,
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

    fn check_lease(&self, store: &dyn RootStore) -> Result<Lease, NodeError> {
        if self.deposed.load(Ordering::Acquire) {
            return Err(NodeError::Deposed(self.name.clone()));
        }
        let held = self.lease();
        let stored = store.get_root(&lease::lease_root(&self.name))?;
        if stored.as_deref() == Some(held.encode().as_slice()) {
            Ok(held)
        } else {
            self.deposed.store(true, Ordering::Release);
            Err(NodeError::Deposed(self.name.clone()))
        }
    }
}

/// A running transactor node hosting every database under one data directory.
pub struct TransactorNode {
    config: NodeConfig,
    store: Arc<FsStore>,
    dbs: std::sync::RwLock<HashMap<String, Arc<DbState>>>,
    gc_lock: tokio::sync::Mutex<()>,
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
    pub fn open(config: NodeConfig) -> Result<Arc<Self>, NodeError> {
        let store = Arc::new(FsStore::open(config.data_dir.join("store"))?);
        let node = Arc::new(Self {
            config,
            store,
            dbs: std::sync::RwLock::new(HashMap::new()),
            gc_lock: tokio::sync::Mutex::new(()),
            shutdown: watch::channel(None).0,
        });
        let names: Vec<String> = node
            .store
            .list_roots("meta:")?
            .into_iter()
            .filter_map(|root| root.strip_prefix("meta:").map(str::to_owned))
            .collect();
        for name in names {
            let state = node.open_db(&name)?;
            node.dbs
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(name, state);
        }
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

    /// Watch channel that reports a shutdown reason when the node deposes.
    #[must_use]
    pub fn shutdown_watch(&self) -> watch::Receiver<Option<String>> {
        self.shutdown.subscribe()
    }

    fn depose(&self, state: &DbState, reason: &str) {
        state.deposed.store(true, Ordering::Release);
        let _ = self
            .shutdown
            .send(Some(format!("database {:?}: {reason}", state.name)));
    }

    fn log_path(&self, name: &str) -> PathBuf {
        self.config
            .data_dir
            .join("logs")
            .join(format!("{name}.log"))
    }

    fn acquire_with_wait(&self, name: &str) -> Result<Lease, NodeError> {
        let deadline = now_unix_ms() + self.config.lease_wait_ms;
        loop {
            match lease::acquire(
                self.store.as_ref(),
                name,
                &self.config.owner,
                self.config.lease_ttl_ms,
                now_unix_ms(),
            ) {
                Ok(held) => return Ok(held),
                Err(LeaseError::Held { .. }) if now_unix_ms() < deadline => {
                    std::thread::sleep(Duration::from_millis(200));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn open_db(self: &Arc<Self>, name: &str) -> Result<Arc<DbState>, NodeError> {
        let meta = self
            .store
            .get_root(&meta_root_name(name))?
            .ok_or_else(|| NodeError::UnknownDb(name.to_owned()))?;
        let (schema, idents, interner) = decode_meta(&meta)?;
        let held = self.acquire_with_wait(name)?;
        // Fence bump: ensure the db root carries our lease version so any
        // deposed writer's pending CAS fails and observes the new version.
        let root_name = db_root_name(name);
        let current = self
            .store
            .get_root(&root_name)?
            .as_deref()
            .and_then(DbRoot::decode);
        if current
            .as_ref()
            .is_none_or(|root| root.lease_version < held.version)
        {
            publish_root(
                self.store.as_ref(),
                &root_name,
                &DbRoot {
                    lease_version: held.version,
                    index_basis_t: current.as_ref().map_or(0, |root| root.index_basis_t),
                    roots: current.and_then(|root| root.roots),
                },
            )?;
        }
        let log = Arc::new(FileLog::open(self.log_path(name))?);
        let base = Db::new(schema.clone()).with_naming(idents.clone(), interner.clone());
        let transactor = EmbeddedTransactor::recover_from(base, Arc::clone(&log) as _)?;
        let basis_t = transactor.db().basis_t();
        let index_basis = self
            .store
            .get_root(&root_name)?
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
                let held = db.lease();
                let store = Arc::clone(&node.store);
                let name = db.name.clone();
                let renewed = tokio::task::spawn_blocking(move || {
                    lease::renew(store.as_ref(), &name, &held, ttl, now_unix_ms())
                })
                .await;
                match renewed {
                    Ok(Ok(renewed)) => {
                        *db.held_lease
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = renewed;
                    }
                    Ok(Err(LeaseError::Lost)) => {
                        node.depose(&db, "write lease lost");
                        return;
                    }
                    Ok(Err(_)) | Err(_) => {}
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
                let store = Arc::clone(&node.store);
                let version = db.lease().version;
                let root_name = db_root_name(&db.name);
                let worker = Arc::clone(&db);
                let published = tokio::task::spawn_blocking(move || {
                    worker
                        .transactor
                        .publish_indexes(store.as_ref(), &root_name, version)
                })
                .await;
                match published {
                    Ok(Ok(root)) => {
                        db.index_basis.store(root.index_basis_t, Ordering::Release);
                        let _ = db.broadcast.send(pb::subscribe_item::Item::IndexBasis(
                            pb::IndexBasis {
                                index_basis_t: root.index_basis_t,
                            },
                        ));
                    }
                    Ok(Err(TransactError::Deposed { .. })) => {
                        node.depose(&db, "database root fenced by a newer lease");
                        return;
                    }
                    Ok(Err(_)) | Err(_) => {}
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
    /// Returns [`NodeError::UnknownDb`] when absent.
    pub fn db_state(&self, name: &str) -> Result<Arc<DbState>, NodeError> {
        self.dbs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
            .ok_or_else(|| NodeError::UnknownDb(name.to_owned()))
    }

    /// Creates a database with the supplied EDN schema forms; returns
    /// `false` when it already exists.
    ///
    /// # Errors
    /// Returns an error for invalid names/schema or store failures.
    pub fn create_db(self: &Arc<Self>, name: &str, schema_edn: &[u8]) -> Result<bool, NodeError> {
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
        match self.store.cas_root(&meta_root_name(name), None, &meta) {
            Ok(()) => {}
            Err(StoreError::CasFailed { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        let state = self.open_db(name)?;
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
    pub fn delete_db(&self, name: &str) -> Result<bool, NodeError> {
        let Some(state) = self
            .dbs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name)
        else {
            return Ok(false);
        };
        state.deposed.store(true, Ordering::Release);
        let _ = lease::release(self.store.as_ref(), name, &state.lease());
        self.store.delete_root(&db_root_name(name))?;
        self.store.delete_root(&meta_root_name(name))?;
        self.store.delete_root(&lease::lease_root(name))?;
        match std::fs::remove_file(self.log_path(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NodeError::Store(StoreError::Io(error))),
        }
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
        let _gc = self.gc_lock.lock().await;
        let store = Arc::clone(&self.store);
        let swept = tokio::task::spawn_blocking(move || -> Result<u64, NodeError> {
            let mut live = Vec::new();
            for root_name in store.list_roots("db:")? {
                if let Some(root) = store
                    .get_root(&root_name)?
                    .as_deref()
                    .and_then(DbRoot::decode)
                {
                    if let Some(roots) = root.roots {
                        live.extend(roots);
                    }
                }
            }
            let report = mark_and_sweep(store.as_ref(), live, |_, _| Ok(Vec::new()))?;
            Ok(report.swept as u64)
        })
        .await
        .map_err(|error| NodeError::BadRequest(format!("gc task failed: {error}")))??;
        Ok(swept)
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
        let state = self.db_state(name)?;
        let decoded = codec::decode_edn(tx_data)?;
        let forms = decoded
            .as_seq()
            .ok_or_else(|| NodeError::BadRequest("tx-data must be a vector".into()))?
            .to_vec();
        let _commit = state.commit.lock().await;
        state.check_lease(self.store.as_ref())?;
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
                let current = self.store.get_root(&meta_root_name(name))?;
                match self
                    .store
                    .cas_root(&meta_root_name(name), current.as_deref(), &meta)
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
    pub fn status(&self, name: &str) -> Result<pb::StatusResponse, NodeError> {
        let state = self.db_state(name)?;
        let db = state.db();
        let counts = db.stats();
        let held = state.lease();
        Ok(pb::StatusResponse {
            basis_t: db.basis_t(),
            index_basis_t: state.index_basis(),
            lease_owner: held.owner,
            lease_version: held.version,
            lease_expires_unix_ms: held.expires_unix_ms,
            datom_count: counts.datoms as u64,
            entity_count: counts.entities as u64,
            attribute_count: counts.attributes as u64,
        })
    }

    /// Waits until the database basis reaches `t`, returning the basis seen.
    ///
    /// # Errors
    /// Returns [`NodeError::UnknownDb`] when absent.
    pub async fn sync(&self, name: &str, t: u64) -> Result<u64, NodeError> {
        let state = self.db_state(name)?;
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
