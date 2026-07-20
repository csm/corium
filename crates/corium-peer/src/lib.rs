//! Peer library: remote connection to a transactor, tx-report handling,
//! local database values, sync, and the segment cache
//! (see `docs/architecture.md` and `docs/design/protocol.md`).
//!
//! A [`Connection`] subscribes to the transactor's tx-report stream and
//! folds every report into an immutable [`Db`] value locally; queries never
//! block on the transactor. On disconnect it reconnects and resubscribes
//! from its basis, and the server backfills the gap from the durable log.

pub mod metrics;
pub mod segment;
pub mod server;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use corium_core::{Datom, EntityId, KeywordInterner, Schema};
use corium_db::{Db, Idents};
use corium_log::TxRecord;
use corium_protocol::auth::TokenInterceptor;
use corium_protocol::codec::{self, CodecError};
use corium_protocol::pb;
use corium_protocol::pb::catalog_client::CatalogClient;
use corium_protocol::pb::transactor_client::TransactorClient;
use corium_query::edn::Edn;
use thiserror::Error;
use tokio::sync::{broadcast, watch};
use tonic::Status;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::segment::{PeerStorage, SnapshotError, load_current_snapshot};

type Client = TransactorClient<InterceptedService<Channel, TokenInterceptor>>;

/// Peer failure.
#[derive(Debug, Error)]
pub enum PeerError {
    /// Transport-level failure.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// RPC failure.
    #[error(transparent)]
    Rpc(#[from] Status),
    /// Payload failed to decode.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// Published storage snapshot could not be loaded.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// Protocol contract violation.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// The connection background task has stopped.
    #[error("connection closed")]
    Closed,
}

/// Connection configuration.
#[derive(Clone)]
pub struct ConnectConfig {
    /// Candidate transactor endpoints in preference order, e.g.
    /// `http://127.0.0.1:4334`. With an HA pair, list the active and every
    /// standby: the connection sticks to whichever endpoint accepts its
    /// subscription and rotates through the rest on failure, so failover
    /// needs no reconfiguration.
    pub endpoints: Vec<String>,
    /// Database name.
    pub db: String,
    /// Optional bearer token.
    pub token: Option<String>,
    /// Optional TLS configuration (`https` endpoints).
    pub tls: Option<ClientTlsConfig>,
    /// Minimum reconnect backoff.
    pub reconnect_min: Duration,
    /// Maximum reconnect backoff.
    pub reconnect_max: Duration,
    /// How long [`Connection::transact`] keeps retrying failures that are
    /// provably pre-commit (standby/deposed rejections, connection
    /// establishment) while a failover completes. Ambiguous failures — a
    /// connection that died with the request in flight — are surfaced
    /// immediately; the transaction may or may not have committed.
    pub failover_timeout: Duration,
    /// Heartbeat-silence timeout override. `None` derives three times the
    /// server-advertised heartbeat interval; streams silent for longer are
    /// dropped and the connection fails over.
    pub heartbeat_timeout: Option<Duration>,
    /// Optional direct blob/root storage used to bootstrap from the newest
    /// published index before subscribing to the transaction-log tail.
    storage: Option<Arc<dyn PeerStorage>>,
}

impl std::fmt::Debug for ConnectConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectConfig")
            .field("endpoints", &self.endpoints)
            .field("db", &self.db)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("tls", &self.tls.is_some())
            .field("reconnect_min", &self.reconnect_min)
            .field("reconnect_max", &self.reconnect_max)
            .field("failover_timeout", &self.failover_timeout)
            .field("heartbeat_timeout", &self.heartbeat_timeout)
            .field("storage", &self.storage.is_some())
            .finish()
    }
}

impl ConnectConfig {
    /// Plaintext connection with default backoff.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, db: impl Into<String>) -> Self {
        Self::with_failover(vec![endpoint.into()], db)
    }

    /// Plaintext connection over an ordered endpoint candidate list.
    #[must_use]
    pub fn with_failover(endpoints: Vec<String>, db: impl Into<String>) -> Self {
        Self {
            endpoints,
            db: db.into(),
            token: None,
            tls: None,
            reconnect_min: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
            failover_timeout: Duration::from_secs(30),
            heartbeat_timeout: None,
            storage: None,
        }
    }

    /// Gives this peer direct read access to the transactor's blob/root
    /// storage service.
    ///
    /// A storage-aware connection loads the newest published EAVT snapshot
    /// and asks the transactor only for transactions after that snapshot.
    #[must_use]
    pub fn with_storage(mut self, storage: Arc<dyn PeerStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Whether direct peer storage has been configured.
    #[must_use]
    pub fn has_storage(&self) -> bool {
        self.storage.is_some()
    }
}

/// One transaction applied to the peer's local database value.
#[derive(Clone, Debug)]
pub struct PeerReport {
    /// Transaction number.
    pub t: u64,
    /// Commit timestamp (Unix milliseconds).
    pub tx_instant: i64,
    /// Datoms asserted/retracted by the transaction.
    pub datoms: Vec<Datom>,
    /// Database value including the transaction.
    pub db_after: Db,
}

/// Result of a transaction submitted through a peer.
#[derive(Clone, Debug)]
pub struct TxResult {
    /// Basis before the transaction.
    pub basis_before: u64,
    /// The transaction's `t`.
    pub basis_t: u64,
    /// Commit timestamp.
    pub tx_instant: i64,
    /// Tempid allocations.
    pub tempids: BTreeMap<String, EntityId>,
    /// Database value including the transaction.
    pub db_after: Db,
}

struct PeerState {
    schema: Schema,
    idents: Idents,
    interner: KeywordInterner,
    db: Db,
    instants: BTreeMap<u64, i64>,
}

struct Inner {
    config: ConnectConfig,
    state: RwLock<Option<PeerState>>,
    basis: watch::Sender<u64>,
    index_basis: watch::Sender<u64>,
    reports: broadcast::Sender<PeerReport>,
    client: Mutex<Client>,
    /// Index into `config.endpoints` of the endpoint currently serving the
    /// subscription (and the cached client).
    endpoint_index: std::sync::atomic::AtomicUsize,
    /// Server-advertised heartbeat interval (ms); 0 disables the
    /// heartbeat-silence timeout.
    heartbeat_ms: std::sync::atomic::AtomicU64,
}

impl Inner {
    /// Deadline of stream silence after which the transactor is presumed
    /// dead and the connection fails over.
    fn heartbeat_deadline(&self) -> Option<Duration> {
        if let Some(timeout) = self.config.heartbeat_timeout {
            return Some(timeout);
        }
        let advertised = self.heartbeat_ms.load(std::sync::atomic::Ordering::Relaxed);
        (advertised > 0).then(|| Duration::from_millis(advertised.saturating_mul(3)))
    }
}

/// A live peer connection to a transactor-hosted database.
pub struct Connection {
    inner: Arc<Inner>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn open_channel(config: &ConnectConfig, endpoint: &str) -> Result<Channel, PeerError> {
    let mut endpoint = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| PeerError::Protocol(format!("bad endpoint: {error}")))?
        .connect_timeout(Duration::from_secs(10));
    if let Some(tls) = &config.tls {
        endpoint = endpoint.tls_config(tls.clone())?;
    }
    Ok(endpoint.connect().await?)
}

fn make_client(channel: Channel, token: Option<String>) -> Client {
    TransactorClient::with_interceptor(channel, TokenInterceptor::new(token))
}

impl Connection {
    /// Connects and waits until the handshake and required log tail have
    /// been applied locally. With direct storage configured, the peer first
    /// loads the newest published index and subscribes from its basis;
    /// otherwise it subscribes from basis zero. Candidate endpoints are tried
    /// in order; a standby transactor rejects the subscription and the next
    /// candidate is tried.
    ///
    /// # Errors
    /// Returns [`PeerError`] when no endpoint accepts the subscription.
    pub async fn connect(config: ConnectConfig) -> Result<Self, PeerError> {
        if config.endpoints.is_empty() {
            return Err(PeerError::Protocol("no endpoints configured".into()));
        }
        let snapshot = match &config.storage {
            Some(storage) => load_current_snapshot(storage.as_ref(), &config.db).await?,
            None => None,
        };
        let start_basis = snapshot.as_ref().map_or(0, Db::basis_t);
        // Establish the first subscription before returning so `db()` is
        // populated and connection errors surface synchronously.
        let mut last_error = PeerError::Closed;
        let mut first = None;
        for (index, endpoint) in config.endpoints.iter().enumerate() {
            match open_channel(&config, endpoint).await {
                Ok(channel) => {
                    let mut client = make_client(channel, config.token.clone());
                    match subscribe_with(&mut client, &config.db, start_basis).await {
                        Ok(stream) => {
                            first = Some((index, client, stream));
                            break;
                        }
                        Err(error) => last_error = error,
                    }
                }
                Err(error) => last_error = error,
            }
        }
        let Some((index, client, mut stream)) = first else {
            return Err(last_error);
        };
        let initial_state = snapshot.map(|db| PeerState {
            schema: db.schema().clone(),
            idents: db.idents().clone(),
            interner: db.interner().clone(),
            db,
            instants: BTreeMap::new(),
        });
        let inner = Arc::new(Inner {
            state: RwLock::new(initial_state),
            basis: watch::channel(start_basis).0,
            index_basis: watch::channel(0).0,
            reports: broadcast::channel(1024).0,
            client: Mutex::new(client),
            endpoint_index: std::sync::atomic::AtomicUsize::new(index),
            heartbeat_ms: std::sync::atomic::AtomicU64::new(0),
            config,
        });
        let handshake_basis = pump_handshake(&inner, &mut stream).await?;
        drain_until(&inner, &mut stream, handshake_basis).await?;
        let task_inner = Arc::clone(&inner);
        let task = tokio::spawn(async move {
            run_loop(task_inner, Some(stream)).await;
        });
        Ok(Self { inner, task })
    }

    /// The connected database name.
    #[must_use]
    pub fn db_name(&self) -> &str {
        &self.inner.config.db
    }

    /// Returns the current local database value without blocking on the
    /// transactor.
    ///
    /// # Panics
    /// Panics if called before the initial handshake (impossible through
    /// [`Connection::connect`]).
    #[must_use]
    pub fn db(&self) -> Db {
        self.inner
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("connection is initialized")
            .db
            .clone()
    }

    /// Basis of the newest locally applied transaction.
    #[must_use]
    pub fn basis_t(&self) -> u64 {
        *self.inner.basis.subscribe().borrow()
    }

    /// Basis of the newest published durable index announced by the
    /// transactor.
    #[must_use]
    pub fn index_basis_t(&self) -> u64 {
        *self.inner.index_basis.subscribe().borrow()
    }

    /// Subscribes to reports applied after this call.
    #[must_use]
    pub fn tx_reports(&self) -> broadcast::Receiver<PeerReport> {
        self.inner.reports.subscribe()
    }

    /// Transaction instants recorded locally for `[start, end)`, paired
    /// with their datoms (the peer-side `tx-range`).
    #[must_use]
    pub fn tx_range(&self, start: u64, end: Option<u64>) -> Vec<TxRecord> {
        let guard = self
            .inner
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(state) = guard.as_ref() else {
            return Vec::new();
        };
        state
            .db
            .tx_range(start, end)
            .into_iter()
            .map(|(t, datoms)| TxRecord {
                t,
                tx_instant: state.instants.get(&t).copied().unwrap_or_default(),
                datoms,
            })
            .collect()
    }

    /// Submits a transaction (EDN transaction forms) and waits until it is
    /// applied locally, so a following [`Connection::db`] observes it.
    ///
    /// Failures that are provably pre-commit — a standby or deposed
    /// transactor rejecting the request, or a connection that could not be
    /// established — are retried until [`ConnectConfig::failover_timeout`],
    /// riding out an HA takeover. A connection that dies with the request
    /// in flight is ambiguous (the transaction may or may not have
    /// committed) and surfaces as an error, exactly like a transactor
    /// crash between durability and reply; callers decide whether to check
    /// and resubmit.
    ///
    /// # Errors
    /// Returns [`PeerError`] for rejected transactions or transport failure.
    pub async fn transact(&self, forms: Vec<Edn>) -> Result<TxResult, PeerError> {
        let tx_data = codec::encode_edn(&Edn::Vector(forms));
        let deadline = tokio::time::Instant::now() + self.inner.config.failover_timeout;
        let response = loop {
            match self.transact_raw(tx_data.clone()).await {
                Ok(response) => break response,
                Err(error) if retry_is_safe(&error) && tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(self.inner.config.reconnect_min).await;
                }
                Err(error) => return Err(error),
            }
        };
        let tempids = decode_tempids(&response.tempids)?;
        let db_after = self.sync_to(response.basis_t).await?;
        Ok(TxResult {
            basis_before: response.basis_before,
            basis_t: response.basis_t,
            tx_instant: response.tx_instant,
            tempids,
            db_after,
        })
    }

    /// Submits already-encoded transaction data, returning the raw wire
    /// response (used by the peer server's transact proxy).
    ///
    /// # Errors
    /// Returns [`PeerError`] for rejected transactions or transport failure.
    pub async fn transact_raw(&self, tx_data: Vec<u8>) -> Result<pb::TransactResponse, PeerError> {
        let request = pb::TransactRequest {
            db: self.inner.config.db.clone(),
            protocol_version: corium_protocol::PROTOCOL_VERSION,
            tx_data,
        };
        let mut client = self
            .inner
            .client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(client.transact(request).await?.into_inner())
    }

    /// Waits until the local basis reaches the transactor's current basis.
    ///
    /// # Errors
    /// Returns [`PeerError`] on transport failure.
    pub async fn sync(&self) -> Result<Db, PeerError> {
        let mut client = self
            .inner
            .client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let response = client
            .sync(pb::SyncRequest {
                db: self.inner.config.db.clone(),
                t: 0,
            })
            .await?
            .into_inner();
        self.sync_to(response.basis_t).await
    }

    /// Waits until the local basis reaches `t`, returning the database value.
    ///
    /// # Errors
    /// Returns [`PeerError::Closed`] if the connection task stops.
    pub async fn sync_to(&self, t: u64) -> Result<Db, PeerError> {
        let mut basis = self.inner.basis.subscribe();
        loop {
            if *basis.borrow() >= t {
                return Ok(self.db());
            }
            basis.changed().await.map_err(|_| PeerError::Closed)?;
        }
    }

    /// Transactor-side status for the connected database.
    ///
    /// # Errors
    /// Returns [`PeerError`] on transport failure.
    pub async fn status(&self) -> Result<pb::StatusResponse, PeerError> {
        let mut client = self
            .inner
            .client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(client
            .status(pb::StatusRequest {
                db: self.inner.config.db.clone(),
            })
            .await?
            .into_inner())
    }

    /// Opens an independent upstream subscription (used by the peer server
    /// to relay tx-report streams to thin clients).
    ///
    /// # Errors
    /// Returns [`PeerError`] on transport failure.
    pub async fn subscribe_raw(
        &self,
        from_basis_t: u64,
    ) -> Result<tonic::Streaming<pb::SubscribeItem>, PeerError> {
        let mut client = self
            .inner
            .client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(client
            .subscribe(pb::SubscribeRequest {
                db: self.inner.config.db.clone(),
                protocol_version: corium_protocol::PROTOCOL_VERSION,
                from_basis_t,
            })
            .await?
            .into_inner())
    }
}

/// Whether a transact failure is provably pre-commit and therefore safe to
/// retry without risking a duplicate transaction.
fn retry_is_safe(error: &PeerError) -> bool {
    match error {
        // The channel could not even be built; no request was sent.
        PeerError::Transport(_) => true,
        PeerError::Rpc(status) => match status.code() {
            // A standby (not lease holder) or freshly deposed transactor
            // refuses before reaching the commit point.
            tonic::Code::FailedPrecondition => {
                let message = status.message();
                message.contains("standby") || message.contains("deposed")
            }
            // Unavailable with a connect-phase source means the request was
            // never sent. Anything else (a connection that died mid-call)
            // is ambiguous and must surface.
            tonic::Code::Unavailable => status.message().contains("connect"),
            _ => false,
        },
        _ => false,
    }
}

fn decode_tempids(bytes: &[u8]) -> Result<BTreeMap<String, EntityId>, PeerError> {
    let Edn::Map(pairs) = codec::decode_edn(bytes)? else {
        return Err(PeerError::Protocol("tempids must be a map".into()));
    };
    let mut tempids = BTreeMap::new();
    for (key, value) in pairs {
        let (Edn::Str(name), Edn::Long(raw)) = (key, value) else {
            return Err(PeerError::Protocol("bad tempid entry".into()));
        };
        let raw = u64::try_from(raw).map_err(|_| PeerError::Protocol("bad entity id".into()))?;
        tempids.insert(name, EntityId::from_raw(raw));
    }
    Ok(tempids)
}

async fn subscribe_with(
    client: &mut Client,
    db: &str,
    from_basis_t: u64,
) -> Result<tonic::Streaming<pb::SubscribeItem>, PeerError> {
    Ok(client
        .subscribe(pb::SubscribeRequest {
            db: db.to_owned(),
            protocol_version: corium_protocol::PROTOCOL_VERSION,
            from_basis_t,
        })
        .await?
        .into_inner())
}

/// Consumes the stream's handshake, installing schema/naming, and returns
/// the server basis at subscription time.
async fn pump_handshake(
    inner: &Arc<Inner>,
    stream: &mut tonic::Streaming<pb::SubscribeItem>,
) -> Result<u64, PeerError> {
    let first = stream
        .message()
        .await?
        .and_then(|item| item.item)
        .ok_or_else(|| PeerError::Protocol("subscription ended before handshake".into()))?;
    let pb::subscribe_item::Item::Handshake(handshake) = first else {
        return Err(PeerError::Protocol(
            "subscription must begin with a handshake".into(),
        ));
    };
    let local_basis = *inner.basis.subscribe().borrow();
    if handshake.basis_t < local_basis {
        return Err(PeerError::Protocol(format!(
            "published snapshot basis {local_basis} is newer than transactor basis {}",
            handshake.basis_t
        )));
    }
    let (schema, idents) = codec::decode_schema(&handshake.schema)?;
    inner.heartbeat_ms.store(
        handshake.heartbeat_interval_ms,
        std::sync::atomic::Ordering::Relaxed,
    );
    {
        let mut guard = inner
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(state) = guard.as_mut() {
            // Reconnect: schema/idents are fixed after bootstrap; keep the
            // locally accumulated database value and naming.
            state.schema = schema;
            state.idents = idents;
        } else {
            let interner = KeywordInterner::default();
            let db = Db::new(schema.clone()).with_naming(idents.clone(), interner.clone());
            *guard = Some(PeerState {
                schema,
                idents,
                interner,
                db,
                instants: BTreeMap::new(),
            });
        }
    }
    let _ = inner.index_basis.send_replace(handshake.index_basis_t);
    Ok(handshake.basis_t)
}

/// Applies stream items until the local basis reaches `target`.
async fn drain_until(
    inner: &Arc<Inner>,
    stream: &mut tonic::Streaming<pb::SubscribeItem>,
    target: u64,
) -> Result<(), PeerError> {
    while *inner.basis.subscribe().borrow() < target {
        let Some(item) = stream.message().await? else {
            return Err(PeerError::Protocol(
                "subscription ended during backfill".into(),
            ));
        };
        if let Some(item) = item.item {
            apply_item(inner, item)?;
        }
    }
    Ok(())
}

fn apply_item(inner: &Arc<Inner>, item: pb::subscribe_item::Item) -> Result<(), PeerError> {
    match item {
        pb::subscribe_item::Item::Report(report) => {
            let peer_report = {
                let mut guard = inner
                    .state
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let state = guard
                    .as_mut()
                    .ok_or_else(|| PeerError::Protocol("report before handshake".into()))?;
                if report.t <= state.db.basis_t() {
                    return Ok(());
                }
                let before = state.interner.len();
                let datoms = codec::decode_datoms(&report.datoms, &mut state.interner)?;
                if state.interner.len() > before {
                    state.db = state
                        .db
                        .clone()
                        .with_naming(state.idents.clone(), state.interner.clone());
                }
                state.db = state.db.with_transaction(report.t, &datoms);
                state.instants.insert(report.t, report.tx_instant);
                PeerReport {
                    t: report.t,
                    tx_instant: report.tx_instant,
                    datoms,
                    db_after: state.db.clone(),
                }
            };
            let _ = inner.basis.send_replace(peer_report.t);
            let _ = inner.reports.send(peer_report);
        }
        pb::subscribe_item::Item::IndexBasis(index) => {
            let _ = inner.index_basis.send_replace(index.index_basis_t);
        }
        pb::subscribe_item::Item::Heartbeat(_) | pb::subscribe_item::Item::Handshake(_) => {}
    }
    Ok(())
}

/// Long-running consume/reconnect loop: on stream end, error, or heartbeat
/// silence past the timeout, rebuilds the channel with exponential backoff
/// and resubscribes from the local basis; the server backfills the gap.
/// Reconnection rotates through the candidate endpoints, so when the
/// active transactor dies the loop lands on whichever standby takes over
/// the lease (a standby rejects the subscription until then).
async fn run_loop(inner: Arc<Inner>, initial: Option<tonic::Streaming<pb::SubscribeItem>>) {
    let mut stream = initial;
    let mut backoff = inner.config.reconnect_min;
    let mut candidate = inner
        .endpoint_index
        .load(std::sync::atomic::Ordering::Relaxed);
    loop {
        if let Some(active) = stream.as_mut() {
            let next = match inner.heartbeat_deadline() {
                Some(deadline) => match tokio::time::timeout(deadline, active.message()).await {
                    Ok(next) => next,
                    // Heartbeat silence: the transactor is presumed dead
                    // even though the transport has not noticed (partition,
                    // stalled process); drop the stream and fail over.
                    Err(_elapsed) => Ok(None),
                },
                None => active.message().await,
            };
            match next {
                Ok(Some(item)) => {
                    backoff = inner.config.reconnect_min;
                    if let Some(item) = item.item
                        && apply_item(&inner, item).is_err()
                    {
                        stream = None;
                    }
                    continue;
                }
                Ok(None) | Err(_) => {
                    stream = None;
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(inner.config.reconnect_max);
        let endpoints = &inner.config.endpoints;
        let endpoint = &endpoints[candidate % endpoints.len()];
        let Ok(channel) = open_channel(&inner.config, endpoint).await else {
            candidate = (candidate + 1) % endpoints.len();
            continue;
        };
        let mut client = make_client(channel, inner.config.token.clone());
        let from = *inner.basis.subscribe().borrow();
        match subscribe_with(&mut client, &inner.config.db, from).await {
            Ok(mut fresh) => {
                if pump_handshake(&inner, &mut fresh).await.is_ok() {
                    // Sticky success: transact/status/sync now go here too.
                    *inner
                        .client
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = client;
                    inner.endpoint_index.store(
                        candidate % endpoints.len(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    stream = Some(fresh);
                } else {
                    candidate = (candidate + 1) % endpoints.len();
                }
            }
            Err(_) => {
                candidate = (candidate + 1) % endpoints.len();
            }
        }
    }
}

/// Catalog (admin) client for a transactor endpoint.
pub struct Admin {
    client: CatalogClient<InterceptedService<Channel, TokenInterceptor>>,
}

impl Admin {
    /// Connects to a transactor's catalog service.
    ///
    /// # Errors
    /// Returns [`PeerError`] when the endpoint is unreachable.
    pub async fn connect(
        endpoint: &str,
        token: Option<String>,
        tls: Option<ClientTlsConfig>,
    ) -> Result<Self, PeerError> {
        let config = ConnectConfig {
            tls,
            token: token.clone(),
            ..ConnectConfig::new(endpoint, "")
        };
        let channel = open_channel(&config, endpoint).await?;
        Ok(Self {
            client: CatalogClient::with_interceptor(channel, TokenInterceptor::new(token)),
        })
    }

    /// Creates a database with EDN schema forms; `false` when it existed.
    ///
    /// # Errors
    /// Returns [`PeerError`] for invalid schema or transport failure.
    pub async fn create_database(&mut self, db: &str, schema: &[Edn]) -> Result<bool, PeerError> {
        let response = self
            .client
            .create_database(pb::CreateDatabaseRequest {
                db: db.to_owned(),
                schema: codec::encode_edn(&Edn::Vector(schema.to_vec())),
            })
            .await?;
        Ok(response.into_inner().created)
    }

    /// Deletes a database; `false` when it did not exist.
    ///
    /// # Errors
    /// Returns [`PeerError`] on transport failure.
    pub async fn delete_database(&mut self, db: &str) -> Result<bool, PeerError> {
        let response = self
            .client
            .delete_database(pb::DeleteDatabaseRequest { db: db.to_owned() })
            .await?;
        Ok(response.into_inner().deleted)
    }

    /// Lists hosted databases.
    ///
    /// # Errors
    /// Returns [`PeerError`] on transport failure.
    pub async fn list_databases(&mut self) -> Result<Vec<String>, PeerError> {
        let response = self
            .client
            .list_databases(pb::ListDatabasesRequest {})
            .await?;
        Ok(response.into_inner().dbs)
    }

    /// Sweeps blobs unreachable from every live database root.
    ///
    /// # Errors
    /// Returns [`PeerError`] on transport failure.
    pub async fn gc_deleted_databases(&mut self) -> Result<u64, PeerError> {
        self.gc_deleted_databases_with_retention(None).await
    }

    /// Sweeps unreachable blobs with an optional minimum retention window.
    ///
    /// # Errors
    /// Returns [`PeerError`] on transport failure.
    pub async fn gc_deleted_databases_with_retention(
        &mut self,
        retention: Option<Duration>,
    ) -> Result<u64, PeerError> {
        let response = self
            .client
            .gc_deleted_databases(pb::GcDeletedDatabasesRequest {
                retention_millis: retention.map(duration_millis),
            })
            .await?;
        Ok(response.into_inner().swept_blobs)
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_retention_wire_value_preserves_zero_and_subseconds() {
        assert_eq!(duration_millis(Duration::ZERO), 0);
        assert_eq!(duration_millis(Duration::from_millis(500)), 500);
    }
}
