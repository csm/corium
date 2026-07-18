//! Peer library: remote connection to a transactor, tx-report handling,
//! local database values, sync, and the segment cache
//! (see `docs/architecture.md` and `docs/design/protocol.md`).
//!
//! A [`Connection`] subscribes to the transactor's tx-report stream and
//! folds every report into an immutable [`Db`] value locally; queries never
//! block on the transactor. On disconnect it reconnects and resubscribes
//! from its basis, and the server backfills the gap from the durable log.

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
    /// Protocol contract violation.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// The connection background task has stopped.
    #[error("connection closed")]
    Closed,
}

/// Connection configuration.
#[derive(Clone, Debug)]
pub struct ConnectConfig {
    /// Transactor endpoint, e.g. `http://127.0.0.1:4334`.
    pub endpoint: String,
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
}

impl ConnectConfig {
    /// Plaintext connection with default backoff.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, db: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            db: db.into(),
            token: None,
            tls: None,
            reconnect_min: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
        }
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

async fn open_channel(config: &ConnectConfig) -> Result<Channel, PeerError> {
    let mut endpoint = Endpoint::from_shared(config.endpoint.clone())
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
    /// Connects, subscribes from basis 0, and waits until the handshake and
    /// its backfill have been applied locally.
    ///
    /// # Errors
    /// Returns [`PeerError`] when the endpoint is unreachable or the
    /// subscription cannot be established.
    pub async fn connect(config: ConnectConfig) -> Result<Self, PeerError> {
        let channel = open_channel(&config).await?;
        let client = make_client(channel, config.token.clone());
        let inner = Arc::new(Inner {
            config,
            state: RwLock::new(None),
            basis: watch::channel(0).0,
            index_basis: watch::channel(0).0,
            reports: broadcast::channel(1024).0,
            client: Mutex::new(client.clone()),
        });
        // Establish the first subscription before returning so `db()` is
        // populated and connection errors surface synchronously.
        let mut stream = subscribe(&inner, 0).await?;
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
    /// # Errors
    /// Returns [`PeerError`] for rejected transactions or transport failure.
    pub async fn transact(&self, forms: Vec<Edn>) -> Result<TxResult, PeerError> {
        let response = self
            .transact_raw(codec::encode_edn(&Edn::Vector(forms)))
            .await?;
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

async fn subscribe(
    inner: &Arc<Inner>,
    from_basis_t: u64,
) -> Result<tonic::Streaming<pb::SubscribeItem>, PeerError> {
    let mut client = inner
        .client
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    Ok(client
        .subscribe(pb::SubscribeRequest {
            db: inner.config.db.clone(),
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
    let (schema, idents) = codec::decode_schema(&handshake.schema)?;
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

/// Long-running consume/reconnect loop: on stream end or error, rebuilds
/// the channel with exponential backoff and resubscribes from the local
/// basis; the server backfills the gap.
async fn run_loop(inner: Arc<Inner>, initial: Option<tonic::Streaming<pb::SubscribeItem>>) {
    let mut stream = initial;
    let mut backoff = inner.config.reconnect_min;
    loop {
        if let Some(active) = stream.as_mut() {
            match active.message().await {
                Ok(Some(item)) => {
                    backoff = inner.config.reconnect_min;
                    if let Some(item) = item.item {
                        if apply_item(&inner, item).is_err() {
                            stream = None;
                        }
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
        match open_channel(&inner.config).await {
            Ok(channel) => {
                let client = make_client(channel, inner.config.token.clone());
                *inner
                    .client
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = client;
            }
            Err(_) => continue,
        }
        let from = *inner.basis.subscribe().borrow();
        if let Ok(mut fresh) = subscribe(&inner, from).await {
            if pump_handshake(&inner, &mut fresh).await.is_ok() {
                stream = Some(fresh);
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
        let channel = open_channel(&config).await?;
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
        let response = self
            .client
            .gc_deleted_databases(pb::GcDeletedDatabasesRequest {})
            .await?;
        Ok(response.into_inner().swept_blobs)
    }
}
