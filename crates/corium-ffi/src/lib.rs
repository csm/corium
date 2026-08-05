//! Owned, runtime-neutral facade for native language adapters.
//!
//! This crate keeps `PyO3` and other language runtimes out of Corium's core
//! crates. It exposes opaque peer/database handles, owned composite-protocol
//! payloads, and a stable error taxonomy. Dropping a future cancels the
//! caller's wait; [`PeerHandle::close`] deterministically prevents further
//! operations and releases the live peer held by the facade.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

#[cfg(feature = "s3")]
use aws_credential_types::Credentials;
#[cfg(feature = "s3")]
use aws_credential_types::provider::{
    ProvideCredentials, SharedCredentialsProvider, error::CredentialsError,
    future::ProvideCredentials as ProvideCredentialsFuture,
};
use corium_client::{
    ClientError, Db, LocalPeer, Peer, RemotePeer, ResultShape as ClientResultShape, TxData,
};
use corium_peer::segment::PeerStorage;
use corium_peer::{Admin, ConnectConfig, DiscoveredPeerStorage, PeerError, SegmentCacheConfig};
use corium_protocol::codec;
use corium_protocol::pb;
use corium_query::QueryError;
use corium_query::edn::Edn;
use corium_store::{
    DiscoveredStoreSpec, StorageConnectionError,
    available_storage_backends as registered_storage_backends,
};
use thiserror::Error;
use tonic::transport::ClientTlsConfig;
use tonic::{Code, Status};

/// Stable categories that language adapters map to native exception classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// A peer or server could not be reached.
    Connection,
    /// Credentials were missing or rejected.
    Authentication,
    /// The caller is authenticated but not authorized.
    PermissionDenied,
    /// The peer violated or rejected the protocol contract.
    Protocol,
    /// Query parsing or execution failed.
    Query,
    /// Transaction validation or commit failed.
    Transaction,
    /// A boundary value could not be decoded.
    Decode,
    /// Direct storage access failed.
    Storage,
    /// Query execution consumed its fuel budget.
    FuelExhausted,
    /// The facade handle was explicitly closed.
    Closed,
}

/// A language-neutral facade error.
#[derive(Clone, Debug, Error)]
#[error("{message}")]
pub struct FfiError {
    kind: ErrorKind,
    message: String,
    grpc_code: Option<i32>,
}

impl FfiError {
    /// The stable error category.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// An owned diagnostic message.
    #[must_use]
    pub fn message(&self) -> String {
        self.message.clone()
    }

    /// The numeric gRPC status code, when the failure came from an RPC.
    #[must_use]
    pub fn grpc_code(&self) -> Option<i32> {
        self.grpc_code
    }

    fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            grpc_code: None,
        }
    }

    fn from_status(status: &Status) -> Self {
        let kind = match status.code() {
            Code::Unauthenticated => ErrorKind::Authentication,
            Code::PermissionDenied => ErrorKind::PermissionDenied,
            Code::Unavailable | Code::DeadlineExceeded => ErrorKind::Connection,
            Code::ResourceExhausted => ErrorKind::FuelExhausted,
            Code::InvalidArgument => ErrorKind::Query,
            Code::FailedPrecondition | Code::Aborted => ErrorKind::Transaction,
            _ => ErrorKind::Protocol,
        };
        Self {
            kind,
            message: status.to_string(),
            grpc_code: Some(status.code() as i32),
        }
    }

    fn from_client(error: ClientError) -> Self {
        match error {
            ClientError::Peer(PeerError::Transport(error)) => {
                Self::new(ErrorKind::Connection, error.to_string())
            }
            ClientError::Peer(PeerError::Rpc(status)) | ClientError::Rpc(status) => {
                Self::from_status(&status)
            }
            ClientError::Peer(PeerError::Codec(error)) | ClientError::Codec(error) => {
                Self::new(ErrorKind::Decode, error.to_string())
            }
            ClientError::Peer(PeerError::Snapshot(error)) => {
                Self::new(ErrorKind::Storage, error.to_string())
            }
            ClientError::Peer(PeerError::Protocol(message)) | ClientError::Protocol(message) => {
                Self::new(ErrorKind::Protocol, message)
            }
            ClientError::Peer(PeerError::Closed) => {
                Self::new(ErrorKind::Closed, "connection closed")
            }
            // Sealing failures happen while the transaction is still being
            // built, so they are transaction failures, not connection ones.
            ClientError::Peer(
                error
                @ (PeerError::TxForm(_) | PeerError::MissingKey(_) | PeerError::Protection(_)),
            ) => Self::new(ErrorKind::Transaction, error.to_string()),
            ClientError::Query(QueryError::FuelExhausted) => {
                Self::new(ErrorKind::FuelExhausted, "query fuel exhausted")
            }
            ClientError::Query(error) => Self::new(ErrorKind::Query, error.to_string()),
            ClientError::Transport(error) => Self::new(ErrorKind::Connection, error.to_string()),
            ClientError::Decode(message) => Self::new(ErrorKind::Decode, message),
        }
    }
}

/// One validated, owned Corium composite-protocol value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompositeValue(Edn);

impl CompositeValue {
    /// Validates and owns one standalone composite value.
    ///
    /// [`Self::to_bytes`] canonically re-encodes the decoded value, so its
    /// output may differ from a valid but non-canonical input byte sequence.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Decode`] for malformed or trailing bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FfiError> {
        codec::decode_edn(bytes)
            .map(Self)
            .map_err(|error| FfiError::new(ErrorKind::Decode, error.to_string()))
    }

    /// Copies the encoded bytes for a language adapter.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        codec::encode_edn(&self.0)
    }

    fn from_edn(value: &Edn) -> Self {
        Self(value.clone())
    }

    fn decode(&self) -> Edn {
        self.0.clone()
    }
}

/// Runtime-neutral client TLS configuration for a language adapter.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientTlsOptions {
    /// Optional PEM certificate authority in addition to platform roots.
    pub ca_certificate: Option<Vec<u8>>,
    /// Optional certificate DNS name override.
    pub domain_name: Option<String>,
}

impl ClientTlsOptions {
    fn into_config(self) -> ClientTlsConfig {
        let mut config = ClientTlsConfig::new().with_enabled_roots();
        if let Some(certificate) = self.ca_certificate {
            config = config.ca_certificate(tonic::transport::Certificate::from_pem(certificate));
        }
        if let Some(domain_name) = self.domain_name {
            config = config.domain_name(domain_name);
        }
        config
    }
}

/// Bounded local segment-cache settings for a direct-storage peer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentCacheOptions {
    /// Dedicated directory owned by this peer process.
    pub directory: PathBuf,
    /// Maximum accounted bytes in the disk tier.
    pub capacity_bytes: u64,
    /// Maximum accounted bytes in the in-process tier.
    pub memory_capacity_bytes: u64,
}

impl SegmentCacheOptions {
    /// Default size of the in-process cache tier.
    pub const DEFAULT_MEMORY_CAPACITY_BYTES: u64 =
        SegmentCacheConfig::DEFAULT_MEMORY_CAPACITY_BYTES;
}

/// Direct-storage bootstrap discovered from the transactor.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DirectStorageOptions {
    /// Optional bounded disk/memory cache in front of authoritative storage.
    pub segment_cache: Option<SegmentCacheOptions>,
}

/// Options for an in-process full peer.
#[derive(Clone, Eq, PartialEq)]
pub struct LocalConnectOptions {
    /// Ordered transactor endpoints.
    pub endpoints: Vec<String>,
    /// Database name.
    pub database_name: String,
    /// Optional bearer token.
    pub token: Option<String>,
    /// TLS configuration for the endpoints, or plaintext when absent.
    pub tls: Option<ClientTlsOptions>,
    /// Discover and open the transactor's storage backend directly.
    pub direct_storage: Option<DirectStorageOptions>,
}

/// Options for a lightweight peer-server client.
#[derive(Clone, Eq, PartialEq)]
pub struct RemoteConnectOptions {
    /// Peer-server endpoint.
    pub endpoint: String,
    /// Database name.
    pub database_name: String,
    /// Optional bearer token.
    pub token: Option<String>,
    /// TLS configuration for the endpoint, or plaintext when absent.
    pub tls: Option<ClientTlsOptions>,
}

impl fmt::Debug for LocalConnectOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalConnectOptions")
            .field("endpoints", &self.endpoints)
            .field("database_name", &self.database_name)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("tls", &self.tls.is_some())
            .field("direct_storage", &self.direct_storage)
            .finish()
    }
}

impl fmt::Debug for RemoteConnectOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteConnectOptions")
            .field("endpoint", &self.endpoint)
            .field("database_name", &self.database_name)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("tls", &self.tls.is_some())
            .finish()
    }
}

fn register_static_storage_backends() {
    static REGISTER: std::sync::Once = std::sync::Once::new();
    REGISTER.call_once(|| {
        #[cfg(feature = "postgres")]
        let _ = corium_store_postgres::register();
        #[cfg(feature = "turso")]
        let _ = corium_store_turso::register();
        #[cfg(feature = "s3")]
        let _ = corium_store_s3::register();
    });
}

/// Direct-storage backends currently registered in this process.
#[must_use]
pub fn available_storage_backends() -> Vec<String> {
    register_static_storage_backends();
    registered_storage_backends()
}

/// Loads and registers a dynamic storage plugin by explicit path.
///
/// # Errors
/// Returns a storage error for load, ABI, or duplicate-kind failures.
pub fn load_storage_plugin(path: impl AsRef<std::path::Path>) -> Result<Vec<String>, FfiError> {
    corium_store_plugin::load_storage_plugin(path)
        .map(|plugin| plugin.backends)
        .map_err(storage_error)
}

fn peer_error(error: PeerError) -> FfiError {
    FfiError::from_client(ClientError::Peer(error))
}

fn storage_error(error: impl fmt::Display) -> FfiError {
    FfiError::new(ErrorKind::Storage, error.to_string())
}

struct StorageDiscovery {
    endpoints: Vec<String>,
    database: String,
    token: Option<String>,
    tls: Option<ClientTlsOptions>,
}

async fn discover_storage(
    endpoints: &[String],
    database: &str,
    token: Option<String>,
    tls: Option<ClientTlsOptions>,
) -> Result<Arc<dyn PeerStorage>, FfiError> {
    let discovery = StorageDiscovery {
        endpoints: endpoints.to_vec(),
        database: database.to_owned(),
        token,
        tls,
    };
    let storage = fetch_storage_connection(
        &discovery.endpoints,
        &discovery.database,
        discovery.token.clone(),
        discovery.tls.clone(),
    )
    .await?;
    open_discovered_storage(storage, discovery).await
}

async fn fetch_storage_connection(
    endpoints: &[String],
    database: &str,
    token: Option<String>,
    tls: Option<ClientTlsOptions>,
) -> Result<pb::StorageConnection, FfiError> {
    let mut last_error = FfiError::new(
        ErrorKind::Connection,
        "no transactor endpoint accepted storage discovery",
    );
    for endpoint in endpoints {
        let mut admin = match Admin::connect(
            endpoint,
            token.clone(),
            tls.clone().map(ClientTlsOptions::into_config),
        )
        .await
        {
            Ok(admin) => admin,
            Err(error) => {
                last_error = peer_error(error);
                continue;
            }
        };
        match admin.get_storage_info(database).await {
            Ok(info) => {
                return info.storage.ok_or_else(|| {
                    FfiError::new(
                        ErrorKind::Storage,
                        "transactor returned no direct-storage backend",
                    )
                });
            }
            Err(error) => last_error = peer_error(error),
        }
    }
    Err(last_error)
}

/// Returns the backend advertised by a transactor without opening its storage.
///
/// # Errors
/// Returns a categorized connection, authentication, protocol, or storage failure.
pub async fn discover_storage_backend(
    endpoints: &[String],
    database: &str,
    token: Option<String>,
    tls: Option<ClientTlsOptions>,
) -> Result<String, FfiError> {
    let storage = fetch_storage_connection(endpoints, database, token, tls).await?;
    storage_backend_name(&storage)
}

fn storage_backend_name(storage: &pb::StorageConnection) -> Result<String, FfiError> {
    use pb::storage_connection::Backend;

    match &storage.backend {
        Some(Backend::Memory(_)) => Ok("memory".into()),
        Some(Backend::Filesystem(_)) => Ok("filesystem".into()),
        Some(Backend::Postgres(_)) => Ok("postgres".into()),
        Some(Backend::Turso(_)) => Ok("turso".into()),
        Some(Backend::S3(_)) => Ok("s3".into()),
        Some(Backend::Plugin(plugin)) => Ok(plugin.kind.clone()),
        None => Err(FfiError::new(
            ErrorKind::Storage,
            "transactor returned no direct-storage backend",
        )),
    }
}

async fn open_discovered_storage(
    storage: pb::StorageConnection,
    discovery: StorageDiscovery,
) -> Result<Arc<dyn PeerStorage>, FfiError> {
    register_static_storage_backends();
    let spec = DiscoveredStoreSpec::from_connection(storage).map_err(|error| match error {
        StorageConnectionError::Missing => FfiError::new(
            ErrorKind::Storage,
            "transactor returned no direct-storage backend",
        ),
        StorageConnectionError::Unsupported(detail) => FfiError::new(ErrorKind::Storage, detail),
        StorageConnectionError::InvalidConfig { kind, detail } => FfiError::new(
            ErrorKind::Storage,
            format!("invalid {kind:?} storage config: {detail}"),
        ),
    })?;
    #[cfg(feature = "s3")]
    let mut spec = spec;
    #[cfg(feature = "s3")]
    if spec.kind() == "s3" {
        let config = spec.config();
        let provider = StorageInfoCredentialsProvider {
            endpoints: discovery.endpoints,
            database: discovery.database,
            token: discovery.token,
            tls: discovery.tls,
            bucket: config_string(config, "bucket")?,
            prefix: config_string_or_default(config, "prefix"),
            region: config_optional_string(config, "region"),
            endpoint_url: config_optional_string(config, "endpoint_url"),
            initial: Mutex::new(Some(s3_credentials(config)?)),
        };
        spec.config_mut()
            .insert_extension(SharedCredentialsProvider::new(provider));
    }
    #[cfg(not(feature = "s3"))]
    let _ = discovery;
    let store = spec.open_existing().await.map_err(storage_error)?;
    Ok(Arc::new(DiscoveredPeerStorage::new(store)))
}

#[cfg(feature = "s3")]
fn config_optional_string(config: &corium_store::StorageConfig, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

#[cfg(feature = "s3")]
fn config_string_or_default(config: &corium_store::StorageConfig, key: &str) -> String {
    config_optional_string(config, key).unwrap_or_default()
}

#[cfg(feature = "s3")]
fn config_string(config: &corium_store::StorageConfig, key: &str) -> Result<String, FfiError> {
    config_optional_string(config, key)
        .ok_or_else(|| FfiError::new(ErrorKind::Storage, format!("S3 storage info omitted {key}")))
}

#[cfg(feature = "s3")]
struct StorageInfoCredentialsProvider {
    endpoints: Vec<String>,
    database: String,
    token: Option<String>,
    tls: Option<ClientTlsOptions>,
    bucket: String,
    prefix: String,
    region: Option<String>,
    endpoint_url: Option<String>,
    initial: Mutex<Option<Credentials>>,
}

#[cfg(feature = "s3")]
impl fmt::Debug for StorageInfoCredentialsProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageInfoCredentialsProvider")
            .field("endpoints", &self.endpoints)
            .field("database", &self.database)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "s3")]
impl ProvideCredentials for StorageInfoCredentialsProvider {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFuture<'a>
    where
        Self: 'a,
    {
        if let Some(credentials) = self
            .initial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            return ProvideCredentialsFuture::ready(Ok(credentials));
        }
        ProvideCredentialsFuture::new(async move {
            self.fetch()
                .await
                .map_err(|error| CredentialsError::provider_error(std::io::Error::other(error)))
        })
    }
}

#[cfg(feature = "s3")]
impl StorageInfoCredentialsProvider {
    async fn fetch(&self) -> Result<Credentials, String> {
        let storage = fetch_storage_connection(
            &self.endpoints,
            &self.database,
            self.token.clone(),
            self.tls.clone(),
        )
        .await
        .map_err(|error| format!("cannot refresh S3 storage credentials: {error}"))?;
        let spec = DiscoveredStoreSpec::from_connection(storage)
            .map_err(|error| format!("cannot use refreshed storage info: {error}"))?;
        if spec.kind() != "s3"
            || config_optional_string(spec.config(), "bucket").as_deref() != Some(&self.bucket)
            || config_string_or_default(spec.config(), "prefix") != self.prefix
            || config_optional_string(spec.config(), "region") != self.region
            || config_optional_string(spec.config(), "endpoint_url") != self.endpoint_url
        {
            return Err("S3 storage identity changed while refreshing credentials".into());
        }
        s3_credentials(spec.config()).map_err(|error| error.to_string())
    }
}

#[cfg(feature = "s3")]
fn s3_credentials(config: &corium_store::StorageConfig) -> Result<Credentials, FfiError> {
    let expires_after = config
        .get("expires_unix_seconds")
        .and_then(serde_json::Value::as_u64)
        .and_then(|seconds| {
            std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(seconds))
        });
    Ok(Credentials::new(
        config_string(config, "access_key_id")?,
        config_string(config, "secret_access_key")?,
        config_optional_string(config, "session_token"),
        expires_after,
        "corium-storage-info-refresh",
    ))
}

struct PeerState {
    database_name: String,
    peer: Mutex<Option<Arc<dyn Peer>>>,
    dbs: Mutex<Vec<Weak<DbState>>>,
}

impl PeerState {
    fn peer(&self) -> Result<Arc<dyn Peer>, FfiError> {
        let guard = self
            .peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .clone()
            .ok_or_else(|| FfiError::new(ErrorKind::Closed, "peer is closed"))
    }

    fn is_closed(&self) -> bool {
        self.peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    }

    fn close(&self) {
        // Never hold `peer` while acquiring `dbs`: `register_db` takes the
        // inverse (`dbs` then `peer`) so it can make registration atomic with
        // the closed-state check. Releasing this temporary peer guard before
        // locking `dbs` is the lock-ordering invariant that prevents deadlock.
        self.peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let dbs = {
            let mut guard = self
                .dbs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *guard)
        };
        for db in dbs {
            if let Some(db) = db.upgrade() {
                db.close();
            }
        }
    }

    fn register_db(&self, db: &Arc<DbState>) {
        let mut guard = self
            .dbs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_closed() {
            drop(guard);
            db.close();
            return;
        }
        guard.retain(|handle| handle.strong_count() > 0);
        guard.push(Arc::downgrade(db));
    }
}

/// Opaque handle to either a local or remote peer.
#[derive(Clone)]
pub struct PeerHandle {
    state: Arc<PeerState>,
}

impl PeerHandle {
    /// Connects an in-process full peer.
    ///
    /// # Errors
    /// Returns a categorized connection or protocol failure.
    pub async fn connect_local(options: LocalConnectOptions) -> Result<Self, FfiError> {
        if options.endpoints.is_empty() {
            return Err(FfiError::new(
                ErrorKind::Connection,
                "at least one transactor endpoint is required",
            ));
        }
        let endpoints = options.endpoints;
        let tls = options.tls.clone().map(ClientTlsOptions::into_config);
        let mut config =
            ConnectConfig::with_failover(endpoints.clone(), options.database_name.clone());
        config.token.clone_from(&options.token);
        config.tls = tls;
        if let Some(direct) = options.direct_storage {
            let storage = discover_storage(
                &endpoints,
                &options.database_name,
                options.token,
                options.tls,
            )
            .await?;
            config = if let Some(cache) = direct.segment_cache {
                config
                    .with_storage_cache(
                        storage,
                        &SegmentCacheConfig {
                            directory: cache.directory,
                            capacity_bytes: cache.capacity_bytes,
                            memory_capacity_bytes: cache.memory_capacity_bytes,
                        },
                    )
                    .map_err(storage_error)?
            } else {
                config.with_storage(storage)
            };
        }
        let peer = LocalPeer::connect(config)
            .await
            .map_err(FfiError::from_client)?;
        Ok(Self::new(Arc::new(peer)))
    }

    /// Connects a lightweight peer-server client.
    ///
    /// # Errors
    /// Returns a categorized connection or protocol failure.
    pub async fn connect_remote(options: RemoteConnectOptions) -> Result<Self, FfiError> {
        if options.endpoint.trim().is_empty() {
            return Err(FfiError::new(
                ErrorKind::Connection,
                "a peer-server endpoint is required",
            ));
        }
        let peer = RemotePeer::connect(
            options.endpoint,
            options.database_name,
            options.token,
            options.tls.map(ClientTlsOptions::into_config),
        )
        .await
        .map_err(FfiError::from_client)?;
        Ok(Self::new(Arc::new(peer)))
    }

    /// The connected database name.
    #[must_use]
    pub fn database_name(&self) -> String {
        self.state.database_name.clone()
    }

    /// Whether [`Self::close`] has been called.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Returns the current immutable database value.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after explicit close.
    pub async fn db(&self) -> Result<DbHandle, FfiError> {
        let db = self
            .state
            .peer()?
            .db()
            .await
            .map_err(FfiError::from_client)?;
        Ok(DbHandle::new(db, Arc::clone(&self.state)))
    }

    /// Synchronizes with the transactor and returns the resulting database.
    ///
    /// # Errors
    /// Returns a categorized client failure.
    pub async fn sync(&self) -> Result<DbHandle, FfiError> {
        let db = self
            .state
            .peer()?
            .sync()
            .await
            .map_err(FfiError::from_client)?;
        Ok(DbHandle::new(db, Arc::clone(&self.state)))
    }

    /// Submits raw transaction forms encoded as a composite vector.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Decode`] unless `forms` is a vector, or a
    /// categorized transaction/client failure.
    pub async fn transact(&self, forms: CompositeValue) -> Result<TxReport, FfiError> {
        let Edn::Vector(forms) = forms.decode() else {
            return Err(FfiError::new(
                ErrorKind::Decode,
                "transaction data must be a vector of forms",
            ));
        };
        let report = self
            .state
            .peer()?
            .transact(TxData::from(forms))
            .await
            .map_err(FfiError::from_client)?;
        Ok(TxReport {
            basis_before: report.basis_before,
            basis_t: report.basis_t,
            tx_instant: report.tx_instant,
            tempids: report
                .tempids
                .into_iter()
                .map(|(name, id)| (name, id.raw()))
                .collect(),
            db_after: DbHandle::new(report.db_after, Arc::clone(&self.state)),
        })
    }

    /// Idempotently prevents new work and releases the facade's live peer.
    pub fn close(&self) {
        self.state.close();
    }

    fn new(peer: Arc<dyn Peer>) -> Self {
        Self {
            state: Arc::new(PeerState {
                database_name: peer.db_name().to_owned(),
                peer: Mutex::new(Some(peer)),
                dbs: Mutex::new(Vec::new()),
            }),
        }
    }
}

/// Opaque immutable database handle.
#[derive(Clone)]
pub struct DbHandle {
    inner: Arc<DbState>,
    state: Arc<PeerState>,
}

struct DbState {
    db: Mutex<Option<Db>>,
}

impl DbState {
    fn db(&self) -> Result<Db, FfiError> {
        self.db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| FfiError::new(ErrorKind::Closed, "peer is closed"))
    }

    fn close(&self) {
        self.db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

impl DbHandle {
    fn new(db: Db, state: Arc<PeerState>) -> Self {
        let inner = Arc::new(DbState {
            db: Mutex::new(Some(db)),
        });
        state.register_db(&inner);
        Self { inner, state }
    }

    fn db(&self) -> Result<Db, FfiError> {
        if self.state.is_closed() {
            Err(FfiError::new(ErrorKind::Closed, "peer is closed"))
        } else {
            self.inner.db()
        }
    }

    /// The database name.
    #[must_use]
    pub fn database_name(&self) -> String {
        self.state.database_name.clone()
    }

    /// Derives an as-of transaction view.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn as_of(&self, t: u64) -> Result<Self, FfiError> {
        Ok(Self::new(self.db()?.as_of(t), Arc::clone(&self.state)))
    }

    /// Derives a since-transaction view.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn since(&self, t: u64) -> Result<Self, FfiError> {
        Ok(Self::new(self.db()?.since(t), Arc::clone(&self.state)))
    }

    /// Derives a full-history view.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn history(&self) -> Result<Self, FfiError> {
        Ok(Self::new(self.db()?.history(), Arc::clone(&self.state)))
    }

    /// Derives an as-of wall-clock view using Unix milliseconds.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn as_of_instant(&self, unix_millis: i64) -> Result<Self, FfiError> {
        Ok(Self::new(
            self.db()?.as_of_instant(unix_millis),
            Arc::clone(&self.state),
        ))
    }

    /// Derives a since wall-clock view using Unix milliseconds.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn since_instant(&self, unix_millis: i64) -> Result<Self, FfiError> {
        Ok(Self::new(
            self.db()?.since_instant(unix_millis),
            Arc::clone(&self.state),
        ))
    }

    /// Executes a raw query and returns its explicit result shape.
    ///
    /// # Errors
    /// Returns a categorized decode, query, connection, or close failure.
    pub async fn query(
        &self,
        query: CompositeValue,
        args: Vec<CompositeValue>,
        fuel: Option<u64>,
    ) -> Result<QueryOutput, FfiError> {
        let db = self.db()?;
        let query = query.decode();
        let args = args.into_iter().map(|arg| arg.decode()).collect();
        let result = db
            .query_edn_with_fuel(query, args, fuel)
            .await
            .map_err(FfiError::from_client)?;
        Ok(QueryOutput {
            shape: result.shape().into(),
            value: CompositeValue::from_edn(&result.into_edn()),
        })
    }

    /// Executes a raw Pull pattern.
    ///
    /// # Errors
    /// Returns a categorized decode, query, connection, or close failure.
    pub async fn pull(
        &self,
        pattern: CompositeValue,
        entity: CompositeValue,
    ) -> Result<CompositeValue, FfiError> {
        let db = self.db()?;
        let value = db
            .pull_edn(pattern.decode(), entity.decode())
            .await
            .map_err(FfiError::from_client)?;
        Ok(CompositeValue::from_edn(&value))
    }

    /// Scans a covering index from a component prefix.
    ///
    /// # Errors
    /// Returns a categorized decode, query, connection, or close failure.
    pub async fn datoms(
        &self,
        index: Index,
        components: Vec<CompositeValue>,
        limit: u64,
    ) -> Result<Vec<Datom>, FfiError> {
        let db = self.db()?;
        let components = components
            .into_iter()
            .map(|component| component.decode())
            .collect();
        let limit = usize::try_from(limit)
            .map_err(|_| FfiError::new(ErrorKind::Decode, "datom limit is out of range"))?;
        db.datoms(index.into(), components, limit)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| Datom {
                        e: row.e,
                        a: row.a,
                        value: CompositeValue::from_edn(&row.v),
                        tx: row.tx,
                        added: row.added,
                    })
                    .collect()
            })
            .map_err(FfiError::from_client)
    }

    /// Returns coarse statistics for this database view.
    ///
    /// # Errors
    /// Returns a categorized connection or close failure.
    pub async fn stats(&self) -> Result<DbStats, FfiError> {
        self.db()?
            .stats()
            .await
            .map(DbStats::from)
            .map_err(FfiError::from_client)
    }

    /// Returns the basis transaction for this database view.
    ///
    /// # Errors
    /// Returns a categorized connection or close failure.
    pub async fn basis_t(&self) -> Result<u64, FfiError> {
        Ok(self.stats().await?.basis_t)
    }
}

/// Query result shape independent of the Rust client type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultShape {
    /// A set of tuples.
    Relation,
    /// A flat collection.
    Collection,
    /// A single tuple.
    Tuple,
    /// A single scalar.
    Scalar,
}

impl From<ClientResultShape> for ResultShape {
    fn from(value: ClientResultShape) -> Self {
        match value {
            ClientResultShape::Relation => Self::Relation,
            ClientResultShape::Collection => Self::Collection,
            ClientResultShape::Tuple => Self::Tuple,
            ClientResultShape::Scalar => Self::Scalar,
        }
    }
}

/// Owned raw query output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryOutput {
    /// Shape declared by the query's find specification.
    pub shape: ResultShape,
    /// Composite-encoded result value.
    pub value: CompositeValue,
}

/// A covering datom index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Index {
    /// Entity, attribute, value, transaction.
    Eavt,
    /// Attribute, entity, value, transaction.
    Aevt,
    /// Attribute, value, entity, transaction.
    Avet,
    /// Value, attribute, entity, transaction.
    Vaet,
}

impl From<Index> for corium_client::Index {
    fn from(value: Index) -> Self {
        match value {
            Index::Eavt => Self::Eavt,
            Index::Aevt => Self::Aevt,
            Index::Avet => Self::Avet,
            Index::Vaet => Self::Vaet,
        }
    }
}

/// One owned datom returned by an index scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Datom {
    /// Entity id.
    pub e: u64,
    /// Attribute id.
    pub a: u64,
    /// Composite-encoded value.
    pub value: CompositeValue,
    /// Transaction id.
    pub tx: u64,
    /// Whether this is an assertion rather than a retraction.
    pub added: bool,
}

/// Coarse database statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DbStats {
    /// Basis transaction.
    pub basis_t: u64,
    /// Datom count.
    pub datoms: u64,
    /// Distinct entity count.
    pub entities: u64,
    /// Attribute count.
    pub attributes: u64,
}

impl From<corium_client::DbStats> for DbStats {
    fn from(value: corium_client::DbStats) -> Self {
        Self {
            basis_t: value.basis_t,
            datoms: value.datoms,
            entities: value.entities,
            attributes: value.attributes,
        }
    }
}

/// Result of a committed transaction.
#[derive(Clone)]
pub struct TxReport {
    /// Basis before the transaction.
    pub basis_before: u64,
    /// The transaction's basis.
    pub basis_t: u64,
    /// Commit timestamp in Unix milliseconds.
    pub tx_instant: i64,
    /// Tempid string to allocated entity id.
    pub tempids: BTreeMap<String, u64>,
    /// Immutable database value immediately after the commit.
    pub db_after: DbHandle,
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::time::Duration;

    use async_trait::async_trait;
    use corium_peer::server::{PeerServerConfig, serve as serve_peer};
    use corium_peer::{Admin, ConnectConfig, Connection};
    use corium_protocol::authz::{AllowAll, Guard, Principal, StaticTokens};
    use corium_transactor::node::{NodeConfig, TransactorNode};
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose,
    };

    use super::*;

    #[test]
    fn storage_backend_name_does_not_depend_on_compiled_drivers() {
        use pb::storage_connection::Backend;

        let cases = [
            (Backend::Memory(pb::MemoryStorage {}), "memory"),
            (
                Backend::Filesystem(pb::FilesystemStorage::default()),
                "filesystem",
            ),
            (
                Backend::Postgres(pb::PostgreSqlStorage::default()),
                "postgres",
            ),
            (Backend::Turso(pb::TursoStorage::default()), "turso"),
            (Backend::S3(pb::S3Storage::default()), "s3"),
        ];
        for (backend, expected) in cases {
            assert_eq!(
                storage_backend_name(&pb::StorageConnection {
                    backend: Some(backend)
                })
                .expect("backend name"),
                expected
            );
        }
    }

    struct FailingPeer;

    #[async_trait]
    impl Peer for FailingPeer {
        #[allow(clippy::unnecessary_literal_bound)]
        fn db_name(&self) -> &str {
            "test"
        }

        async fn db(&self) -> Result<Db, ClientError> {
            Err(ClientError::Protocol("called".into()))
        }

        async fn transact(&self, _tx: TxData) -> Result<corium_client::TxReport, ClientError> {
            Err(ClientError::Protocol("called".into()))
        }

        async fn sync(&self) -> Result<Db, ClientError> {
            Err(ClientError::Protocol("called".into()))
        }
    }

    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("addr")
            .port()
    }

    fn storage_discovery() -> StorageDiscovery {
        StorageDiscovery {
            endpoints: vec!["http://127.0.0.1:4334".into()],
            database: "people".into(),
            token: None,
            tls: None,
        }
    }

    async fn connected_transactor(
        database: &str,
        schema: &[Edn],
    ) -> (String, tokio::sync::oneshot::Sender<()>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = NodeConfig::new(dir.path().join("data"));
        config.owner = "ffi-test".into();
        config.lease_ttl_ms = 600_000;
        config.index_interval = Duration::from_secs(600);
        config.heartbeat_interval = Duration::from_secs(600);
        let node = TransactorNode::open(config).await.expect("open node");
        let address: std::net::SocketAddr =
            format!("127.0.0.1:{}", free_port()).parse().expect("addr");
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(corium_transactor::server::serve(
            node,
            address,
            Guard::disabled(),
            None,
            async move {
                let _ = stop_rx.await;
            },
        ));
        let endpoint = format!("http://{address}");
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut admin = loop {
            if let Ok(admin) = Admin::connect(&endpoint, None, None).await {
                break admin;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "transactor never ready"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        admin
            .create_database(database, schema)
            .await
            .expect("create database");
        (endpoint, stop_tx, dir)
    }

    async fn connected_peer() -> (
        PeerHandle,
        tokio::sync::oneshot::Sender<()>,
        tempfile::TempDir,
    ) {
        let (endpoint, stop_tx, dir) = connected_transactor("test", &[]).await;
        let peer = LocalPeer::connect(ConnectConfig::new(endpoint, "test"))
            .await
            .expect("connect local peer");
        (PeerHandle::new(Arc::new(peer)), stop_tx, dir)
    }

    #[tokio::test]
    async fn local_facade_discovers_filesystem_storage_and_opens_cache() {
        let (endpoint, stop_tx, dir) = connected_transactor("storage-test", &[]).await;
        let cache_directory = dir.path().join("segment-cache");
        let peer = PeerHandle::connect_local(LocalConnectOptions {
            endpoints: vec![endpoint],
            database_name: "storage-test".into(),
            token: None,
            tls: None,
            direct_storage: Some(DirectStorageOptions {
                segment_cache: Some(SegmentCacheOptions {
                    directory: cache_directory.clone(),
                    capacity_bytes: 1024 * 1024,
                    memory_capacity_bytes: 64 * 1024,
                }),
            }),
        })
        .await
        .expect("connect storage-aware local peer");

        assert_eq!(peer.database_name(), "storage-test");
        let _stats = peer
            .db()
            .await
            .expect("database")
            .stats()
            .await
            .expect("stats");
        assert!(cache_directory.join("LOCK").is_file());
        peer.close();
        let _ = stop_tx.send(());
    }

    #[test]
    fn composite_values_validate_and_round_trip() {
        let form = Edn::Vector(vec![
            Edn::keyword("person/name"),
            Edn::Str("Ada".into()),
            Edn::Tagged("eid".into(), Box::new(Edn::Long(42))),
        ]);
        let value = CompositeValue::from_edn(&form);
        assert_eq!(value.decode(), form);
        assert_eq!(
            CompositeValue::from_bytes(&[0xff])
                .expect_err("unknown tag")
                .kind(),
            ErrorKind::Decode
        );
    }

    #[tokio::test]
    async fn close_is_shared_idempotent_and_prevents_new_work() {
        let peer = PeerHandle::new(Arc::new(FailingPeer));
        let clone = peer.clone();
        assert_eq!(peer.database_name(), "test");
        let Err(error) = peer.db().await else {
            panic!("fake peer unexpectedly returned a database");
        };
        assert_eq!(error.kind(), ErrorKind::Protocol);

        peer.close();
        peer.close();
        assert!(clone.is_closed());
        let Err(error) = clone.db().await else {
            panic!("closed peer unexpectedly returned a database");
        };
        assert_eq!(error.kind(), ErrorKind::Closed);
    }

    #[test]
    fn query_fuel_has_a_stable_category() {
        let error = FfiError::from_client(ClientError::Query(QueryError::FuelExhausted));
        assert_eq!(error.kind(), ErrorKind::FuelExhausted);
    }

    #[test]
    fn remote_caller_and_transaction_statuses_have_stable_categories() {
        let local_query = FfiError::from_client(ClientError::Query(QueryError::Arity(
            "missing argument".into(),
        )));
        let remote_query = FfiError::from_client(ClientError::Rpc(Status::invalid_argument(
            "missing argument",
        )));
        assert_eq!(local_query.kind(), ErrorKind::Query);
        assert_eq!(
            remote_query.kind(),
            local_query.kind(),
            "local and remote caller-input failures must share a category"
        );

        for status in [
            Status::failed_precondition("transaction cannot run"),
            Status::aborted("basis changed"),
        ] {
            assert_eq!(
                FfiError::from_client(ClientError::Rpc(status)).kind(),
                ErrorKind::Transaction
            );
        }
    }

    #[test]
    fn connect_options_redact_bearer_tokens() {
        let local = LocalConnectOptions {
            endpoints: vec!["https://example.test".into()],
            database_name: "people".into(),
            token: Some("local-secret".into()),
            tls: Some(ClientTlsOptions {
                ca_certificate: Some(b"local-private-ca".to_vec()),
                domain_name: Some("local.test".into()),
            }),
            direct_storage: Some(DirectStorageOptions {
                segment_cache: Some(SegmentCacheOptions {
                    directory: PathBuf::from("/tmp/corium-cache"),
                    capacity_bytes: 1024,
                    memory_capacity_bytes: 128,
                }),
            }),
        };
        let remote = RemoteConnectOptions {
            endpoint: "https://example.test".into(),
            database_name: "people".into(),
            token: Some("remote-secret".into()),
            tls: Some(ClientTlsOptions {
                ca_certificate: Some(b"remote-private-ca".to_vec()),
                domain_name: Some("remote.test".into()),
            }),
        };
        let rendered = format!("{local:?} {remote:?}");
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("local-secret"));
        assert!(!rendered.contains("remote-secret"));
        assert!(!rendered.contains("private-ca"));
    }

    #[test]
    fn available_backends_are_registry_driven() {
        let backends = available_storage_backends();
        assert!(backends.iter().any(|backend| backend == "filesystem"));
        assert!(backends.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test]
    async fn memory_storage_discovery_is_rejected() {
        let error = open_discovered_storage(
            pb::StorageConnection {
                backend: Some(pb::storage_connection::Backend::Memory(
                    pb::MemoryStorage {},
                )),
            },
            storage_discovery(),
        )
        .await
        .err()
        .expect("memory storage must be rejected");
        assert_eq!(error.kind(), ErrorKind::Storage);
        assert!(error.message().contains("confined"));
    }

    #[tokio::test]
    async fn filesystem_discovery_rejects_an_unreachable_store_without_creating_it() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let error = open_discovered_storage(
            pb::StorageConnection {
                backend: Some(pb::storage_connection::Backend::Filesystem(
                    pb::FilesystemStorage {
                        data_dir: directory.path().to_string_lossy().into_owned(),
                    },
                )),
            },
            storage_discovery(),
        )
        .await
        .err()
        .expect("unreachable filesystem storage must be rejected");
        assert_eq!(error.kind(), ErrorKind::Storage);
        assert!(error.message().contains("not reachable"));
        assert!(!root.exists());
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_discovery_rejects_an_unreachable_database_without_creating_it() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("missing.db");
        let error = open_discovered_storage(
            pb::StorageConnection {
                backend: Some(pb::storage_connection::Backend::Turso(pb::TursoStorage {
                    path: path.to_string_lossy().into_owned(),
                })),
            },
            storage_discovery(),
        )
        .await
        .err()
        .expect("unreachable Turso storage must be rejected");
        assert_eq!(error.kind(), ErrorKind::Storage);
        assert!(error.message().contains("not reachable"));
        assert!(!path.exists());
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn postgres_discovery_is_rejected_when_backend_is_disabled() {
        if available_storage_backends()
            .iter()
            .any(|backend| backend == "postgres")
        {
            return;
        }
        let error = open_discovered_storage(
            pb::StorageConnection {
                backend: Some(pb::storage_connection::Backend::Postgres(
                    pb::PostgreSqlStorage {
                        connection_string: "postgresql://reader:secret@example.test/db".into(),
                    },
                )),
            },
            storage_discovery(),
        )
        .await
        .err()
        .expect("disabled PostgreSQL backend must be rejected");
        assert_eq!(error.kind(), ErrorKind::Storage);
        assert!(error.message().contains("not available"));
        assert!(!error.message().contains("secret"));
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_open_errors_do_not_expose_the_connection_password() {
        let error = open_discovered_storage(
            pb::StorageConnection {
                backend: Some(pb::storage_connection::Backend::Postgres(
                    pb::PostgreSqlStorage {
                        connection_string:
                            "postgresql://reader:postgres-secret@127.0.0.1:1/corium?connect_timeout=1"
                                .into(),
                    },
                )),
            },
            storage_discovery(),
        )
        .await
        .err()
        .expect("unreachable PostgreSQL storage must fail");
        assert_eq!(error.kind(), ErrorKind::Storage);
        assert!(!error.message().contains("postgres-secret"));
    }

    #[cfg(not(feature = "turso"))]
    #[tokio::test]
    async fn turso_discovery_is_rejected_when_backend_is_disabled() {
        if available_storage_backends()
            .iter()
            .any(|backend| backend == "turso")
        {
            return;
        }
        let error = open_discovered_storage(
            pb::StorageConnection {
                backend: Some(pb::storage_connection::Backend::Turso(pb::TursoStorage {
                    path: "/private/turso.db".into(),
                })),
            },
            storage_discovery(),
        )
        .await
        .err()
        .expect("disabled Turso backend must be rejected");
        assert_eq!(error.kind(), ErrorKind::Storage);
        assert!(error.message().contains("not available"));
    }

    #[cfg(not(feature = "s3"))]
    #[tokio::test]
    async fn s3_discovery_is_rejected_when_backend_is_disabled() {
        if available_storage_backends()
            .iter()
            .any(|backend| backend == "s3")
        {
            return;
        }
        let error = open_discovered_storage(
            pb::StorageConnection {
                backend: Some(pb::storage_connection::Backend::S3(pb::S3Storage {
                    bucket: "corium-data".into(),
                    prefix: "people/".into(),
                    access_key_id: "reader".into(),
                    secret_access_key: "secret".into(),
                    ..Default::default()
                })),
            },
            storage_discovery(),
        )
        .await
        .err()
        .expect("disabled S3 backend must be rejected");
        assert_eq!(error.kind(), ErrorKind::Storage);
        assert!(error.message().contains("not available"));
        assert!(!error.message().contains("secret"));
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_discovery_requires_read_only_credentials_and_redacts_provider() {
        register_static_storage_backends();
        let mut storage = pb::S3Storage {
            bucket: "corium-data".into(),
            prefix: "people/".into(),
            access_key_id: "reader".into(),
            secret_access_key: String::new(),
            session_token: "session-secret".into(),
            region: "us-west-2".into(),
            endpoint_url: String::new(),
            expires_unix_seconds: 0,
        };
        let error = DiscoveredStoreSpec::from_connection(pb::StorageConnection {
            backend: Some(pb::storage_connection::Backend::S3(storage.clone())),
        })
        .expect_err("incomplete credentials");
        assert!(error.to_string().contains("read-only secret access key"));

        storage.secret_access_key = "reader-secret".into();
        let spec = DiscoveredStoreSpec::from_connection(pb::StorageConnection {
            backend: Some(pb::storage_connection::Backend::S3(storage)),
        })
        .expect("complete S3 storage spec");
        let rendered = format!("{spec:?}");
        assert!(!rendered.contains("reader-secret"));
        assert!(!rendered.contains("session-secret"));
    }

    fn private_ca_server_identity(server_name: &str) -> (String, String, String) {
        let ca_key = KeyPair::generate().expect("generate CA key");
        let mut ca_params = CertificateParams::new(Vec::new()).expect("CA parameters");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
        let ca_cert = ca_params.self_signed(&ca_key).expect("generate CA");

        let server_key = KeyPair::generate().expect("generate server key");
        let mut server_params =
            CertificateParams::new(vec![server_name.into()]).expect("server parameters");
        server_params
            .key_usages
            .push(KeyUsagePurpose::DigitalSignature);
        server_params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ServerAuth);
        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .expect("sign server certificate");

        (ca_cert.pem(), server_cert.pem(), server_key.serialize_pem())
    }

    async fn exercise_remote_facade(peer: &PeerHandle) {
        let fixture = corium_query::edn::read_one(r#"[{:db/id "ada" :person/name "Ada"}]"#)
            .expect("fixture transaction");
        let db = peer
            .transact(CompositeValue::from_edn(&fixture))
            .await
            .expect("remote fixture transaction")
            .db_after;
        let stats = db
            .stats()
            .await
            .expect("private-CA TLS request with the right token succeeds");

        let relation = corium_query::edn::read_one(r#"[:find ?e :where [?e :person/name "Ada"]]"#)
            .expect("relation query");
        let output = db
            .query(CompositeValue::from_edn(&relation), Vec::new(), None)
            .await
            .expect("remote relation query");
        assert_eq!(output.shape, ResultShape::Relation);
        assert!(
            matches!(output.value.decode(), Edn::Vector(ref rows) if !rows.is_empty()),
            "fixture query returns Ada"
        );

        let scalar = corium_query::edn::read_one(r#"[:find ?e . :where [?e :person/name "Ada"]]"#)
            .expect("scalar query");
        let entity = db
            .query(CompositeValue::from_edn(&scalar), Vec::new(), None)
            .await
            .expect("remote scalar query");
        assert_eq!(entity.shape, ResultShape::Scalar);
        let pulled = db
            .pull(
                CompositeValue::from_edn(
                    &corium_query::edn::read_one("[:person/name]").expect("pull pattern"),
                ),
                entity.value,
            )
            .await
            .expect("remote pull");
        assert!(matches!(pulled.decode(), Edn::Map(ref pairs) if !pairs.is_empty()));

        let ident = CompositeValue::from_edn(&Edn::keyword("person/name"));
        assert_eq!(
            db.datoms(Index::Aevt, vec![ident], 1)
                .await
                .expect("remote datom scan")
                .len(),
            1
        );
        let missing_argument =
            corium_query::edn::read_one("[:find ?e :in $ ?name :where [?e :person/name ?name]]")
                .expect("argument query");
        assert_eq!(
            db.query(
                CompositeValue::from_edn(&missing_argument),
                Vec::new(),
                None,
            )
            .await
            .expect_err("missing argument is rejected")
            .kind(),
            ErrorKind::Query
        );

        assert_eq!(
            db.as_of(stats.basis_t)
                .expect("as-of view")
                .stats()
                .await
                .expect("remote as-of stats"),
            stats
        );
        let report = peer
            .transact(CompositeValue::from_edn(&Edn::Vector(Vec::new())))
            .await
            .expect("remote transaction");
        assert_eq!(report.basis_before, stats.basis_t);
        assert_eq!(
            report
                .db_after
                .stats()
                .await
                .expect("post-transaction database")
                .basis_t,
            report.basis_t
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn private_ca_tls_domain_and_bearer_tokens_cross_the_remote_facade() {
        // Workspace feature unification can enable both rustls crypto providers,
        // so select the provider used by tonic before starting the TLS server.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let schema = corium_query::edn::read_all(
            "{:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity}",
        )
        .expect("schema");
        let (transactor_endpoint, transactor_stop, dir) =
            connected_transactor("secure", &schema).await;
        let hosted = Arc::new(
            Connection::connect(ConnectConfig::new(transactor_endpoint, "secure"))
                .await
                .expect("hosted connection"),
        );
        hosted.sync().await.expect("hosted peer syncs");

        let server_name = "corium.internal";
        let (ca_pem, server_cert_pem, server_key_pem) = private_ca_server_identity(server_name);
        let cert_path = dir.path().join("peer-cert.pem");
        let key_path = dir.path().join("peer-key.pem");
        std::fs::write(&cert_path, server_cert_pem).expect("write certificate");
        std::fs::write(&key_path, server_key_pem).expect("write key");

        let tokens = StaticTokens::new().with(
            "secret-token",
            Principal::new("ffi-test", "authorized-client"),
        );
        let guard = Guard::new(Arc::new(tokens), Arc::new(AllowAll));
        let peer_address: std::net::SocketAddr =
            format!("127.0.0.1:{}", free_port()).parse().expect("addr");
        let (peer_stop_tx, peer_stop_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(serve_peer(
            hosted,
            peer_address,
            guard,
            Some(
                corium_protocol::auth::server_tls(&cert_path, &key_path)
                    .expect("server TLS config"),
            ),
            PeerServerConfig::default(),
            async move {
                let _ = peer_stop_rx.await;
            },
        ));

        let endpoint = format!("https://127.0.0.1:{}", peer_address.port());
        let options = |token: Option<&str>, domain_name: Option<&str>| RemoteConnectOptions {
            endpoint: endpoint.clone(),
            database_name: "secure".into(),
            token: token.map(str::to_owned),
            tls: Some(ClientTlsOptions {
                ca_certificate: Some(ca_pem.as_bytes().to_vec()),
                domain_name: domain_name.map(str::to_owned),
            }),
        };

        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let authorized = loop {
            if let Ok(peer) =
                PeerHandle::connect_remote(options(Some("secret-token"), Some(server_name))).await
                && let Ok(db) = peer.db().await
                && db.stats().await.is_ok()
            {
                break peer;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "TLS peer server never ready"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        exercise_remote_facade(&authorized).await;

        let Err(error) = PeerHandle::connect_remote(options(Some("secret-token"), None)).await
        else {
            panic!("TLS connection without the certificate domain override unexpectedly succeeded");
        };
        assert_eq!(error.kind(), ErrorKind::Connection);

        for token in [None, Some("wrong-token")] {
            let peer = PeerHandle::connect_remote(options(token, Some(server_name)))
                .await
                .expect("TLS handshake succeeds before request authentication");
            let error = peer
                .db()
                .await
                .expect("remote database handle")
                .stats()
                .await
                .expect_err("missing or wrong bearer token must fail");
            assert_eq!(error.kind(), ErrorKind::Authentication);
            assert_eq!(error.grpc_code(), Some(Code::Unauthenticated as i32));
        }

        let _ = peer_stop_tx.send(());
        let _ = transactor_stop.send(());
    }

    #[tokio::test]
    async fn close_invalidates_database_handles_and_prunes_registry() {
        let (peer, stop, _dir) = connected_peer().await;
        let db = peer.db().await.expect("database");
        db.stats().await.expect("open database works");
        let query =
            corium_query::edn::read_one("[:find ?e :in $ ?ident :where [?e :db/ident ?ident]]")
                .expect("query");
        assert_eq!(
            db.query(CompositeValue::from_edn(&query), Vec::new(), None)
                .await
                .expect_err("missing query argument")
                .kind(),
            ErrorKind::Query,
            "local caller errors retain the query category without a facade heuristic"
        );
        assert_eq!(peer.state.dbs.lock().expect("registry").len(), 1);

        let derived = db.history().expect("derive history");
        assert_eq!(peer.state.dbs.lock().expect("registry").len(), 2);
        drop(derived);
        let replacement = db.since(0).expect("derive since");
        assert_eq!(
            peer.state.dbs.lock().expect("registry").len(),
            2,
            "dead weak handles are pruned during registration"
        );
        drop(replacement);

        let raw_db = db.db().expect("raw database for close race");
        peer.close();
        assert_eq!(
            db.stats().await.expect_err("closed database").kind(),
            ErrorKind::Closed
        );
        let Err(error) = db.as_of(0) else {
            panic!("closed as-of unexpectedly returned a handle");
        };
        assert_eq!(error.kind(), ErrorKind::Closed);
        let Err(error) = db.since(0) else {
            panic!("closed since unexpectedly returned a handle");
        };
        assert_eq!(error.kind(), ErrorKind::Closed);
        let Err(error) = db.history() else {
            panic!("closed history unexpectedly returned a handle");
        };
        assert_eq!(error.kind(), ErrorKind::Closed);

        let registered_after_close = DbHandle::new(raw_db, Arc::clone(&peer.state));
        assert_eq!(
            registered_after_close
                .stats()
                .await
                .expect_err("late handle closes immediately")
                .kind(),
            ErrorKind::Closed
        );
        let _ = stop.send(());
    }

    #[tokio::test]
    async fn transact_requires_a_vector_and_remote_requires_an_endpoint() {
        let peer = PeerHandle::new(Arc::new(FailingPeer));
        let Err(error) = peer.transact(CompositeValue::from_edn(&Edn::Long(1))).await else {
            panic!("non-vector transaction unexpectedly succeeded");
        };
        assert_eq!(error.kind(), ErrorKind::Decode);

        let Err(error) = PeerHandle::connect_remote(RemoteConnectOptions {
            endpoint: " ".into(),
            database_name: "test".into(),
            token: None,
            tls: None,
        })
        .await
        else {
            panic!("empty endpoint unexpectedly connected");
        };
        assert_eq!(error.kind(), ErrorKind::Connection);
    }
}
