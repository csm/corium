//! Pluggable transactor storage backends.
//!
//! A transactor keeps two kinds of durable state: the content-addressed
//! blob store plus fenced root pointers (the "storage service"), and the
//! per-database transaction log. [`StoreSpec`] selects the storage service
//! backend — in-memory, filesystem, `PostgreSQL`, Turso, or S3 — and [`NodeStore`]
//! dispatches the [`BlobStore`]/[`RootStore`] operations to it. Native
//! service backends keep transaction logs in the same storage system as blobs
//! and roots; memory and filesystem retain their existing log stores.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "s3")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use corium_log::{
    LogCipher, LogError, MemLogRegistry, NativeLogStorage, NativeVersionedLog, TransactionLog,
    TxRecord, VersionedLog,
};
pub use corium_store::StorageConnectionError;
use corium_store::{
    DiscoveredStore, DiscoveredStoreSpec, FullStore, LogPlacement, RootStore, StorageConfig,
    StoreError, storage_backend,
};
#[cfg(feature = "s3")]
use corium_store_s3::{S3ClientConfig, normalize_s3_prefix};

/// Selects the transactor's storage-service backend (blobs + roots).
#[derive(Clone)]
pub struct StoreSpec {
    kind: String,
    config: StorageConfig,
}

impl Default for StoreSpec {
    fn default() -> Self {
        Self::filesystem()
    }
}

impl fmt::Debug for StoreSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreSpec")
            .field("kind", &self.kind)
            .field("config", &self.config)
            .finish()
    }
}

/// Credentials advertised by `GetStorageInfo` for S3 read-only clients.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub enum S3ReadOnlyCredentials {
    /// Operator-provisioned credentials whose IAM/bucket policy permits only
    /// reads of the configured Corium prefix.
    Static {
        /// Access-key id.
        access_key_id: String,
        /// Secret access key.
        secret_access_key: String,
        /// Optional session token.
        session_token: Option<String>,
    },
    /// Temporary credentials obtained from AWS STS for this request.
    ///
    /// Corium attaches a restrictive session policy allowing only
    /// `GetObject` and prefix-scoped `ListBucket`, so the issued credential
    /// cannot write even if the role itself has broader permissions.
    AssumeRole {
        /// IAM role to assume.
        role_arn: String,
        /// STS role-session name.
        session_name: String,
        /// Requested token lifetime in seconds.
        duration_seconds: i32,
        /// Optional trust-policy external id.
        external_id: Option<String>,
    },
}

#[cfg(feature = "s3")]
impl fmt::Debug for S3ReadOnlyCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static { .. } => formatter.write_str("Static([REDACTED])"),
            Self::AssumeRole {
                role_arn,
                session_name,
                duration_seconds,
                external_id,
            } => formatter
                .debug_struct("AssumeRole")
                .field("role_arn", role_arn)
                .field("session_name", session_name)
                .field("duration_seconds", duration_seconds)
                .field("external_id", &external_id.as_ref().map(|_| "[REDACTED]"))
                .finish(),
        }
    }
}

/// S3 endpoint metadata and read-only credential source advertised to peers.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub struct S3ReadOnlyConfig {
    /// Region clients should use.
    pub region: Option<String>,
    /// Optional S3-compatible endpoint.
    pub endpoint_url: Option<String>,
    /// Static or STS-generated credentials.
    pub credentials: S3ReadOnlyCredentials,
    runtime: Arc<S3ReadOnlyRuntime>,
}

#[cfg(feature = "s3")]
impl fmt::Debug for S3ReadOnlyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3ReadOnlyConfig")
            .field("region", &self.region)
            .field("endpoint_url", &self.endpoint_url)
            .field("credentials", &self.credentials)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "s3")]
#[derive(Default)]
struct S3ReadOnlyRuntime {
    aws: tokio::sync::OnceCell<ResolvedAwsConfig>,
    cached_credentials: tokio::sync::Mutex<Option<CachedS3Credentials>>,
}

#[cfg(feature = "s3")]
struct ResolvedAwsConfig {
    sts: aws_sdk_sts::Client,
    region: Option<String>,
    endpoint_url: Option<String>,
}

#[cfg(feature = "s3")]
#[derive(Clone)]
struct CachedS3Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expires_unix_seconds: i64,
    refresh_at: SystemTime,
}

/// Separately provisioned credentials returned by `GetStorageInfo`.
///
/// Local storage backends do not consult this value. Service backends fail
/// storage discovery rather than falling back to the transactor's primary
/// read/write credentials.
#[derive(Clone, Default)]
pub struct StorageInfoConfig {
    /// Read-only `PostgreSQL` URL.
    pub postgres_connection_string: Option<String>,
    /// Read-only S3 access.
    #[cfg(feature = "s3")]
    pub s3: Option<S3ReadOnlyConfig>,
    /// Opaque read-only configuration advertised for a plugin backend.
    ///
    /// This must be provisioned separately from [`StoreSpec::config`]. Corium
    /// never derives it from or falls back to the primary read/write config.
    pub plugin_config: Option<StorageConfig>,
}

impl fmt::Debug for StorageInfoConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("StorageInfoConfig");
        debug.field(
            "postgres_connection_string",
            &self
                .postgres_connection_string
                .as_ref()
                .map(|_| "[REDACTED]"),
        );
        #[cfg(feature = "s3")]
        debug.field("s3", &self.s3);
        debug.field("plugin_config", &self.plugin_config);
        debug.finish()
    }
}

impl StoreSpec {
    /// Creates a specification for any registered backend.
    #[must_use]
    pub fn new(kind: impl Into<String>, config: StorageConfig) -> Self {
        Self {
            kind: kind.into(),
            config,
        }
    }

    /// In-memory blobs, roots, and logs.
    #[must_use]
    pub fn memory() -> Self {
        Self::new("memory", StorageConfig::default())
    }

    /// Filesystem blobs and roots under the node data directory.
    #[must_use]
    pub fn filesystem() -> Self {
        Self::new("filesystem", StorageConfig::default())
    }

    /// PostgreSQL blobs, roots, and logs.
    #[cfg(feature = "postgres")]
    pub fn postgres(connection_string: impl Into<String>) -> Self {
        Self::new(
            "postgres",
            StorageConfig::from_value(serde_json::json!({
                "connection_string": connection_string.into()
            }))
            .expect("object storage configuration"),
        )
    }

    /// Turso blobs, roots, and logs.
    #[cfg(feature = "turso")]
    pub fn turso(path: impl Into<PathBuf>) -> Self {
        Self::new(
            "turso",
            StorageConfig::from_value(serde_json::json!({ "path": path.into() }))
                .expect("object storage configuration"),
        )
    }

    /// S3 blobs, roots, and logs.
    #[cfg(feature = "s3")]
    pub fn s3(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        client: &S3ClientConfig,
    ) -> Self {
        let expires_unix_seconds = client.expires_after.and_then(|expires| {
            expires
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|duration| duration.as_secs())
        });
        Self::new(
            "s3",
            StorageConfig::from_value(serde_json::json!({
                "bucket": bucket.into(),
                "prefix": prefix.into(),
                "region": client.region,
                "endpoint_url": client.endpoint_url,
                "access_key_id": client.access_key_id,
                "secret_access_key": client.secret_access_key,
                "session_token": client.session_token,
                "expires_unix_seconds": expires_unix_seconds,
            }))
            .expect("object storage configuration"),
        )
    }

    /// Backend kind.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Redacted backend configuration.
    #[must_use]
    pub const fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Describes how an administrative client can independently open this
    /// node's storage service.
    ///
    /// Local paths are made absolute because the client need not share the
    /// transactor's working directory. The memory backend is described too,
    /// but cannot be opened by another process.
    ///
    /// # Errors
    /// Returns an error when a local path cannot be represented on the wire.
    #[cfg_attr(not(feature = "s3"), allow(clippy::unused_async))]
    pub async fn connection_info(
        &self,
        data_dir: &std::path::Path,
        #[cfg_attr(
            not(any(feature = "postgres", feature = "s3")),
            allow(unused_variables)
        )]
        read_only: &StorageInfoConfig,
    ) -> Result<corium_protocol::pb::StorageConnection, String> {
        use corium_protocol::pb;
        use pb::storage_connection::Backend;

        fn absolute(path: &std::path::Path) -> Result<String, String> {
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()
                    .map_err(|error| error.to_string())?
                    .join(path)
            };
            path.into_os_string()
                .into_string()
                .map_err(|_| "storage path is not valid UTF-8".to_owned())
        }

        let text = |name: &str| self.config.get(name).and_then(serde_json::Value::as_str);
        let backend = match self.kind.as_str() {
            "memory" => Backend::Memory(pb::MemoryStorage {}),
            "filesystem" => Backend::Filesystem(pb::FilesystemStorage {
                data_dir: absolute(data_dir)?,
            }),
            "postgres" => {
                let connection_string =
                    read_only
                        .postgres_connection_string
                        .clone()
                        .ok_or_else(|| {
                            "PostgreSQL storage discovery requires a separately configured \
                         read-only connection string"
                                .to_owned()
                        })?;
                Backend::Postgres(pb::PostgreSqlStorage { connection_string })
            }
            "turso" => Backend::Turso(pb::TursoStorage {
                path: absolute(std::path::Path::new(text("path").ok_or_else(|| {
                    "Turso storage configuration omitted path".to_owned()
                })?))?,
            }),
            #[cfg(feature = "s3")]
            "s3" => {
                let read_only = read_only.s3.as_ref().ok_or_else(|| {
                    "S3 storage discovery requires separately configured read-only credentials"
                        .to_owned()
                })?;
                let bucket = text("bucket")
                    .ok_or_else(|| "S3 storage configuration omitted bucket".to_owned())?;
                let prefix = text("prefix").unwrap_or_default();
                Backend::S3(read_only.s3_storage(bucket, prefix).await?)
            }
            kind => {
                let config = read_only.plugin_config.as_ref().ok_or_else(|| {
                    format!(
                        "storage plugin {kind:?} discovery requires separately configured \
                         read-only plugin configuration"
                    )
                })?;
                Backend::Plugin(pb::PluginStorage {
                    kind: kind.to_owned(),
                    config_json: config.to_json(),
                })
            }
        };
        Ok(pb::StorageConnection {
            backend: Some(backend),
        })
    }
}

#[cfg(feature = "s3")]
fn unix_timestamp(seconds: i64) -> Option<SystemTime> {
    u64::try_from(seconds)
        .ok()
        .filter(|seconds| *seconds > 0)
        .and_then(|seconds| UNIX_EPOCH.checked_add(Duration::from_secs(seconds)))
}

#[cfg(feature = "s3")]
impl S3ReadOnlyConfig {
    /// Creates read-only S3 discovery configuration with a shared AWS client
    /// and temporary-credential cache.
    #[must_use]
    pub fn new(
        region: Option<String>,
        endpoint_url: Option<String>,
        credentials: S3ReadOnlyCredentials,
    ) -> Self {
        Self {
            region,
            endpoint_url,
            credentials,
            runtime: Arc::new(S3ReadOnlyRuntime::default()),
        }
    }

    async fn aws(&self) -> &ResolvedAwsConfig {
        self.runtime
            .aws
            .get_or_init(|| async {
                let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
                if let Some(region) = &self.region {
                    loader = loader.region(aws_sdk_sts::config::Region::new(region.clone()));
                }
                let config = loader.load().await;
                let region = self
                    .region
                    .clone()
                    .or_else(|| config.region().map(ToString::to_string));
                let endpoint_url = self.endpoint_url.clone().or_else(ambient_s3_endpoint);
                ResolvedAwsConfig {
                    sts: aws_sdk_sts::Client::new(&config),
                    region,
                    endpoint_url,
                }
            })
            .await
    }

    pub(crate) async fn initialize(&self) {
        self.aws().await;
    }

    async fn s3_storage(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<corium_protocol::pb::S3Storage, String> {
        let prefix = normalize_s3_prefix(prefix.to_owned());
        let aws = self.aws().await;
        let (access_key_id, secret_access_key, session_token, expires_unix_seconds) =
            match &self.credentials {
                S3ReadOnlyCredentials::Static {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } => (
                    access_key_id.clone(),
                    secret_access_key.clone(),
                    session_token.clone().unwrap_or_default(),
                    0,
                ),
                S3ReadOnlyCredentials::AssumeRole {
                    role_arn,
                    session_name,
                    duration_seconds,
                    external_id,
                } => {
                    let issued = self
                        .assume_role_credentials(
                            bucket,
                            &prefix,
                            role_arn,
                            session_name,
                            *duration_seconds,
                            external_id.as_deref(),
                        )
                        .await?;
                    (
                        issued.access_key_id,
                        issued.secret_access_key,
                        issued.session_token,
                        issued.expires_unix_seconds,
                    )
                }
            };
        Ok(corium_protocol::pb::S3Storage {
            bucket: bucket.to_owned(),
            prefix,
            access_key_id,
            secret_access_key,
            session_token,
            region: aws.region.clone().unwrap_or_default(),
            endpoint_url: aws.endpoint_url.clone().unwrap_or_default(),
            expires_unix_seconds,
        })
    }

    async fn assume_role_credentials(
        &self,
        bucket: &str,
        prefix: &str,
        role_arn: &str,
        session_name: &str,
        duration_seconds: i32,
        external_id: Option<&str>,
    ) -> Result<CachedS3Credentials, String> {
        let mut cached = self.runtime.cached_credentials.lock().await;
        if let Some(credentials) = cached
            .as_ref()
            .filter(|credentials| SystemTime::now() < credentials.refresh_at)
        {
            return Ok(credentials.clone());
        }
        let partition = role_arn.split(':').nth(1).unwrap_or("aws");
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [format!("arn:{partition}:s3:::{bucket}/{prefix}*")]
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [format!("arn:{partition}:s3:::{bucket}")],
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": [format!("{prefix}*")]
                        }
                    }
                }
            ]
        })
        .to_string();
        let issued_at = SystemTime::now();
        let output = self
            .aws()
            .await
            .sts
            .assume_role()
            .role_arn(role_arn)
            .role_session_name(session_name)
            .duration_seconds(duration_seconds)
            .policy(policy)
            .set_external_id(external_id.map(str::to_owned))
            .send()
            .await
            .map_err(|error| format!("cannot assume S3 read-only role: {error}"))?;
        let credentials = output.credentials().ok_or_else(|| {
            "AWS STS returned no credentials for the S3 read-only role".to_owned()
        })?;
        let expires_unix_seconds = credentials.expiration().secs();
        let expiration = unix_timestamp(expires_unix_seconds).unwrap_or(issued_at);
        let lifetime = expiration
            .duration_since(issued_at)
            .unwrap_or(Duration::ZERO);
        let issued = CachedS3Credentials {
            access_key_id: credentials.access_key_id().to_owned(),
            secret_access_key: credentials.secret_access_key().to_owned(),
            session_token: credentials.session_token().to_owned(),
            expires_unix_seconds,
            refresh_at: issued_at
                .checked_add(lifetime.mul_f64(0.8))
                .unwrap_or(issued_at),
        };
        *cached = Some(issued.clone());
        Ok(issued)
    }
}

#[cfg(feature = "s3")]
fn ambient_s3_endpoint() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL_S3")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("AWS_ENDPOINT_URL")
                .ok()
                .filter(|value| !value.is_empty())
        })
}

impl StorageInfoConfig {
    #[cfg_attr(not(feature = "s3"), allow(clippy::unused_async))]
    pub(crate) async fn initialize(&self) {
        #[cfg(feature = "s3")]
        if let Some(s3) = &self.s3 {
            s3.initialize().await;
        }
    }
}

/// The backend-neutral blob + root storage service used by a node.
pub type NodeStore = dyn FullStore;

/// Registers storage drivers selected by this crate's Cargo features.
pub fn register_static_storage_backends() {
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

/// Opens the backend selected by `spec`.
///
/// # Errors
/// Returns an error when the backend is unavailable or cannot be opened.
pub async fn open_node_store(
    spec: &StoreSpec,
    data_dir: &std::path::Path,
) -> Result<Arc<NodeStore>, StoreError> {
    register_static_storage_backends();
    let backend = storage_backend(spec.kind())
        .ok_or_else(|| StoreError::BackendUnavailable(spec.kind().to_owned()))?;
    let config = if spec.kind() == "filesystem" && spec.config().get("root").is_none() {
        StorageConfig::from_value(serde_json::json!({ "root": data_dir.join("store") })).map_err(
            |error| StoreError::InvalidBackendConfig {
                kind: "filesystem".into(),
                detail: error.to_string(),
            },
        )?
    } else {
        spec.config().clone()
    };
    backend.open(&config).await
}

/// Where a node's per-database transaction logs live.
pub enum LogBackend {
    /// Versioned log files under this directory.
    Fs(PathBuf),
    /// In-memory versioned logs shared across a process.
    Mem(MemLogRegistry),
    /// Versioned logs stored through the native root store.
    Native(Arc<dyn NativeLogStorage>),
}

/// Runs filesystem log operations on Tokio's blocking pool while exposing the
/// same async interface used by native storage logs.
struct BlockingTransactionLog(Arc<dyn TransactionLog>);

#[async_trait]
impl TransactionLog for BlockingTransactionLog {
    fn append(&self, record: &TxRecord) -> Result<(), LogError> {
        self.0.append(record)
    }

    async fn append_async(&self, record: &TxRecord) -> Result<(), LogError> {
        let log = Arc::clone(&self.0);
        let record = record.clone();
        tokio::task::spawn_blocking(move || log.append(&record))
            .await
            .map_err(|error| LogError::Native(format!("log task failed: {error}")))?
    }

    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        self.0.tx_range(start, end)
    }

    async fn tx_range_async(
        &self,
        start: u64,
        end: Option<u64>,
    ) -> Result<Vec<TxRecord>, LogError> {
        let log = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || log.tx_range(start, end))
            .await
            .map_err(|error| LogError::Native(format!("log task failed: {error}")))?
    }
}

/// Prevents a storage handle opened for replay from being used to append even
/// when its concrete implementation also supports writes.
struct ReadOnlyTransactionLog(Arc<dyn TransactionLog>);

#[async_trait]
impl TransactionLog for ReadOnlyTransactionLog {
    fn append(&self, _record: &TxRecord) -> Result<(), LogError> {
        Err(LogError::Native("transaction log is read-only".into()))
    }

    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        self.0.tx_range(start, end)
    }

    async fn tx_range_async(
        &self,
        start: u64,
        end: Option<u64>,
    ) -> Result<Vec<TxRecord>, LogError> {
        self.0.tx_range_async(start, end).await
    }
}

impl LogBackend {
    /// The log backend that pairs with `spec`.
    ///
    /// # Panics
    /// Panics if `spec` was not used to open `store` through the registry.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn for_spec(spec: &StoreSpec, data_dir: &std::path::Path, store: Arc<NodeStore>) -> Self {
        let placement = storage_backend(spec.kind())
            .expect("store was opened from a registered backend")
            .capabilities()
            .log_placement;
        match placement {
            LogPlacement::Memory => Self::Mem(MemLogRegistry::new()),
            LogPlacement::Filesystem => Self::Fs(data_dir.join("logs")),
            LogPlacement::RootStore => Self::Native(Arc::new(NativeRootLogStore::new(store))),
        }
    }

    /// Returns the read-only log backend paired with a discovered storage service.
    ///
    /// # Errors
    /// Returns an error when this transactor build lacks the advertised
    /// backend, even if Cargo feature unification enabled it in `corium-store`.
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn for_discovered(
        spec: &DiscoveredStoreSpec,
        store: DiscoveredStore,
    ) -> Result<Self, StorageConnectionError> {
        let backend = storage_backend(spec.kind()).ok_or_else(|| {
            StorageConnectionError::Unsupported(format!(
                "storage backend {:?} is not available",
                spec.kind()
            ))
        })?;
        match backend.capabilities().log_placement {
            LogPlacement::Memory => Err(StorageConnectionError::Unsupported(
                "memory storage cannot be discovered from another process".into(),
            )),
            LogPlacement::Filesystem => {
                let root = spec.filesystem_root().ok_or_else(|| {
                    StorageConnectionError::InvalidConfig {
                        kind: spec.kind().to_owned(),
                        detail: "missing filesystem root".into(),
                    }
                })?;
                Ok(Self::Fs(root.parent().map_or_else(
                    || PathBuf::from("logs"),
                    |data_dir| data_dir.join("logs"),
                )))
            }
            LogPlacement::RootStore => Ok(Self::Native(Arc::new(
                NativeRootLogStore::new_discovered(store),
            ))),
        }
    }

    /// Opens the named log for writing under `write_version`.
    ///
    /// # Errors
    /// Returns an error when a transaction log cannot be opened.
    pub async fn open(
        &self,
        name: &str,
        write_version: u64,
        cipher: Option<Arc<LogCipher>>,
    ) -> Result<Arc<dyn TransactionLog>, LogError> {
        match self {
            Self::Fs(dir) => {
                let dir = dir.clone();
                let name = name.to_owned();
                let log = tokio::task::spawn_blocking(move || match cipher {
                    Some(cipher) => VersionedLog::open_sealed(dir, &name, write_version, cipher),
                    None => VersionedLog::open(dir, &name, write_version),
                })
                .await
                .map_err(|error| LogError::Native(format!("log task failed: {error}")))??;
                Ok(Arc::new(BlockingTransactionLog(Arc::new(log))))
            }
            // In-memory logs hold decoded records in this process's heap and
            // never reach a durable medium, which is the only thing storage
            // encryption protects. Sealing them would encrypt nothing.
            Self::Mem(registry) => Ok(Arc::new(registry.open(name, write_version))),
            Self::Native(storage) => Ok(Arc::new(match cipher {
                Some(cipher) => {
                    NativeVersionedLog::open_sealed(
                        Arc::clone(storage),
                        name,
                        write_version,
                        cipher,
                    )
                    .await?
                }
                None => NativeVersionedLog::open(Arc::clone(storage), name, write_version).await?,
            })),
        }
    }

    /// Opens the named log for independent read-only replay.
    ///
    /// Native logs use a non-writing versioned-log handle; filesystem logs
    /// use the explicit read-only opener so this path never creates or
    /// truncates a source log.
    ///
    /// # Errors
    /// Returns an error when a transaction log cannot be opened.
    pub async fn open_read_only(
        &self,
        name: &str,
        cipher: Option<Arc<LogCipher>>,
    ) -> Result<Arc<dyn TransactionLog>, LogError> {
        match self {
            Self::Fs(dir) => {
                let dir = dir.clone();
                let name = name.to_owned();
                let log = tokio::task::spawn_blocking(move || match cipher {
                    Some(cipher) => VersionedLog::open_read_only_sealed(dir, &name, cipher),
                    None => VersionedLog::open_read_only(dir, &name),
                })
                .await
                .map_err(|error| LogError::Native(format!("log task failed: {error}")))??;
                Ok(Arc::new(ReadOnlyTransactionLog(Arc::new(
                    BlockingTransactionLog(Arc::new(log)),
                ))))
            }
            Self::Mem(registry) => Ok(Arc::new(ReadOnlyTransactionLog(Arc::new(
                registry.open(name, 0),
            )))),
            Self::Native(storage) => Ok(Arc::new(ReadOnlyTransactionLog(Arc::new(match cipher {
                Some(cipher) => {
                    NativeVersionedLog::open_read_only_sealed(Arc::clone(storage), name, cipher)
                }
                None => NativeVersionedLog::open_read_only(Arc::clone(storage), name),
            })))),
        }
    }

    /// Reports whether any log exists for `name`.
    #[must_use]
    pub async fn exists(&self, name: &str) -> bool {
        match self {
            Self::Fs(dir) => {
                let dir = dir.clone();
                let name = name.to_owned();
                tokio::task::spawn_blocking(move || VersionedLog::exists(dir, &name))
                    .await
                    .unwrap_or(false)
            }
            Self::Mem(registry) => registry.exists(name),
            Self::Native(storage) => {
                storage
                    .list_records(name)
                    .await
                    .is_ok_and(|records| !records.is_empty())
                    || storage
                        .list_legacy_chunks(name)
                        .await
                        .is_ok_and(|chunks| !chunks.is_empty())
            }
        }
    }

    /// Deletes every log for `name`.
    ///
    /// # Errors
    /// Returns an error when a transaction log cannot be removed.
    pub async fn delete_all(&self, name: &str) -> Result<(), LogError> {
        match self {
            Self::Fs(dir) => {
                let dir = dir.clone();
                let name = name.to_owned();
                tokio::task::spawn_blocking(move || VersionedLog::delete_all(dir, &name))
                    .await
                    .map_err(|error| LogError::Native(format!("log task failed: {error}")))?
            }
            Self::Mem(registry) => {
                registry.delete_all(name);
                Ok(())
            }
            Self::Native(storage) => storage.delete_all(name).await,
        }
    }
}

struct NativeRootLogStore {
    store: NativeLogRootStore,
}

enum NativeLogRootStore {
    ReadWrite(Arc<NodeStore>),
    Discovered(DiscoveredStore),
}

impl NativeRootLogStore {
    fn new(store: Arc<NodeStore>) -> Self {
        Self {
            store: NativeLogRootStore::ReadWrite(store),
        }
    }

    fn new_discovered(store: DiscoveredStore) -> Self {
        Self {
            store: NativeLogRootStore::Discovered(store),
        }
    }

    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, LogError> {
        match &self.store {
            NativeLogRootStore::ReadWrite(store) => store.get_root(name).await,
            NativeLogRootStore::Discovered(store) => store.get_root(name).await,
        }
        .map_err(|error| LogError::Native(error.to_string()))
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, LogError> {
        match &self.store {
            NativeLogRootStore::ReadWrite(store) => store.list_roots(prefix).await,
            NativeLogRootStore::Discovered(store) => store.list_roots(prefix).await,
        }
        .map_err(|error| LogError::Native(error.to_string()))
    }
}

/// A parsed log object key: either a per-transaction record or a legacy chunk
/// written by the pre-per-record layout.
enum LogKey {
    /// Per-transaction record object `(version, t)`.
    Record(u64, u64),
    /// Legacy chunk object `(version, chunk)`.
    Legacy(u64, u64),
}

impl NativeRootLogStore {
    /// Object key for one per-transaction record: `log:<db>:v<version>:r<t>`,
    /// with both numbers zero-padded so a listing sorts by `(version, t)`.
    fn record_key(name: &str, version: u64, t: u64) -> String {
        format!("log:{name}:v{version:020}:r{t:020}")
    }

    /// Object key for one legacy chunk. Chunk `0` keeps the historical
    /// unsuffixed key, so logs written by earlier releases read back without
    /// migration.
    fn legacy_key(name: &str, version: u64, chunk: u64) -> String {
        if chunk == 0 {
            format!("log:{name}:v{version:020}")
        } else {
            format!("log:{name}:v{version:020}:c{chunk:020}")
        }
    }

    fn prefix(name: &str) -> String {
        format!("log:{name}:v")
    }

    /// Classifies a listed log key. A `:r` suffix marks a per-record object; a
    /// `:c` suffix or a bare version marks a legacy chunk (chunk `0` when
    /// bare). The version is pure digits, so it never contains either marker.
    fn parse_key(prefix: &str, key: &str) -> Option<LogKey> {
        let rest = key.strip_prefix(prefix)?;
        if let Some((version, t)) = rest.split_once(":r") {
            Some(LogKey::Record(version.parse().ok()?, t.parse().ok()?))
        } else if let Some((version, chunk)) = rest.split_once(":c") {
            Some(LogKey::Legacy(version.parse().ok()?, chunk.parse().ok()?))
        } else {
            Some(LogKey::Legacy(rest.parse().ok()?, 0))
        }
    }
}

#[async_trait]
impl NativeLogStorage for NativeRootLogStore {
    async fn put_batch(
        &self,
        name: &str,
        version: u64,
        records: &[(u64, Vec<u8>)],
    ) -> Result<bool, LogError> {
        let Some((last_t, _)) = records.last() else {
            return Ok(true);
        };
        // The whole batch is one immutable object keyed by its last `t`,
        // holding the batch's framed records concatenated — the same encoding
        // a multi-record chunk uses, so the reader decodes it unchanged. On
        // SQL backends this is one row insert (one fsync for the batch); on an
        // object store, one create-only `PUT`. A create-only root CAS
        // (expected `None`) makes it atomic and fenced; a lost race surfaces
        // as `CasFailed`, which the caller maps to `false`.
        let mut bytes = Vec::new();
        for (_, framed) in records {
            bytes.extend_from_slice(framed);
        }
        let NativeLogRootStore::ReadWrite(store) = &self.store else {
            return Err(LogError::Native(
                "transaction log storage is read-only".into(),
            ));
        };
        match store
            .cas_root(&Self::record_key(name, version, *last_t), None, &bytes)
            .await
        {
            Ok(()) => Ok(true),
            Err(StoreError::CasFailed { .. }) => Ok(false),
            Err(error) => Err(LogError::Native(error.to_string())),
        }
    }

    async fn read_record(
        &self,
        name: &str,
        version: u64,
        t: u64,
    ) -> Result<Option<Vec<u8>>, LogError> {
        self.get_root(&Self::record_key(name, version, t)).await
    }

    async fn list_records(&self, name: &str) -> Result<Vec<(u64, u64)>, LogError> {
        let prefix = Self::prefix(name);
        let names = self.list_roots(&prefix).await?;
        names
            .into_iter()
            .filter_map(|key| match Self::parse_key(&prefix, &key) {
                Some(LogKey::Record(version, t)) => Some(Ok((version, t))),
                Some(LogKey::Legacy(..)) => None,
                None => Some(Err(LogError::Corrupt)),
            })
            .collect()
    }

    async fn read_legacy_chunk(
        &self,
        name: &str,
        version: u64,
        chunk: u64,
    ) -> Result<Option<Vec<u8>>, LogError> {
        self.get_root(&Self::legacy_key(name, version, chunk)).await
    }

    async fn list_legacy_chunks(&self, name: &str) -> Result<Vec<(u64, u64)>, LogError> {
        let prefix = Self::prefix(name);
        let names = self.list_roots(&prefix).await?;
        names
            .into_iter()
            .filter_map(|key| match Self::parse_key(&prefix, &key) {
                Some(LogKey::Legacy(version, chunk)) => Some(Ok((version, chunk))),
                Some(LogKey::Record(..)) => None,
                None => Some(Err(LogError::Corrupt)),
            })
            .collect()
    }

    async fn delete_all(&self, name: &str) -> Result<(), LogError> {
        let prefix = Self::prefix(name);
        let NativeLogRootStore::ReadWrite(store) = &self.store else {
            return Err(LogError::Native(
                "transaction log storage is read-only".into(),
            ));
        };
        let names = store
            .list_roots(&prefix)
            .await
            .map_err(|error| LogError::Native(error.to_string()))?;
        for key in names {
            store
                .delete_root(&key)
                .await
                .map_err(|error| LogError::Native(error.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::{Datom, EntityId, Value};

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_storage_info_uses_only_the_read_only_url() {
        let primary = "postgresql://writer:primary-secret@db/corium";
        let read_only = "postgresql://reader:read-secret@db/corium";
        let spec = StoreSpec::postgres(primary);
        let missing = spec
            .connection_info(std::path::Path::new("."), &StorageInfoConfig::default())
            .await
            .expect_err("primary URL must not be advertised");
        assert!(missing.contains("read-only"));

        let info = spec
            .connection_info(
                std::path::Path::new("."),
                &StorageInfoConfig {
                    postgres_connection_string: Some(read_only.into()),
                    ..StorageInfoConfig::default()
                },
            )
            .await
            .expect("read-only info");
        let Some(corium_protocol::pb::storage_connection::Backend::Postgres(postgres)) =
            info.backend
        else {
            panic!("PostgreSQL storage info");
        };
        assert_eq!(postgres.connection_string, read_only);
        assert!(!postgres.connection_string.contains("primary-secret"));
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn s3_storage_info_carries_static_read_only_credentials() {
        let missing =
            DiscoveredStoreSpec::from_connection(corium_protocol::pb::StorageConnection {
                backend: Some(corium_protocol::pb::storage_connection::Backend::S3(
                    corium_protocol::pb::S3Storage {
                        bucket: "bucket".into(),
                        prefix: String::new(),
                        ..Default::default()
                    },
                )),
            })
            .expect_err("S3 clients must not fall back to ambient credentials");
        assert!(missing.to_string().contains("read-only access key"));

        let spec = StoreSpec::s3(
            "bucket",
            "tenant/",
            &S3ClientConfig {
                access_key_id: Some("PRIMARY".into()),
                secret_access_key: Some("primary-secret".into()),
                ..S3ClientConfig::default()
            },
        );
        let info = spec
            .connection_info(
                std::path::Path::new("."),
                &StorageInfoConfig {
                    s3: Some(S3ReadOnlyConfig::new(
                        Some("us-west-2".into()),
                        Some("https://objects.example".into()),
                        S3ReadOnlyCredentials::Static {
                            access_key_id: "READONLY".into(),
                            secret_access_key: "read-secret".into(),
                            session_token: Some("session".into()),
                        },
                    )),
                    ..StorageInfoConfig::default()
                },
            )
            .await
            .expect("read-only info");
        let Some(corium_protocol::pb::storage_connection::Backend::S3(mut s3)) = info.backend
        else {
            panic!("S3 storage info");
        };
        assert_eq!(s3.bucket, "bucket");
        assert_eq!(s3.prefix, "tenant/");
        assert_eq!(s3.access_key_id, "READONLY");
        assert_eq!(s3.secret_access_key, "read-secret");
        assert_eq!(s3.session_token, "session");
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.endpoint_url, "https://objects.example");
        assert_eq!(s3.expires_unix_seconds, 0);
        assert_ne!(s3.access_key_id, "PRIMARY");

        s3.expires_unix_seconds = 1_900_000_000;
        let discovered =
            DiscoveredStoreSpec::from_connection(corium_protocol::pb::StorageConnection {
                backend: Some(corium_protocol::pb::storage_connection::Backend::S3(s3)),
            })
            .expect("parse S3 storage info");
        assert_eq!(
            discovered
                .config()
                .get("expires_unix_seconds")
                .and_then(serde_json::Value::as_i64)
                .expect("temporary credential expiration"),
            1_900_000_000
        );
    }

    #[tokio::test]
    async fn plugin_storage_info_never_falls_back_to_primary_config() {
        let spec = StoreSpec::new(
            "acme-gcs",
            StorageConfig::from_value(serde_json::json!({
                "bucket": "primary",
                "credential": "PRIMARY_WRITE_SECRET"
            }))
            .expect("primary plugin config"),
        );
        let missing = spec
            .connection_info(std::path::Path::new("."), &StorageInfoConfig::default())
            .await
            .expect_err("primary config must not be advertised");
        assert!(missing.contains("separately configured read-only"));

        let info = spec
            .connection_info(
                std::path::Path::new("."),
                &StorageInfoConfig {
                    plugin_config: Some(
                        StorageConfig::from_value(serde_json::json!({
                            "bucket": "primary",
                            "credential": "READ_ONLY_SECRET"
                        }))
                        .expect("read-only plugin config"),
                    ),
                    ..StorageInfoConfig::default()
                },
            )
            .await
            .expect("read-only plugin info");
        let Some(corium_protocol::pb::storage_connection::Backend::Plugin(plugin)) = info.backend
        else {
            panic!("plugin storage info");
        };
        assert_eq!(plugin.kind, "acme-gcs");
        assert!(plugin.config_json.contains("READ_ONLY_SECRET"));
        assert!(!plugin.config_json.contains("PRIMARY_WRITE_SECRET"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_root_log_writes_one_object_per_record_on_the_runtime() {
        let run = async {
            let dir = tempfile::tempdir().expect("tempdir");
            let store = open_node_store(&StoreSpec::memory(), dir.path())
                .await
                .expect("memory store");
            let storage = Arc::new(NativeRootLogStore::new(store));
            let log = NativeVersionedLog::open(Arc::clone(&storage), "db", 1)
                .await
                .expect("open native log");
            // Large records too: each transaction is its own object, so there
            // is no chunk cap to cross.
            for t in 1..=3 {
                log.append_async(&TxRecord {
                    t,
                    tx_instant: i64::try_from(t).expect("small t"),
                    datoms: vec![Datom {
                        e: EntityId::from_raw(t),
                        a: EntityId::from_raw(1),
                        v: Value::Bytes(vec![0; 300 * 1024].into()),
                        tx: EntityId::from_raw(t),
                        added: true,
                    }],
                })
                .await
                .expect("append");
            }
            let records = storage.list_records("db").await.expect("list records");
            assert_eq!(records.len(), 3);
            assert!(
                storage
                    .list_legacy_chunks("db")
                    .await
                    .expect("list legacy")
                    .is_empty()
            );
            assert_eq!(log.replay_async().await.expect("replay").len(), 3);
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .expect("native log operation stalled on its runtime");
    }
}
