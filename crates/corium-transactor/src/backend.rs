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
use std::time::SystemTime;

use async_trait::async_trait;
use corium_log::{LogError, MemLogRegistry, TransactionLog, VersionedLog};
#[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
use corium_log::{NativeLogStorage, NativeVersionedLog};
use corium_store::{BlobId, BlobIdStream, BlobStore, FsStore, MemoryStore, RootStore, StoreError};

#[cfg(feature = "postgres")]
use corium_store::PostgresBlobStore;
#[cfg(feature = "s3")]
use corium_store::S3BlobStore;
#[cfg(feature = "turso")]
use corium_store::TursoBlobStore;

/// Selects the transactor's storage-service backend (blobs + roots).
#[derive(Clone, Default)]
pub enum StoreSpec {
    /// In-memory blobs and roots; fully ephemeral and confined to one
    /// process. The transaction log is in memory too, so the whole database
    /// vanishes when the process exits — ideal for demos and tests.
    Memory,
    /// Blobs and roots under `{data_dir}/store`, log under `{data_dir}/logs`.
    #[default]
    Fs,
    /// Blobs, roots, and transaction logs in `PostgreSQL`.
    #[cfg(feature = "postgres")]
    Postgres {
        /// `PostgreSQL` URL or keyword/value connection string.
        connection_string: String,
    },
    /// Blobs, roots, and transaction logs in a Turso (embeddable `SQLite`)
    /// database at `path`. `path` is a local database file.
    #[cfg(feature = "turso")]
    Turso {
        /// Filesystem path of the Turso database.
        path: String,
    },
    /// Blobs, roots, and transaction logs in an S3 (or S3-compatible) bucket.
    #[cfg(feature = "s3")]
    S3 {
        /// Target bucket name.
        bucket: String,
        /// Key prefix namespacing every object this store touches.
        prefix: String,
    },
}

impl fmt::Debug for StoreSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => formatter.write_str("Memory"),
            Self::Fs => formatter.write_str("Fs"),
            #[cfg(feature = "postgres")]
            Self::Postgres { .. } => formatter
                .debug_struct("Postgres")
                .field("connection_string", &"[REDACTED]")
                .finish(),
            #[cfg(feature = "turso")]
            Self::Turso { path } => formatter.debug_struct("Turso").field("path", path).finish(),
            #[cfg(feature = "s3")]
            Self::S3 { bucket, prefix } => formatter
                .debug_struct("S3")
                .field("bucket", bucket)
                .field("prefix", prefix)
                .finish(),
        }
    }
}

/// The blob + root storage service a [`crate::node::TransactorNode`] runs
/// over, chosen by [`StoreSpec`]. Dispatch is an enum rather than a trait
/// object so every existing `impl BlobStore + RootStore` / `&dyn RootStore`
/// call site keeps working unchanged.
pub enum NodeStore {
    /// In-memory backend.
    Mem(MemoryStore),
    /// Filesystem backend.
    Fs(FsStore),
    /// `PostgreSQL` backend.
    #[cfg(feature = "postgres")]
    Postgres(PostgresBlobStore),
    /// Turso backend.
    #[cfg(feature = "turso")]
    Turso(TursoBlobStore),
    /// S3 backend.
    #[cfg(feature = "s3")]
    S3(S3BlobStore),
}

impl NodeStore {
    /// Opens the storage service for `spec`, relative to `data_dir` for the
    /// filesystem backend.
    ///
    /// # Errors
    /// Returns an error when the backing store cannot be opened.
    // Only optional database-backed arms await; mem/fs are synchronous.
    #[allow(clippy::unused_async)]
    pub async fn open(spec: &StoreSpec, data_dir: &std::path::Path) -> Result<Self, StoreError> {
        match spec {
            StoreSpec::Memory => Ok(Self::Mem(MemoryStore::default())),
            StoreSpec::Fs => Ok(Self::Fs(FsStore::open(data_dir.join("store"))?)),
            #[cfg(feature = "postgres")]
            StoreSpec::Postgres { connection_string } => Ok(Self::Postgres(
                PostgresBlobStore::connect(connection_string).await?,
            )),
            #[cfg(feature = "turso")]
            StoreSpec::Turso { path } => Ok(Self::Turso(TursoBlobStore::open(path).await?)),
            #[cfg(feature = "s3")]
            StoreSpec::S3 { bucket, prefix } => {
                Ok(Self::S3(S3BlobStore::connect(bucket, prefix).await?))
            }
        }
    }

    /// Opens an existing storage service for peer reads without running
    /// backend schema initialization.
    ///
    /// # Errors
    /// Returns an error when the backing store cannot be opened.
    #[allow(clippy::unused_async)]
    pub async fn open_existing(
        spec: &StoreSpec,
        data_dir: &std::path::Path,
    ) -> Result<Self, StoreError> {
        match spec {
            StoreSpec::Memory => Ok(Self::Mem(MemoryStore::default())),
            StoreSpec::Fs => Ok(Self::Fs(FsStore::open(data_dir.join("store"))?)),
            #[cfg(feature = "postgres")]
            StoreSpec::Postgres { connection_string } => Ok(Self::Postgres(
                PostgresBlobStore::connect_existing(connection_string).await?,
            )),
            #[cfg(feature = "turso")]
            StoreSpec::Turso { path } => {
                Ok(Self::Turso(TursoBlobStore::open_existing(path).await?))
            }
            #[cfg(feature = "s3")]
            StoreSpec::S3 { bucket, prefix } => {
                Ok(Self::S3(S3BlobStore::connect(bucket, prefix).await?))
            }
        }
    }
}

#[async_trait]
impl BlobStore for NodeStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        match self {
            Self::Mem(store) => store.put(bytes).await,
            Self::Fs(store) => store.put(bytes).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.put(bytes).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.put(bytes).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.put(bytes).await,
        }
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        match self {
            Self::Mem(store) => store.get(id).await,
            Self::Fs(store) => store.get(id).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.get(id).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.get(id).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.get(id).await,
        }
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        match self {
            Self::Mem(store) => store.contains(id).await,
            Self::Fs(store) => store.contains(id).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.contains(id).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.contains(id).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.contains(id).await,
        }
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        match self {
            Self::Mem(store) => store.delete(id).await,
            Self::Fs(store) => store.delete(id).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.delete(id).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.delete(id).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.delete(id).await,
        }
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        match self {
            Self::Mem(store) => store.list().await,
            Self::Fs(store) => store.list().await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.list().await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.list().await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.list().await,
        }
    }

    async fn modified_at(&self, id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        match self {
            Self::Mem(store) => store.modified_at(id).await,
            Self::Fs(store) => store.modified_at(id).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.modified_at(id).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.modified_at(id).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.modified_at(id).await,
        }
    }
}

#[async_trait]
impl RootStore for NodeStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        match self {
            Self::Mem(store) => store.get_root(name).await,
            Self::Fs(store) => store.get_root(name).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.get_root(name).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.get_root(name).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.get_root(name).await,
        }
    }

    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        match self {
            Self::Mem(store) => store.cas_root(name, expected, new).await,
            Self::Fs(store) => store.cas_root(name, expected, new).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.cas_root(name, expected, new).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.cas_root(name, expected, new).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.cas_root(name, expected, new).await,
        }
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        match self {
            Self::Mem(store) => store.delete_root(name).await,
            Self::Fs(store) => store.delete_root(name).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.delete_root(name).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.delete_root(name).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.delete_root(name).await,
        }
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        match self {
            Self::Mem(store) => store.list_roots(prefix).await,
            Self::Fs(store) => store.list_roots(prefix).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.list_roots(prefix).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.list_roots(prefix).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.list_roots(prefix).await,
        }
    }
}

/// Where a node's per-database transaction logs live.
pub enum LogBackend {
    /// Versioned log files under this directory.
    Fs(PathBuf),
    /// In-memory versioned logs shared across a process.
    Mem(MemLogRegistry),
    /// Versioned logs stored through the native root store.
    #[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
    Native(Arc<dyn NativeLogStorage>),
}

impl LogBackend {
    /// The log backend that pairs with `spec`.
    #[must_use]
    a#[allow(clippy::needless_pass_by_value)]
    pub fn for_spec(
        spec: &StoreSpec,
        data_dir: &std::path::Path,
        #[cfg_attr(
            not(any(feature = "postgres", feature = "turso", feature = "s3")),
            allow(unused_variables)
        )]
        store: Arc<NodeStore>,
    ) -> Self {
        match spec {
            StoreSpec::Memory => Self::Mem(MemLogRegistry::new()),
            StoreSpec::Fs => Self::Fs(data_dir.join("logs")),
            #[cfg(feature = "postgres")]
            StoreSpec::Postgres { .. } => Self::Native(Arc::new(NativeRootLogStore::new(store))),
            #[cfg(feature = "turso")]
            StoreSpec::Turso { .. } => Self::Native(Arc::new(NativeRootLogStore::new(store))),
            #[cfg(feature = "s3")]
            StoreSpec::S3 { .. } => Self::Native(Arc::new(NativeRootLogStore::new(store))),
        }
    }

    /// Opens the named log for writing under `write_version`.
    ///
    /// # Errors
    /// Returns an error when a filesystem log cannot be opened.
    pub fn open(
        &self,
        name: &str,
        write_version: u64,
    ) -> Result<Arc<dyn TransactionLog>, LogError> {
        match self {
            Self::Fs(dir) => Ok(Arc::new(VersionedLog::open(dir, name, write_version)?)),
            Self::Mem(registry) => Ok(Arc::new(registry.open(name, write_version))),
            #[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
            Self::Native(storage) => Ok(Arc::new(NativeVersionedLog::open(
                Arc::clone(storage),
                name,
                write_version,
            )?)),
        }
    }

    /// Reports whether any log exists for `name`.
    #[must_use]
    pub fn exists(&self, name: &str) -> bool {
        match self {
            Self::Fs(dir) => VersionedLog::exists(dir, name),
            Self::Mem(registry) => registry.exists(name),
            #[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
            Self::Native(storage) => storage
                .versions(name)
                .is_ok_and(|versions| !versions.is_empty()),
        }
    }

    /// Deletes every log for `name`.
    ///
    /// # Errors
    /// Returns an error when a filesystem log cannot be removed.
    pub fn delete_all(&self, name: &str) -> Result<(), LogError> {
        match self {
            Self::Fs(dir) => VersionedLog::delete_all(dir, name),
            Self::Mem(registry) => {
                registry.delete_all(name);
                Ok(())
            }
            #[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
            Self::Native(storage) => storage.delete_versions(name),
        }
    }
}

#[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
struct NativeRootLogStore {
    store: Arc<NodeStore>,
}

#[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
impl NativeRootLogStore {
    fn new(store: Arc<NodeStore>) -> Self {
        Self { store }
    }
}

#[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
impl NativeRootLogStore {
    fn key(name: &str, version: u64) -> String {
        format!("log:{name}:v{version:020}")
    }

    fn prefix(name: &str) -> String {
        format!("log:{name}:v")
    }

    fn block_on<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, StoreError>>,
    ) -> Result<T, LogError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(future)
                .map_err(|error| LogError::Native(error.to_string()))
        })
    }
}

#[cfg(any(feature = "postgres", feature = "turso", feature = "s3"))]
impl NativeLogStorage for NativeRootLogStore {
    fn read_version(&self, name: &str, version: u64) -> Result<Option<Vec<u8>>, LogError> {
        self.block_on(self.store.get_root(&Self::key(name, version)))
    }

    fn cas_version(
        &self,
        name: &str,
        version: u64,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), LogError> {
        self.block_on(
            self.store
                .cas_root(&Self::key(name, version), expected, new),
        )
    }

    fn versions(&self, name: &str) -> Result<Vec<u64>, LogError> {
        let prefix = Self::prefix(name);
        let names = self.block_on(self.store.list_roots(&prefix))?;
        names
            .into_iter()
            .map(|key| {
                key.strip_prefix(&prefix)
                    .and_then(|version| version.parse::<u64>().ok())
                    .ok_or(LogError::Corrupt)
            })
            .collect()
    }

    fn delete_versions(&self, name: &str) -> Result<(), LogError> {
        for version in self.versions(name)? {
            self.block_on(self.store.delete_root(&Self::key(name, version)))?;
        }
        Ok(())
    }
}
