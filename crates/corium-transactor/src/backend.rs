//! Pluggable transactor storage backends.
//!
//! A transactor keeps two kinds of durable state: the content-addressed
//! blob store plus fenced root pointers (the "storage service"), and the
//! per-database transaction log. [`StoreSpec`] selects the storage service
//! backend — in-memory, filesystem, or Turso — and [`NodeStore`] dispatches
//! the [`BlobStore`]/[`RootStore`] operations to it. The log stays local
//! (in-memory for `mem`, filesystem otherwise) because the commit pipeline
//! appends to it synchronously; see `docs/design/log-and-transactor.md`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use corium_log::{LogError, MemLogRegistry, TransactionLog, VersionedLog};
use corium_store::{BlobId, BlobIdStream, BlobStore, FsStore, MemoryStore, RootStore, StoreError};

#[cfg(feature = "turso")]
use corium_store::TursoBlobStore;

/// Selects the transactor's storage-service backend (blobs + roots).
#[derive(Clone, Debug, Default)]
pub enum StoreSpec {
    /// In-memory blobs and roots; fully ephemeral and confined to one
    /// process. The transaction log is in memory too, so the whole database
    /// vanishes when the process exits — ideal for demos and tests.
    Memory,
    /// Blobs and roots under `{data_dir}/store`, log under `{data_dir}/logs`.
    #[default]
    Fs,
    /// Blobs and roots in a Turso (embeddable `SQLite`) database at `path`;
    /// the transaction log stays on the local filesystem under the data
    /// directory. `path` is a local database file.
    #[cfg(feature = "turso")]
    Turso {
        /// Filesystem path of the Turso database.
        path: String,
    },
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
    /// Turso backend.
    #[cfg(feature = "turso")]
    Turso(TursoBlobStore),
}

impl NodeStore {
    /// Opens the storage service for `spec`, relative to `data_dir` for the
    /// filesystem backend.
    ///
    /// # Errors
    /// Returns an error when the backing store cannot be opened.
    // Only the Turso arm awaits; the mem/fs arms are synchronous.
    #[allow(clippy::unused_async)]
    pub async fn open(spec: &StoreSpec, data_dir: &std::path::Path) -> Result<Self, StoreError> {
        match spec {
            StoreSpec::Memory => Ok(Self::Mem(MemoryStore::default())),
            StoreSpec::Fs => Ok(Self::Fs(FsStore::open(data_dir.join("store"))?)),
            #[cfg(feature = "turso")]
            StoreSpec::Turso { path } => Ok(Self::Turso(TursoBlobStore::open(path).await?)),
        }
    }
}

#[async_trait]
impl BlobStore for NodeStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        match self {
            Self::Mem(store) => store.put(bytes).await,
            Self::Fs(store) => store.put(bytes).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.put(bytes).await,
        }
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        match self {
            Self::Mem(store) => store.get(id).await,
            Self::Fs(store) => store.get(id).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.get(id).await,
        }
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        match self {
            Self::Mem(store) => store.contains(id).await,
            Self::Fs(store) => store.contains(id).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.contains(id).await,
        }
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        match self {
            Self::Mem(store) => store.delete(id).await,
            Self::Fs(store) => store.delete(id).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.delete(id).await,
        }
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        match self {
            Self::Mem(store) => store.list().await,
            Self::Fs(store) => store.list().await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.list().await,
        }
    }

    async fn modified_at(&self, id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        match self {
            Self::Mem(store) => store.modified_at(id).await,
            Self::Fs(store) => store.modified_at(id).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.modified_at(id).await,
        }
    }
}

#[async_trait]
impl RootStore for NodeStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        match self {
            Self::Mem(store) => store.get_root(name).await,
            Self::Fs(store) => store.get_root(name).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.get_root(name).await,
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
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.cas_root(name, expected, new).await,
        }
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        match self {
            Self::Mem(store) => store.delete_root(name).await,
            Self::Fs(store) => store.delete_root(name).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.delete_root(name).await,
        }
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        match self {
            Self::Mem(store) => store.list_roots(prefix).await,
            Self::Fs(store) => store.list_roots(prefix).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.list_roots(prefix).await,
        }
    }
}

/// Where a node's per-database transaction logs live. The mem backend keeps
/// them in a process-shared registry; every other backend uses versioned
/// files under a directory, exactly as before store selection existed.
pub enum LogBackend {
    /// Versioned log files under this directory.
    Fs(PathBuf),
    /// In-memory versioned logs shared across a process.
    Mem(MemLogRegistry),
}

impl LogBackend {
    /// The log backend that pairs with `spec`.
    #[must_use]
    pub fn for_spec(spec: &StoreSpec, data_dir: &std::path::Path) -> Self {
        match spec {
            StoreSpec::Memory => Self::Mem(MemLogRegistry::new()),
            StoreSpec::Fs => Self::Fs(data_dir.join("logs")),
            #[cfg(feature = "turso")]
            StoreSpec::Turso { .. } => Self::Fs(data_dir.join("logs")),
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
        }
    }
}
