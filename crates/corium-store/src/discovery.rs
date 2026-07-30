//! Shared decoding and opening of storage advertised by a transactor.

use std::fmt;
use std::path::PathBuf;
use std::time::SystemTime;
#[cfg(feature = "s3")]
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use corium_protocol::pb;

#[cfg(feature = "postgres")]
use crate::PostgresBlobStore;
#[cfg(feature = "turso")]
use crate::TursoBlobStore;
use crate::{BlobId, BlobIdStream, BlobStore, FsStore, RootStore, StoreError};
#[cfg(feature = "s3")]
use crate::{S3BlobStore, S3ClientConfig};

/// Failure translating storage information advertised by a transactor.
#[derive(Debug, thiserror::Error)]
pub enum StorageConnectionError {
    /// The response carried no storage backend.
    #[error("transactor returned no storage backend")]
    Missing,
    /// The backend cannot be opened by another process, lacks required
    /// read-only credentials, or was omitted from this build.
    #[error("{0}")]
    Unsupported(String),
}

/// A read-only storage-service connection advertised by a transactor.
///
/// This is the single protobuf-to-store mapping used by peers, backups, and
/// language facades. Local paths are resolved to the concrete store root here
/// so the `{data_dir}/store` convention cannot drift between clients.
#[derive(Clone)]
pub enum DiscoveredStoreSpec {
    /// Filesystem blobs and roots under an existing store root.
    Filesystem {
        /// Absolute or process-local path to the store root.
        root: PathBuf,
    },
    /// Existing `PostgreSQL` store.
    #[cfg(feature = "postgres")]
    Postgres {
        /// Read-only connection string.
        connection_string: String,
    },
    /// Existing local Turso database.
    #[cfg(feature = "turso")]
    Turso {
        /// Database file path.
        path: PathBuf,
    },
    /// Existing S3 prefix.
    #[cfg(feature = "s3")]
    S3 {
        /// Bucket name.
        bucket: String,
        /// Corium key prefix.
        prefix: String,
        /// Read-only client configuration.
        client: S3ClientConfig,
    },
}

impl fmt::Debug for DiscoveredStoreSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filesystem { root } => formatter
                .debug_struct("Filesystem")
                .field("root", root)
                .finish(),
            #[cfg(feature = "postgres")]
            Self::Postgres { .. } => formatter
                .debug_struct("Postgres")
                .field("connection_string", &"[REDACTED]")
                .finish(),
            #[cfg(feature = "turso")]
            Self::Turso { path } => formatter.debug_struct("Turso").field("path", path).finish(),
            #[cfg(feature = "s3")]
            Self::S3 {
                bucket,
                prefix,
                client,
            } => formatter
                .debug_struct("S3")
                .field("bucket", bucket)
                .field("prefix", prefix)
                .field("client", client)
                .finish(),
        }
    }
}

impl DiscoveredStoreSpec {
    /// Decodes storage information returned by `GetStorageInfo`.
    ///
    /// # Errors
    /// Returns an error for a missing or process-local memory backend,
    /// missing read-only credentials, or a backend omitted from this build.
    pub fn from_connection(
        connection: pb::StorageConnection,
    ) -> Result<Self, StorageConnectionError> {
        use pb::storage_connection::Backend;

        match connection.backend.ok_or(StorageConnectionError::Missing)? {
            Backend::Memory(_) => Err(StorageConnectionError::Unsupported(
                "memory storage is confined to the transactor process".into(),
            )),
            Backend::Filesystem(storage) => Ok(Self::Filesystem {
                root: PathBuf::from(storage.data_dir).join("store"),
            }),
            Backend::Postgres(storage) => {
                #[cfg(feature = "postgres")]
                {
                    Ok(Self::Postgres {
                        connection_string: storage.connection_string,
                    })
                }
                #[cfg(not(feature = "postgres"))]
                {
                    let _ = storage;
                    Err(StorageConnectionError::Unsupported(
                        "this build lacks PostgreSQL direct-storage support".into(),
                    ))
                }
            }
            Backend::Turso(storage) => {
                #[cfg(feature = "turso")]
                {
                    Ok(Self::Turso {
                        path: storage.path.into(),
                    })
                }
                #[cfg(not(feature = "turso"))]
                {
                    let _ = storage;
                    Err(StorageConnectionError::Unsupported(
                        "this build lacks Turso direct-storage support".into(),
                    ))
                }
            }
            Backend::S3(storage) => {
                #[cfg(feature = "s3")]
                {
                    let access_key_id = nonempty(storage.access_key_id).ok_or_else(|| {
                        StorageConnectionError::Unsupported(
                            "S3 storage info omitted the read-only access key id".into(),
                        )
                    })?;
                    let secret_access_key =
                        nonempty(storage.secret_access_key).ok_or_else(|| {
                            StorageConnectionError::Unsupported(
                                "S3 storage info omitted the read-only secret access key".into(),
                            )
                        })?;
                    Ok(Self::S3 {
                        bucket: storage.bucket,
                        prefix: storage.prefix,
                        client: S3ClientConfig {
                            region: nonempty(storage.region),
                            endpoint_url: nonempty(storage.endpoint_url),
                            access_key_id: Some(access_key_id),
                            secret_access_key: Some(secret_access_key),
                            session_token: nonempty(storage.session_token),
                            expires_after: unix_timestamp(storage.expires_unix_seconds),
                            ..S3ClientConfig::default()
                        },
                    })
                }
                #[cfg(not(feature = "s3"))]
                {
                    let _ = storage;
                    Err(StorageConnectionError::Unsupported(
                        "this build lacks S3 direct-storage support".into(),
                    ))
                }
            }
        }
    }

    /// Opens the advertised store without creating storage or schema.
    ///
    /// # Errors
    /// Returns [`StoreError::UnreachableLocalStorage`] before opening a
    /// filesystem or Turso path that does not already exist. Other errors
    /// report connection, credential, or backend failures.
    #[allow(clippy::unused_async)]
    pub async fn open_existing(self) -> Result<DiscoveredStore, StoreError> {
        match self {
            Self::Filesystem { root } => {
                if !root.join("blobs").is_dir() || !root.join("roots").is_dir() {
                    return Err(StoreError::UnreachableLocalStorage(root));
                }
                Ok(DiscoveredStore::Filesystem(FsStore::open(root)?))
            }
            #[cfg(feature = "postgres")]
            Self::Postgres { connection_string } => Ok(DiscoveredStore::Postgres(
                PostgresBlobStore::connect_existing(connection_string).await?,
            )),
            #[cfg(feature = "turso")]
            Self::Turso { path } => {
                if !path.is_file() {
                    return Err(StoreError::UnreachableLocalStorage(path));
                }
                Ok(DiscoveredStore::Turso(
                    TursoBlobStore::open_existing(path).await?,
                ))
            }
            #[cfg(feature = "s3")]
            Self::S3 {
                bucket,
                prefix,
                client,
            } => Ok(DiscoveredStore::S3(
                S3BlobStore::connect_existing_with_config(bucket, prefix, &client).await?,
            )),
        }
    }
}

/// An opened store discovered through the transactor.
pub enum DiscoveredStore {
    /// Existing filesystem store.
    Filesystem(FsStore),
    /// Existing `PostgreSQL` store.
    #[cfg(feature = "postgres")]
    Postgres(PostgresBlobStore),
    /// Existing Turso store.
    #[cfg(feature = "turso")]
    Turso(TursoBlobStore),
    /// Existing S3 store.
    #[cfg(feature = "s3")]
    S3(S3BlobStore),
}

#[async_trait]
impl BlobStore for DiscoveredStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        match self {
            Self::Filesystem(store) => store.put(bytes).await,
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
            Self::Filesystem(store) => store.get(id).await,
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
            Self::Filesystem(store) => store.contains(id).await,
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
            Self::Filesystem(store) => store.delete(id).await,
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
            Self::Filesystem(store) => store.list().await,
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
            Self::Filesystem(store) => store.modified_at(id).await,
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
impl RootStore for DiscoveredStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        match self {
            Self::Filesystem(store) => store.get_root(name).await,
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
            Self::Filesystem(store) => store.cas_root(name, expected, new).await,
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
            Self::Filesystem(store) => store.delete_root(name).await,
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
            Self::Filesystem(store) => store.list_roots(prefix).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.list_roots(prefix).await,
            #[cfg(feature = "turso")]
            Self::Turso(store) => store.list_roots(prefix).await,
            #[cfg(feature = "s3")]
            Self::S3(store) => store.list_roots(prefix).await,
        }
    }
}

#[cfg(feature = "s3")]
fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

#[cfg(feature = "s3")]
fn unix_timestamp(seconds: i64) -> Option<SystemTime> {
    u64::try_from(seconds)
        .ok()
        .filter(|seconds| *seconds > 0)
        .and_then(|seconds| UNIX_EPOCH.checked_add(Duration::from_secs(seconds)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn filesystem_discovery_does_not_create_an_unreachable_store() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let spec = DiscoveredStoreSpec::from_connection(pb::StorageConnection {
            backend: Some(pb::storage_connection::Backend::Filesystem(
                pb::FilesystemStorage {
                    data_dir: directory.path().to_string_lossy().into_owned(),
                },
            )),
        })
        .expect("filesystem spec");

        let error = spec
            .open_existing()
            .await
            .err()
            .expect("missing store must fail");
        assert!(matches!(error, StoreError::UnreachableLocalStorage(path) if path == root));
        assert!(!root.exists());
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_discovery_does_not_create_an_unreachable_database() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("missing.db");
        let spec = DiscoveredStoreSpec::from_connection(pb::StorageConnection {
            backend: Some(pb::storage_connection::Backend::Turso(pb::TursoStorage {
                path: path.to_string_lossy().into_owned(),
            })),
        })
        .expect("Turso spec");

        let error = spec
            .open_existing()
            .await
            .err()
            .expect("missing database must fail");
        assert!(matches!(error, StoreError::UnreachableLocalStorage(found) if found == path));
        assert!(!path.exists());
    }
}
