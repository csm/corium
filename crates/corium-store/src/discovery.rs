//! Shared decoding and opening of storage advertised by a transactor.

use std::fmt;
use std::path::PathBuf;

use corium_protocol::pb;
use serde_json::json;

use crate::{ReadStore, StorageConfig, StoreError, storage_backend};

/// Failure translating storage information advertised by a transactor.
#[derive(Debug, thiserror::Error)]
pub enum StorageConnectionError {
    /// The response carried no storage backend.
    #[error("transactor returned no storage backend")]
    Missing,
    /// The backend cannot be opened by another process, lacks required
    /// read-only credentials, or is not registered in this process.
    #[error("{0}")]
    Unsupported(String),
    /// The backend configuration on the wire is malformed.
    #[error("invalid configuration for storage backend {kind:?}: {detail}")]
    InvalidConfig {
        /// Backend kind whose configuration was invalid.
        kind: String,
        /// Parse or validation detail.
        detail: String,
    },
}

/// A backend-neutral storage-service connection advertised by a transactor.
#[derive(Clone)]
pub struct DiscoveredStoreSpec {
    kind: String,
    config: StorageConfig,
}

impl fmt::Debug for DiscoveredStoreSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiscoveredStoreSpec")
            .field("kind", &self.kind)
            .field("config", &self.config)
            .finish()
    }
}

impl DiscoveredStoreSpec {
    /// Creates a backend-neutral discovered store specification.
    #[must_use]
    pub fn new(kind: impl Into<String>, config: StorageConfig) -> Self {
        Self {
            kind: kind.into(),
            config,
        }
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

    /// Mutable configuration access for host-owned runtime extensions.
    #[must_use]
    pub const fn config_mut(&mut self) -> &mut StorageConfig {
        &mut self.config
    }

    /// Returns the filesystem store root when this is a filesystem spec.
    #[must_use]
    pub fn filesystem_root(&self) -> Option<PathBuf> {
        (self.kind == "filesystem")
            .then(|| self.config.get("root")?.as_str().map(PathBuf::from))
            .flatten()
    }

    /// Decodes storage information returned by `GetStorageInfo`.
    ///
    /// # Errors
    /// Returns an error for missing, process-local, unavailable, or malformed
    /// backend information.
    pub fn from_connection(
        connection: pb::StorageConnection,
    ) -> Result<Self, StorageConnectionError> {
        use pb::storage_connection::Backend;

        let (kind, config) = match connection.backend.ok_or(StorageConnectionError::Missing)? {
            Backend::Memory(_) => {
                return Err(StorageConnectionError::Unsupported(
                    "memory storage is confined to the transactor process".into(),
                ));
            }
            Backend::Filesystem(storage) => (
                "filesystem".to_owned(),
                StorageConfig::from_value(json!({
                    "root": PathBuf::from(storage.data_dir).join("store")
                })),
            ),
            Backend::Postgres(storage) => (
                "postgres".to_owned(),
                StorageConfig::from_value(json!({
                    "connection_string": storage.connection_string
                })),
            ),
            Backend::Turso(storage) => (
                "turso".to_owned(),
                StorageConfig::from_value(json!({ "path": storage.path })),
            ),
            Backend::S3(storage) => {
                if storage.access_key_id.is_empty() {
                    return Err(StorageConnectionError::Unsupported(
                        "S3 storage info omitted the read-only access key id".into(),
                    ));
                }
                if storage.secret_access_key.is_empty() {
                    return Err(StorageConnectionError::Unsupported(
                        "S3 storage info omitted the read-only secret access key".into(),
                    ));
                }
                (
                    "s3".to_owned(),
                    StorageConfig::from_value(json!({
                        "bucket": storage.bucket,
                        "prefix": storage.prefix,
                        "region": nonempty(storage.region),
                        "endpoint_url": nonempty(storage.endpoint_url),
                        "access_key_id": storage.access_key_id,
                        "secret_access_key": storage.secret_access_key,
                        "session_token": nonempty(storage.session_token),
                        "expires_unix_seconds": (storage.expires_unix_seconds > 0)
                            .then_some(storage.expires_unix_seconds),
                    })),
                )
            }
            Backend::Plugin(storage) => {
                (storage.kind, StorageConfig::from_json(&storage.config_json))
            }
        };
        let config = config.map_err(|error| StorageConnectionError::InvalidConfig {
            kind: kind.clone(),
            detail: error.to_string(),
        })?;
        if storage_backend(&kind).is_none() {
            return Err(StorageConnectionError::Unsupported(format!(
                "storage backend {kind:?} is not available; install and load its plugin"
            )));
        }
        Ok(Self { kind, config })
    }

    /// Opens the advertised store without creating storage or schema.
    ///
    /// # Errors
    /// Returns a reachability, configuration, or backend error.
    pub async fn open_existing(self) -> Result<DiscoveredStore, StoreError> {
        let backend = storage_backend(&self.kind)
            .ok_or_else(|| StoreError::BackendUnavailable(self.kind.clone()))?;
        backend.open_existing(&self.config).await
    }
}

/// An opened, read-only store discovered through the transactor.
pub type DiscoveredStore = std::sync::Arc<dyn ReadStore>;

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
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

    #[test]
    fn plugin_configuration_is_redacted() {
        let spec = DiscoveredStoreSpec::new(
            "acme-gcs",
            StorageConfig::from_json(r#"{"token":"secret"}"#).expect("config"),
        );
        let debug = format!("{spec:?}");
        assert!(debug.contains("acme-gcs"));
        assert!(!debug.contains("secret"));
    }
}
