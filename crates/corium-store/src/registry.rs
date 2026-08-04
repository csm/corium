//! Runtime storage-backend registry and backend-neutral configuration.

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, OnceLock, RwLock};

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{BlobStore, FsStore, MemoryStore, RootStore, StoreError};

/// A JSON object passed unchanged to a storage backend.
///
/// Its debug representation is always redacted because backend configuration
/// commonly contains credentials.
#[derive(Clone, Default)]
pub struct StorageConfig {
    json: serde_json::Map<String, serde_json::Value>,
    extensions: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl PartialEq for StorageConfig {
    fn eq(&self, other: &Self) -> bool {
        self.json == other.json
    }
}

impl StorageConfig {
    /// Creates an empty configuration object.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Converts a serializable object into backend configuration.
    ///
    /// # Errors
    /// Returns an error when `value` is not representable as a JSON object.
    pub fn from_value(value: impl Serialize) -> Result<Self, serde_json::Error> {
        match serde_json::to_value(value)? {
            serde_json::Value::Object(json) => Ok(Self {
                json,
                extensions: HashMap::new(),
            }),
            _ => Err(<serde_json::Error as serde::de::Error>::custom(
                "storage configuration must be a JSON object",
            )),
        }
    }

    /// Parses a JSON configuration object.
    ///
    /// # Errors
    /// Returns an error for invalid JSON or a non-object value.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        Self::from_value(serde_json::from_str::<serde_json::Value>(json)?)
    }

    /// Decodes this configuration into a backend-owned type.
    ///
    /// # Errors
    /// Returns an error when fields do not match the requested type.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(serde_json::Value::Object(self.json.clone()))
    }

    /// Returns the canonical JSON object text used at plugin boundaries.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::Value::Object(self.json.clone()).to_string()
    }

    /// Returns one configuration field.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.json.get(key)
    }

    /// Attaches process-local runtime state for a statically linked backend.
    ///
    /// Extensions never cross the plugin ABI or appear in serialized config.
    pub fn insert_extension<T: Any + Send + Sync>(&mut self, value: T) {
        self.extensions.insert(TypeId::of::<T>(), Arc::new(value));
    }

    /// Returns process-local runtime state attached by the host.
    #[must_use]
    pub fn extension<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        self.extensions
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast().ok())
    }
}

impl fmt::Debug for StorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StorageConfig([REDACTED])")
    }
}

/// Placement of transaction logs for stores opened by a backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogPlacement {
    /// Logs are process-local and ephemeral.
    Memory,
    /// Logs are files adjacent to the filesystem store root.
    Filesystem,
    /// Logs are records in the backend's root store.
    RootStore,
}

/// Backend behavior the engine needs without knowing the backend's name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendCapabilities {
    /// Where transaction logs for the backend are stored.
    pub log_placement: LogPlacement,
    /// Whether another process can open a read-only connection to this store.
    pub direct_access: bool,
}

/// A read/write blob and root store.
pub trait FullStore: BlobStore + RootStore {
    /// Concrete type access for backend-specific diagnostics.
    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T> FullStore for T
where
    T: BlobStore + RootStore + 'static,
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A store opened for the engine's read-only discovery path.
///
/// The underlying traits retain mutation methods for source compatibility;
/// conforming backends must return handles that do not grant write authority.
pub trait ReadStore: BlobStore + RootStore {}

impl<T> ReadStore for T where T: BlobStore + RootStore + ?Sized {}

/// Factory for one runtime-selectable storage backend.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Stable backend kind, such as `"s3"` or `"acme-gcs"`.
    fn kind(&self) -> &str;

    /// Behavior declared by this backend.
    fn capabilities(&self) -> BackendCapabilities;

    /// Opens or initializes a read/write store.
    ///
    /// # Errors
    /// Returns a backend or configuration error.
    async fn open(&self, config: &StorageConfig) -> Result<Arc<dyn FullStore>, StoreError>;

    /// Opens an existing store for reads without creating it.
    ///
    /// # Errors
    /// Returns a backend or configuration error.
    async fn open_existing(&self, config: &StorageConfig)
    -> Result<Arc<dyn ReadStore>, StoreError>;
}

/// Failure registering a storage backend.
#[derive(Debug, thiserror::Error)]
pub enum StorageRegistrationError {
    /// Backend kinds must be non-empty lowercase identifiers.
    #[error("invalid storage backend kind {0:?}")]
    InvalidKind(String),
    /// A backend already owns this kind.
    #[error("storage backend {0:?} is already registered")]
    Duplicate(String),
}

type Registry = BTreeMap<String, Arc<dyn StorageBackend>>;

fn registry() -> &'static RwLock<Registry> {
    static REGISTRY: OnceLock<RwLock<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut backends: Registry = BTreeMap::new();
        backends.insert("memory".into(), Arc::new(MemoryBackend));
        backends.insert("filesystem".into(), Arc::new(FilesystemBackend));
        RwLock::new(backends)
    })
}

fn valid_kind(kind: &str) -> bool {
    !kind.is_empty()
        && kind
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// Registers a compiled-in or dynamically loaded backend for this process.
///
/// # Errors
/// Returns an error for an invalid or already registered kind.
pub fn register_storage_backend(
    backend: Arc<dyn StorageBackend>,
) -> Result<(), StorageRegistrationError> {
    register_storage_backends([backend])
}

/// Atomically registers several factories from one compiled unit or plugin.
///
/// # Errors
/// Returns an error without changing the registry if any kind is invalid or
/// already registered, including duplicates within `backends`.
pub fn register_storage_backends(
    backends: impl IntoIterator<Item = Arc<dyn StorageBackend>>,
) -> Result<(), StorageRegistrationError> {
    let backends: Vec<_> = backends.into_iter().collect();
    let mut registry = registry()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut kinds = std::collections::BTreeSet::new();
    for backend in &backends {
        let kind = backend.kind().to_owned();
        if !valid_kind(&kind) {
            return Err(StorageRegistrationError::InvalidKind(kind));
        }
        if registry.contains_key(&kind) || !kinds.insert(kind.clone()) {
            return Err(StorageRegistrationError::Duplicate(kind));
        }
    }
    for backend in backends {
        registry.insert(backend.kind().to_owned(), backend);
    }
    Ok(())
}

/// Looks up a backend factory by kind.
#[must_use]
pub fn storage_backend(kind: &str) -> Option<Arc<dyn StorageBackend>> {
    registry()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(kind)
        .cloned()
}

/// Lists backend kinds available for direct-storage connections.
#[must_use]
pub fn available_storage_backends() -> Vec<String> {
    registry()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|(_, backend)| backend.capabilities().direct_access)
        .map(|(kind, _)| kind.clone())
        .collect()
}

struct MemoryBackend;

#[async_trait]
impl StorageBackend for MemoryBackend {
    fn kind(&self) -> &'static str {
        "memory"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            log_placement: LogPlacement::Memory,
            direct_access: false,
        }
    }

    async fn open(&self, _config: &StorageConfig) -> Result<Arc<dyn FullStore>, StoreError> {
        Ok(Arc::new(MemoryStore::default()))
    }

    async fn open_existing(
        &self,
        _config: &StorageConfig,
    ) -> Result<Arc<dyn ReadStore>, StoreError> {
        Err(StoreError::BackendUnavailable("memory".into()))
    }
}

struct FilesystemBackend;

#[derive(serde::Deserialize)]
struct FilesystemConfig {
    root: std::path::PathBuf,
}

impl FilesystemBackend {
    fn config(config: &StorageConfig) -> Result<FilesystemConfig, StoreError> {
        config
            .decode()
            .map_err(|error| StoreError::InvalidBackendConfig {
                kind: "filesystem".into(),
                detail: error.to_string(),
            })
    }
}

#[async_trait]
impl StorageBackend for FilesystemBackend {
    fn kind(&self) -> &'static str {
        "filesystem"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            log_placement: LogPlacement::Filesystem,
            direct_access: true,
        }
    }

    async fn open(&self, config: &StorageConfig) -> Result<Arc<dyn FullStore>, StoreError> {
        Ok(Arc::new(FsStore::open(Self::config(config)?.root)?))
    }

    async fn open_existing(
        &self,
        config: &StorageConfig,
    ) -> Result<Arc<dyn ReadStore>, StoreError> {
        let root = Self::config(config)?.root;
        if !root.join("blobs").is_dir() || !root.join("roots").is_dir() {
            return Err(StoreError::UnreachableLocalStorage(root));
        }
        Ok(Arc::new(FsStore::open(root)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestBackend;

    #[async_trait]
    impl StorageBackend for TestBackend {
        fn kind(&self) -> &'static str {
            "test-registry"
        }
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                log_placement: LogPlacement::RootStore,
                direct_access: true,
            }
        }
        async fn open(&self, _: &StorageConfig) -> Result<Arc<dyn FullStore>, StoreError> {
            Ok(Arc::new(MemoryStore::default()))
        }
        async fn open_existing(&self, _: &StorageConfig) -> Result<Arc<dyn ReadStore>, StoreError> {
            Ok(Arc::new(MemoryStore::default()))
        }
    }

    #[test]
    fn registers_external_backend_and_rejects_duplicates() {
        register_storage_backend(Arc::new(TestBackend)).expect("register backend");
        assert!(storage_backend("test-registry").is_some());
        assert!(matches!(
            register_storage_backend(Arc::new(TestBackend)),
            Err(StorageRegistrationError::Duplicate(kind)) if kind == "test-registry"
        ));
    }

    #[test]
    fn configuration_debug_is_redacted() {
        let config = StorageConfig::from_json(r#"{"token":"secret"}"#).expect("config");
        assert_eq!(format!("{config:?}"), "StorageConfig([REDACTED])");
    }
}
