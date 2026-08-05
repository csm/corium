//! Safe host adapter for loading Corium storage plugin libraries.

/// Reusable ABI adapters for official plugin crates.
pub mod export;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use abi_stable::library::{RawLibrary, RootModule, lib_header_from_raw_library};
use abi_stable::sabi_trait::TD_Opaque;
use async_trait::async_trait;
use corium_store::{
    BackendCapabilities, BlobId, BlobIdStream, BlobStore, FullStore, LogPlacement, ReadStore,
    RootStore, StorageBackend, StorageConfig, StorageRegistrationError, StoreError,
    register_storage_backends,
};
use corium_store_abi::{
    ABI_VERSION, AbiLogLevel, AbiLogPlacement, BackendBox, LogSink, LogSink_TO, LogSinkBox,
    StoreBox, StorePluginModuleRef,
};
use tokio_stream::wrappers::ReceiverStream;

/// Information about a successfully loaded plugin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedPlugin {
    /// Dynamic-library path.
    pub path: PathBuf,
    /// Plugin-reported package version.
    pub version: String,
    /// Backend kinds registered by this library.
    pub backends: Vec<String>,
}

/// Failure loading or registering a plugin.
#[derive(Debug, thiserror::Error)]
pub enum PluginLoadError {
    /// The library could not be opened or failed `abi_stable` layout checks.
    #[error("cannot load storage plugin {path}: {detail}")]
    Load {
        /// Attempted library path.
        path: PathBuf,
        /// Loader diagnostic, including layout differences.
        detail: String,
    },
    /// The plugin targets another Corium ABI generation.
    #[error("storage plugin ABI mismatch: host is v{host}, plugin is v{plugin}")]
    AbiMismatch {
        /// Host ABI generation.
        host: u32,
        /// Plugin ABI generation.
        plugin: u32,
    },
    /// The plugin exported no backend factories.
    #[error("storage plugin exported no backends")]
    Empty,
    /// A plugin backend could not enter the process registry.
    #[error(transparent)]
    Registration(#[from] StorageRegistrationError),
}

/// Loads one plugin, verifies its ABI, and registers all of its backends.
///
/// Loaded libraries are retained by `abi_stable` for process lifetime; this
/// function never exposes an unload operation.
///
/// # Errors
/// Returns load, layout, version, or registry errors.
pub fn load_storage_plugin(path: impl AsRef<Path>) -> Result<LoadedPlugin, PluginLoadError> {
    let path = path.as_ref().to_path_buf();
    let module = load_module(&path).map_err(|error| PluginLoadError::Load {
        path: path.clone(),
        detail: error.to_string(),
    })?;
    let plugin_abi = module.abi_version();
    if plugin_abi != ABI_VERSION {
        return Err(PluginLoadError::AbiMismatch {
            host: ABI_VERSION,
            plugin: plugin_abi,
        });
    }
    let version = (module.plugin_version())().to_string();
    let factories = (module.backends())();
    if factories.is_empty() {
        return Err(PluginLoadError::Empty);
    }
    let mut backends = Vec::with_capacity(factories.len());
    let mut adapters: Vec<Arc<dyn StorageBackend>> = Vec::with_capacity(factories.len());
    for factory in factories {
        let kind = factory.kind().to_string();
        adapters.push(Arc::new(AbiBackend {
            kind: kind.clone(),
            inner: factory,
        }));
        backends.push(kind);
    }
    register_storage_backends(adapters)?;
    Ok(LoadedPlugin {
        path,
        version,
        backends,
    })
}

fn load_module(path: &Path) -> Result<StorePluginModuleRef, abi_stable::library::LibraryError> {
    // Every library must have its own `RawLibrary`: `RootModule::load_from_file`
    // intentionally caches a single implementation per root-module type.
    // Leaking is required because trait-object vtables, allocation destructors,
    // worker threads, and TLS may all continue pointing into the library.
    let library = Box::leak(Box::new(RawLibrary::load_at(path)?));
    // SAFETY: `lib_header_from_raw_library` first resolves abi_stable's fixed
    // header symbol and validates its own ABI header before returning it.
    let header = unsafe { lib_header_from_raw_library(library) }?;
    header
        .init_root_module::<StorePluginModuleRef>()?
        .initialization()
}

/// Resolves plugin files from `CORIUM_STORE_PLUGINS`.
///
/// Entries are path-separator-delimited files or directories. Directories are
/// searched only for platform dynamic libraries and the current directory is
/// never added implicitly.
#[must_use]
pub fn environment_plugin_paths() -> Vec<PathBuf> {
    std::env::var_os("CORIUM_STORE_PLUGINS")
        .map(|value| {
            std::env::split_paths(&value)
                .flat_map(expand_path)
                .collect()
        })
        .unwrap_or_default()
}

fn expand_path(path: PathBuf) -> Vec<PathBuf> {
    if path.is_file() {
        return vec![path];
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return Vec::new();
    };
    let extension = std::env::consts::DLL_EXTENSION;
    let mut files: Vec<_> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|found| found == extension))
        .collect();
    files.sort();
    files
}

struct AbiBackend {
    kind: String,
    inner: BackendBox,
}

#[async_trait]
impl StorageBackend for AbiBackend {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn capabilities(&self) -> BackendCapabilities {
        let capabilities = self.inner.capabilities();
        BackendCapabilities {
            log_placement: match capabilities.log_placement {
                AbiLogPlacement::Memory => LogPlacement::Memory,
                AbiLogPlacement::Filesystem => LogPlacement::Filesystem,
                AbiLogPlacement::RootStore => LogPlacement::RootStore,
            },
            direct_access: capabilities.direct_access,
        }
    }

    async fn open(&self, config: &StorageConfig) -> Result<Arc<dyn FullStore>, StoreError> {
        let store = abi_result(
            self.inner
                .open(config.to_json().into(), self.log_sink())
                .await,
            self.kind(),
        )?;
        Ok(Arc::new(AbiStore {
            inner: store,
            kind: self.kind().into(),
        }))
    }

    async fn open_existing(
        &self,
        config: &StorageConfig,
    ) -> Result<Arc<dyn ReadStore>, StoreError> {
        let store = abi_result(
            self.inner
                .open_existing(config.to_json().into(), self.log_sink())
                .await,
            self.kind(),
        )?;
        Ok(Arc::new(AbiStore {
            inner: store,
            kind: self.kind().into(),
        }))
    }
}

impl AbiBackend {
    fn log_sink(&self) -> LogSinkBox {
        LogSink_TO::from_value(
            TracingLogSink {
                kind: self.kind.clone(),
            },
            TD_Opaque,
        )
    }
}

struct TracingLogSink {
    kind: String,
}

impl LogSink for TracingLogSink {
    fn event(
        &self,
        level: AbiLogLevel,
        message: abi_stable::std_types::RString,
        fields_json: abi_stable::std_types::RString,
    ) {
        let message = message.as_str();
        let fields = fields_json.as_str();
        match level {
            AbiLogLevel::Debug => tracing::debug!(backend = %self.kind, fields, "{message}"),
            AbiLogLevel::Info => tracing::info!(backend = %self.kind, fields, "{message}"),
            AbiLogLevel::Warn => tracing::warn!(backend = %self.kind, fields, "{message}"),
            AbiLogLevel::Error => tracing::error!(backend = %self.kind, fields, "{message}"),
        }
    }
}

fn abi_result<T>(
    result: abi_stable::std_types::RResult<T, abi_stable::std_types::RString>,
    kind: &str,
) -> Result<T, StoreError> {
    result.into_result().map_err(|detail| StoreError::Backend {
        kind: kind.into(),
        detail: detail.to_string(),
    })
}

struct AbiStore {
    inner: StoreBox,
    kind: String,
}

impl AbiStore {
    fn id(&self, text: impl AsRef<str>) -> Result<BlobId, StoreError> {
        BlobId::from_hex(text.as_ref()).ok_or_else(|| StoreError::Backend {
            kind: self.kind.clone(),
            detail: format!("plugin returned invalid blob id {:?}", text.as_ref()),
        })
    }
}

#[async_trait]
impl BlobStore for AbiStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let id = abi_result(self.inner.put(bytes.to_vec().into()).await, &self.kind)?;
        self.id(id.as_str())
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(
            abi_result(self.inner.get(id.as_str().into()).await, &self.kind)?
                .into_option()
                .map(Into::into),
        )
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        abi_result(self.inner.contains(id.as_str().into()).await, &self.kind)
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        abi_result(self.inner.delete(id.as_str().into()).await, &self.kind)
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        let cursor = abi_result(self.inner.list_open().await, &self.kind)?;
        let kind = self.kind.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let batch = match abi_result(cursor.next(64).await, &kind) {
                    Ok(batch) => batch,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                if batch.is_empty() {
                    return;
                }
                for text in batch {
                    let item = BlobId::from_hex(text.as_str()).ok_or_else(|| StoreError::Backend {
                        kind: kind.clone(),
                        detail: format!("plugin listed invalid blob id {text:?}"),
                    });
                    if sender.send(item).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    async fn modified_at(&self, id: &BlobId) -> Result<Option<std::time::SystemTime>, StoreError> {
        Ok(
            abi_result(self.inner.modified_at(id.as_str().into()).await, &self.kind)?
                .into_option()
                .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis))),
        )
    }
}

#[async_trait]
impl RootStore for AbiStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(
            abi_result(self.inner.get_root(name.into()).await, &self.kind)?
                .into_option()
                .map(Into::into),
        )
    }

    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        abi_result(
            self.inner
                .cas_root(
                    name.into(),
                    expected.map(|bytes| bytes.to_vec().into()).into(),
                    new.to_vec().into(),
                )
                .await,
            &self.kind,
        )
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        abi_result(self.inner.delete_root(name.into()).await, &self.kind)
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        Ok(
            abi_result(self.inner.list_roots(prefix.into()).await, &self.kind)?
                .into_iter()
                .map(Into::into)
                .collect(),
        )
    }
}
