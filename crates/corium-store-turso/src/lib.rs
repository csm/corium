//! Turso's statically linkable backend and dynamically loadable plugin.

#![allow(non_local_definitions)]

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

#[cfg(not(feature = "static-link"))]
use abi_stable::export_root_module;
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{RString, RVec};
use corium_store::{
    BackendCapabilities, FullStore, LogPlacement, ReadStore, StorageBackend, StorageConfig,
    StorageRegistrationError, StoreError, register_storage_backend,
};
use corium_store_abi::{
    ABI_VERSION, AbiBackendCapabilities, AbiFuture, AbiLogLevel, AbiLogPlacement, Backend,
    BackendBox, LogSinkBox, StoreBox, StorePluginModule, StorePluginModuleRef,
};
use corium_store_plugin::export::{spawn, store_box};

mod store;
pub use store::TursoBlobStore;

/// Registers the statically linked Turso backend.
///
/// # Errors
/// Returns an error if another backend already owns the `turso` kind.
pub fn register() -> Result<(), StorageRegistrationError> {
    register_storage_backend(Arc::new(StaticBackend))
}

struct StaticBackend;

#[async_trait::async_trait]
impl StorageBackend for StaticBackend {
    fn kind(&self) -> &'static str {
        "turso"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            log_placement: LogPlacement::RootStore,
            direct_access: true,
        }
    }

    async fn open(&self, config: &StorageConfig) -> Result<Arc<dyn FullStore>, StoreError> {
        let config: Config = config
            .decode()
            .map_err(|error| StoreError::InvalidBackendConfig {
                kind: "turso".into(),
                detail: error.to_string(),
            })?;
        Ok(Arc::new(TursoBlobStore::open(config.path).await?))
    }

    async fn open_existing(
        &self,
        config: &StorageConfig,
    ) -> Result<Arc<dyn ReadStore>, StoreError> {
        let config: Config = config
            .decode()
            .map_err(|error| StoreError::InvalidBackendConfig {
                kind: "turso".into(),
                detail: error.to_string(),
            })?;
        if !config.path.is_file() {
            return Err(StoreError::UnreachableLocalStorage(config.path));
        }
        Ok(Arc::new(TursoBlobStore::open_existing(config.path).await?))
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("corium-turso-plugin")
            .build()
            .expect("Turso plugin runtime")
    })
}

#[derive(serde::Deserialize)]
struct Config {
    path: PathBuf,
}

fn invalid_config(error: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidBackendConfig {
        kind: "turso".into(),
        detail: error.to_string(),
    }
}

struct TursoBackend;

impl Backend for TursoBackend {
    fn kind(&self) -> RString {
        "turso".into()
    }

    fn capabilities(&self) -> AbiBackendCapabilities {
        AbiBackendCapabilities {
            log_placement: AbiLogPlacement::RootStore,
            direct_access: true,
        }
    }

    fn open(&self, config_json: RString, log: LogSinkBox) -> AbiFuture<StoreBox> {
        log.event(
            AbiLogLevel::Info,
            "opening Turso storage".into(),
            "{}".into(),
        );
        spawn(runtime(), async move {
            let config: Config =
                serde_json::from_str(config_json.as_str()).map_err(invalid_config)?;
            let store = TursoBlobStore::open(config.path).await?;
            Ok(store_box(Arc::new(store), runtime()))
        })
    }

    fn open_existing(&self, config_json: RString, log: LogSinkBox) -> AbiFuture<StoreBox> {
        log.event(
            AbiLogLevel::Info,
            "opening existing Turso storage".into(),
            "{}".into(),
        );
        spawn(runtime(), async move {
            let config: Config =
                serde_json::from_str(config_json.as_str()).map_err(invalid_config)?;
            if !config.path.is_file() {
                return Err(StoreError::UnreachableLocalStorage(config.path));
            }
            let store = TursoBlobStore::open_existing(config.path).await?;
            Ok(store_box(Arc::new(store), runtime()))
        })
    }
}

extern "C" fn plugin_version() -> RString {
    env!("CARGO_PKG_VERSION").into()
}

extern "C" fn backends() -> RVec<BackendBox> {
    vec![BackendBox::from_value(TursoBackend, TD_Opaque)].into()
}

/// Exports the v1 Corium storage root module.
#[cfg_attr(not(feature = "static-link"), export_root_module)]
#[must_use]
pub fn corium_store_plugin_v1() -> StorePluginModuleRef {
    StorePluginModule {
        abi_version: ABI_VERSION,
        plugin_version,
        backends,
    }
    .leak_into_prefix()
}

/// The statically linked Turso implementation.
pub use TursoBlobStore as StaticTursoBlobStore;
