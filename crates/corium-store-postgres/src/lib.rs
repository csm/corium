//! Statically linkable `PostgreSQL` storage backend.

#![allow(missing_docs, non_local_definitions)]

use std::sync::Arc;
use std::sync::OnceLock;

#[cfg(not(feature = "static-link"))]
use abi_stable::export_root_module;
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{RString, RVec};
use async_trait::async_trait;
use corium_store::{
    BackendCapabilities, FullStore, LogPlacement, ReadStore, StorageBackend, StorageConfig,
    StorageRegistrationError, StoreError, register_storage_backend,
};
use corium_store_abi::{
    ABI_VERSION, AbiBackendCapabilities, AbiFuture, AbiLogLevel, AbiLogPlacement,
    Backend as AbiBackend, BackendBox, LogSinkBox, StoreBox, StorePluginModule,
    StorePluginModuleRef,
};
use corium_store_plugin::export::{spawn, store_box};

mod store;
pub use store::PostgresBlobStore;

/// Registers the statically linked `PostgreSQL` backend.
///
/// # Errors
/// Returns an error if another backend already owns the `postgres` kind.
pub fn register() -> Result<(), StorageRegistrationError> {
    register_storage_backend(Arc::new(PostgresBackend))
}

struct PostgresBackend;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("corium-postgres-plugin")
            .build()
            .expect("PostgreSQL plugin runtime")
    })
}

#[derive(serde::Deserialize)]
struct Config {
    connection_string: String,
}

fn invalid_config(error: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidBackendConfig {
        kind: "postgres".into(),
        detail: error.to_string(),
    }
}

#[async_trait]
impl StorageBackend for PostgresBackend {
    fn kind(&self) -> &'static str {
        "postgres"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            log_placement: LogPlacement::RootStore,
            direct_access: true,
        }
    }

    async fn open(&self, config: &StorageConfig) -> Result<Arc<dyn FullStore>, StoreError> {
        let config: Config = config.decode().map_err(invalid_config)?;
        Ok(Arc::new(
            PostgresBlobStore::connect(config.connection_string).await?,
        ))
    }

    async fn open_existing(
        &self,
        config: &StorageConfig,
    ) -> Result<Arc<dyn ReadStore>, StoreError> {
        let config: Config = config.decode().map_err(invalid_config)?;
        Ok(Arc::new(
            PostgresBlobStore::connect_existing(config.connection_string).await?,
        ))
    }
}

struct PluginBackend;

impl AbiBackend for PluginBackend {
    fn kind(&self) -> RString {
        "postgres".into()
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
            "opening PostgreSQL storage".into(),
            "{}".into(),
        );
        spawn(runtime(), async move {
            let config: Config =
                serde_json::from_str(config_json.as_str()).map_err(invalid_config)?;
            let store = PostgresBlobStore::connect(config.connection_string).await?;
            Ok(store_box(Arc::new(store), runtime()))
        })
    }

    fn open_existing(&self, config_json: RString, log: LogSinkBox) -> AbiFuture<StoreBox> {
        log.event(
            AbiLogLevel::Info,
            "opening existing PostgreSQL storage".into(),
            "{}".into(),
        );
        spawn(runtime(), async move {
            let config: Config =
                serde_json::from_str(config_json.as_str()).map_err(invalid_config)?;
            let store = PostgresBlobStore::connect_existing(config.connection_string).await?;
            Ok(store_box(Arc::new(store), runtime()))
        })
    }
}

extern "C" fn plugin_version() -> RString {
    env!("CARGO_PKG_VERSION").into()
}

extern "C" fn backends() -> RVec<BackendBox> {
    vec![BackendBox::from_value(PluginBackend, TD_Opaque)].into()
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
