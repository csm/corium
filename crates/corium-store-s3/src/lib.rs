//! Statically linkable S3 storage backend.

#![allow(missing_docs, non_local_definitions)]

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, UNIX_EPOCH};

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
pub use store::{S3BlobStore, S3ClientConfig, normalize_s3_prefix};

/// Registers the statically linked S3 backend.
///
/// # Errors
/// Returns an error if another backend already owns the `s3` kind.
pub fn register() -> Result<(), StorageRegistrationError> {
    register_storage_backend(Arc::new(S3Backend))
}

struct S3Backend;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("corium-s3-plugin")
            .build()
            .expect("S3 plugin runtime")
    })
}

#[derive(serde::Deserialize)]
struct Config {
    bucket: String,
    #[serde(default)]
    prefix: String,
    region: Option<String>,
    endpoint_url: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    expires_unix_seconds: Option<u64>,
}

impl Config {
    fn client(&self, storage: &StorageConfig) -> S3ClientConfig {
        S3ClientConfig {
            region: self.region.clone(),
            endpoint_url: self.endpoint_url.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            session_token: self.session_token.clone(),
            expires_after: self
                .expires_unix_seconds
                .and_then(|seconds| UNIX_EPOCH.checked_add(Duration::from_secs(seconds))),
            credentials_provider: storage
                .extension::<aws_credential_types::provider::SharedCredentialsProvider>()
                .map(|provider| (*provider).clone()),
        }
    }
}

fn invalid_config(error: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidBackendConfig {
        kind: "s3".into(),
        detail: error.to_string(),
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    fn kind(&self) -> &'static str {
        "s3"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            log_placement: LogPlacement::RootStore,
            direct_access: true,
        }
    }

    async fn open(&self, config: &StorageConfig) -> Result<Arc<dyn FullStore>, StoreError> {
        let decoded: Config = config.decode().map_err(invalid_config)?;
        let client = decoded.client(config);
        Ok(Arc::new(
            S3BlobStore::connect_with_config(decoded.bucket, decoded.prefix, &client).await?,
        ))
    }

    async fn open_existing(
        &self,
        config: &StorageConfig,
    ) -> Result<Arc<dyn ReadStore>, StoreError> {
        let decoded: Config = config.decode().map_err(invalid_config)?;
        let client = decoded.client(config);
        Ok(Arc::new(
            S3BlobStore::connect_existing_with_config(decoded.bucket, decoded.prefix, &client)
                .await?,
        ))
    }
}

struct PluginBackend;

impl AbiBackend for PluginBackend {
    fn kind(&self) -> RString {
        "s3".into()
    }

    fn capabilities(&self) -> AbiBackendCapabilities {
        AbiBackendCapabilities {
            log_placement: AbiLogPlacement::RootStore,
            direct_access: true,
        }
    }

    fn open(&self, config_json: RString, log: LogSinkBox) -> AbiFuture<StoreBox> {
        log.event(AbiLogLevel::Info, "opening S3 storage".into(), "{}".into());
        spawn(runtime(), async move {
            let storage = StorageConfig::from_json(config_json.as_str()).map_err(invalid_config)?;
            let config: Config = storage.decode().map_err(invalid_config)?;
            let client = config.client(&storage);
            let store =
                S3BlobStore::connect_with_config(config.bucket, config.prefix, &client).await?;
            Ok(store_box(Arc::new(store), runtime()))
        })
    }

    fn open_existing(&self, config_json: RString, log: LogSinkBox) -> AbiFuture<StoreBox> {
        log.event(
            AbiLogLevel::Info,
            "opening existing S3 storage".into(),
            "{}".into(),
        );
        spawn(runtime(), async move {
            let storage = StorageConfig::from_json(config_json.as_str()).map_err(invalid_config)?;
            let config: Config = storage.decode().map_err(invalid_config)?;
            let client = config.client(&storage);
            let store =
                S3BlobStore::connect_existing_with_config(config.bucket, config.prefix, &client)
                    .await?;
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
