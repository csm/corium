//! Turso's statically linkable backend and dynamically loadable plugin.

#![allow(non_local_definitions)]

use std::collections::VecDeque;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::UNIX_EPOCH;

#[cfg(not(feature = "static-link"))]
use abi_stable::export_root_module;
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use async_ffi::{FfiFuture, FutureExt};
use corium_store::{
    BackendCapabilities, BlobId, BlobStore, FullStore, LogPlacement, ReadStore, RootStore,
    StorageBackend, StorageConfig, StorageRegistrationError, StoreError, register_storage_backend,
};
use corium_store_abi::{
    ABI_VERSION, AbiBackendCapabilities, AbiFuture, AbiLogLevel, AbiLogPlacement, AbiResult,
    Backend, BackendBox, ListCursor, ListCursor_TO, ListCursorBox, LogSinkBox, Store, Store_TO,
    StoreBox, StorePluginModule, StorePluginModuleRef,
};
use tokio_stream::StreamExt;

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

struct AbortOnDrop<T> {
    receiver: tokio::sync::oneshot::Receiver<AbiResult<T>>,
    task: tokio::task::AbortHandle,
}

impl<T> Future for AbortOnDrop<T> {
    type Output = AbiResult<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(context) {
            Poll::Ready(Ok(output)) => Poll::Ready(output),
            Poll::Ready(Err(_)) => Poll::Ready(RResult::RErr("plugin task stopped".into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn spawn<T, F>(future: F) -> FfiFuture<AbiResult<T>>
where
    T: Send + 'static,
    F: Future<Output = AbiResult<T>> + Send + 'static,
{
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let task = runtime().spawn(async move {
        let _ = sender.send(future.await);
    });
    AbortOnDrop {
        receiver,
        task: task.abort_handle(),
    }
    .into_ffi()
}

fn result<T>(value: Result<T, impl std::fmt::Display>) -> AbiResult<T> {
    match value {
        Ok(value) => RResult::ROk(value),
        Err(error) => RResult::RErr(error.to_string().into()),
    }
}

#[derive(serde::Deserialize)]
struct Config {
    path: PathBuf,
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
        spawn(async move {
            let config: Config = match serde_json::from_str(config_json.as_str()) {
                Ok(config) => config,
                Err(error) => return RResult::RErr(error.to_string().into()),
            };
            result(
                TursoBlobStore::open(config.path)
                    .await
                    .map(|store| Store_TO::from_value(TursoStore(Arc::new(store)), TD_Opaque)),
            )
        })
    }

    fn open_existing(&self, config_json: RString, log: LogSinkBox) -> AbiFuture<StoreBox> {
        log.event(
            AbiLogLevel::Info,
            "opening existing Turso storage".into(),
            "{}".into(),
        );
        spawn(async move {
            let config: Config = match serde_json::from_str(config_json.as_str()) {
                Ok(config) => config,
                Err(error) => return RResult::RErr(error.to_string().into()),
            };
            if !config.path.is_file() {
                return RResult::RErr(
                    format!("Turso database is unreachable: {}", config.path.display()).into(),
                );
            }
            result(
                TursoBlobStore::open_existing(config.path)
                    .await
                    .map(|store| Store_TO::from_value(TursoStore(Arc::new(store)), TD_Opaque)),
            )
        })
    }
}

struct TursoStore(Arc<TursoBlobStore>);

impl Store for TursoStore {
    fn put(&self, bytes: RVec<u8>) -> AbiFuture<RString> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            result(
                store
                    .put(bytes.as_slice())
                    .await
                    .map(|id| id.to_string().into()),
            )
        })
    }

    fn get(&self, id: RString) -> AbiFuture<ROption<RVec<u8>>> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let Some(id) = BlobId::from_hex(id.as_str()) else {
                return RResult::RErr("invalid blob id".into());
            };
            result(
                store
                    .get(&id)
                    .await
                    .map(|bytes| bytes.map(Into::into).into()),
            )
        })
    }

    fn contains(&self, id: RString) -> AbiFuture<bool> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let Some(id) = BlobId::from_hex(id.as_str()) else {
                return RResult::RErr("invalid blob id".into());
            };
            result(store.contains(&id).await)
        })
    }

    fn delete(&self, id: RString) -> AbiFuture<()> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let Some(id) = BlobId::from_hex(id.as_str()) else {
                return RResult::RErr("invalid blob id".into());
            };
            result(store.delete(&id).await)
        })
    }

    fn list_open(&self) -> AbiFuture<ListCursorBox> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let mut stream = match store.list().await {
                Ok(stream) => stream,
                Err(error) => return RResult::RErr(error.to_string().into()),
            };
            let mut ids = VecDeque::new();
            while let Some(id) = stream.next().await {
                match id {
                    Ok(id) => ids.push_back(id.to_string().into()),
                    Err(error) => return RResult::RErr(error.to_string().into()),
                }
            }
            RResult::ROk(ListCursor_TO::from_value(
                TursoCursor(Mutex::new(ids)),
                TD_Opaque,
            ))
        })
    }

    fn modified_at(&self, id: RString) -> AbiFuture<ROption<u64>> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let Some(id) = BlobId::from_hex(id.as_str()) else {
                return RResult::RErr("invalid blob id".into());
            };
            result(store.modified_at(&id).await.map(|instant| {
                instant
                    .and_then(|instant| instant.duration_since(UNIX_EPOCH).ok())
                    .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                    .into()
            }))
        })
    }

    fn get_root(&self, name: RString) -> AbiFuture<ROption<RVec<u8>>> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            result(
                store
                    .get_root(name.as_str())
                    .await
                    .map(|value| value.map(Into::into).into()),
            )
        })
    }

    fn cas_root(&self, name: RString, expected: ROption<RVec<u8>>, new: RVec<u8>) -> AbiFuture<()> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let expected: Option<RVec<u8>> = expected.into_option();
            result(
                store
                    .cas_root(
                        name.as_str(),
                        expected.as_ref().map(RVec::as_slice),
                        new.as_slice(),
                    )
                    .await,
            )
        })
    }

    fn delete_root(&self, name: RString) -> AbiFuture<()> {
        let store = Arc::clone(&self.0);
        spawn(async move { result(store.delete_root(name.as_str()).await) })
    }

    fn list_roots(&self, prefix: RString) -> AbiFuture<RVec<RString>> {
        let store = Arc::clone(&self.0);
        spawn(async move {
            result(store.list_roots(prefix.as_str()).await.map(|roots| {
                roots
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<RString>>()
                    .into()
            }))
        })
    }
}

struct TursoCursor(Mutex<VecDeque<RString>>);

impl ListCursor for TursoCursor {
    fn next(&self, max_items: u32) -> AbiFuture<RVec<RString>> {
        let mut items = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let batch: Vec<_> = (0..max_items).filter_map(|_| items.pop_front()).collect();
        async move { RResult::ROk(batch.into()) }.into_ffi()
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

#[cfg(test)]
mod plugin_tests {
    use super::*;

    struct DropSignal(std::sync::mpsc::Sender<()>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn dropping_abi_future_aborts_plugin_task() {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let future = spawn(async move {
            let _guard = DropSignal(dropped_tx);
            let _ = started_tx.send(());
            std::future::pending::<AbiResult<()>>().await
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("plugin task started");
        drop(future);
        dropped_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("plugin task was aborted");
    }
}
