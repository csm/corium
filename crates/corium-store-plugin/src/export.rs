//! Helpers for exporting a core store through the stable plugin ABI.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::UNIX_EPOCH;

use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use async_ffi::{FfiFuture, FutureExt};
use corium_store::{BlobId, BlobIdStream, BlobStore, FullStore, RootStore, StoreError};
use corium_store_abi::{
    AbiFuture, AbiResult, ListCursor, ListCursor_TO, ListCursorBox, Store, Store_TO, StoreBox,
};
use tokio_stream::StreamExt;

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

/// Runs plugin work on its owning runtime and makes future drop cancel it.
pub fn spawn<T, F>(runtime: &'static tokio::runtime::Runtime, future: F) -> FfiFuture<AbiResult<T>>
where
    T: Send + 'static,
    F: Future<Output = Result<T, StoreError>> + Send + 'static,
{
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let task = runtime.spawn(async move {
        let output = match future.await {
            Ok(value) => RResult::ROk(value),
            Err(error) => RResult::RErr(error.to_string().into()),
        };
        let _ = sender.send(output);
    });
    AbortOnDrop {
        receiver,
        task: task.abort_handle(),
    }
    .into_ffi()
}

/// Wraps a core store in an allocator-safe ABI trait object.
#[must_use]
pub fn store_box(store: Arc<dyn FullStore>, runtime: &'static tokio::runtime::Runtime) -> StoreBox {
    Store_TO::from_value(StoreAdapter { store, runtime }, TD_Opaque)
}

struct StoreAdapter {
    store: Arc<dyn FullStore>,
    runtime: &'static tokio::runtime::Runtime,
}

impl Store for StoreAdapter {
    fn put(&self, bytes: RVec<u8>) -> AbiFuture<RString> {
        let store = Arc::clone(&self.store);
        spawn(self.runtime, async move {
            store
                .put(bytes.as_slice())
                .await
                .map(|id| id.to_string().into())
        })
    }

    fn get(&self, id: RString) -> AbiFuture<ROption<RVec<u8>>> {
        let store = Arc::clone(&self.store);
        spawn(self.runtime, async move {
            let id = blob_id(&id)?;
            store
                .get(&id)
                .await
                .map(|bytes| bytes.map(Into::into).into())
        })
    }

    fn contains(&self, id: RString) -> AbiFuture<bool> {
        let store = Arc::clone(&self.store);
        spawn(
            self.runtime,
            async move { store.contains(&blob_id(&id)?).await },
        )
    }

    fn delete(&self, id: RString) -> AbiFuture<()> {
        let store = Arc::clone(&self.store);
        spawn(
            self.runtime,
            async move { store.delete(&blob_id(&id)?).await },
        )
    }

    fn list_open(&self) -> AbiFuture<ListCursorBox> {
        let store = Arc::clone(&self.store);
        let runtime = self.runtime;
        spawn(self.runtime, async move {
            Ok(ListCursor_TO::from_value(
                Cursor {
                    stream: Arc::new(tokio::sync::Mutex::new(store.list().await?)),
                    runtime,
                },
                TD_Opaque,
            ))
        })
    }

    fn modified_at(&self, id: RString) -> AbiFuture<ROption<u64>> {
        let store = Arc::clone(&self.store);
        spawn(self.runtime, async move {
            Ok(store
                .modified_at(&blob_id(&id)?)
                .await?
                .and_then(|instant| instant.duration_since(UNIX_EPOCH).ok())
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                .into())
        })
    }

    fn get_root(&self, name: RString) -> AbiFuture<ROption<RVec<u8>>> {
        let store = Arc::clone(&self.store);
        spawn(self.runtime, async move {
            store
                .get_root(name.as_str())
                .await
                .map(|value| value.map(Into::into).into())
        })
    }

    fn cas_root(&self, name: RString, expected: ROption<RVec<u8>>, new: RVec<u8>) -> AbiFuture<()> {
        let store = Arc::clone(&self.store);
        spawn(self.runtime, async move {
            let expected: Option<RVec<u8>> = expected.into_option();
            store
                .cas_root(
                    name.as_str(),
                    expected.as_ref().map(RVec::as_slice),
                    new.as_slice(),
                )
                .await
        })
    }

    fn delete_root(&self, name: RString) -> AbiFuture<()> {
        let store = Arc::clone(&self.store);
        spawn(self.runtime, async move {
            store.delete_root(name.as_str()).await
        })
    }

    fn list_roots(&self, prefix: RString) -> AbiFuture<RVec<RString>> {
        let store = Arc::clone(&self.store);
        spawn(self.runtime, async move {
            store.list_roots(prefix.as_str()).await.map(|roots| {
                roots
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<RString>>()
                    .into()
            })
        })
    }
}

fn blob_id(id: &RString) -> Result<BlobId, StoreError> {
    BlobId::from_hex(id.as_str()).ok_or_else(|| StoreError::Backend {
        kind: "plugin".into(),
        detail: "invalid blob id".into(),
    })
}

struct Cursor {
    stream: Arc<tokio::sync::Mutex<BlobIdStream>>,
    runtime: &'static tokio::runtime::Runtime,
}

impl ListCursor for Cursor {
    fn next(&self, max_items: u32) -> AbiFuture<RVec<RString>> {
        let stream = Arc::clone(&self.stream);
        spawn(self.runtime, async move {
            let mut stream = stream.lock().await;
            let mut batch = Vec::with_capacity(max_items as usize);
            for _ in 0..max_items {
                match stream.next().await {
                    Some(Ok(id)) => batch.push(id.to_string().into()),
                    Some(Err(error)) => return Err(error),
                    None => break,
                }
            }
            Ok(batch.into())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use corium_store::{BlobIdStream, BlobStore, RootStore, digest};

    use super::*;

    struct LazyListStore {
        yielded: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BlobStore for LazyListStore {
        async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
            Ok(digest(bytes))
        }

        async fn get(&self, _id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
            Ok(None)
        }

        async fn delete(&self, _id: &BlobId) -> Result<(), StoreError> {
            Ok(())
        }

        async fn list(&self) -> Result<BlobIdStream, StoreError> {
            let yielded = Arc::clone(&self.yielded);
            let ids = [digest(b"one"), digest(b"two"), digest(b"three")];
            Ok(Box::pin(tokio_stream::iter(ids).map(move |id| {
                yielded.fetch_add(1, Ordering::SeqCst);
                Ok(id)
            })))
        }
    }

    #[async_trait]
    impl RootStore for LazyListStore {
        async fn get_root(&self, _name: &str) -> Result<Option<Vec<u8>>, StoreError> {
            Ok(None)
        }

        async fn cas_root(
            &self,
            _name: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> Result<(), StoreError> {
            Ok(())
        }

        async fn delete_root(&self, _name: &str) -> Result<(), StoreError> {
            Ok(())
        }

        async fn list_roots(&self, _prefix: &str) -> Result<Vec<String>, StoreError> {
            Ok(Vec::new())
        }
    }

    fn runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("test runtime")
        })
    }

    #[test]
    fn list_cursor_pulls_only_the_requested_batch() {
        let yielded = Arc::new(AtomicUsize::new(0));
        let store = store_box(
            Arc::new(LazyListStore {
                yielded: Arc::clone(&yielded),
            }),
            runtime(),
        );

        runtime().block_on(async {
            let cursor = store.list_open().await.into_result().expect("open cursor");
            assert_eq!(yielded.load(Ordering::SeqCst), 0);

            let first = cursor.next(2).await.into_result().expect("first batch");
            assert_eq!(first.len(), 2);
            assert_eq!(yielded.load(Ordering::SeqCst), 2);

            let second = cursor.next(2).await.into_result().expect("second batch");
            assert_eq!(second.len(), 1);
            assert_eq!(yielded.load(Ordering::SeqCst), 3);
        });
    }

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
        let future = spawn(runtime(), async move {
            let _guard = DropSignal(dropped_tx);
            let _ = started_tx.send(());
            std::future::pending::<Result<(), StoreError>>().await
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
