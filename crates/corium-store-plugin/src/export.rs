//! Helpers for exporting a core store through the stable plugin ABI.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::UNIX_EPOCH;

use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use async_ffi::{FfiFuture, FutureExt};
use corium_store::{BlobId, BlobStore, FullStore, RootStore, StoreError};
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
        spawn(self.runtime, async move {
            let mut stream = store.list().await?;
            let mut ids = VecDeque::new();
            while let Some(id) = stream.next().await {
                ids.push_back(id?.to_string().into());
            }
            Ok(ListCursor_TO::from_value(
                Cursor(Mutex::new(ids)),
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

struct Cursor(Mutex<VecDeque<RString>>);

impl ListCursor for Cursor {
    fn next(&self, max_items: u32) -> AbiFuture<RVec<RString>> {
        let mut items = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let batch: Vec<_> = (0..max_items).filter_map(|_| items.pop_front()).collect();
        async move { RResult::ROk(batch.into()) }.into_ffi()
    }
}
