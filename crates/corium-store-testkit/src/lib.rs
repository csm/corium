//! Reusable conformance checks for Corium blob and root stores.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use corium_store::{FullStore, StoreError};
use tokio_stream::StreamExt;

/// Successful live-backend verification summary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerificationReport {
    /// Number of contract checks completed.
    pub checks: u32,
}

/// Storage operation or contract failure reported by [`verify`].
#[derive(Debug, thiserror::Error)]
pub enum VerificationError {
    /// The backend returned an operation error.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The backend completed an operation but violated the storage contract.
    #[error("storage conformance failed: {0}")]
    Contract(String),
}

fn contract(condition: bool, detail: impl Into<String>) -> Result<(), VerificationError> {
    condition
        .then_some(())
        .ok_or_else(|| VerificationError::Contract(detail.into()))
}

/// Runs non-destructive blob and fenced-root contract checks against a live store.
///
/// The verifier creates uniquely named data, then removes the blob and roots it
/// created. Backends should run it against a disposable namespace when possible.
///
/// # Errors
/// Returns the first backend operation or contract failure.
pub async fn verify(store: Arc<dyn FullStore>) -> Result<VerificationReport, VerificationError> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let root = format!("testkit:{}:{nonce}", std::process::id());
    let prefix = format!("testkit:{}:", std::process::id());
    let payload = format!("corium-store-testkit:{}:{nonce}", std::process::id()).into_bytes();

    let id = store.put(&payload).await?;
    contract(
        store.put(&payload).await? == id,
        "blob put is not idempotent",
    )?;
    contract(store.contains(&id).await?, "new blob is not contained")?;
    contract(
        store.get(&id).await? == Some(payload),
        "blob did not round-trip",
    )?;

    let mut listed = store.list().await?;
    let mut found = false;
    while let Some(item) = listed.next().await {
        if item? == id {
            found = true;
        }
    }
    contract(found, "new blob is absent from listing")?;

    store.cas_root(&root, None, b"v1").await?;
    contract(
        store.get_root(&root).await? == Some(b"v1".to_vec()),
        "root did not round-trip",
    )?;
    contract(
        store.cas_root(&root, None, b"stale").await.is_err(),
        "stale root creation was not fenced",
    )?;
    store.cas_root(&root, Some(b"v1"), b"v2").await?;
    contract(
        store
            .list_roots(&prefix)
            .await?
            .iter()
            .any(|name| name == &root),
        "new root is absent from prefix listing",
    )?;

    store.delete_root(&root).await?;
    contract(store.get_root(&root).await?.is_none(), "root delete failed")?;
    store.delete(&id).await?;
    contract(!store.contains(&id).await?, "blob delete failed")?;

    Ok(VerificationReport { checks: 10 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_store_conforms() {
        let report = verify(Arc::new(corium_store::MemoryStore::default()))
            .await
            .expect("memory conformance");
        assert_eq!(report.checks, 10);
    }
}
