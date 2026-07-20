//! `PostgreSQL` store integration tests.
//!
//! Set `CORIUM_TEST_POSTGRES_URL` to run these against a disposable database.
#![cfg(feature = "postgres")]

use std::time::{SystemTime, UNIX_EPOCH};

use corium_store::{BlobStore, PostgresBlobStore, RootStore, StoreError};
use tokio_stream::StreamExt;

#[tokio::test]
async fn postgres_store_conforms() {
    let Ok(url) = std::env::var("CORIUM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let prefix = format!("corium-test:{}:{nonce}:%_:", std::process::id());
    let root = format!("{prefix}root");
    let other_root = format!("{prefix}other");
    let race_root = format!("{prefix}race");
    let bytes = format!("postgres-segment:{}:{nonce}", std::process::id()).into_bytes();

    let store = PostgresBlobStore::connect(&url)
        .await
        .expect("connect store");
    let id = store.put(&bytes).await.expect("put blob");
    assert_eq!(store.put(&bytes).await.expect("repeat put"), id);
    assert!(store.contains(&id).await.expect("contains blob"));
    assert_eq!(store.get(&id).await.expect("get blob"), Some(bytes));
    assert!(
        store
            .modified_at(&id)
            .await
            .expect("blob timestamp")
            .is_some_and(|timestamp| timestamp <= SystemTime::now())
    );

    let mut listed = store.list().await.expect("list blobs");
    let mut found = false;
    while let Some(listed_id) = listed.next().await {
        if listed_id.expect("listed blob") == id {
            found = true;
        }
    }
    assert!(found, "new blob was not listed");

    store
        .cas_root(&root, None, b"v1")
        .await
        .expect("initial root publish");
    assert!(matches!(
        store.cas_root(&root, None, b"stale").await,
        Err(StoreError::CasFailed { actual: Some(actual), .. }) if actual == b"v1"
    ));
    store
        .cas_root(&root, Some(b"v1"), b"v2")
        .await
        .expect("fenced root update");
    store
        .cas_root(&other_root, None, b"other")
        .await
        .expect("second root");
    assert_eq!(
        store.list_roots(&prefix).await.expect("prefix scan"),
        vec![other_root.clone(), root.clone()]
    );

    // A second pool observes the same durable data and shares the CAS fence.
    let reopened = PostgresBlobStore::connect(&url)
        .await
        .expect("second store");
    assert_eq!(
        reopened.get_root(&root).await.expect("read root"),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        reopened.get(&id).await.expect("read blob"),
        store.get(&id).await.expect("read original store")
    );

    let first_store = store.clone();
    let first_root = race_root.clone();
    let second_store = reopened.clone();
    let second_root = race_root.clone();
    let (first, second) = tokio::join!(
        async move { first_store.cas_root(&first_root, None, b"first").await },
        async move { second_store.cas_root(&second_root, None, b"second").await }
    );
    assert!(
        (first.is_ok() && matches!(&second, Err(StoreError::CasFailed { .. })))
            || (second.is_ok() && matches!(&first, Err(StoreError::CasFailed { .. }))),
        "exactly one concurrent publisher must cross an absent-root fence"
    );

    reopened.delete_root(&root).await.expect("delete root");
    reopened
        .delete_root(&other_root)
        .await
        .expect("delete second root");
    reopened
        .delete_root(&race_root)
        .await
        .expect("delete raced root");
    reopened.delete(&id).await.expect("delete blob");
    reopened.delete(&id).await.expect("repeat delete");
    assert!(!reopened.contains(&id).await.expect("deleted blob"));
}
