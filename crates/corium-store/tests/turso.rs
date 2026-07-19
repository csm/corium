//! Turso blob-store backend conformance tests.
#![cfg(feature = "turso")]

use std::time::SystemTime;

use corium_store::{BlobStore, StoreError, TursoBlobStore};
use tokio_stream::StreamExt;

#[tokio::test]
async fn turso_blob_store_round_trips_and_reopens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("blobs.db");
    let store = TursoBlobStore::open(&path).await.expect("open store");
    let id = store.put(b"segment").await.expect("put blob");

    assert!(store.contains(&id).await.expect("contains blob"));
    assert_eq!(
        store.get(&id).await.expect("get blob"),
        Some(b"segment".to_vec())
    );
    assert!(
        store
            .modified_at(&id)
            .await
            .expect("blob timestamp")
            .is_some_and(|timestamp| timestamp <= SystemTime::now())
    );

    drop(store);
    let reopened = TursoBlobStore::open(&path).await.expect("reopen store");
    assert_eq!(
        reopened.get(&id).await.expect("get reopened blob"),
        Some(b"segment".to_vec())
    );
}

#[tokio::test]
async fn turso_blob_store_lists_and_deletes_blobs() {
    let database = turso::Builder::new_local(":memory:")
        .build()
        .await
        .expect("in-memory database");
    let store = TursoBlobStore::from_database(database)
        .await
        .expect("initialize store");
    let first = store.put(b"first").await.expect("first blob");
    let second = store.put(b"second").await.expect("second blob");

    let mut listed = store.list().await.expect("list blobs");
    let mut ids = Vec::new();
    while let Some(id) = listed.next().await {
        ids.push(id.expect("listed blob"));
    }
    ids.sort();
    let mut expected = vec![first.clone(), second];
    expected.sort();
    assert_eq!(ids, expected);

    store.delete(&first).await.expect("delete blob");
    store.delete(&first).await.expect("delete missing blob");
    assert!(!store.contains(&first).await.expect("blob was deleted"));
    assert_eq!(store.modified_at(&first).await.expect("missing time"), None);
}

#[tokio::test]
async fn turso_blob_store_detects_corrupt_content() {
    let database = turso::Builder::new_local(":memory:")
        .build()
        .await
        .expect("in-memory database");
    let store = TursoBlobStore::from_database(database.clone())
        .await
        .expect("initialize store");
    let id = store.put(b"original").await.expect("put blob");
    database
        .connect()
        .expect("connect")
        .execute(
            "UPDATE corium_blobs SET data = ?1 WHERE id = ?2",
            (b"corrupt".as_slice(), id.as_str()),
        )
        .await
        .expect("corrupt blob");

    assert!(matches!(
        store.get(&id).await,
        Err(StoreError::CorruptBlob(actual)) if actual == id
    ));
}
