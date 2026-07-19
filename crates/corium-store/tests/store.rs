//! Store backend conformance tests.

use std::collections::HashMap;

use corium_store::{BlobStore, FsStore, MemoryStore, RootStore, SegmentCache, mark_and_sweep};
use tokio_stream::StreamExt;

#[tokio::test]
async fn trait_objects_support_async_calls_and_streamed_blob_listing() {
    let store = MemoryStore::default();
    let blobs: &dyn BlobStore = &store;
    let first = blobs.put(b"first").await.expect("put first");
    let second = blobs.put(b"second").await.expect("put second");

    let mut listed = blobs.list().await.expect("start listing");
    let mut ids = Vec::new();
    while let Some(id) = listed.next().await {
        ids.push(id.expect("listed id"));
    }
    ids.sort();
    let mut expected = vec![first, second];
    expected.sort();
    assert_eq!(ids, expected);

    let roots: &dyn RootStore = &store;
    roots
        .cas_root("db:test", None, b"root")
        .await
        .expect("publish root");
    assert_eq!(
        roots.list_roots("db:").await.expect("list roots"),
        vec!["db:test".to_owned()]
    );
}

#[tokio::test]
async fn filesystem_blob_listing_is_streamed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");
    let first = store.put(b"first").await.expect("put first");
    let second = store.put(b"second").await.expect("put second");
    std::fs::write(dir.path().join("blobs/not-a-blob"), b"ignored").expect("write unrelated file");

    let mut listed = store.list().await.expect("start listing");
    let mut ids = Vec::new();
    while let Some(id) = listed.next().await {
        ids.push(id.expect("listed id"));
    }
    ids.sort();
    let mut expected = vec![first, second];
    expected.sort();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn memory_store_cas_fences_roots() {
    let store = MemoryStore::default();
    store
        .cas_root("eavt", None, b"old")
        .await
        .expect("initial publish");
    assert!(store.cas_root("eavt", None, b"new").await.is_err());
    store
        .cas_root("eavt", Some(b"old"), b"new")
        .await
        .expect("fenced publish");
    assert_eq!(
        store.get_root("eavt").await.expect("root"),
        Some(b"new".to_vec())
    );
}

#[tokio::test]
async fn filesystem_store_round_trips_blobs_and_roots() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");
    let id = store.put(b"segment").await.expect("put blob");
    assert_eq!(
        store.get(&id).await.expect("get blob"),
        Some(b"segment".to_vec())
    );
    store
        .cas_root("avet", None, id.as_str().as_bytes())
        .await
        .expect("publish root");

    let reopened = FsStore::open(dir.path()).expect("reopen store");
    assert_eq!(
        reopened.get_root("avet").await.expect("root"),
        Some(id.as_str().as_bytes().to_vec())
    );
}

#[tokio::test]
async fn filesystem_root_lock_file_remains_for_future_contenders() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");

    store
        .cas_root("eavt", None, b"root")
        .await
        .expect("publish root");

    assert!(dir.path().join("roots/eavt.lock").is_file());
}

#[tokio::test]
async fn cache_loads_blob_once_visible() {
    let store = MemoryStore::default();
    let id = store.put(b"cached").await.expect("put blob");
    let cache = SegmentCache::default();
    let loaded = cache
        .get_or_load(&store, &id)
        .await
        .expect("load")
        .expect("present");
    assert_eq!(&*loaded, b"cached");
}

#[tokio::test]
async fn filesystem_store_detects_corrupt_blob() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");
    let id = store.put(b"original").await.expect("put blob");
    std::fs::write(dir.path().join("blobs").join(id.as_str()), b"corrupt").expect("corrupt blob");
    assert!(store.get(&id).await.is_err());
}

#[tokio::test]
async fn blob_put_is_idempotent_for_same_content() {
    let store = MemoryStore::default();
    let first = store.put(b"same").await.expect("first put");
    let second = store.put(b"same").await.expect("second put");
    assert_eq!(first, second);
    assert_eq!(
        store.get(&first).await.expect("get blob"),
        Some(b"same".to_vec())
    );
}

#[tokio::test]
async fn filesystem_roots_reject_path_traversal_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");
    assert!(store.get_root("../bad").await.is_err());
    assert!(store.cas_root("nested/bad", None, b"root").await.is_err());
}

#[tokio::test]
async fn crash_during_publish_leaves_old_or_new_root_dereferenceable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (old, new) = {
        let store = FsStore::open(dir.path()).expect("open store");
        let old = store.put(b"old tree").await.expect("put old tree");
        store
            .cas_root("eavt", None, old.as_str().as_bytes())
            .await
            .expect("publish old root");
        let new = store.put(b"new tree").await.expect("upload before publish");
        // Dropping the process-local store here simulates a crash after upload but
        // before the root CAS.
        (old, new)
    };

    let store = FsStore::open(dir.path()).expect("recover after pre-publish crash");
    assert_eq!(
        store.get_root("eavt").await.expect("old root after crash"),
        Some(old.as_str().as_bytes().to_vec())
    );
    assert_eq!(
        store.get(&old).await.expect("dereference old"),
        Some(b"old tree".to_vec())
    );

    store
        .cas_root(
            "eavt",
            Some(old.as_str().as_bytes()),
            new.as_str().as_bytes(),
        )
        .await
        .expect("publish new root");
    drop(store);
    let store = FsStore::open(dir.path()).expect("recover after completed publish");
    assert_eq!(
        store
            .get_root("eavt")
            .await
            .expect("new root after publish"),
        Some(new.as_str().as_bytes().to_vec())
    );
    assert_eq!(
        store.get(&new).await.expect("dereference new"),
        Some(b"new tree".to_vec())
    );
}

#[tokio::test]
async fn mark_and_sweep_preserves_every_reachable_blob() {
    let store = MemoryStore::default();
    let leaf_a = store.put(b"leaf a").await.expect("leaf a");
    let leaf_b = store.put(b"leaf b").await.expect("leaf b");
    let inner = store.put(b"inner").await.expect("inner");
    let root = store.put(b"root").await.expect("root");
    let garbage = store.put(b"abandoned").await.expect("garbage");
    let graph = HashMap::from([
        (root.clone(), vec![inner.clone(), leaf_b.clone()]),
        (inner.clone(), vec![leaf_a.clone()]),
    ]);

    let report = mark_and_sweep(&store, [root.clone()], |id, _bytes| {
        Ok(graph.get(id).cloned().unwrap_or_default())
    })
    .await
    .expect("collect garbage");

    assert_eq!(report.marked, 4);
    assert_eq!(report.swept, 1);
    for reachable in [&root, &inner, &leaf_a, &leaf_b] {
        assert!(store.contains(reachable).await.expect("reachable blob"));
    }
    assert!(!store.contains(&garbage).await.expect("garbage blob"));
}
