//! Store backend conformance tests.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use corium_store::{
    BlobStore, FsStore, MemoryStore, RootStore, SegmentCache, mark_and_sweep,
    mark_and_sweep_retained,
};

#[test]
fn memory_store_cas_fences_roots() {
    let store = MemoryStore::default();
    store
        .cas_root("eavt", None, b"old")
        .expect("initial publish");
    assert!(store.cas_root("eavt", None, b"new").is_err());
    store
        .cas_root("eavt", Some(b"old"), b"new")
        .expect("fenced publish");
    assert_eq!(store.get_root("eavt").expect("root"), Some(b"new".to_vec()));
}

#[test]
fn filesystem_store_round_trips_blobs_and_roots() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");
    let id = store.put(b"segment").expect("put blob");
    assert_eq!(store.get(&id).expect("get blob"), Some(b"segment".to_vec()));
    store
        .cas_root("avet", None, id.as_str().as_bytes())
        .expect("publish root");

    let reopened = FsStore::open(dir.path()).expect("reopen store");
    assert_eq!(
        reopened.get_root("avet").expect("root"),
        Some(id.as_str().as_bytes().to_vec())
    );
}

#[test]
fn filesystem_root_lock_file_remains_for_future_contenders() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");

    store.cas_root("eavt", None, b"root").expect("publish root");

    assert!(dir.path().join("roots/eavt.lock").is_file());
}

#[test]
fn cache_loads_blob_once_visible() {
    let store = MemoryStore::default();
    let id = store.put(b"cached").expect("put blob");
    let cache = SegmentCache::default();
    let loaded = cache
        .get_or_load(&store, &id)
        .expect("load")
        .expect("present");
    assert_eq!(&*loaded, b"cached");
}

#[test]
fn filesystem_store_detects_corrupt_blob() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");
    let id = store.put(b"original").expect("put blob");
    std::fs::write(dir.path().join("blobs").join(id.as_str()), b"corrupt").expect("corrupt blob");
    assert!(store.get(&id).is_err());
}

#[test]
fn blob_put_is_idempotent_for_same_content() {
    let store = MemoryStore::default();
    let first = store.put(b"same").expect("first put");
    let second = store.put(b"same").expect("second put");
    assert_eq!(first, second);
    assert_eq!(store.get(&first).expect("get blob"), Some(b"same".to_vec()));
}

#[test]
fn filesystem_roots_reject_path_traversal_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("open store");
    assert!(store.get_root("../bad").is_err());
    assert!(store.cas_root("nested/bad", None, b"root").is_err());
}

#[test]
fn crash_during_publish_leaves_old_or_new_root_dereferenceable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (old, new) = {
        let store = FsStore::open(dir.path()).expect("open store");
        let old = store.put(b"old tree").expect("put old tree");
        store
            .cas_root("eavt", None, old.as_str().as_bytes())
            .expect("publish old root");
        let new = store.put(b"new tree").expect("upload before publish");
        // Dropping the process-local store here simulates a crash after upload but
        // before the root CAS.
        (old, new)
    };

    let store = FsStore::open(dir.path()).expect("recover after pre-publish crash");
    assert_eq!(
        store.get_root("eavt").expect("old root after crash"),
        Some(old.as_str().as_bytes().to_vec())
    );
    assert_eq!(
        store.get(&old).expect("dereference old"),
        Some(b"old tree".to_vec())
    );

    store
        .cas_root(
            "eavt",
            Some(old.as_str().as_bytes()),
            new.as_str().as_bytes(),
        )
        .expect("publish new root");
    drop(store);
    let store = FsStore::open(dir.path()).expect("recover after completed publish");
    assert_eq!(
        store.get_root("eavt").expect("new root after publish"),
        Some(new.as_str().as_bytes().to_vec())
    );
    assert_eq!(
        store.get(&new).expect("dereference new"),
        Some(b"new tree".to_vec())
    );
}

#[test]
fn mark_and_sweep_preserves_every_reachable_blob() {
    let store = MemoryStore::default();
    let leaf_a = store.put(b"leaf a").expect("leaf a");
    let leaf_b = store.put(b"leaf b").expect("leaf b");
    let inner = store.put(b"inner").expect("inner");
    let root = store.put(b"root").expect("root");
    let garbage = store.put(b"abandoned").expect("garbage");
    let graph = HashMap::from([
        (root.clone(), vec![inner.clone(), leaf_b.clone()]),
        (inner.clone(), vec![leaf_a.clone()]),
    ]);

    let report = mark_and_sweep(&store, [root.clone()], |id, _bytes| {
        Ok(graph.get(id).cloned().unwrap_or_default())
    })
    .expect("collect garbage");

    assert_eq!(report.marked, 4);
    assert_eq!(report.swept, 1);
    for reachable in [&root, &inner, &leaf_a, &leaf_b] {
        assert!(store.contains(reachable).expect("reachable blob"));
    }
    assert!(!store.contains(&garbage).expect("garbage blob"));
}

#[test]
fn retention_window_keeps_new_unreachable_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FsStore::open(dir.path()).expect("store");
    let garbage = store.put(b"new garbage").expect("blob");

    let report = mark_and_sweep_retained(
        &store,
        [],
        |_, _| Ok(Vec::new()),
        Duration::from_secs(60),
        SystemTime::now(),
    )
    .expect("retained collection");
    assert_eq!(report.swept, 0);
    assert_eq!(report.retained, 1);
    assert!(store.contains(&garbage).expect("retained blob"));

    let report = mark_and_sweep_retained(
        &store,
        [],
        |_, _| Ok(Vec::new()),
        Duration::ZERO,
        SystemTime::now(),
    )
    .expect("zero-window collection");
    assert_eq!(report.swept, 1);
    assert!(!store.contains(&garbage).expect("swept blob"));
}

#[test]
fn retention_keeps_blobs_when_backend_has_no_timestamps() {
    let store = MemoryStore::default();
    let garbage = store.put(b"unknown age").expect("blob");

    let report = mark_and_sweep_retained(
        &store,
        [],
        |_, _| Ok(Vec::new()),
        Duration::from_secs(60),
        SystemTime::now(),
    )
    .expect("retained collection");

    assert_eq!(report.swept, 0);
    assert_eq!(report.retained, 1);
    assert!(store.contains(&garbage).expect("retained blob"));
}

#[test]
fn db_root_round_trips_lease_fields() {
    use corium_store::{DbRoot, FORMAT_VERSION};
    let root = DbRoot {
        format_version: FORMAT_VERSION,
        lease_version: 7,
        owner: "transactor-a".into(),
        lease_expires_unix_ms: 123_456,
        owner_endpoint: "http://transactor-a:4334".into(),
        index_basis_t: 42,
        roots: None,
    };
    assert_eq!(DbRoot::decode(&root.encode()), Some(root.clone()));
    let released = DbRoot {
        owner_endpoint: String::new(),
        lease_expires_unix_ms: 0,
        ..root
    };
    assert_eq!(DbRoot::decode(&released.encode()), Some(released));
}

#[test]
fn format_one_roots_decode_with_an_unowned_lease() {
    use corium_store::DbRoot;
    // A pre-M7 root: header, lease version, index basis, four id slots,
    // no lease fields.
    let legacy = b"corium-root-v1\n3\n17\n-\n-\n-\n-\n";
    let decoded = DbRoot::decode(legacy).expect("legacy decodes");
    assert_eq!(decoded.format_version, 1);
    assert_eq!(decoded.lease_version, 3);
    assert_eq!(decoded.index_basis_t, 17);
    assert!(decoded.owner.is_empty());
    assert_eq!(decoded.lease_expires_unix_ms, 0);
    assert!(decoded.owner_endpoint.is_empty());
}
