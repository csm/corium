//! Store backend conformance tests.

use corium_store::{BlobStore, FsStore, MemoryStore, RootStore, SegmentCache};

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
