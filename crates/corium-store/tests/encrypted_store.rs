//! Encrypted blob-store decorator integration tests.

use std::collections::HashMap;

use corium_crypt::{BLOB_MAGIC, KeyId, SecretKey, StaticKeyring};
use corium_store::{
    BlobStore, EncryptedBlobStore, KeyManifest, MemoryStore, RootStore, StoreError, digest,
    keys_root_name, mark_and_sweep,
};

fn key(byte: u8) -> SecretKey {
    SecretKey::new([byte; 32])
}

#[tokio::test]
async fn encrypted_store_round_trips_without_storing_plaintext() {
    let store = EncryptedBlobStore::with_key(MemoryStore::default(), 7, key(7));
    let id = store
        .put(b"highly-visible-sentinel")
        .await
        .expect("put encrypted blob");

    assert_ne!(id, digest(b"highly-visible-sentinel"));
    assert_eq!(
        store.get(&id).await.expect("read blob"),
        Some(b"highly-visible-sentinel".to_vec())
    );

    let stored = store
        .inner()
        .get(&id)
        .await
        .expect("read raw blob")
        .expect("raw blob exists");
    assert!(stored.starts_with(BLOB_MAGIC));
    assert!(
        !stored
            .windows(b"highly-visible-sentinel".len())
            .any(|window| window == b"highly-visible-sentinel")
    );
}

#[tokio::test]
async fn encrypted_content_ids_are_deterministic_and_key_scoped() {
    let first = EncryptedBlobStore::with_key(MemoryStore::default(), 1, key(1));
    let second = EncryptedBlobStore::with_key(MemoryStore::default(), 1, key(2));

    let first_id = first.put_if_absent(b"shared").await.expect("first put");
    assert_eq!(
        first.put_if_absent(b"shared").await.expect("repeat put"),
        first_id
    );
    assert_ne!(
        second.put(b"shared").await.expect("different key put"),
        first_id
    );
}

#[tokio::test]
async fn mixed_epochs_read_and_missing_old_epoch_fails_cleanly() {
    let inner = MemoryStore::default();
    let old_writer = EncryptedBlobStore::with_key(inner.clone(), 1, key(1));
    let old = old_writer.put(b"old").await.expect("old blob");

    let rotating = EncryptedBlobStore::new(inner.clone(), 2, [(1, key(1)), (2, key(2))])
        .expect("rotating store");
    let new = rotating.put(b"new").await.expect("new blob");
    assert_eq!(
        rotating.get(&old).await.expect("read old"),
        Some(b"old".to_vec())
    );
    assert_eq!(
        rotating.get(&new).await.expect("read new"),
        Some(b"new".to_vec())
    );

    let new_only = EncryptedBlobStore::with_key(inner, 2, key(2));
    assert!(matches!(
        new_only.get(&old).await,
        Err(StoreError::MissingEncryptionKey(1))
    ));
}

#[tokio::test]
async fn garbage_collection_walks_plaintext_and_deletes_ciphertext_ids() {
    let store = EncryptedBlobStore::with_key(MemoryStore::default(), 1, key(3));
    let leaf = store.put(b"leaf").await.expect("leaf");
    let root = store.put(b"root").await.expect("root");
    let garbage = store.put(b"garbage").await.expect("garbage");
    let graph = HashMap::from([(root.clone(), vec![leaf.clone()])]);

    let report = mark_and_sweep(&store, [root.clone()], |id, plaintext| {
        if id == &root {
            assert_eq!(plaintext, b"root");
        }
        Ok(graph.get(id).cloned().unwrap_or_default())
    })
    .await
    .expect("collect");

    assert_eq!(report.marked, 2);
    assert_eq!(report.swept, 1);
    assert!(store.contains(&root).await.expect("root"));
    assert!(store.contains(&leaf).await.expect("leaf"));
    assert!(!store.contains(&garbage).await.expect("garbage"));
}

#[tokio::test]
async fn root_records_remain_cleartext() {
    let store = EncryptedBlobStore::with_key(MemoryStore::default(), 1, key(4));
    store
        .cas_root("db:test", None, b"clear root")
        .await
        .expect("publish root");

    assert_eq!(
        store.inner().get_root("db:test").await.expect("raw root"),
        Some(b"clear root".to_vec())
    );
}

#[tokio::test]
async fn a_manifest_bootstraps_the_decorator_across_a_rotation() {
    // The path a process actually walks at open: read `keys:<db>`, unwrap
    // every epoch through the keyring, and hand the snapshot to the decorator.
    // No KMS call happens after this point.
    let kek = KeyId::new("file:/etc/corium/storage.key").expect("kek");
    let mut keyring = StaticKeyring::default();
    keyring.insert(kek.clone(), 1, key(3), true);

    let store = MemoryStore::default();
    let mut manifest = KeyManifest::create(&keyring, kek, 1_700_000_000_000)
        .await
        .expect("create manifest");
    RootStore::cas_root(&store, &keys_root_name("people"), None, &manifest.encode())
        .await
        .expect("publish manifest");

    let old_id = open_encrypted(&store, &manifest, &keyring)
        .await
        .put(b"carried-over-leaf")
        .await
        .expect("put under epoch 1");

    // Rotating opens a new epoch. Nothing stored is rewritten, and the
    // manifest is CAS-replaced under the same root name.
    let stored = RootStore::get_root(&store, &keys_root_name("people"))
        .await
        .expect("read manifest")
        .expect("manifest present");
    manifest = KeyManifest::decode(&stored).expect("decode manifest");
    assert_eq!(
        manifest
            .rotate_storage_key(&keyring, 1)
            .await
            .expect("rotate"),
        2
    );
    RootStore::cas_root(
        &store,
        &keys_root_name("people"),
        Some(&stored),
        &manifest.encode(),
    )
    .await
    .expect("replace manifest");

    let rotated = open_encrypted(&store, &manifest, &keyring).await;
    // A mixed-epoch tree is legal: the carried-over leaf keeps its old id and
    // old epoch, and new writes take the new one.
    assert_eq!(
        rotated.get(&old_id).await.expect("read old epoch"),
        Some(b"carried-over-leaf".to_vec())
    );
    let new_id = rotated.put(b"carried-over-leaf").await.expect("re-encrypt");
    assert_ne!(new_id, old_id, "a new epoch gives a leaf a new identity");
    assert_eq!(
        rotated.get(&new_id).await.expect("read new epoch"),
        Some(b"carried-over-leaf".to_vec())
    );
}

#[tokio::test]
async fn a_process_without_the_kek_fails_at_open() {
    let kek = KeyId::new("awskms:arn:aws:kms:us-west-2:1:key/2f1c").expect("kek");
    let mut keyring = StaticKeyring::default();
    keyring.insert(kek.clone(), 1, key(3), true);
    let manifest = KeyManifest::create(&keyring, kek, 1).await.expect("create");

    let error = manifest
        .unwrap_storage_keys(&StaticKeyring::default())
        .await
        .expect_err("no KEK material");
    assert!(
        error
            .to_string()
            .contains("awskms:arn:aws:kms:us-west-2:1:key/2f1c"),
        "the failure must name the manifest's key id: {error}"
    );
}

async fn open_encrypted(
    store: &MemoryStore,
    manifest: &KeyManifest,
    keyring: &StaticKeyring,
) -> EncryptedBlobStore<MemoryStore> {
    EncryptedBlobStore::new(
        store.clone(),
        manifest
            .active_storage_epoch()
            .expect("an encrypted database has an active epoch"),
        manifest
            .unwrap_storage_keys(keyring)
            .await
            .expect("unwrap storage keys"),
    )
    .expect("open decorator")
}
