//! Storage encryption end to end: a database created with a storage key
//! writes no plaintext to its data directory, reopens, rotates, and stays
//! garbage-collectable.

use std::path::Path;
use std::sync::Arc;

use corium_crypt::{KeyId, StaticKeyring};
use corium_protocol::codec;
use corium_query::edn::read_one;
use corium_store::{KeyManifest, RootStore, StorageKeyState, keys_root_name};
use corium_transactor::node::{NodeConfig, TransactorNode};

/// A value distinctive enough that finding it anywhere under the data
/// directory means something wrote it in the clear.
const SENTINEL: &str = "kaleidoscope-pangolin";

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
}

fn schema() -> Vec<u8> {
    encoded(
        "[{:db/ident :note/text
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/one}]",
    )
}

/// Writes a key file and returns the identity naming it.
fn key_file(dir: &Path, name: &str, byte: u8) -> KeyId {
    let path = dir.join(name);
    std::fs::write(&path, [byte; 32]).expect("write key");
    KeyId::new(format!("file:{}", path.display())).expect("key id")
}

fn config(data_dir: &Path, keys: &[KeyId]) -> NodeConfig {
    let mut config = NodeConfig::new(data_dir.to_path_buf());
    // Scheduled GC would race the assertions below.
    config.gc_interval = None;
    if !keys.is_empty() {
        config.keyring = Some(Arc::new(
            StaticKeyring::resolve(keys.to_vec()).expect("resolve keys"),
        ));
    }
    config
}

/// Every byte the node made durable, so a plaintext search covers blobs,
/// roots, and log files alike.
fn durable_bytes(dir: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).expect("read dir") {
            let entry = entry.expect("dir entry");
            if entry.file_type().expect("file type").is_dir() {
                pending.push(entry.path());
            } else {
                bytes.extend(std::fs::read(entry.path()).expect("read file"));
            }
        }
    }
    bytes
}

fn contains_sentinel(bytes: &[u8]) -> bool {
    bytes
        .windows(SENTINEL.len())
        .any(|window| window == SENTINEL.as_bytes())
}

#[tokio::test]
async fn an_encrypted_database_writes_no_plaintext_and_reopens() {
    let dir = tempfile::tempdir().expect("data dir");
    let kek = key_file(dir.path(), "storage.key", 3);

    let node = TransactorNode::open(config(&dir.path().join("data"), std::slice::from_ref(&kek)))
        .await
        .expect("node");
    assert!(
        node.create_db("secrets", &schema(), Some(kek.clone()))
            .await
            .expect("create")
    );
    node.transact(
        "secrets",
        &encoded(&format!("[{{:db/id \"n\" :note/text \"{SENTINEL}\"}}]")),
    )
    .await
    .expect("transact");
    node.request_index("secrets").await.expect("publish");

    let durable = durable_bytes(&dir.path().join("data"));
    assert!(
        !contains_sentinel(&durable),
        "the sentinel reached storage in the clear"
    );
    // The blob header proves the segments really were encrypted, rather than
    // the sentinel merely being absent because nothing was written.
    assert!(
        durable
            .windows(corium_crypt::BLOB_MAGIC.len())
            .any(|window| window == corium_crypt::BLOB_MAGIC),
        "no encrypted blob was written"
    );
    assert!(
        durable
            .windows(corium_crypt::LOG_MAGIC.len())
            .any(|window| window == corium_crypt::LOG_MAGIC),
        "no encrypted log record was written"
    );

    // Reopening replays the sealed log and rehydrates the same value.
    drop(node);
    let node = TransactorNode::open(config(&dir.path().join("data"), std::slice::from_ref(&kek)))
        .await
        .expect("reopen");
    let state = node.db_state("secrets").await.expect("hosted");
    assert_eq!(state.db().basis_t(), 1);
    assert!(state.is_encrypted());
    assert!(
        format!("{:?}", state.db().datoms()).contains(SENTINEL),
        "the reopened database lost its data"
    );
}

#[tokio::test]
async fn opening_an_encrypted_database_without_its_key_fails_loudly() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let kek = key_file(dir.path(), "storage.key", 5);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&kek)))
        .await
        .expect("node");
    assert!(
        node.create_db("secrets", &schema(), Some(kek))
            .await
            .expect("create")
    );
    drop(node);

    // A keyless process must refuse at open, naming the key it needs, rather
    // than failing later with a decode error on the first segment read.
    let Err(error) = TransactorNode::open(config(&data_dir, &[])).await else {
        panic!("open must fail without the key")
    };
    let message = error.to_string();
    assert!(message.contains("storage.key"), "{message}");
    assert!(message.contains("--storage-key"), "{message}");
}

#[tokio::test]
async fn creating_an_encrypted_database_needs_a_resolvable_key() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let known = key_file(dir.path(), "storage.key", 7);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&known)))
        .await
        .expect("node");

    let unknown = KeyId::new("file:/nonexistent/corium/other.key").expect("key id");
    let error = node
        .create_db("secrets", &schema(), Some(unknown))
        .await
        .expect_err("an unresolvable key must fail the creation");
    assert!(error.to_string().contains("other.key"), "{error}");
    // Nothing was catalogued, so the name is still free.
    assert!(node.db_state("secrets").await.is_err());
}

#[tokio::test]
async fn rotation_opens_an_epoch_new_writes_use_and_keeps_old_data_readable() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let kek = key_file(dir.path(), "storage.key", 9);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&kek)))
        .await
        .expect("node");
    assert!(
        node.create_db("secrets", &schema(), Some(kek.clone()))
            .await
            .expect("create")
    );
    node.transact(
        "secrets",
        &encoded("[{:db/id \"first\" :note/text \"before rotation\"}]"),
    )
    .await
    .expect("first");

    assert_eq!(
        node.rotate_storage_key("secrets").await.expect("rotate"),
        2,
        "rotation opens the next epoch"
    );

    // New writes go out under the new epoch, and the records written under
    // the old one still replay — which is the whole point of retaining it.
    node.transact(
        "secrets",
        &encoded("[{:db/id \"second\" :note/text \"after rotation\"}]"),
    )
    .await
    .expect("second");
    let state = node.db_state("secrets").await.expect("hosted");
    assert_eq!(state.store().storage_epoch(), Some(2));
    assert_eq!(state.db().basis_t(), 2);
    node.request_index("secrets").await.expect("publish");

    let (manifest, basis_t) = node.key_status("secrets").await.expect("status");
    let manifest = manifest.expect("encrypted");
    assert_eq!(manifest.active_storage_epoch(), Some(2));
    assert_eq!(
        manifest.storage_key(1).expect("first epoch").state,
        StorageKeyState::Retiring
    );
    // The retired epoch's nonce span closed where the new one opened.
    assert_eq!(manifest.log_records_sealed(1, basis_t), Some(1));

    // A fresh process reads both epochs.
    drop(node);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&kek)))
        .await
        .expect("reopen");
    let rendered = format!(
        "{:?}",
        node.db_state("secrets")
            .await
            .expect("hosted")
            .db()
            .datoms()
    );
    assert!(rendered.contains("before rotation"), "{rendered}");
    assert!(rendered.contains("after rotation"), "{rendered}");
}

#[tokio::test]
async fn rewrapping_changes_the_key_encryption_key_without_touching_data() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let first = key_file(dir.path(), "first.key", 11);
    let second = key_file(dir.path(), "second.key", 13);
    let node = TransactorNode::open(config(&data_dir, &[first.clone(), second.clone()]))
        .await
        .expect("node");
    assert!(
        node.create_db("secrets", &schema(), Some(first))
            .await
            .expect("create")
    );
    node.transact(
        "secrets",
        &encoded(&format!("[{{:db/id \"n\" :note/text \"{SENTINEL}\"}}]")),
    )
    .await
    .expect("transact");
    node.request_index("secrets").await.expect("publish");
    let before = durable_bytes(&data_dir.join("store").join("blobs"));

    node.rewrap_keys("secrets", second.clone())
        .await
        .expect("rewrap");
    assert_eq!(
        node.key_status("secrets")
            .await
            .expect("status")
            .0
            .expect("encrypted")
            .kek,
        second
    );
    // Re-wrapping is a manifest edit: not one stored object changes.
    assert_eq!(durable_bytes(&data_dir.join("store").join("blobs")), before);

    // The new KEK alone now opens the database.
    drop(node);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&second)))
        .await
        .expect("reopen under the new KEK");
    assert!(
        format!(
            "{:?}",
            node.db_state("secrets")
                .await
                .expect("hosted")
                .db()
                .datoms()
        )
        .contains(SENTINEL)
    );
}

#[tokio::test]
async fn garbage_collection_keeps_an_encrypted_database_reachable() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let kek = key_file(dir.path(), "storage.key", 17);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&kek)))
        .await
        .expect("node");
    assert!(
        node.create_db("secrets", &schema(), Some(kek.clone()))
            .await
            .expect("create")
    );
    for index in 0..4 {
        node.transact(
            "secrets",
            &encoded(&format!(
                "[{{:db/id \"n{index}\" :note/text \"{SENTINEL}\"}}]"
            )),
        )
        .await
        .expect("transact");
        node.request_index("secrets").await.expect("publish");
    }

    // The mark pass must decrypt this database's index manifests to find their
    // chunks; a keyless mark would sweep every chunk it could not follow.
    node.gc_deleted_with_retention(std::time::Duration::ZERO)
        .await
        .expect("gc");

    drop(node);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&kek)))
        .await
        .expect("reopen");
    assert_eq!(
        node.db_state("secrets")
            .await
            .expect("hosted")
            .db()
            .basis_t(),
        4
    );
}

#[tokio::test]
async fn a_fork_of_an_encrypted_database_gets_its_own_keys() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let kek = key_file(dir.path(), "storage.key", 19);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&kek)))
        .await
        .expect("node");
    assert!(
        node.create_db("secrets", &schema(), Some(kek))
            .await
            .expect("create")
    );
    node.transact(
        "secrets",
        &encoded(&format!("[{{:db/id \"n\" :note/text \"{SENTINEL}\"}}]")),
    )
    .await
    .expect("transact");

    assert_eq!(
        node.fork_db("secrets", "sandbox", 0).await.expect("fork"),
        Some(1)
    );
    let fork = node.db_state("sandbox").await.expect("hosted fork");
    assert!(fork.is_encrypted());
    assert!(format!("{:?}", fork.db().datoms()).contains(SENTINEL));

    // Same KEK, different data key: the fork's grant can be revoked on its
    // own, and neither database's ciphertext opens under the other's key.
    let source = read_manifest(&node, "secrets").await;
    let target = read_manifest(&node, "sandbox").await;
    assert_eq!(source.kek, target.kek);
    assert_ne!(
        source.storage_keys[0].wrapped_dek,
        target.storage_keys[0].wrapped_dek
    );
}

async fn read_manifest(node: &TransactorNode, db: &str) -> KeyManifest {
    let bytes = node
        .store()
        .get_root(&keys_root_name(db))
        .await
        .expect("root read")
        .expect("manifest");
    KeyManifest::decode(&bytes).expect("decode")
}
