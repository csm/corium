//! Storage encryption end to end: a database created with a storage key
//! writes no plaintext to its data directory, reopens, rotates, and stays
//! garbage-collectable.

use std::path::Path;
use std::sync::Arc;

use corium_crypt::testing::InMemoryKms;
use corium_crypt::{KeyId, KmsKeyring, StaticKeyring};
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

    let status = node.key_status("secrets").await.expect("status");
    let basis_t = status.basis_t;
    let manifest = status.manifest.expect("encrypted");
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
            .manifest
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

#[tokio::test]
async fn a_rotation_this_node_cannot_load_fences_writes_but_not_reads() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let kek = key_file(dir.path(), "storage.key", 23);
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
        &encoded(&format!("[{{:db/id \"n\" :note/text \"{SENTINEL}\"}}]")),
    )
    .await
    .expect("transact");
    let state = node.db_state("secrets").await.expect("hosted");
    assert!(!state.keys_fenced());

    // Simulate an operator rotating through another process: the manifest
    // opens epoch 2 and the root's generation counter announces it. This node
    // cannot unwrap it, because the manifest now names a KEK it has no
    // material for.
    let elsewhere = key_file(dir.path(), "elsewhere.key", 29);
    let mut rotated = read_manifest(&node, "secrets").await;
    // The rewrap has to resolve both the outgoing and the incoming KEK, which
    // is exactly what the other process would hold and this node does not.
    let keyring = StaticKeyring::resolve([kek, elsewhere.clone()]).expect("resolve");
    rotated
        .rewrap(&keyring, elsewhere)
        .await
        .expect("rewrap under a key this node lacks");
    rotated
        .rotate_storage_key(&keyring, 1, state.db().basis_t())
        .await
        .expect("rotate elsewhere");
    write_manifest(&node, "secrets", &rotated).await;
    bump_key_manifest_version(&node, "secrets").await;

    // The maintenance loop notices within a renewal tick and fences writes:
    // sealing another record would draw a nonce under an epoch the manifest
    // has closed, and the budget for it has stopped counting.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !state.keys_fenced() {
        assert!(
            std::time::Instant::now() < deadline,
            "the node never noticed the epoch it cannot load"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // A rotation it cannot adopt is not the same as a manifest it cannot
    // read at all, so only the fence is raised.
    assert!(!state.keys_unavailable());

    let error = node
        .transact(
            "secrets",
            &encoded("[{:db/id \"m\" :note/text \"refused\"}]"),
        )
        .await
        .expect_err("writes must refuse while fenced");
    let message = error.to_string();
    assert!(message.contains("epoch 2"), "{message}");
    assert!(message.contains("--storage-key"), "{message}");

    // Reads keep serving from the keys already held, and the lease is not
    // given up: this is a repairable misconfiguration, not a depose.
    assert_eq!(state.db().basis_t(), 1);
    assert!(format!("{:?}", state.db().datoms()).contains(SENTINEL));
    let status = node.key_status("secrets").await.expect("status");
    assert!(status.keys_fenced);
    assert_eq!(node.list_dbs(), vec!["secrets".to_owned()]);
}

async fn write_manifest(node: &TransactorNode, db: &str, manifest: &KeyManifest) {
    let previous = node
        .store()
        .get_root(&keys_root_name(db))
        .await
        .expect("root read");
    node.store()
        .cas_root(&keys_root_name(db), previous.as_deref(), &manifest.encode())
        .await
        .expect("publish manifest");
}

/// Announces a manifest change the way `corium keys rotate` does, so the
/// node's maintenance loop notices it.
///
/// Lease renewal rewrites the same root on its own cadence, so the
/// compare-and-set is retried against whatever is current — exactly as the
/// node's own bump does.
async fn bump_key_manifest_version(node: &TransactorNode, db: &str) {
    use corium_store::{DbRoot, db_root_name};
    for _ in 0..20 {
        let stored = node
            .store()
            .get_root(&db_root_name(db))
            .await
            .expect("root read");
        let mut root = DbRoot::decode(stored.as_deref().expect("root")).expect("decode");
        root.key_manifest_version = root.key_manifest_version.saturating_add(1);
        if node
            .store()
            .cas_root(&db_root_name(db), stored.as_deref(), &root.encode())
            .await
            .is_ok()
        {
            return;
        }
    }
    panic!("lease renewal kept winning the root CAS");
}

#[tokio::test]
async fn a_rewrap_this_node_cannot_load_warns_without_refusing_writes() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let kek = key_file(dir.path(), "storage.key", 31);
    let node = TransactorNode::open(config(&data_dir, std::slice::from_ref(&kek)))
        .await
        .expect("node");
    assert!(
        node.create_db("secrets", &schema(), Some(kek.clone()))
            .await
            .expect("create")
    );
    let state = node.db_state("secrets").await.expect("hosted");

    // Re-wrapping changes how the data keys are stored, not what they are, so
    // this node's snapshot is still correct and still writes under the epoch
    // the manifest calls active. Refusing here would turn a KMS outage into a
    // write outage for no confidentiality gain.
    let elsewhere = key_file(dir.path(), "elsewhere.key", 37);
    let mut rewrapped = read_manifest(&node, "secrets").await;
    let keyring = StaticKeyring::resolve([kek, elsewhere.clone()]).expect("resolve");
    rewrapped
        .rewrap(&keyring, elsewhere)
        .await
        .expect("rewrap under a key this node lacks");
    write_manifest(&node, "secrets", &rewrapped).await;
    bump_key_manifest_version(&node, "secrets").await;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !state.keys_unavailable() {
        assert!(
            std::time::Instant::now() < deadline,
            "the node never noticed the manifest change"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(!state.keys_fenced(), "a rewrap must not fence writes");

    node.transact(
        "secrets",
        &encoded(&format!("[{{:db/id \"n\" :note/text \"{SENTINEL}\"}}]")),
    )
    .await
    .expect("writes continue on the keys already held");
    assert_eq!(state.db().basis_t(), 1);
    // The condition is still reported, so it is a visible warning rather than
    // a silent divergence from what the operator asked for.
    let status = node.key_status("secrets").await.expect("status");
    assert!(status.keys_unavailable && !status.keys_fenced);
}

/// A node whose keys resolve through `service` instead of the filesystem.
///
/// The service holds the KEK the way a real one does — nothing hands it out —
/// so a database that opens here opens because wrapping and unwrapping really
/// happened remotely, not because the process could read a file.
fn kms_config(data_dir: &Path, service: &Arc<InMemoryKms>, id: &KeyId) -> NodeConfig {
    let mut config = NodeConfig::new(data_dir.to_path_buf());
    config.gc_interval = None;
    config.keyring = Some(Arc::new(KmsKeyring::new(
        Arc::clone(service) as Arc<dyn corium_crypt::KmsClient>,
        [id.clone()],
    )));
    config
}

#[tokio::test]
async fn a_database_opens_under_a_key_that_never_leaves_its_key_service() {
    let dir = tempfile::tempdir().expect("data dir");
    let data_dir = dir.path().join("data");
    let kek = KeyId::new("awskms:arn:aws:kms:us-west-2:111122223333:key/2f1c").expect("key id");

    let service = Arc::new(InMemoryKms::new(29));
    let node = TransactorNode::open(kms_config(&data_dir, &service, &kek))
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
    assert!(service.calls() > 0, "the key service was never asked");

    let durable = durable_bytes(&data_dir);
    assert!(
        !contains_sentinel(&durable),
        "the sentinel reached storage in the clear"
    );
    // The KEK is the service's key. Nothing durable may contain it — the
    // manifest holds a data key wrapped under it, and that is all.
    assert!(
        !durable
            .windows(32)
            .any(|window| window == [29_u8; 32].as_slice()),
        "the key-encryption key reached storage"
    );
    assert_eq!(
        node.key_status("secrets")
            .await
            .expect("status")
            .manifest
            .expect("encrypted")
            .kek,
        kek
    );
    drop(node);

    // Another process holding the same grant — a separate client over the same
    // service key — opens the database and reads it.
    let node = TransactorNode::open(kms_config(&data_dir, &Arc::new(InMemoryKms::new(29)), &kek))
        .await
        .expect("reopen through the key service");
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
    drop(node);

    // A service holding different material is not that grant, and the failure
    // names the key rather than surfacing as a decode error.
    let Err(error) =
        TransactorNode::open(kms_config(&data_dir, &Arc::new(InMemoryKms::new(30)), &kek)).await
    else {
        panic!("the wrong key must not open the database")
    };
    assert!(error.to_string().contains(kek.as_str()), "{error}");
}
