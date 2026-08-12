//! M6 backup/restore acceptance coverage, including backup format 2 — the
//! container that carries an encrypted database.

use std::io::{Seek, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use corium_crypt::{KeyId, Keyring, StaticKeyring};
use corium_db::Db;
use corium_protocol::codec;
use corium_query::edn::read_one;
use corium_store::{BlobStore, DbRoot, RootStore, db_root_name, keys_root_name};
use corium_transactor::StoreSpec;
use corium_transactor::backup::{
    BACKUP_FORMAT_VERSION, BackupError, BackupSource, ContentEncryption, backup, restore,
};
use corium_transactor::node::{NodeConfig, TransactorNode};

/// A value distinctive enough that finding it in an archive means the backup
/// wrote it in the clear.
const SENTINEL: &str = "kaleidoscope-pangolin";

/// Writes a key file and returns the identity naming it.
fn key_file(dir: &Path, name: &str, byte: u8) -> KeyId {
    let path = dir.join(name);
    std::fs::write(&path, [byte; 32]).expect("write key");
    KeyId::new(format!("file:{}", path.display())).expect("key id")
}

fn keyring(keys: &[KeyId]) -> Arc<dyn Keyring> {
    Arc::new(StaticKeyring::resolve(keys.to_vec()).expect("resolve keys"))
}

fn contains_sentinel(bytes: &[u8]) -> bool {
    bytes
        .windows(SENTINEL.len())
        .any(|window| window == SENTINEL.as_bytes())
}

/// Every byte under `dir`, so a plaintext search covers blobs, roots, and log
/// files alike.
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

/// Asserts that `bytes` carries the database's content and none of it in the
/// clear. The magic checks matter as much as the sentinel: absence of plaintext
/// proves nothing if nothing was written.
fn assert_ciphertext_only(bytes: &[u8], what: &str) {
    assert!(
        !contains_sentinel(bytes),
        "{what} holds plaintext user data"
    );
    assert!(
        bytes
            .windows(corium_crypt::BLOB_MAGIC.len())
            .any(|window| window == corium_crypt::BLOB_MAGIC),
        "no encrypted blob reached {what}"
    );
    assert!(
        bytes
            .windows(corium_crypt::LOG_MAGIC.len())
            .any(|window| window == corium_crypt::LOG_MAGIC),
        "no encrypted log record reached {what}"
    );
}

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
}

fn assert_history_matches(restored: &Db, original: &Db) {
    assert!(restored.has_complete_history());
    assert_eq!(restored.history().datoms(), original.history().datoms());
    for t in 0..=restored.basis_t() {
        assert_eq!(
            restored.as_of(t).datoms(),
            original.as_of(t).datoms(),
            "restore lost the as-of view at transaction {t}"
        );
    }
}

async fn wait_index(node: &TransactorNode, db: &str, basis: u64) {
    for _ in 0..100 {
        let root = node
            .store()
            .get_root(&db_root_name(db))
            .await
            .expect("root read")
            .as_deref()
            .and_then(DbRoot::decode);
        if root.is_some_and(|root| root.roots.is_some() && root.index_basis_t >= basis) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("index did not reach basis {basis}");
}

async fn assert_future_format_rejected(backup_file: &std::path::Path, target: &std::path::Path) {
    let future = backup_file.with_file_name("future.corium");
    std::fs::copy(backup_file, &future).expect("copy archive");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&future)
        .expect("open future archive");
    file.seek(std::io::SeekFrom::Start(16))
        .expect("seek version");
    file.write_all(&(BACKUP_FORMAT_VERSION + 1).to_be_bytes())
        .expect("write future version");
    let error = restore(&future, target, "future", None)
        .await
        .expect_err("future format must be rejected");
    assert!(matches!(
        error,
        BackupError::UnsupportedBackupFormat {
            found,
            supported,
            writer,
        } if found == BACKUP_FORMAT_VERSION + 1
            && supported == BACKUP_FORMAT_VERSION
            && writer == env!("CARGO_PKG_VERSION")
    ));
}

#[tokio::test]
async fn full_incremental_and_clone_restore_preserve_basis_and_data() {
    let source = tempfile::tempdir().expect("source");
    let backup_dir = tempfile::tempdir().expect("backup");
    let backup_file = backup_dir.path().join("main.corium");
    let restored = tempfile::tempdir().expect("restore");
    let mut config = NodeConfig::new(source.path().to_path_buf());
    config.index_interval = Duration::from_millis(10);
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("node");
    let schema = encoded(
        "[{:db/ident :item/value
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one
           :db/index true}]",
    );
    assert!(node.create_db("main", &schema, None).await.expect("create"));
    node.transact("main", &encoded("[{:db/id \"item\" :item/value 1}]"))
        .await
        .expect("tx one");
    node.transact("main", &encoded("[[:db/add 1000 :item/value 2]]"))
        .await
        .expect("tx two");
    wait_index(&node, "main", 2).await;

    let first_source =
        BackupSource::from_info(node.backup_info("main").await.expect("backup info"))
            .expect("filesystem source");
    let first = backup(&first_source, "main", &backup_file, None)
        .await
        .expect("full backup");
    assert!(backup_file.is_file());
    assert_eq!(first.backup_format_version, BACKUP_FORMAT_VERSION);
    assert_eq!(first.writer_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(first.basis_t, 2);
    assert_eq!(first.index_basis_t, 2);
    assert_eq!(first.replayed_transactions, 2);
    assert!(first.copied_blobs > 0);

    let incremental_source =
        BackupSource::from_info(node.backup_info("main").await.expect("backup info"))
            .expect("filesystem source");
    let incremental = backup(&incremental_source, "main", &backup_file, None)
        .await
        .expect("incremental");
    assert_eq!(incremental.copied_blobs, 0);
    assert_eq!(incremental.reused_blobs, 0);
    assert_eq!(incremental.replayed_transactions, 0);

    node.transact("main", &encoded("[[:db/add 1000 :item/value 3]]"))
        .await
        .expect("tx three");
    wait_index(&node, "main", 3).await;
    // Fix the checkpoint, then let the live log grow. This run must stop at
    // the discovered t; the next incremental run picks up the later record.
    let fixed = BackupSource::from_info(node.backup_info("main").await.expect("backup info"))
        .expect("filesystem source");
    node.transact("main", &encoded("[[:db/add 1000 :item/value 4]]"))
        .await
        .expect("tx four");
    // An interrupted append leaves only a partial trailing frame. The next
    // incremental run truncates it back to the last complete checkpoint.
    std::fs::OpenOptions::new()
        .append(true)
        .open(&backup_file)
        .expect("open archive for partial append")
        .write_all(b"CKPT\0\0")
        .expect("write partial checkpoint");
    let delta = backup(&fixed, "main", &backup_file, None)
        .await
        .expect("incremental delta");
    assert_eq!(delta.basis_t, 3);
    assert_eq!(delta.index_basis_t, 2);
    assert_eq!(delta.copied_blobs, 0);
    assert_eq!(delta.replayed_transactions, 1);

    let latest = BackupSource::from_info(node.backup_info("main").await.expect("backup info"))
        .expect("filesystem source");
    let catch_up = backup(&latest, "main", &backup_file, None)
        .await
        .expect("catch-up backup");
    assert_eq!(catch_up.basis_t, 4);
    assert_eq!(catch_up.replayed_transactions, 1);

    let report = restore(&backup_file, restored.path(), "clone", None)
        .await
        .expect("restore clone");
    assert_eq!(report.backup_format_version, BACKUP_FORMAT_VERSION);
    assert_eq!(report.writer_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(report.source_db, "main");
    assert_eq!(report.target_db, "clone");
    assert_eq!(report.basis_t, 4);

    let mut restored_config = NodeConfig::new(restored.path().to_path_buf());
    restored_config.gc_interval = None;
    let restored_node = TransactorNode::open(restored_config)
        .await
        .expect("open restored node");
    let restored_db = restored_node
        .db_state("clone")
        .await
        .expect("clone state")
        .db();
    let original_db = node.db_state("main").await.expect("main state").db();
    assert_eq!(restored_db.basis_t(), original_db.basis_t());
    assert_eq!(restored_db.datoms(), original_db.datoms());
    assert_history_matches(&restored_db, &original_db);

    let error = restore(&backup_file, restored.path(), "clone", None)
        .await
        .expect_err("existing target");
    assert!(matches!(error, BackupError::TargetExists(name) if name == "clone"));

    assert_future_format_rejected(&backup_file, restored.path()).await;
}

#[tokio::test]
async fn empty_database_round_trips_through_a_binary_checkpoint() {
    let source = tempfile::tempdir().expect("source");
    let backup_dir = tempfile::tempdir().expect("backup");
    let backup_file = backup_dir.path().join("empty.corium");
    let restored = tempfile::tempdir().expect("restore");
    let mut config = NodeConfig::new(source.path().to_path_buf());
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("node");
    assert!(
        node.create_db("empty", &encoded("[]"), None)
            .await
            .expect("create")
    );

    let source = BackupSource::from_info(node.backup_info("empty").await.expect("backup info"))
        .expect("filesystem source");
    let report = backup(&source, "empty", &backup_file, None)
        .await
        .expect("empty backup");
    assert_eq!(report.basis_t, 0);
    assert_eq!(report.replayed_transactions, 0);

    let report = restore(&backup_file, restored.path(), "clone", None)
        .await
        .expect("restore empty database");
    assert_eq!(report.basis_t, 0);
    let mut restored_config = NodeConfig::new(restored.path().to_path_buf());
    restored_config.gc_interval = None;
    let restored_node = TransactorNode::open(restored_config)
        .await
        .expect("restored node");
    assert_eq!(
        restored_node
            .db_state("clone")
            .await
            .expect("clone")
            .db()
            .basis_t(),
        0
    );
}

#[tokio::test]
async fn scheduled_gc_sweeps_only_after_configured_retention() {
    let dir = tempfile::tempdir().expect("data dir");
    let mut config = NodeConfig::new(dir.path().to_path_buf());
    config.gc_interval = Some(Duration::from_millis(10));
    config.gc_retention = Duration::ZERO;
    let node = TransactorNode::open(config).await.expect("node");
    let orphan = node.store().put(b"orphan").await.expect("orphan blob");
    // Generous wall-clock deadline: the whole workspace test suite runs in
    // parallel and can starve the 10ms GC ticker for a while.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if !node.store().contains(&orphan).await.expect("contains") {
            assert!(node.metrics().snapshot().gc_runs > 0);
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("scheduled GC did not sweep the orphan");
}

#[tokio::test]
async fn process_local_memory_source_is_rejected_explicitly() {
    let dir = tempfile::tempdir().expect("data dir");
    let mut config = NodeConfig::new(dir.path().to_path_buf());
    config.store = StoreSpec::memory();
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("node");
    assert!(
        node.create_db("memory", &encoded("[]"), None)
            .await
            .expect("create")
    );

    let error = BackupSource::from_info(node.backup_info("memory").await.expect("backup info"))
        .expect_err("memory cannot be shared with the backup process");
    assert!(matches!(error, BackupError::UnsupportedSource(_)));
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn native_turso_log_is_backed_up_through_the_same_replay_path() {
    let source = tempfile::tempdir().expect("source");
    let backup_dir = tempfile::tempdir().expect("backup");
    let backup_file = backup_dir.path().join("native.corium");
    let restored = tempfile::tempdir().expect("restore");
    let mut config = NodeConfig::new(source.path().join("node"));
    config.store = StoreSpec::turso(source.path().join("source.db"));
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("turso node");
    let schema = encoded(
        "[{:db/ident :item/value
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one}]",
    );
    assert!(
        node.create_db("native", &schema, None)
            .await
            .expect("create")
    );
    node.transact("native", &encoded("[[:db/add 1000 :item/value 1]]"))
        .await
        .expect("transaction");

    let source = BackupSource::from_info(node.backup_info("native").await.expect("backup info"))
        .expect("turso source");
    let report = backup(&source, "native", &backup_file, None)
        .await
        .expect("native backup");
    assert_eq!(report.basis_t, 1);
    assert_eq!(report.replayed_transactions, 1);

    restore(&backup_file, restored.path(), "clone", None)
        .await
        .expect("restore");
    let mut restored_config = NodeConfig::new(restored.path().to_path_buf());
    restored_config.gc_interval = None;
    let restored_node = TransactorNode::open(restored_config)
        .await
        .expect("restored node");
    assert_eq!(
        restored_node
            .db_state("clone")
            .await
            .expect("clone")
            .db()
            .basis_t(),
        1
    );
}

/// Backup format 2 end to end: an encrypted database is archived without ever
/// being decrypted, and restores — under a new name — into a working database.
#[tokio::test]
async fn an_encrypted_database_round_trips_through_an_encrypted_archive() {
    let dir = tempfile::tempdir().expect("workspace");
    let source_dir = dir.path().join("source");
    let restored_dir = dir.path().join("restored");
    let backup_file = dir.path().join("vault.corium");
    let kek = key_file(dir.path(), "storage.key", 11);
    let keys = keyring(std::slice::from_ref(&kek));

    let mut config = NodeConfig::new(source_dir.clone());
    config.index_interval = Duration::from_millis(10);
    config.gc_interval = None;
    config.keyring = Some(Arc::clone(&keys));
    let node = TransactorNode::open(config).await.expect("node");
    let schema = encoded(
        "[{:db/ident :note/text
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/one}]",
    );
    assert!(
        node.create_db("vault", &schema, Some(kek.clone()))
            .await
            .expect("create")
    );
    node.transact(
        "vault",
        &encoded(&format!("[{{:db/id \"n\" :note/text \"{SENTINEL}\"}}]")),
    )
    .await
    .expect("transact");
    wait_index(&node, "vault", 1).await;

    // Copying needs the key only to follow index blobs' child references, but
    // it does need one: without it the archive would be missing segments.
    let source = BackupSource::from_info(node.backup_info("vault").await.expect("backup info"))
        .expect("filesystem source");
    let error = backup(&source, "vault", &backup_file, None)
        .await
        .expect_err("a keyless backup must refuse");
    assert!(
        matches!(&error, BackupError::MissingKeyring { db, kek: named }
            if db == "vault" && named == &kek),
        "{error}"
    );
    assert!(!backup_file.exists(), "a refused backup left an archive");

    let report = backup(&source, "vault", &backup_file, Some(&keys))
        .await
        .expect("encrypted backup");
    assert_eq!(report.backup_format_version, BACKUP_FORMAT_VERSION);
    assert_eq!(report.content_encryption, ContentEncryption::Storage);
    assert_eq!(report.basis_t, 1);
    assert_eq!(report.replayed_transactions, 1);
    assert!(report.copied_blobs > 0);

    // The archive is the artifact the threat model cares about: a copied
    // backup file must be as opaque as the storage it came from.
    assert_ciphertext_only(
        &std::fs::read(&backup_file).expect("read archive"),
        "the archive",
    );

    // Reopening takes a new lease, so the next record lands in a new log
    // version file — and a sealed record authenticates the version it was
    // written under, so the archive has to carry that with the bytes.
    drop(node);
    let mut reopened = NodeConfig::new(source_dir.clone());
    reopened.index_interval = Duration::from_millis(10);
    reopened.gc_interval = None;
    reopened.keyring = Some(Arc::clone(&keys));
    let node = TransactorNode::open(reopened).await.expect("reopen node");

    // A rotation between incremental runs moves the manifest forward, so the
    // archive's newest checkpoint carries both epochs and its records span
    // them.
    assert_eq!(
        node.rotate_storage_key("vault").await.expect("rotate"),
        2,
        "rotation opens the next epoch"
    );
    node.transact(
        "vault",
        &encoded("[{:db/id \"second\" :note/text \"after rotation\"}]"),
    )
    .await
    .expect("post-rotation transact");
    let source = BackupSource::from_info(node.backup_info("vault").await.expect("backup info"))
        .expect("filesystem source");
    let incremental = backup(&source, "vault", &backup_file, Some(&keys))
        .await
        .expect("incremental backup");
    assert_eq!(incremental.basis_t, 2);
    assert_eq!(incremental.replayed_transactions, 1);

    // Restoring is where the key is genuinely required: the copied records
    // move onto the restored database's own lineage.
    let error = restore(&backup_file, &restored_dir, "clone", None)
        .await
        .expect_err("a keyless restore must refuse");
    assert!(
        matches!(&error, BackupError::MissingKeyring { kek: named, .. } if named == &kek),
        "{error}"
    );

    let report = restore(&backup_file, &restored_dir, "clone", Some(&keys))
        .await
        .expect("restore clone");
    assert_eq!(report.content_encryption, ContentEncryption::Storage);
    assert_eq!(report.source_db, "vault");
    assert_eq!(report.target_db, "clone");
    assert_eq!(report.basis_t, 2);

    let original = node.db_state("vault").await.expect("vault state").db();
    assert_restored_clone(&restored_dir, &keys, &kek, &original).await;

    // ...and the restored data directory is as opaque as the source's.
    assert_ciphertext_only(&durable_bytes(&restored_dir), "the restored database");
}

/// Opens the restored clone and checks it is the source database, still
/// encrypted, and metered honestly.
async fn assert_restored_clone(
    restored_dir: &Path,
    keys: &Arc<dyn Keyring>,
    kek: &KeyId,
    original: &corium_db::Db,
) {
    let mut config = NodeConfig::new(restored_dir.to_path_buf());
    config.gc_interval = None;
    config.keyring = Some(Arc::clone(keys));
    let node = TransactorNode::open(config)
        .await
        .expect("open restored node");
    let state = node.db_state("clone").await.expect("clone state");
    assert!(
        state.is_encrypted(),
        "the restored clone lost its encryption"
    );
    assert_eq!(state.db().basis_t(), original.basis_t());
    assert_eq!(state.db().datoms(), original.datoms());
    assert!(
        format!("{:?}", state.db().datoms()).contains(SENTINEL),
        "the restored clone lost its data"
    );

    // The clone carries the archive's manifest, so its own `keys:` root is
    // what a later rotation or re-wrap acts on.
    let manifest = corium_store::KeyManifest::decode(
        &node
            .store()
            .get_root(&keys_root_name("clone"))
            .await
            .expect("keys root")
            .expect("the clone has no key manifest"),
    )
    .expect("decode manifest");
    assert_eq!(&manifest.kek, kek);
    assert_eq!(manifest.active_storage_epoch(), Some(2));
    // Every record was re-sealed under the active epoch, so the nonce budget
    // must credit that epoch with the whole log rather than with the tail the
    // source happened to write under it.
    assert_eq!(manifest.log_records_sealed(2, 2), Some(2));
    assert_eq!(manifest.log_records_sealed(1, 2), Some(0));
    // A later rotation on the clone still works from any basis.
    assert_eq!(
        node.rotate_storage_key("clone")
            .await
            .expect("rotate the clone"),
        3
    );
}
