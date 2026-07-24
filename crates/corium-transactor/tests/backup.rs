//! M6 backup/restore acceptance coverage.

use std::io::{Seek, Write};
use std::time::Duration;

use corium_protocol::codec;
use corium_query::edn::read_one;
use corium_store::{BlobStore, DbRoot, RootStore, db_root_name};
use corium_transactor::StoreSpec;
use corium_transactor::backup::{
    BACKUP_FORMAT_VERSION, BackupError, BackupSource, backup, restore,
};
use corium_transactor::node::{NodeConfig, TransactorNode};

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
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
    let error = restore(&future, target, "future")
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
    assert!(node.create_db("main", &schema).await.expect("create"));
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
    let first = backup(&first_source, "main", &backup_file)
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
    let incremental = backup(&incremental_source, "main", &backup_file)
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
    let delta = backup(&fixed, "main", &backup_file)
        .await
        .expect("incremental delta");
    assert_eq!(delta.basis_t, 3);
    assert_eq!(delta.index_basis_t, 2);
    assert_eq!(delta.copied_blobs, 0);
    assert_eq!(delta.replayed_transactions, 1);

    let latest = BackupSource::from_info(node.backup_info("main").await.expect("backup info"))
        .expect("filesystem source");
    let catch_up = backup(&latest, "main", &backup_file)
        .await
        .expect("catch-up backup");
    assert_eq!(catch_up.basis_t, 4);
    assert_eq!(catch_up.replayed_transactions, 1);

    let report = restore(&backup_file, restored.path(), "clone")
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

    let error = restore(&backup_file, restored.path(), "clone")
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
        node.create_db("empty", &encoded("[]"))
            .await
            .expect("create")
    );

    let source = BackupSource::from_info(node.backup_info("empty").await.expect("backup info"))
        .expect("filesystem source");
    let report = backup(&source, "empty", &backup_file)
        .await
        .expect("empty backup");
    assert_eq!(report.basis_t, 0);
    assert_eq!(report.replayed_transactions, 0);

    let report = restore(&backup_file, restored.path(), "clone")
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
    config.store = StoreSpec::Memory;
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("node");
    assert!(
        node.create_db("memory", &encoded("[]"))
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
    config.store = StoreSpec::Turso {
        path: source.path().join("source.db").display().to_string(),
    };
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("turso node");
    let schema = encoded(
        "[{:db/ident :item/value
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one}]",
    );
    assert!(node.create_db("native", &schema).await.expect("create"));
    node.transact("native", &encoded("[[:db/add 1000 :item/value 1]]"))
        .await
        .expect("transaction");

    let source = BackupSource::from_info(node.backup_info("native").await.expect("backup info"))
        .expect("turso source");
    let report = backup(&source, "native", &backup_file)
        .await
        .expect("native backup");
    assert_eq!(report.basis_t, 1);
    assert_eq!(report.replayed_transactions, 1);

    restore(&backup_file, restored.path(), "clone")
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
