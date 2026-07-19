//! M6 backup/restore acceptance coverage.

use std::time::Duration;

use corium_protocol::codec;
use corium_query::edn::read_one;
use corium_store::{BlobStore, DbRoot, RootStore, db_root_name};
use corium_transactor::backup::{BackupError, backup, restore};
use corium_transactor::node::{NodeConfig, TransactorNode};

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
}

async fn wait_index(node: &TransactorNode, db: &str, basis: u64) {
    for _ in 0..100 {
        let root = node
            .store()
            .get_root(&db_root_name(db))
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

#[tokio::test]
async fn full_incremental_and_clone_restore_preserve_basis_and_data() {
    let source = tempfile::tempdir().expect("source");
    let backup_dir = tempfile::tempdir().expect("backup");
    let restored = tempfile::tempdir().expect("restore");
    let mut config = NodeConfig::new(source.path().to_path_buf());
    config.index_interval = Duration::from_millis(10);
    config.gc_interval = None;
    let node = TransactorNode::open(config).expect("node");
    let schema = encoded(
        "[{:db/ident :item/value
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one
           :db/index true}]",
    );
    assert!(node.create_db("main", &schema).expect("create"));
    node.transact("main", &encoded("[{:db/id \"item\" :item/value 1}]"))
        .await
        .expect("tx one");
    node.transact("main", &encoded("[[:db/add 1000 :item/value 2]]"))
        .await
        .expect("tx two");
    wait_index(&node, "main", 2).await;

    let first = backup(source.path(), "main", backup_dir.path()).expect("full backup");
    assert_eq!(first.basis_t, 2);
    assert_eq!(first.index_basis_t, 2);
    assert!(first.copied_blobs > 0);

    let incremental = backup(source.path(), "main", backup_dir.path()).expect("incremental");
    assert_eq!(incremental.copied_blobs, 0);
    assert_eq!(incremental.reused_blobs, 4);

    node.transact("main", &encoded("[[:db/add 1000 :item/value 3]]"))
        .await
        .expect("tx three");
    wait_index(&node, "main", 3).await;
    let delta = backup(source.path(), "main", backup_dir.path()).expect("incremental delta");
    assert_eq!(delta.basis_t, 3);
    assert!(delta.copied_blobs > 0);
    assert_eq!(delta.copied_blobs + delta.reused_blobs, 4);

    let report = restore(backup_dir.path(), restored.path(), "clone").expect("restore clone");
    assert_eq!(report.source_db, "main");
    assert_eq!(report.target_db, "clone");
    assert_eq!(report.basis_t, 3);

    let mut restored_config = NodeConfig::new(restored.path().to_path_buf());
    restored_config.gc_interval = None;
    let restored_node = TransactorNode::open(restored_config).expect("open restored node");
    let restored_db = restored_node.db_state("clone").expect("clone state").db();
    let original_db = node.db_state("main").expect("main state").db();
    assert_eq!(restored_db.basis_t(), original_db.basis_t());
    assert_eq!(restored_db.datoms(), original_db.datoms());

    assert!(matches!(
        restore(backup_dir.path(), restored.path(), "clone"),
        Err(BackupError::TargetExists(name)) if name == "clone"
    ));
}

#[tokio::test]
async fn scheduled_gc_sweeps_only_after_configured_retention() {
    let dir = tempfile::tempdir().expect("data dir");
    let mut config = NodeConfig::new(dir.path().to_path_buf());
    config.gc_interval = Some(Duration::from_millis(10));
    config.gc_retention = Duration::ZERO;
    let node = TransactorNode::open(config).expect("node");
    let orphan = node.store().put(b"orphan").expect("orphan blob");
    for _ in 0..100 {
        if !node.store().contains(&orphan).expect("contains") {
            assert!(node.metrics().snapshot().gc_runs > 0);
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("scheduled GC did not sweep the orphan");
}
