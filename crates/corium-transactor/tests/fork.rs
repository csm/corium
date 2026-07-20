//! Point-in-time database fork acceptance coverage.

use std::time::Duration;

use corium_protocol::codec;
use corium_query::edn::read_one;
use corium_store::{DbRoot, RootStore, db_root_name};
use corium_transactor::node::{NodeConfig, NodeError, TransactorNode};

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
}

fn schema() -> Vec<u8> {
    encoded(
        "[{:db/ident :item/value
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one
           :db/index true}]",
    )
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

#[tokio::test]
async fn fork_duplicates_the_source_at_a_past_basis_and_diverges() {
    let dir = tempfile::tempdir().expect("data dir");
    let mut config = NodeConfig::new(dir.path().to_path_buf());
    config.index_interval = Duration::from_millis(10);
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("node");
    assert!(node.create_db("main", &schema()).await.expect("create"));
    node.transact("main", &encoded("[{:db/id \"item\" :item/value 1}]"))
        .await
        .expect("tx one");
    node.transact("main", &encoded("[[:db/add 1000 :item/value 2]]"))
        .await
        .expect("tx two");
    node.transact("main", &encoded("[[:db/add 1000 :item/value 3]]"))
        .await
        .expect("tx three");

    assert_eq!(
        node.fork_db("main", "sandbox", 2).await.expect("fork"),
        Some(2)
    );
    let main_db = node.db_state("main").await.expect("main state").db();
    let sandbox = node.db_state("sandbox").await.expect("sandbox state").db();
    assert_eq!(sandbox.basis_t(), 2);
    assert_eq!(sandbox.datoms(), main_db.as_of(2).datoms());

    // The fork accepts writes and diverges without touching the source.
    node.transact("sandbox", &encoded("[[:db/add 1000 :item/value 42]]"))
        .await
        .expect("sandbox tx");
    let main_db = node.db_state("main").await.expect("main state").db();
    let sandbox = node.db_state("sandbox").await.expect("sandbox state").db();
    assert_eq!(main_db.basis_t(), 3);
    assert_eq!(sandbox.basis_t(), 3);
    assert_ne!(sandbox.datoms(), main_db.datoms());

    // An omitted basis (0) forks at the source's current basis, and the
    // fork's indexing job publishes covering indexes of its own.
    assert_eq!(
        node.fork_db("main", "copy", 0).await.expect("fork current"),
        Some(3)
    );
    let copy = node.db_state("copy").await.expect("copy state").db();
    assert_eq!(copy.datoms(), main_db.datoms());
    wait_index(&node, "sandbox", 3).await;
    wait_index(&node, "copy", 3).await;
}

#[tokio::test]
async fn fork_validates_inputs_and_existing_targets() {
    let dir = tempfile::tempdir().expect("data dir");
    let mut config = NodeConfig::new(dir.path().to_path_buf());
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("node");
    assert!(node.create_db("main", &schema()).await.expect("create"));
    node.transact("main", &encoded("[{:db/id \"item\" :item/value 1}]"))
        .await
        .expect("tx one");

    let error = node
        .fork_db("absent", "sandbox", 0)
        .await
        .expect_err("unknown source");
    assert!(matches!(error, NodeError::UnknownDb(name) if name == "absent"));
    let error = node
        .fork_db("main", "bad name!", 0)
        .await
        .expect_err("invalid target");
    assert!(matches!(error, NodeError::InvalidName(_)));
    let error = node
        .fork_db("main", "main", 0)
        .await
        .expect_err("self fork");
    assert!(matches!(error, NodeError::BadRequest(_)));
    let error = node
        .fork_db("main", "future", 99)
        .await
        .expect_err("basis ahead of source");
    assert!(matches!(error, NodeError::BadRequest(_)));

    assert_eq!(
        node.fork_db("main", "twin", 0).await.expect("fork"),
        Some(1)
    );
    assert_eq!(
        node.fork_db("main", "twin", 0)
            .await
            .expect("existing twin"),
        None
    );
}

#[tokio::test]
async fn forked_database_survives_restart() {
    let dir = tempfile::tempdir().expect("data dir");
    {
        let mut config = NodeConfig::new(dir.path().to_path_buf());
        config.gc_interval = None;
        let node = TransactorNode::open(config).await.expect("node");
        assert!(node.create_db("main", &schema()).await.expect("create"));
        node.transact("main", &encoded("[{:db/id \"item\" :item/value 1}]"))
            .await
            .expect("tx one");
        node.transact("main", &encoded("[[:db/add 1000 :item/value 2]]"))
            .await
            .expect("tx two");
        assert_eq!(
            node.fork_db("main", "sandbox", 1).await.expect("fork"),
            Some(1)
        );
        node.release_leases().await;
    }
    let mut config = NodeConfig::new(dir.path().to_path_buf());
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("reopen");
    let main_db = node.db_state("main").await.expect("main state").db();
    let sandbox = node.db_state("sandbox").await.expect("sandbox state").db();
    assert_eq!(main_db.basis_t(), 2);
    assert_eq!(sandbox.basis_t(), 1);
    assert_eq!(sandbox.datoms(), main_db.as_of(1).datoms());
}
