//! Opening a database recovers its current value from the published index
//! root plus the log tail, not a full-history replay.

use std::time::Duration;

use corium_core::Datom;
use corium_protocol::codec;
use corium_query::edn::read_one;
use corium_transactor::node::{NodeConfig, TransactorNode};

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
}

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut config = NodeConfig::new(dir.to_path_buf());
    // Long enough that only explicit requests publish, so the test controls
    // exactly where the index basis falls relative to the log tail.
    config.index_interval = Duration::from_secs(600);
    config.gc_interval = None;
    config
}

#[tokio::test]
async fn reopening_recovers_current_value_from_the_index_root() {
    let dir = tempfile::tempdir().expect("data dir");
    let schema = encoded(
        "[{:db/ident :item/value
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one}]",
    );

    // First node: load two transactions, publish a snapshot at t=2, then
    // commit a third that lives only in the log tail beyond the index basis.
    let expected: Vec<Datom> = {
        let node = TransactorNode::open(config(dir.path()))
            .await
            .expect("node1");
        assert!(node.create_db("main", &schema).await.expect("create"));
        node.transact("main", &encoded("[{:db/id \"a\" :item/value 1}]"))
            .await
            .expect("tx a");
        node.transact("main", &encoded("[{:db/id \"b\" :item/value 2}]"))
            .await
            .expect("tx b");
        assert_eq!(node.request_index("main").await.expect("publish"), 2);
        node.transact("main", &encoded("[{:db/id \"c\" :item/value 3}]"))
            .await
            .expect("tx c");
        let db = node.db_state("main").await.expect("state").db();
        assert_eq!(db.basis_t(), 3);
        // Release so a same-owner restart takes over without waiting.
        node.release_leases().await;
        db.datoms()
    };

    // Second node over the same directory: startup recovers "main" from the
    // published index root (index-basis 2) plus the (2, 3] log tail.
    let node = TransactorNode::open(config(dir.path()))
        .await
        .expect("node2");
    let recovered = node.db_state("main").await.expect("state").db();
    assert_eq!(
        recovered.basis_t(),
        3,
        "log tail replayed onto the snapshot"
    );
    assert_eq!(
        recovered.datoms(),
        expected,
        "recovered current value must match the pre-restart value"
    );

    // Allocation and commit continue cleanly from the recovered basis.
    node.transact("main", &encoded("[{:db/id \"d\" :item/value 4}]"))
        .await
        .expect("tx d after recovery");
    assert_eq!(
        node.db_state("main").await.expect("state").db().basis_t(),
        4
    );
}
