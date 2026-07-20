//! Runtime index-publication control: `RequestIndex` and `SetIndexPolicy`.

use std::time::Duration;

use corium_protocol::codec;
use corium_query::edn::read_one;
use corium_store::{DbRoot, RootStore, db_root_name};
use corium_transactor::node::{IndexPolicyUpdate, NodeConfig, TransactorNode};

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
}

async fn published_basis(node: &TransactorNode, db: &str) -> u64 {
    node.store()
        .get_root(&db_root_name(db))
        .await
        .expect("root read")
        .as_deref()
        .and_then(DbRoot::decode)
        .filter(|root| root.roots.is_some())
        .map_or(0, |root| root.index_basis_t)
}

#[tokio::test]
async fn request_index_publishes_now_and_policy_updates_apply_at_runtime() {
    let dir = tempfile::tempdir().expect("data dir");
    let mut config = NodeConfig::new(dir.path().to_path_buf());
    // Long enough that only explicit requests or a runtime override publish.
    config.index_interval = Duration::from_secs(600);
    config.gc_interval = None;
    let node = TransactorNode::open(config).await.expect("node");
    let schema = encoded(
        "[{:db/ident :item/value
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one}]",
    );
    assert!(node.create_db("main", &schema).await.expect("create"));
    node.transact("main", &encoded("[{:db/id \"item\" :item/value 1}]"))
        .await
        .expect("tx one");

    // The 600s interval means nothing has published yet.
    assert_eq!(published_basis(&node, "main").await, 0);

    // An explicit request publishes immediately, bypassing pacing…
    let basis = node.request_index("main").await.expect("request index");
    assert_eq!(basis, 1);
    assert_eq!(published_basis(&node, "main").await, 1);
    // …and a caught-up request is a no-op returning the current basis.
    assert_eq!(node.request_index("main").await.expect("no-op"), 1);

    // An empty update reads the configured policy.
    let policy = node
        .set_index_policy("main", IndexPolicyUpdate::default())
        .await
        .expect("read policy");
    assert_eq!(policy.interval, Duration::from_secs(600));

    // Lowering the interval at runtime makes the background job pick up
    // new work without any request.
    let policy = node
        .set_index_policy(
            "main",
            IndexPolicyUpdate {
                interval: Some(Duration::from_millis(10)),
                ..IndexPolicyUpdate::default()
            },
        )
        .await
        .expect("override interval");
    assert_eq!(policy.interval, Duration::from_millis(10));
    node.transact("main", &encoded("[[:db/add 1000 :item/value 2]]"))
        .await
        .expect("tx two");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while published_basis(&node, "main").await < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "background job never applied the runtime interval override"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(matches!(
        node.request_index("missing").await,
        Err(corium_transactor::node::NodeError::UnknownDb(_))
    ));
}
