//! Group-commit correctness at the node: concurrent transactions batch into
//! shared durable writes, yet each keeps its own `t`, report, and
//! acknowledgement, and a rejected transaction never aborts its batchmates.

use std::sync::Arc;

use corium_protocol::codec::encode_edn;
use corium_query::edn::Edn;
use corium_transactor::StoreSpec;
use corium_transactor::node::{NodeConfig, TransactorNode};

fn schema() -> Vec<u8> {
    encode_edn(&Edn::Vector(vec![Edn::Map(vec![
        (Edn::keyword("db/ident"), Edn::keyword("item/key")),
        (Edn::keyword("db/valueType"), Edn::keyword("db.type/long")),
        (
            Edn::keyword("db/cardinality"),
            Edn::keyword("db.cardinality/one"),
        ),
        (
            Edn::keyword("db/unique"),
            Edn::keyword("db.unique/identity"),
        ),
    ])]))
}

/// A fresh entity keyed by `key`.
fn tx(key: i64) -> Vec<u8> {
    encode_edn(&Edn::Vector(vec![Edn::Map(vec![
        (Edn::keyword("db/id"), Edn::Str("e".into())),
        (Edn::keyword("item/key"), Edn::Long(key)),
    ])]))
}

/// `item/key` expects a long; a string is a type mismatch that fails
/// validation.
fn bad_tx() -> Vec<u8> {
    encode_edn(&Edn::Vector(vec![Edn::Map(vec![
        (Edn::keyword("db/id"), Edn::Str("e".into())),
        (Edn::keyword("item/key"), Edn::Str("not-a-long".into())),
    ])]))
}

async fn mem_node() -> Arc<TransactorNode> {
    mem_node_with_batch(None).await
}

async fn mem_node_with_batch(max_commit_batch: Option<usize>) -> Arc<TransactorNode> {
    let mut config = NodeConfig::new(std::path::PathBuf::from("/nonexistent-mem-node"));
    config.store = StoreSpec::Memory;
    if let Some(max) = max_commit_batch {
        config.max_commit_batch = max;
    }
    TransactorNode::open(config).await.expect("open mem node")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_transactions_all_commit_with_contiguous_basis() {
    let node = mem_node().await;
    node.create_db("items", &schema()).await.expect("create");

    // Fire many transactions at once; whichever caller leads a flush batches
    // whatever is queued, so these share durable writes while each still
    // commits as its own transaction.
    let count: u64 = 200;
    let mut handles = Vec::new();
    for key in 0..count {
        let node = Arc::clone(&node);
        let key = i64::try_from(key).expect("key fits i64");
        handles.push(tokio::spawn(async move {
            node.transact("items", &tx(key))
                .await
                .expect("transact")
                .basis_t
        }));
    }
    let mut bases = Vec::new();
    for handle in handles {
        bases.push(handle.await.expect("join"));
    }
    // Every transaction got a distinct basis; together they cover 1..=count
    // with no gaps or duplicates — the total order is intact despite batching.
    bases.sort_unstable();
    assert_eq!(bases, (1..=count).collect::<Vec<_>>());

    // The committed value holds exactly `count` entities.
    let db = node.db_state("items").await.expect("state").db();
    assert_eq!(db.basis_t(), count);
    assert_eq!(db.stats().entities, usize::try_from(count).expect("fits"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_transaction_does_not_abort_its_batchmates() {
    let node = mem_node().await;
    node.create_db("items", &schema()).await.expect("create");

    // A rejected transaction (type mismatch) and a valid one, fired together so
    // they may share a batch: the bad one fails alone, the good one commits.
    let bad_node = Arc::clone(&node);
    let bad = tokio::spawn(async move { bad_node.transact("items", &bad_tx()).await });
    let good_node = Arc::clone(&node);
    let good = tokio::spawn(async move { good_node.transact("items", &tx(7)).await });

    assert!(bad.await.expect("join").is_err(), "type mismatch must fail");
    assert!(
        good.await.expect("join").is_ok(),
        "batchmate must still commit"
    );

    let db = node.db_state("items").await.expect("state").db();
    assert_eq!(db.stats().entities, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_cap_of_one_disables_batching_but_stays_correct() {
    // `max_commit_batch = 1` forces every flush to a single transaction, so
    // batching is off; concurrent transactions must still each commit with a
    // contiguous, gapless basis.
    let node = mem_node_with_batch(Some(1)).await;
    node.create_db("items", &schema()).await.expect("create");

    let count: u64 = 50;
    let mut handles = Vec::new();
    for key in 0..count {
        let node = Arc::clone(&node);
        let key = i64::try_from(key).expect("key fits i64");
        handles.push(tokio::spawn(async move {
            node.transact("items", &tx(key))
                .await
                .expect("transact")
                .basis_t
        }));
    }
    let mut bases = Vec::new();
    for handle in handles {
        bases.push(handle.await.expect("join"));
    }
    bases.sort_unstable();
    assert_eq!(bases, (1..=count).collect::<Vec<_>>());

    let db = node.db_state("items").await.expect("state").db();
    assert_eq!(db.basis_t(), count);
    assert_eq!(db.stats().entities, usize::try_from(count).expect("fits"));
}
