//! Saga branches hosted by a transactor node: id-block leasing, the overlay
//! a branch is built as, step transacting, and what a branch reader sees
//! (ADR-0023).

use std::collections::BTreeMap;
use std::time::Duration;

use corium_core::{EntityId, Partition, Value};
use corium_db::saga::{self, SagaStatus};
use corium_protocol::codec;
use corium_query::edn::{Edn, read_one};
use corium_transactor::branch::{branch_name, parse_branch_name};
use corium_transactor::node::{NodeConfig, TransactorNode};

const SAGA: u128 = 0x2b;

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
}

fn schema() -> Vec<u8> {
    encoded(
        "[{:db/ident :item/value
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one
           :db/index true}
          {:db/ident :item/label
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/one}
          {:db/ident :item/part-of
           :db/valueType :db.type/ref
           :db/cardinality :db.cardinality/one}]",
    )
}

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut config = NodeConfig::new(dir.to_path_buf());
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    config.gc_interval = None;
    config
}

async fn node() -> (std::sync::Arc<TransactorNode>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("data dir");
    let node = TransactorNode::open(config(dir.path()))
        .await
        .expect("node");
    assert!(
        node.create_db("main", &schema(), None)
            .await
            .expect("create")
    );
    (node, dir)
}

/// Transaction data opening `SAGA` at the database's current basis, with an
/// optional reservation clause. Opening is ordinary transaction data — the
/// entity-id block inside it is the transactor's to mint.
fn open_forms(basis_t: u64, reserves: &str) -> Vec<u8> {
    encoded(&format!(
        "[[:db/add \"s\" :db.saga/id #uuid \"{SAGA:032x}\"]
          [:db/add \"s\" :db.saga/status :db.saga.status/open]
          [:db/add \"s\" :db.saga/basis-t {basis_t}]
          [:db/add \"s\" :db.saga/owner \"tester\"]
          [:db/add \"s\" :db.saga/expires-at #inst 99999999999]
          {reserves}]"
    ))
}

/// Transacts `text` against `db`, returning the entity ids its tempids
/// resolved to. Tests name entities by the ids the allocator actually issued,
/// which is the only way to talk about a leased block.
async fn transact(node: &TransactorNode, db: &str, text: &str) -> BTreeMap<String, u64> {
    let response = node
        .transact(db, &encoded(text))
        .await
        .unwrap_or_else(|error| panic!("transacting against {db}: {error}"));
    let Edn::Map(entries) = codec::decode_edn(&response.tempids).expect("tempids") else {
        panic!("tempids are a map");
    };
    entries
        .into_iter()
        .filter_map(|(key, value)| match (key, value) {
            (Edn::Str(tempid), Edn::Long(raw)) => {
                Some((tempid, u64::try_from(raw).expect("entity id")))
            }
            _ => None,
        })
        .collect()
}

async fn open_saga(node: &TransactorNode, reserves: &str) -> u64 {
    let basis = node.db_state("main").await.expect("state").db().basis_t();
    node.transact("main", &open_forms(basis, reserves))
        .await
        .expect("open the saga");
    basis
}

fn item_value(db: &corium_db::Db) -> corium_core::AttrId {
    db.idents()
        .entid(&corium_core::Keyword::new(Some("item"), "value"))
        .expect(":item/value is installed")
}

fn grant(node_db: &corium_db::Db) -> saga::IdGrant {
    let entry = saga::entry(node_db, SAGA).expect("registry entry");
    assert_eq!(entry.status, Some(SagaStatus::Open));
    entry.grants.first().cloned().expect("a leased block")
}

#[tokio::test]
async fn opening_a_saga_leases_a_block_the_parent_allocator_steps_over() {
    let (node, _dir) = node().await;
    transact(&node, "main", "[{:db/id \"a\" :item/value 1}]").await;
    open_saga(&node, "").await;

    let db = node.db_state("main").await.expect("state").db();
    let grant = grant(&db);
    assert_eq!(
        grant.partition,
        Some(i64::from(Partition::User as u32)),
        "blocks are leased per partition"
    );
    assert_eq!(grant.length, Some(saga::DEFAULT_ID_BLOCK));

    // The parent's own allocation resumes above the block, so an id the
    // branch may hand out is never handed out twice.
    let after = transact(&node, "main", "[{:db/id \"b\" :item/value 2}]").await;
    let allocated = EntityId::from_raw(after["b"]);
    let end = grant.end().expect("a complete block");
    assert!(
        i64::try_from(allocated.sequence()).expect("sequence") >= end,
        "the parent allocated {allocated:?} inside or below the leased block ending at {end}"
    );

    // Naming an id inside the block directly is refused: leased id space is
    // the allocator's promise, not a lock, and no parent write belongs there.
    let inside = EntityId::new(
        Partition::User as u32,
        u64::try_from(grant.start.expect("a start")).expect("start") + 5,
    );
    let error = node
        .transact(
            "main",
            &encoded(&format!("[[:db/add #eid {} :item/value 9]]", inside.raw())),
        )
        .await
        .expect_err("granted ids are refused");
    assert!(
        error.to_string().contains("leased to an open saga"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_branch_overlays_the_parent_at_the_opening_basis() {
    let (node, _dir) = node().await;
    node.transact("main", &encoded("[{:db/id \"a\" :item/value 1}]"))
        .await
        .expect("item before the saga");
    let basis = open_saga(&node, "").await;

    // Novelty the parent commits after `t₀` is not the branch's business.
    node.transact("main", &encoded("[{:db/id \"b\" :item/value 7}]"))
        .await
        .expect("item after the saga opened");

    let name = branch_name("main", SAGA);
    assert_eq!(parse_branch_name(&name), Some(("main", SAGA)));
    let branch = node.db_state(&name).await.expect("branch opens on demand");
    let view = branch.db();
    assert_eq!(view.basis_t(), basis, "the branch is rooted at t₀");
    let parent = node.db_state("main").await.expect("state").db();
    assert_eq!(view.datoms(), parent.as_of(basis).datoms());
    assert!(
        !view.datoms().iter().any(|datom| datom.v == Value::Long(7)),
        "the branch must not see parent novelty after t₀"
    );

    // Asking twice hosts one branch, not two.
    let again = node.db_state(&name).await.expect("branch again");
    assert!(std::sync::Arc::ptr_eq(&branch, &again));
    assert_eq!(node.hosted_branches(), vec![name]);
}

#[tokio::test]
async fn steps_land_on_the_branch_and_nowhere_else() {
    let (node, _dir) = node().await;
    let items = transact(&node, "main", "[{:db/id \"a\" :item/value 1}]").await;
    let existing = EntityId::from_raw(items["a"]);
    let basis = open_saga(&node, "").await;
    let name = branch_name("main", SAGA);
    let grant = grant(&node.db_state("main").await.expect("state").db());

    let step = transact(&node, &name, "[{:db/id \"new\" :item/label \"drafted\"}]").await;
    node.transact(
        &name,
        &encoded(&format!(
            "[[:db/add #eid {} :item/value 41]]",
            existing.raw()
        )),
    )
    .await
    .expect("a step may write a pre-existing entity when nothing is reserved");

    let branch = node.db_state(&name).await.expect("branch").db();
    assert_eq!(branch.basis_t(), basis + 2, "the branch keeps its own time");
    let drafted = EntityId::from_raw(step["new"]);
    assert!(
        grant.holds(drafted),
        "{drafted:?} was allocated outside the leased block"
    );
    assert_eq!(
        branch.values(existing, item_value(&branch)),
        vec![Value::Long(41)]
    );

    // Canonical state is untouched: this is the whole isolation promise. The
    // parent's basis is the saga's opening transaction and nothing since.
    let parent = node.db_state("main").await.expect("state").db();
    assert_eq!(parent.basis_t(), basis + 1);
    assert!(
        !parent
            .datoms()
            .iter()
            .any(|datom| datom.v == Value::Str("drafted".into()))
    );

    // A branch reads as one database: its log answers with the parent's
    // history below `t₀` and its own steps above it.
    let state = node.db_state(&name).await.expect("branch state");
    let records = state.tx_range(1, None).await.expect("spliced log");
    let numbers: Vec<u64> = records.iter().map(|record| record.t).collect();
    assert_eq!(
        numbers,
        (1..=basis + 2).collect::<Vec<u64>>(),
        "a branch's log is contiguous through both sides of t₀"
    );
}

#[tokio::test]
async fn a_reserved_saga_writes_only_what_it_reserved() {
    let (node, _dir) = node().await;
    let items = transact(
        &node,
        "main",
        "[{:db/id \"a\" :item/value 1} {:db/id \"b\" :item/value 2}]",
    )
    .await;
    let (reserved, other) = (items["a"], items["b"]);
    open_saga(
        &node,
        &format!("[:db/add \"s\" :db.saga/reserves #eid {reserved}]"),
    )
    .await;
    let name = branch_name("main", SAGA);

    node.transact(
        &name,
        &encoded(&format!("[[:db/add #eid {reserved} :item/value 10]]")),
    )
    .await
    .expect("the reserved entity is the saga's to write");
    let error = node
        .transact(
            &name,
            &encoded(&format!("[[:db/add #eid {other} :item/value 10]]")),
        )
        .await
        .expect_err("an unreserved entity is not");
    assert!(
        error.to_string().contains("reservation set"),
        "unexpected error: {error}"
    );

    // Reverse-ref visibility is why refs close over the reserved set too.
    let error = node
        .transact(
            &name,
            &encoded(&format!("[{{:db/id \"new\" :item/part-of #eid {other}}}]")),
        )
        .await
        .expect_err("a ref out of the reserved set is refused");
    assert!(
        error.to_string().contains("reservation set"),
        "unexpected error: {error}"
    );
    node.transact(
        &name,
        &encoded(&format!(
            "[{{:db/id \"new\" :item/part-of #eid {reserved}}}]"
        )),
    )
    .await
    .expect("a ref into the reserved set is what the closure rule allows");
}

#[tokio::test]
async fn a_branch_refuses_the_registry_and_the_schema() {
    let (node, _dir) = node().await;
    open_saga(&node, "").await;
    let name = branch_name("main", SAGA);

    let error = node
        .transact(
            &name,
            &encoded(
                "[[:db/add \"s\" :db.saga/id #uuid \"000000000000000000000000000000ff\"]
                  [:db/add \"s\" :db.saga/status :db.saga.status/open]
                  [:db/add \"s\" :db.saga/basis-t 1]
                  [:db/add \"s\" :db.saga/owner \"nested\"]
                  [:db/add \"s\" :db.saga/expires-at #inst 99999999999]]",
            ),
        )
        .await
        .expect_err("the registry is parent data");
    assert!(
        error.to_string().contains("saga registry is parent data"),
        "unexpected error: {error}"
    );

    let request = corium_protocol::pb::AlterSchemaRequest {
        db: name.clone(),
        desired_schema: encoded("[]"),
        ..Default::default()
    };
    let error = node
        .alter_schema(&request, "tester")
        .await
        .expect_err("schema changes are refused on a branch");
    assert!(
        error.to_string().contains("saga branch"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_finished_saga_stops_taking_steps() {
    let (node, _dir) = node().await;
    open_saga(&node, "").await;
    let name = branch_name("main", SAGA);
    node.transact(&name, &encoded("[{:db/id \"new\" :item/value 1}]"))
        .await
        .expect("a step while the saga is open");

    node.transact(
        "main",
        &encoded(&format!(
            "[[:db/add [:db.saga/id #uuid \"{SAGA:032x}\"] :db.saga/status \
               :db.saga.status/aborted]]"
        )),
    )
    .await
    .expect("abort");

    let error = node
        .transact(&name, &encoded("[{:db/id \"new\" :item/value 2}]"))
        .await
        .expect_err("an aborted saga's branch takes no more steps");
    assert!(
        error.to_string().contains("no longer accepts steps"),
        "unexpected error: {error}"
    );

    // The branch is still there to read from until it is discarded, and
    // discarding is idempotent.
    assert!(node.db_state(&name).await.is_ok());
    assert!(node.discard_branch("main", SAGA).await.expect("discard"));
    assert!(node.hosted_branches().is_empty());
    assert!(!node.discard_branch("main", SAGA).await.expect("discard"));
}

#[tokio::test]
async fn a_branch_survives_a_restart_with_its_allocations_intact() {
    let dir = tempfile::tempdir().expect("data dir");
    let node = TransactorNode::open(config(dir.path()))
        .await
        .expect("node");
    assert!(
        node.create_db("main", &schema(), None)
            .await
            .expect("create")
    );
    node.transact("main", &encoded("[{:db/id \"a\" :item/value 1}]"))
        .await
        .expect("item");
    open_saga(&node, "").await;
    let name = branch_name("main", SAGA);
    node.transact(
        &name,
        &encoded("[{:db/id \"new\" :item/label \"drafted\"}]"),
    )
    .await
    .expect("step before the restart");
    let before = node.db_state(&name).await.expect("branch").db();
    node.release_leases().await;
    drop(node);

    let node = TransactorNode::open(config(dir.path()))
        .await
        .expect("reopen");
    let after = node.db_state(&name).await.expect("branch reopens").db();
    assert_eq!(after.basis_t(), before.basis_t());
    assert_eq!(after.datoms(), before.datoms());

    // The block is not re-issued from its start: the step already spent part
    // of it, and recovery reads the branch's own log for the rest.
    node.transact(&name, &encoded("[{:db/id \"next\" :item/label \"more\"}]"))
        .await
        .expect("step after the restart");
    let db = node.db_state(&name).await.expect("branch").db();
    let mut entities: Vec<u64> = db
        .datoms()
        .into_iter()
        .filter(|datom| matches!(&datom.v, Value::Str(text) if text.as_ref() == "drafted" || text.as_ref() == "more"))
        .map(|datom| datom.e.sequence())
        .collect();
    entities.sort_unstable();
    entities.dedup();
    assert_eq!(entities.len(), 2, "the restart reused an id: {entities:?}");
}

#[tokio::test]
async fn deleting_a_database_takes_its_branches_with_it() {
    let dir = tempfile::tempdir().expect("data dir");
    let node = TransactorNode::open(config(dir.path()))
        .await
        .expect("node");
    assert!(
        node.create_db("main", &schema(), None)
            .await
            .expect("create")
    );
    open_saga(&node, "").await;
    let name = branch_name("main", SAGA);
    node.transact(&name, &encoded("[{:db/id \"new\" :item/value 1}]"))
        .await
        .expect("a step, so the branch has durable state");

    // Restart, so the branch has a log and a metadata root but is hosted by
    // nobody: this is the state the hosted map cannot see, and the one a
    // deletion would stranded storage behind if it only swept memory.
    drop(node);
    let node = TransactorNode::open(config(dir.path()))
        .await
        .expect("reopen");
    assert!(
        node.hosted_branches().is_empty(),
        "a branch is opened on demand, not by the startup scan"
    );

    assert!(node.delete_db("main").await.expect("delete"));

    // Nothing of the branch is left: recreating the parent must not find a
    // saga's half-written overlay waiting under the same name.
    assert!(
        node.create_db("main", &schema(), None)
            .await
            .expect("recreate")
    );
    open_saga(&node, "").await;
    let parent = node.db_state("main").await.expect("main");
    let attribute = item_value(&parent.db());
    let branch = node.db_state(&name).await.expect("a fresh branch");
    let db = branch.db();
    // The recreated database opens the saga at basis 0, so its branch starts
    // there. The deleted branch had taken a step, so a log that survived the
    // deletion would put this one at 1 instead.
    assert_eq!(
        db.basis_t(),
        0,
        "a fresh branch starts at its own t₀, not on the deleted log's tail"
    );
    assert!(
        !db.datoms()
            .iter()
            .any(|datom| datom.a == attribute && datom.v == corium_core::Value::Long(1)),
        "the deleted branch's step must not come back"
    );
}
