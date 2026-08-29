//! Merging a saga branch into its parent: the squash, the conflict scan,
//! guards, resolutions, and the one atomic commit-and-flip (ADR-0023).

use std::collections::BTreeMap;
use std::time::Duration;

use corium_core::{EntityId, Value};
use corium_db::saga::{self, SagaStatus};
use corium_protocol::codec;
use corium_query::edn::{Edn, read_one};
use corium_transactor::branch::branch_name;
use corium_transactor::node::{NodeConfig, TransactorNode};

const SAGA: u128 = 0x5a6a;

fn encoded(text: &str) -> Vec<u8> {
    codec::encode_edn(&read_one(text).expect("test EDN"))
}

fn forms(text: &str) -> Vec<Edn> {
    match read_one(text).expect("test EDN") {
        Edn::Vector(items) | Edn::List(items) => items,
        other => vec![other],
    }
}

fn schema() -> Vec<u8> {
    encoded(
        "[{:db/ident :order/status
           :db/valueType :db.type/keyword
           :db/cardinality :db.cardinality/one
           :db/index true}
          {:db/ident :order/total
           :db/valueType :db.type/long
           :db/cardinality :db.cardinality/one}
          {:db/ident :order/tag
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/many}
          {:db/ident :order/code
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/one
           :db/unique :db.unique/identity}
          {:db/ident :order/line
           :db/valueType :db.type/ref
           :db/cardinality :db.cardinality/many}
          {:db/ident :line/sku
           :db/valueType :db.type/string
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

/// Opens `SAGA` at the database's current basis, with optional extra clauses.
async fn open_saga(node: &TransactorNode, extra: &str) -> u64 {
    let basis = node.db_state("main").await.expect("state").db().basis_t();
    node.transact(
        "main",
        &encoded(&format!(
            "[[:db/add \"s\" :db.saga/id #uuid \"{SAGA:032x}\"]
              [:db/add \"s\" :db.saga/status :db.saga.status/open]
              [:db/add \"s\" :db.saga/basis-t {basis}]
              [:db/add \"s\" :db.saga/owner \"tester\"]
              [:db/add \"s\" :db.saga/expires-at #inst 99999999999]
              {extra}]"
        )),
    )
    .await
    .expect("open the saga");
    basis
}

fn attribute(db: &corium_db::Db, namespace: &str, name: &str) -> corium_core::AttrId {
    db.idents()
        .entid(&corium_core::Keyword::new(Some(namespace), name))
        .unwrap_or_else(|| panic!(":{namespace}/{name} is installed"))
}

fn keyword(db: &corium_db::Db, value: &Value) -> String {
    match value {
        Value::Keyword(id) => db
            .interner()
            .resolve(*id)
            .map_or_else(|| "?".to_owned(), |keyword| keyword.to_string()),
        other => format!("{other:?}"),
    }
}

fn saga_entry(node_db: &corium_db::Db) -> saga::SagaEntry {
    saga::entry(node_db, SAGA).expect("registry entry")
}

/// The registry flip is not a follow-up transaction: it is in the one the
/// novelty landed as, and that transaction says which saga it is.
fn assert_flip_rode_along(db: &corium_db::Db, basis_t: u64, steps: i64) {
    let entry = saga_entry(db);
    assert_eq!(entry.status, Some(SagaStatus::Committed));
    assert_eq!(entry.steps, Some(steps));
    let merge_tx = entry.merged_tx.expect("the merge transaction");
    assert_eq!(merge_tx.sequence(), basis_t);
    assert_eq!(
        db.values(merge_tx, corium_db::bootstrap::SAGA_TX_ID),
        vec![Value::Uuid(SAGA)]
    );
    assert!(
        db.datoms()
            .iter()
            .filter(|datom| datom.tx == merge_tx)
            .count()
            > 1,
        "the novelty and the flip are one transaction"
    );
}

#[tokio::test]
async fn a_merge_lands_the_whole_branch_in_one_labelled_transaction() {
    let (node, _dir) = node().await;
    let items = transact(
        &node,
        "main",
        "[{:db/id \"a\" :order/status :order.status/draft :order/total 10}]",
    )
    .await;
    let order = EntityId::from_raw(items["a"]);
    open_saga(&node, "").await;
    let branch = branch_name("main", SAGA);

    // Three steps, including one that writes the same pair twice and one that
    // creates new structure hanging off the pre-existing order.
    node.transact(
        &branch,
        &encoded(&format!(
            "[[:db/add #eid {} :order/status :order.status/packed]]",
            order.raw()
        )),
    )
    .await
    .expect("step one");
    node.transact(
        &branch,
        &encoded(&format!(
            "[[:db/add #eid {} :order/status :order.status/shipped]
              [:db/add #eid {} :order/total 12]]",
            order.raw(),
            order.raw()
        )),
    )
    .await
    .expect("step two");
    let lines = transact(
        &node,
        &branch,
        &format!(
            "[{{:db/id \"line\" :line/sku \"SKU-1\"}}
              [:db/add #eid {} :order/line \"line\"]]",
            order.raw()
        ),
    )
    .await;
    let line = EntityId::from_raw(lines["line"]);

    let before = node.db_state("main").await.expect("state").db().basis_t();
    let merged = node
        .saga_commit("main", SAGA, &[], &[])
        .await
        .expect("the merge");
    assert!(merged.committed, "{merged:?}");
    assert_eq!(merged.basis_before, before);
    assert_eq!(merged.basis_t, before + 1, "one append, not three");
    assert_eq!(merged.steps, 3);

    let db = node.db_state("main").await.expect("state").db();
    // The net effect, not the intermediate one: the parent never hears about
    // `:order.status/packed`.
    let status = attribute(&db, "order", "status");
    assert_eq!(
        keyword(&db, &db.values(order, status)[0]),
        ":order.status/shipped"
    );
    assert_eq!(
        db.values(order, attribute(&db, "order", "total")),
        vec![Value::Long(12)]
    );
    // Entity ids survive the merge verbatim, which is what the leased blocks
    // were for: the id a branch reader resolved is the id in the parent.
    assert_eq!(
        db.values(line, attribute(&db, "line", "sku")),
        vec![Value::Str("SKU-1".into())]
    );
    assert_eq!(
        db.values(order, attribute(&db, "order", "line")),
        vec![Value::Ref(line)]
    );
    assert!(
        !db.datoms()
            .iter()
            .any(|datom| datom.a == status && keyword(&db, &datom.v) == ":order.status/packed"),
        "the intermediate value was spliced into the parent"
    );

    assert_flip_rode_along(&db, merged.basis_t, 3);

    // Committing twice is the same answer: a retry after a lost
    // acknowledgement reports the merge it finds.
    let again = node
        .saga_commit("main", SAGA, &[], &[])
        .await
        .expect("the retry");
    assert!(again.committed);
    assert_eq!(again.basis_t, merged.basis_t);
    assert_eq!(
        node.db_state("main").await.expect("state").db().basis_t(),
        merged.basis_t,
        "the retry wrote nothing"
    );
}

#[tokio::test]
async fn drift_on_a_pair_both_sides_wrote_is_reported_and_then_resolved() {
    let (node, _dir) = node().await;
    let items = transact(
        &node,
        "main",
        "[{:db/id \"a\" :order/status :order.status/draft}]",
    )
    .await;
    let order = EntityId::from_raw(items["a"]);
    open_saga(&node, "").await;
    let branch = branch_name("main", SAGA);
    node.transact(
        &branch,
        &encoded(&format!(
            "[[:db/add #eid {} :order/status :order.status/shipped]]",
            order.raw()
        )),
    )
    .await
    .expect("the step");
    // Tier-0 writers race an in-flight saga; the conflict scan, not any
    // reservation, is what protects the saga.
    node.transact(
        "main",
        &encoded(&format!(
            "[[:db/add #eid {} :order/status :order.status/cancelled]]",
            order.raw()
        )),
    )
    .await
    .expect("the parent moves");

    let refused = node
        .saga_commit("main", SAGA, &[], &[])
        .await
        .expect("a refused merge is an answer, not an error");
    assert!(!refused.committed);
    let Edn::Str(report) = codec::decode_edn(&refused.conflict_report).expect("report") else {
        panic!("the report is a string");
    };
    assert!(report.contains(":write-write"), "{report}");
    assert!(report.contains(":order.status/shipped"), "{report}");
    assert!(report.contains(":order.status/cancelled"), "{report}");
    assert!(
        report.contains(":resolutions [:parent :branch]"),
        "{report}"
    );

    let db = node.db_state("main").await.expect("state").db();
    let entry = saga_entry(&db);
    assert_eq!(
        entry.status,
        Some(SagaStatus::Open),
        "a refused merge leaves the saga open"
    );
    assert_eq!(entry.conflict_report.as_deref(), Some(report.as_str()));
    // The branch is untouched, so the owner can keep working on it.
    assert!(node.db_state(&branch).await.is_ok());

    // A resolution fenced to a value the parent no longer holds answers
    // nothing, which is the point of the fence.
    let stale = node
        .saga_commit(
            "main",
            SAGA,
            &[],
            &forms(&format!(
                "[{{:e {} :a :order/status :parent :order.status/draft :take :parent}}]",
                order.raw()
            )),
        )
        .await
        .expect("answered");
    assert!(!stale.committed);

    // Overriding: the branch's value wins, and the parent's is retracted by
    // the ordinary cardinality-one rule.
    let committed = node
        .saga_commit(
            "main",
            SAGA,
            &[],
            &forms(&format!(
                "[{{:e {} :a :order/status :parent :order.status/cancelled :take :branch}}]",
                order.raw()
            )),
        )
        .await
        .expect("answered");
    assert!(committed.committed, "{committed:?}");
    let db = node.db_state("main").await.expect("state").db();
    assert_eq!(
        keyword(&db, &db.values(order, attribute(&db, "order", "status"))[0]),
        ":order.status/shipped"
    );
    let entry = saga_entry(&db);
    assert_eq!(entry.status, Some(SagaStatus::Committed));
    assert_eq!(
        entry.conflict_report, None,
        "a report from an attempt that did not happen must not describe one that did"
    );
}

#[tokio::test]
async fn accepting_the_parent_drops_the_branchs_write_and_keeps_the_rest() {
    let (node, _dir) = node().await;
    let items = transact(
        &node,
        "main",
        "[{:db/id \"a\" :order/status :order.status/draft :order/total 10}]",
    )
    .await;
    let order = EntityId::from_raw(items["a"]);
    open_saga(&node, "").await;
    let branch = branch_name("main", SAGA);
    node.transact(
        &branch,
        &encoded(&format!(
            "[[:db/add #eid {} :order/status :order.status/shipped]
              [:db/add #eid {} :order/total 12]]",
            order.raw(),
            order.raw()
        )),
    )
    .await
    .expect("the step");
    node.transact(
        "main",
        &encoded(&format!(
            "[[:db/add #eid {} :order/status :order.status/cancelled]]",
            order.raw()
        )),
    )
    .await
    .expect("the parent moves");

    let committed = node
        .saga_commit(
            "main",
            SAGA,
            &[],
            &forms(&format!(
                "[{{:e {} :a :order/status :parent :order.status/cancelled :take :parent}}]",
                order.raw()
            )),
        )
        .await
        .expect("answered");
    assert!(committed.committed, "{committed:?}");
    let db = node.db_state("main").await.expect("state").db();
    assert_eq!(
        keyword(&db, &db.values(order, attribute(&db, "order", "status"))[0]),
        ":order.status/cancelled",
        "accept-parent only ever removes something from the merge"
    );
    assert_eq!(
        db.values(order, attribute(&db, "order", "total")),
        vec![Value::Long(12)],
        "the rest of the saga still lands"
    );
}

#[tokio::test]
async fn a_unique_value_the_parent_gave_away_has_no_override() {
    let (node, _dir) = node().await;
    let items = transact(&node, "main", "[{:db/id \"a\" :order/total 1}]").await;
    let order = EntityId::from_raw(items["a"]);
    open_saga(&node, "").await;
    let branch = branch_name("main", SAGA);
    node.transact(
        &branch,
        &encoded(&format!(
            "[[:db/add #eid {} :order/code \"X-1\"]]",
            order.raw()
        )),
    )
    .await
    .expect("the step");
    transact(&node, "main", "[{:db/id \"b\" :order/code \"X-1\"}]").await;

    let refused = node
        .saga_commit("main", SAGA, &[], &[])
        .await
        .expect("answered");
    assert!(!refused.committed);
    let Edn::Str(report) = codec::decode_edn(&refused.conflict_report).expect("report") else {
        panic!("the report is a string");
    };
    assert!(report.contains(":uniqueness"), "{report}");
    assert!(report.contains(":holder"), "{report}");
    assert!(
        report.contains(":resolutions [:parent]"),
        "evicting the parent's claimant would edit an entity the saga never wrote: {report}"
    );

    // Asking for the override anyway is refused with its reason, and the
    // conflict is still there.
    let refused = node
        .saga_commit(
            "main",
            SAGA,
            &[],
            &forms(&format!(
                "[{{:e {} :a :order/code :parent nil :take :branch}}]",
                order.raw()
            )),
        )
        .await
        .expect("answered");
    assert!(!refused.committed);
    let Edn::Str(report) = codec::decode_edn(&refused.conflict_report).expect("report") else {
        panic!("the report is a string");
    };
    assert!(report.contains(":rejected"), "{report}");

    // Accept-parent is available for every class, and it lets the merge
    // through with the rest of the saga.
    let committed = node
        .saga_commit(
            "main",
            SAGA,
            &[],
            &forms(&format!(
                "[{{:e {} :a :order/code :parent nil :take :parent}}]",
                order.raw()
            )),
        )
        .await
        .expect("answered");
    assert!(committed.committed, "{committed:?}");
}

#[tokio::test]
async fn guards_are_the_saga_s_read_dependencies_and_both_sources_are_checked() {
    let (node, _dir) = node().await;
    let items = transact(
        &node,
        "main",
        "[{:db/id \"a\" :order/status :order.status/draft :order/total 10}]",
    )
    .await;
    let order = EntityId::from_raw(items["a"]);
    open_saga(&node, "").await;
    let branch = branch_name("main", SAGA);
    // The step declares, durably in the branch, the read it depended on: it
    // computed a new total from the one it saw.
    node.transact(
        &branch,
        &encoded(&format!(
            "[[:db/add #eid {} :order/total 20]
              [:db/add \"datomic.tx\" :db.saga/guard \"[:db/cas {} :order/total 10]\"]]",
            order.raw(),
            order.raw()
        )),
    )
    .await
    .expect("the step");

    // A guard supplied with the request that does not hold refuses the merge
    // even though nothing collided.
    let refused = node
        .saga_commit(
            "main",
            SAGA,
            &forms("[{:guard {:find [?e] :where [[?e :order/code \"nope\"]]}}]"),
            &[],
        )
        .await
        .expect("answered");
    assert!(!refused.committed);
    let Edn::Str(report) = codec::decode_edn(&refused.conflict_report).expect("report") else {
        panic!("the report is a string");
    };
    assert!(report.contains(":guards"), "{report}");
    assert!(report.contains("expected a result"), "{report}");

    // With no such guard the merge lands, because the step's own guard still
    // holds: the parent has not touched the total.
    let committed = node
        .saga_commit(
            "main",
            SAGA,
            &forms("[{:guard {:find [?e] :where [[?e :order/status :order.status/draft]]}}]"),
            &[],
        )
        .await
        .expect("answered");
    assert!(committed.committed, "{committed:?}");
    let db = node.db_state("main").await.expect("state").db();
    assert_eq!(
        db.values(order, attribute(&db, "order", "total")),
        vec![Value::Long(20)]
    );
}

#[tokio::test]
async fn a_step_declared_guard_outlives_the_process_that_declared_it() {
    let (node, _dir) = node().await;
    let items = transact(
        &node,
        "main",
        "[{:db/id \"a\" :order/status :order.status/draft :order/total 10}]",
    )
    .await;
    let order = EntityId::from_raw(items["a"]);
    open_saga(&node, "").await;
    let branch = branch_name("main", SAGA);
    node.transact(
        &branch,
        &encoded(&format!(
            "[[:db/add #eid {} :order/tag \"reviewed\"]
              [:db/add \"datomic.tx\" :db.saga/guard \"[:db/cas {} :order/total 10]\"]]",
            order.raw(),
            order.raw()
        )),
    )
    .await
    .expect("the step");
    // The read the step depended on is no longer true, and the saga's own
    // novelty does not touch that attribute — nothing but the guard could
    // notice.
    node.transact(
        "main",
        &encoded(&format!("[[:db/add #eid {} :order/total 99]]", order.raw())),
    )
    .await
    .expect("the parent moves");

    let refused = node
        .saga_commit("main", SAGA, &[], &[])
        .await
        .expect("answered");
    assert!(
        !refused.committed,
        "a write-write scan alone would have merged this"
    );
    let Edn::Str(report) = codec::decode_edn(&refused.conflict_report).expect("report") else {
        panic!("the report is a string");
    };
    assert!(report.contains(":step"), "{report}");
    assert!(report.contains(":order/total"), "{report}");
}

#[tokio::test]
async fn a_committed_saga_keeps_its_branch_as_the_step_grain_annex() {
    let (node, _dir) = node().await;
    let basis = open_saga(&node, "").await;
    let branch = branch_name("main", SAGA);
    node.transact(&branch, &encoded("[{:db/id \"l\" :line/sku \"SKU-9\"}]"))
        .await
        .expect("step one");
    node.transact(&branch, &encoded("[{:db/id \"m\" :line/sku \"SKU-8\"}]"))
        .await
        .expect("step two");
    let merged = node
        .saga_commit("main", SAGA, &[], &[])
        .await
        .expect("the merge");
    assert!(merged.committed);

    // The parent's log tells the truth — one commit, at commit time — and the
    // step grain is still there for an auditor who needs it.
    let state = node.db_state(&branch).await.expect("the retained branch");
    let steps = state.tx_range(basis + 1, None).await.expect("branch log");
    assert_eq!(steps.len(), 2, "two steps, still readable one by one");

    // Steps stop the moment the saga does, though.
    let error = node
        .transact(&branch, &encoded("[{:db/id \"n\" :line/sku \"SKU-7\"}]"))
        .await
        .expect_err("a committed saga's branch takes no more steps");
    assert!(
        error.to_string().contains("no longer accepts steps"),
        "unexpected error: {error}"
    );

    // Once retention discards it, saying so beats answering as if the saga
    // had never taken a step.
    assert!(node.discard_branch("main", SAGA).await.expect("discard"));
    let error = node
        .db_state(&branch)
        .await
        .err()
        .expect("a discarded branch is gone");
    assert!(
        error.to_string().contains("discarded"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_saga_that_is_over_cannot_merge_and_a_branch_cannot_nest() {
    let (node, _dir) = node().await;
    open_saga(&node, "").await;
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
        .saga_commit("main", SAGA, &[], &[])
        .await
        .expect_err("an aborted saga has nothing to merge");
    assert!(error.to_string().contains("aborted"), "unexpected: {error}");

    let error = node
        .saga_commit(&branch_name("main", SAGA), SAGA, &[], &[])
        .await
        .expect_err("sagas do not nest");
    assert!(
        error.to_string().contains("do not nest"),
        "unexpected: {error}"
    );

    let error = node
        .saga_commit("main", 0xdead, &[], &[])
        .await
        .expect_err("no such saga");
    assert!(
        error.to_string().contains("registry"),
        "unexpected: {error}"
    );
}
