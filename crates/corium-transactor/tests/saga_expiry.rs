//! Ending a saga when nobody ends it: compensation at abort and expiry, the
//! sweep, the liveness invariant, and branch retention (ADR-0023).

use std::collections::BTreeMap;
use std::time::Duration;

use corium_core::Value;
use corium_db::saga::{self, SagaStatus};
use corium_protocol::codec;
use corium_query::edn::{Edn, read_one};
use corium_transactor::expiry::Intent;
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
          {:db/ident :order/note
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/one}
          {:db/ident :order/failure
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/one}
          {:db/ident :db/fn :db/valueType :db.type/string}]",
    )
}

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut config = NodeConfig::new(dir.to_path_buf());
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    config.gc_interval = None;
    // Every pass in these tests is driven by hand, so the background one
    // cannot race an assertion about what a pass did.
    config.saga_sweep_interval = None;
    config
}

async fn node_at(dir: &std::path::Path) -> std::sync::Arc<TransactorNode> {
    TransactorNode::open(config(dir)).await.expect("node")
}

async fn node() -> (std::sync::Arc<TransactorNode>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("data dir");
    let node = node_at(dir.path()).await;
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
async fn open_saga(node: &TransactorNode, expires_at: i64, extra: &str) {
    let basis = node.db_state("main").await.expect("state").db().basis_t();
    node.transact(
        "main",
        &encoded(&format!(
            "[[:db/add \"s\" :db.saga/id #uuid \"{SAGA:032x}\"]
              [:db/add \"s\" :db.saga/status :db.saga.status/open]
              [:db/add \"s\" :db.saga/basis-t {basis}]
              [:db/add \"s\" :db.saga/owner \"tester\"]
              [:db/add \"s\" :db.saga/expires-at #inst {expires_at}]
              {extra}]"
        )),
    )
    .await
    .expect("open the saga");
}

fn entry(node_db: &corium_db::Db) -> saga::SagaEntry {
    saga::entry(node_db, SAGA).expect("registry entry")
}

async fn parent_db(node: &TransactorNode) -> corium_db::Db {
    node.db_state("main").await.expect("state").db()
}

/// Waits for `db`'s entry for `SAGA` to reach `want`.
///
/// The liveness pass runs in the background when a database is first hosted,
/// so a test that also drives a transition by hand is racing it. Both routes
/// are correct and reach the same state; what matters is the state.
async fn await_status(node: &TransactorNode, db: &str, want: &SagaStatus) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let value = node.db_state(db).await.expect("state").db();
        let status = saga::entry(&value, SAGA).and_then(|entry| entry.status);
        drop(value);
        if status.as_ref() == Some(want) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{db}'s saga never reached {want} (it is {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A deadline far enough out that nothing expires by accident.
const FOREVER: i64 = 99_999_999_999_999;

// ── Compensation ─────────────────────────────────────────────────────────────

/// The registered compensation is the transactor's to apply, and it lands in
/// the very transaction that flips the saga — labelled with the saga id, so a
/// reader can map "the saga I was watching" onto "the record it left".
#[tokio::test(flavor = "multi_thread")]
async fn a_registered_compensation_lands_with_the_flip() {
    let (node, _dir) = node().await;
    let ids = transact(&node, "main", "[{:db/id \"o\" :order/status :placed}]").await;
    let order = ids["o"];
    open_saga(
        &node,
        FOREVER,
        &format!(
            "[:db/add \"s\" :db.saga/on-abort-tx
              \"[[:db/add #eid {order} :order/failure \\\"the supplier said no\\\"]]\"]"
        ),
    )
    .await;

    let response = node
        .saga_finish("main", SAGA, &SagaStatus::Aborted, Intent::Registered)
        .await
        .expect("the abort lands");
    assert!(response.compensated);
    assert!(response.on_abort_error.is_empty());

    let db = parent_db(&node).await;
    let entry = entry(&db);
    assert_eq!(entry.status, Some(SagaStatus::Aborted));
    let failure = db
        .idents()
        .entid(&corium_core::Keyword::new(Some("order"), "failure"))
        .expect(":order/failure is installed");
    assert_eq!(
        db.values(corium_core::EntityId::from_raw(order), failure),
        vec![Value::Str("the supplier said no".into())]
    );
    // One transaction: the flip and the record are one append or neither.
    let flip = db
        .datoms()
        .into_iter()
        .find(|datom| datom.e == entry.entity && datom.a == corium_db::bootstrap::SAGA_STATUS)
        .expect("the flip");
    assert_eq!(
        db.values(flip.tx, corium_db::bootstrap::SAGA_TX_ID),
        vec![Value::Uuid(SAGA)],
        "an abort carrying user-facing data says which saga wrote it"
    );
    assert!(
        db.datoms()
            .iter()
            .any(|datom| datom.tx == flip.tx && datom.a == failure),
        "the compensation is in the flip's own transaction"
    );
}

/// A compensation the owner writes at abort time replaces the registered one
/// rather than joining it; an empty one drops it deliberately.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_time_compensation_replaces_the_registered_one() {
    let (node, _dir) = node().await;
    let ids = transact(&node, "main", "[{:db/id \"o\" :order/status :placed}]").await;
    let order = ids["o"];
    open_saga(
        &node,
        FOREVER,
        &format!(
            "[:db/add \"s\" :db.saga/on-abort-tx
              \"[[:db/add #eid {order} :order/failure \\\"registered\\\"]]\"]"
        ),
    )
    .await;

    node.saga_finish(
        "main",
        SAGA,
        &SagaStatus::Aborted,
        Intent::Replace(forms(&format!(
            "[[:db/add #eid {order} :order/failure \"by hand\"]]"
        ))),
    )
    .await
    .expect("the abort lands");

    let db = parent_db(&node).await;
    let failure = db
        .idents()
        .entid(&corium_core::Keyword::new(Some("order"), "failure"))
        .expect(":order/failure is installed");
    assert_eq!(
        db.values(corium_core::EntityId::from_raw(order), failure),
        vec![Value::Str("by hand".into())]
    );
}

/// An abort has its owner present, so a compensation that does not validate
/// fails the abort and leaves the saga open to be fixed.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_compensation_fails_an_abort_and_leaves_the_saga_open() {
    let (node, _dir) = node().await;
    open_saga(
        &node,
        FOREVER,
        "[:db/add \"s\" :db.saga/on-abort-tx
          \"[[:db/add \\\"x\\\" :order/nonexistent \\\"boom\\\"]]\"]",
    )
    .await;

    let error = node
        .saga_finish("main", SAGA, &SagaStatus::Aborted, Intent::Registered)
        .await
        .expect_err("an unvalidatable compensation fails the abort");
    assert!(
        error.to_string().contains("nonexistent"),
        "unhelpful: {error}"
    );
    assert_eq!(
        entry(&parent_db(&node).await).status,
        Some(SagaStatus::Open),
        "a failed abort leaves the saga to be fixed or aborted without it"
    );
}

/// An expiry has nobody present, so liveness outranks: the saga expires
/// without the compensation, and the reason is recorded where the owner will
/// find it.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_compensation_never_blocks_an_expiry() {
    let (node, _dir) = node().await;
    open_saga(
        &node,
        FOREVER,
        "[:db/add \"s\" :db.saga/on-abort-tx
          \"[[:db/add \\\"x\\\" :order/nonexistent \\\"boom\\\"]]\"]",
    )
    .await;

    let response = node
        .saga_finish("main", SAGA, &SagaStatus::Expired, Intent::Registered)
        .await
        .expect("expiry is not held hostage by a compensation");
    assert!(!response.compensated);

    let entry = entry(&parent_db(&node).await);
    assert_eq!(entry.status, Some(SagaStatus::Expired));
    let recorded = entry.on_abort_error.expect("the reason is recorded");
    assert!(recorded.contains("nonexistent"), "unhelpful: {recorded}");
}

/// A `:db/fn` compensation is invoked with the parent's current value *and*
/// the branch value, which is the whole reason it is a function rather than
/// static data: the failure record is about what the branch did.
#[cfg(feature = "cljrs")]
#[tokio::test(flavor = "multi_thread")]
async fn a_compensation_function_reads_the_branch_it_is_compensating_for() {
    let (node, _dir) = node().await;
    let ids = transact(&node, "main", "[{:db/id \"o\" :order/status :placed}]").await;
    let order = ids["o"];
    // The function reads the note the branch's step left and copies it into
    // the parent as the failure record. Nothing in the parent holds that note,
    // so the assertion below can only come from the branch.
    let installed = transact(
        &node,
        "main",
        &format!(
            "[{{:db/id \"f\"
                :db/ident :order/unwind
                :db/fn \"(fn [db branch]
                           [[:db/add {order}
                             :order/failure
                             (ffirst (corium.api/q (quote [:find ?note
                                                           :where [_ :order/note ?note]])
                                                   branch))]])\"}}]"
        ),
    )
    .await;
    let unwind = corium_core::EntityId::from_raw(installed["f"]);
    open_saga(
        &node,
        FOREVER,
        &format!("[:db/add \"s\" :db.saga/on-abort-fn #eid {}]", unwind.raw()),
    )
    .await;
    let branch = node.saga_branch("main", SAGA).await.expect("branch");
    node.transact(
        branch.name(),
        &encoded("[{:db/id \"note\" :order/note \"shipped before the abort\"}]"),
    )
    .await
    .expect("a step lands on the branch");

    let response = node
        .saga_finish("main", SAGA, &SagaStatus::Aborted, Intent::Registered)
        .await
        .expect("the abort lands");
    assert!(response.compensated, "{response:?}");

    let db = parent_db(&node).await;
    let failure = db
        .idents()
        .entid(&corium_core::Keyword::new(Some("order"), "failure"))
        .expect(":order/failure is installed");
    assert_eq!(
        db.values(corium_core::EntityId::from_raw(order), failure),
        vec![Value::Str("shipped before the abort".into())],
        "the compensation read the branch, not the parent"
    );
}

// ── The sweep ────────────────────────────────────────────────────────────────

/// The sweep is what makes expiry mandatory rather than aspirational: an
/// overdue saga ends whether or not its owner ever comes back.
#[tokio::test(flavor = "multi_thread")]
async fn the_sweep_expires_what_is_overdue_and_leaves_the_rest() {
    let (node, _dir) = node().await;
    open_saga(&node, 1_000, "").await;

    let report = node.saga_sweep("main").await.expect("a sweep");
    assert_eq!(report.expired, vec![SAGA]);
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(
        entry(&parent_db(&node).await).status,
        Some(SagaStatus::Expired)
    );

    // A second pass has nothing left to do: the saga is terminal.
    let again = node.saga_sweep("main").await.expect("a second sweep");
    assert!(again.expired.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_saga_with_time_left_survives_the_sweep() {
    let (node, _dir) = node().await;
    open_saga(&node, FOREVER, "").await;
    let report = node.saga_sweep("main").await.expect("a sweep");
    assert!(report.is_empty(), "{report:?}");
    assert_eq!(
        entry(&parent_db(&node).await).status,
        Some(SagaStatus::Open)
    );
}

// ── Retention ────────────────────────────────────────────────────────────────

/// A finished saga's branch is kept for its window and discarded after it —
/// the grace period a returning owner salvages in, and the step grain an
/// auditor reads.
#[tokio::test(flavor = "multi_thread")]
async fn retention_keeps_a_finished_branch_and_then_discards_it() {
    let dir = tempfile::tempdir().expect("data dir");
    let mut settings = config(dir.path());
    // Long enough that the branch survives the abort's own sweep pass.
    settings.saga_retention = Duration::from_secs(600);
    let node = TransactorNode::open(settings).await.expect("node");
    assert!(
        node.create_db("main", &schema(), None)
            .await
            .expect("create")
    );
    open_saga(&node, FOREVER, "").await;
    node.saga_branch("main", SAGA).await.expect("branch");

    node.saga_finish("main", SAGA, &SagaStatus::Aborted, Intent::Registered)
        .await
        .expect("abort");
    let report = node.saga_sweep("main").await.expect("a sweep");
    assert!(report.discarded.is_empty(), "the window has not closed");
    assert!(
        node.branch_exists("main", SAGA).await.expect("store read"),
        "an aborted saga's branch is retained, not destroyed with the flip"
    );
    assert!(
        entry(&parent_db(&node).await)
            .retention_deadline(600_000)
            .is_some(),
        "a finished saga knows when its branch is released"
    );
    drop(node);

    // The same database under a node whose retention is nothing at all: the
    // window is a policy, and the branch goes when it closes.
    let mut settings = config(dir.path());
    settings.saga_retention = Duration::ZERO;
    let node = TransactorNode::open(settings).await.expect("reopen");
    let report = node.saga_sweep("main").await.expect("a second sweep");
    assert_eq!(report.discarded, vec![SAGA]);
    assert!(!node.branch_exists("main", SAGA).await.expect("store read"));
    // The registry entry is not the branch: it stays as history.
    assert_eq!(
        entry(&parent_db(&node).await).status,
        Some(SagaStatus::Aborted)
    );
}

/// A saga may set its own window at open, for a workload whose audit or
/// salvage needs differ from the database's policy.
#[tokio::test(flavor = "multi_thread")]
async fn a_saga_overrides_the_databases_retention_window() {
    let dir = tempfile::tempdir().expect("data dir");
    let mut settings = config(dir.path());
    settings.saga_retention = Duration::from_secs(600);
    let node = TransactorNode::open(settings).await.expect("node");
    assert!(
        node.create_db("main", &schema(), None)
            .await
            .expect("create")
    );
    open_saga(&node, FOREVER, "[:db/add \"s\" :db.saga/retain-for 0]").await;
    node.saga_branch("main", SAGA).await.expect("branch");
    node.saga_finish("main", SAGA, &SagaStatus::Aborted, Intent::Registered)
        .await
        .expect("abort");

    let report = node.saga_sweep("main").await.expect("a sweep");
    assert_eq!(
        report.discarded,
        vec![SAGA],
        "the saga asked for no window and the node's 10 minutes did not apply"
    );
}

// ── The liveness invariant ───────────────────────────────────────────────────

/// A fork copies the parent's log prefix, registry datoms and all, but never
/// the branches. The entries it inherits are expired on first open, with no
/// compensation — a diverged timeline must not apply the same failure record
/// twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_fork_expires_the_open_sagas_it_inherits() {
    let (node, _dir) = node().await;
    let ids = transact(&node, "main", "[{:db/id \"o\" :order/status :placed}]").await;
    let order = ids["o"];
    open_saga(
        &node,
        FOREVER,
        &format!(
            "[:db/add \"s\" :db.saga/on-abort-tx
              \"[[:db/add #eid {order} :order/failure \\\"never on a fork\\\"]]\"]"
        ),
    )
    .await;
    node.saga_branch("main", SAGA).await.expect("branch");

    node.fork_db("main", "copy", 0).await.expect("fork");
    // Hosting the fork starts the pass; calling it again is idempotent, and
    // either route ends the entry the same way.
    node.expire_branchless_sagas("copy")
        .await
        .expect("the liveness pass");
    await_status(&node, "copy", &SagaStatus::Expired).await;

    let copy = node.db_state("copy").await.expect("state").db();
    let inherited = saga::entry(&copy, SAGA).expect("the fork inherited the entry");
    let recorded = inherited.on_abort_error.expect("the skip is recorded");
    assert!(recorded.contains("branch"), "unhelpful: {recorded}");
    let failure = copy
        .idents()
        .entid(&corium_core::Keyword::new(Some("order"), "failure"))
        .expect(":order/failure is installed");
    assert!(
        copy.values(corium_core::EntityId::from_raw(order), failure)
            .is_empty(),
        "a branchless expiry applies no compensation"
    );

    assert!(
        !node.branch_exists("copy", SAGA).await.expect("store read"),
        "expiring a branchless saga does not stand up a branch for it"
    );

    // The original is untouched: its branch is its own.
    assert_eq!(
        entry(&parent_db(&node).await).status,
        Some(SagaStatus::Open)
    );
    assert!(node.branch_exists("main", SAGA).await.expect("store read"));
}

/// The invariant must not fire on a saga that simply has not been stepped
/// yet. A branch's durable state begins with its registry entry, so a restart
/// finds one for every open saga — including one opened as a workflow record
/// and never touched.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_an_open_saga_that_never_took_a_step() {
    let dir = tempfile::tempdir().expect("data dir");
    let node = node_at(dir.path()).await;
    assert!(
        node.create_db("main", &schema(), None)
            .await
            .expect("create")
    );
    open_saga(&node, FOREVER, "").await;
    assert!(
        node.branch_exists("main", SAGA).await.expect("store read"),
        "opening a saga records its branch"
    );
    node.release_leases().await;
    drop(node);

    let node = node_at(dir.path()).await;
    let expired = node
        .expire_branchless_sagas("main")
        .await
        .expect("the liveness pass");
    assert!(
        expired.is_empty(),
        "a stepless saga is not a branchless one"
    );
    assert_eq!(
        entry(&parent_db(&node).await).status,
        Some(SagaStatus::Open)
    );
}

/// The liveness pass is the ordinary way an inherited entry ends, but it is
/// not the only way one can be reached: a sweep tick, a `corium saga expire`,
/// or an abort by hand can all get there first. None of them may apply the
/// compensation, because the timeline that actually ran the saga is applying
/// its own — and none of them may stand up a branch, because absence is the
/// signal the invariant reads.
#[tokio::test(flavor = "multi_thread")]
async fn ending_an_inherited_saga_by_hand_still_skips_the_compensation() {
    for status in [SagaStatus::Aborted, SagaStatus::Expired] {
        let (node, _dir) = node().await;
        let ids = transact(&node, "main", "[{:db/id \"o\" :order/status :placed}]").await;
        let order = ids["o"];
        open_saga(
            &node,
            FOREVER,
            &format!(
                "[:db/add \"s\" :db.saga/on-abort-tx
                  \"[[:db/add #eid {order} :order/failure \\\"never twice\\\"]]\"]"
            ),
        )
        .await;
        node.fork_db("main", "copy", 0).await.expect("fork");

        // Straight to the transition. The background pass may have expired the
        // entry already, in which case this refuses as a finished saga — what
        // must never happen either way is the compensation landing.
        if let Ok(response) = node
            .saga_finish("copy", SAGA, &status, Intent::Registered)
            .await
        {
            assert!(
                !response.compensated,
                "{status} applied a compensation on a timeline that never ran the saga"
            );
            assert!(
                response.on_abort_error.contains("no branch"),
                "unhelpful: {}",
                response.on_abort_error
            );
        }

        let copy = node.db_state("copy").await.expect("state").db();
        let failure = copy
            .idents()
            .entid(&corium_core::Keyword::new(Some("order"), "failure"))
            .expect(":order/failure is installed");
        assert!(
            copy.values(corium_core::EntityId::from_raw(order), failure)
                .is_empty()
        );
        assert!(
            !node.branch_exists("copy", SAGA).await.expect("store read"),
            "and it did not stand up the branch it just declined to read"
        );
    }
}

/// Reading a branch is not a way to acquire one. A client that opens a branch
/// on an entry this database only inherited would otherwise write the very
/// metadata root whose absence the liveness invariant reads, convincing the
/// database for good that the saga is its own.
#[tokio::test(flavor = "multi_thread")]
async fn opening_an_inherited_sagas_branch_does_not_create_it() {
    let (node, _dir) = node().await;
    open_saga(&node, FOREVER, "").await;
    node.fork_db("main", "copy", 0).await.expect("fork");

    // Hosting the fork starts the liveness pass, so this open may land before
    // or after it. Both orders must refuse, and — the point of the test —
    // neither may leave a metadata root behind, because that root is the
    // signal the invariant reads.
    match node.saga_branch("copy", SAGA).await {
        Err(error) => assert!(
            error.to_string().contains("no branch") || error.to_string().contains("discarded"),
            "unhelpful: {error}"
        ),
        Ok(_) => panic!("a branch this database never had must not open"),
    }
    assert!(
        !node.branch_exists("copy", SAGA).await.expect("store read"),
        "a refused open leaves the invariant's signal intact"
    );

    // Which is what keeps the pass correct whenever it runs.
    node.expire_branchless_sagas("copy")
        .await
        .expect("the liveness pass");
    await_status(&node, "copy", &SagaStatus::Expired).await;
    assert!(!node.branch_exists("copy", SAGA).await.expect("store read"));
}

/// The recurring sweep is a policy an operator may take over; refusing to
/// adopt sagas that were never this database's is not.
#[tokio::test(flavor = "multi_thread")]
async fn the_liveness_pass_runs_even_with_the_sweep_disabled() {
    let dir = tempfile::tempdir().expect("data dir");
    let node = node_at(dir.path()).await;
    assert!(
        node.create_db("main", &schema(), None)
            .await
            .expect("create")
    );
    open_saga(&node, FOREVER, "").await;
    node.fork_db("main", "copy", 0).await.expect("fork");
    node.release_leases().await;
    drop(node);

    // This suite's config sets `saga_sweep_interval` to `None` throughout,
    // which is exactly the operator's `--saga-sweep-interval off`.
    let node = node_at(dir.path()).await;
    node.db_state("copy").await.expect("host the fork");

    await_status(&node, "copy", &SagaStatus::Expired).await;
}

/// A stepless branch is only a metadata root, and discarding one is work the
/// sweep's report has to own up to.
#[tokio::test(flavor = "multi_thread")]
async fn retention_reports_discarding_a_branch_that_never_took_a_step() {
    let (node, _dir) = node().await;
    open_saga(&node, FOREVER, "[:db/add \"s\" :db.saga/retain-for 0]").await;
    assert!(node.branch_exists("main", SAGA).await.expect("store read"));
    node.saga_finish("main", SAGA, &SagaStatus::Aborted, Intent::Registered)
        .await
        .expect("abort");

    let report = node.saga_sweep("main").await.expect("a sweep");
    assert_eq!(report.discarded, vec![SAGA]);
    assert!(!node.branch_exists("main", SAGA).await.expect("store read"));
}
