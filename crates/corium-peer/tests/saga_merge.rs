//! Committing a saga through the client API: the merge round trip a caller
//! actually writes, including the conflict-and-resolve loop (ADR-0023).

use std::net::TcpListener;
use std::time::Duration;

use corium_core::{EntityId, Keyword, Value};
use corium_db::Db;
use corium_db::saga::SagaStatus;
use corium_peer::saga::{MergeOutcome, SagaOptions};
use corium_peer::{Admin, ConnectConfig, Connection, PeerError};
use corium_protocol::authz::Guard;
use corium_query::edn::{Edn, read_all};
use corium_transactor::node::{NodeConfig, TransactorNode};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

async fn start_transactor() -> (String, tokio::sync::oneshot::Sender<()>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = NodeConfig::new(dir.path().join("data"));
    config.owner = "saga-merge-test".into();
    config.lease_ttl_ms = 600_000;
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    let node = TransactorNode::open(config).await.expect("open node");
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().expect("addr");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(corium_transactor::server::serve(
        node,
        addr,
        Guard::disabled(),
        None,
        async move {
            let _ = stop_rx.await;
        },
    ));
    let endpoint = format!("http://{addr}");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(mut admin) = Admin::connect(&endpoint, None, None).await
            && admin.list_databases().await.is_ok()
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "transactor never ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (endpoint, stop_tx, dir)
}

fn forms(text: &str) -> Vec<Edn> {
    read_all(text).expect("test EDN")
}

async fn connect(endpoint: &str, db: &str) -> Connection {
    let mut admin = Admin::connect(endpoint, None, None).await.expect("admin");
    admin
        .create_database(
            db,
            &forms(
                "{:db/ident :order/status
                  :db/valueType :db.type/string
                  :db/cardinality :db.cardinality/one}
                 {:db/ident :order/note
                  :db/valueType :db.type/string
                  :db/cardinality :db.cardinality/one}
                 {:db/ident :order/total
                  :db/valueType :db.type/long
                  :db/cardinality :db.cardinality/one}",
            ),
        )
        .await
        .expect("create db");
    Connection::connect(ConnectConfig::new(endpoint, db))
        .await
        .expect("connect")
}

fn string_of(db: &Db, entity: EntityId, name: &str) -> Option<String> {
    let attribute = db.idents().entid(&Keyword::new(Some("order"), name))?;
    match db.values(entity, attribute).first() {
        Some(Value::Str(text)) => Some(text.to_string()),
        _ => None,
    }
}

fn long_of(db: &Db, entity: EntityId, name: &str) -> Option<i64> {
    let attribute = db.idents().entid(&Keyword::new(Some("order"), name))?;
    match db.values(entity, attribute).first() {
        Some(Value::Long(value)) => Some(*value),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_committed_saga_lands_every_step_at_one_basis() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "orders").await;
    let placed = connection
        .transact(forms(
            "{:db/id \"o\" :order/status \"placed\" :order/total 10}",
        ))
        .await
        .expect("an order exists before the saga");
    let order = placed.tempids["o"];

    let opened = connection
        .saga_open(&SagaOptions::new("alice", 4_000_000_000_000).describing("fulfilment"))
        .await
        .expect("the saga opens");
    let saga = opened.entry.id;
    let branch = connection.saga_branch(saga).await.expect("the branch");

    branch
        .step(forms(&format!(
            "[:db/add #eid {} :order/status \"picked\"]",
            order.raw()
        )))
        .await
        .expect("step one");
    branch
        .step(forms(&format!(
            "[:db/add #eid {} :order/status \"shipped\"]",
            order.raw()
        )))
        .await
        .expect("step two");
    let drafted = branch
        .step(forms("{:db/id \"label\" :order/note \"one widget\"}"))
        .await
        .expect("step three");
    let label = drafted.tempids["label"];

    let before = connection.sync().await.expect("sync").basis_t();
    let outcome = connection
        .saga_commit(saga, vec![], vec![])
        .await
        .expect("the merge");
    let MergeOutcome::Committed(report) = &outcome else {
        panic!("expected a merge, got {outcome:?}");
    };
    assert_eq!(report.basis_before, before);
    assert_eq!(report.basis_t, before + 1, "three steps, one parent commit");
    assert_eq!(report.steps, 3);
    assert!(report.datoms > 0);

    // The connection is caught up to the merge, so the caller reads the
    // saga's whole effect without asking for it.
    let db = connection.db();
    assert!(db.basis_t() >= report.basis_t);
    assert_eq!(
        string_of(&db, order, "status").as_deref(),
        Some("shipped"),
        "the net effect, not the step the branch passed through"
    );
    assert_eq!(
        string_of(&db, label, "note").as_deref(),
        Some("one widget"),
        "ids minted on the branch are the ids in the parent"
    );

    let entry = connection
        .saga(saga)
        .await
        .expect("registry read")
        .expect("the saga is still on the registry");
    assert_eq!(entry.status, Some(SagaStatus::Committed));
    assert_eq!(entry.steps, Some(3));
    assert_eq!(
        entry.merged_tx.map(EntityId::sequence),
        Some(report.basis_t),
        "the registry names the transaction the merge landed as"
    );

    // The branch is retained, and it takes no further steps.
    let error = branch
        .step(forms("{:db/id \"late\" :order/note \"too late\"}"))
        .await
        .expect_err("a committed saga's branch is closed to steps");
    assert!(
        error.to_string().contains("no longer accepts steps"),
        "unexpected: {error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drift_comes_back_as_a_report_the_caller_answers() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "orders").await;
    let placed = connection
        .transact(forms(
            "{:db/id \"o\" :order/status \"placed\" :order/total 10}",
        ))
        .await
        .expect("an order");
    let order = placed.tempids["o"];

    let opened = connection
        .saga_open(&SagaOptions::new("alice", 4_000_000_000_000))
        .await
        .expect("the saga opens");
    let saga = opened.entry.id;
    let branch = connection.saga_branch(saga).await.expect("the branch");
    branch
        .step(forms(&format!(
            "[:db/add #eid {} :order/status \"shipped\"]
             [:db/add #eid {} :order/total 12]",
            order.raw(),
            order.raw()
        )))
        .await
        .expect("the step");

    // A tier-0 writer moves the same pair while the saga is in flight. It is
    // not blocked from doing so; the merge is where that is settled.
    connection
        .transact(forms(&format!(
            "[:db/add #eid {} :order/status \"cancelled\"]",
            order.raw()
        )))
        .await
        .expect("the parent moves");

    let outcome = connection
        .saga_commit(saga, vec![], vec![])
        .await
        .expect("a refused merge is an answer");
    let MergeOutcome::Conflict(report) = &outcome else {
        panic!("expected a conflict, got {outcome:?}");
    };
    assert!(outcome.merged().is_none());
    assert_eq!(report.steps, 1);
    assert!(report.report.contains(":write-write"), "{}", report.report);
    assert!(report.report.contains("shipped"), "{}", report.report);
    assert!(report.report.contains("cancelled"), "{}", report.report);

    // The saga is untouched: still open, its branch still readable, and the
    // report is on the registry for a process that was not the one asking.
    let entry = connection
        .saga(saga)
        .await
        .expect("registry read")
        .expect("still registered");
    assert_eq!(entry.status, Some(SagaStatus::Open));
    assert_eq!(
        entry.conflict_report.as_deref(),
        Some(report.report.as_str())
    );
    assert_eq!(
        string_of(&branch.sync().await.expect("branch sync"), order, "status").as_deref(),
        Some("shipped"),
        "the branch keeps what it wrote"
    );

    // Answering the one unit the report named lets the rest through.
    let outcome = connection
        .saga_commit(
            saga,
            vec![],
            forms(&format!(
                "{{:e {} :a :order/status :parent \"cancelled\" :take :parent}}",
                order.raw()
            )),
        )
        .await
        .expect("the answered merge");
    let MergeOutcome::Committed(report) = &outcome else {
        panic!("expected a merge, got {outcome:?}");
    };
    assert_eq!(report.steps, 1);
    let db = connection.db();
    assert_eq!(
        string_of(&db, order, "status").as_deref(),
        Some("cancelled"),
        "accept-parent drops the branch's write for that pair"
    );
    assert_eq!(
        long_of(&db, order, "total"),
        Some(12),
        "and only that pair: the rest of the saga still lands"
    );
    let entry = connection
        .saga(saga)
        .await
        .expect("registry read")
        .expect("still registered");
    assert_eq!(entry.status, Some(SagaStatus::Committed));
    assert_eq!(
        entry.conflict_report, None,
        "the stale report is cleared by the merge that succeeded"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_guard_is_checked_even_when_nothing_collided() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "orders").await;
    let placed = connection
        .transact(forms(
            "{:db/id \"o\" :order/status \"placed\" :order/total 10}",
        ))
        .await
        .expect("an order");
    let order = placed.tempids["o"];
    let opened = connection
        .saga_open(&SagaOptions::new("alice", 4_000_000_000_000))
        .await
        .expect("the saga opens");
    let saga = opened.entry.id;
    let branch = connection.saga_branch(saga).await.expect("the branch");
    branch
        .step(forms("{:db/id \"n\" :order/note \"packed by hand\"}"))
        .await
        .expect("the step");

    // The saga's novelty touches nothing the parent touched, so only a guard
    // can express the read the caller depended on.
    connection
        .transact(forms(&format!(
            "[:db/add #eid {} :order/total 99]",
            order.raw()
        )))
        .await
        .expect("the parent moves");

    let outcome = connection
        .saga_commit(
            saga,
            forms(&format!("{{:cas [{} :order/total 10]}}", order.raw())),
            vec![],
        )
        .await
        .expect("answered");
    let MergeOutcome::Conflict(report) = &outcome else {
        panic!("a broken guard refuses the merge, got {outcome:?}");
    };
    assert!(report.report.contains(":guards"), "{}", report.report);

    // Without it the same merge lands: guards are the caller's to declare.
    let outcome = connection
        .saga_commit(saga, vec![], vec![])
        .await
        .expect("answered");
    assert!(outcome.merged().is_some(), "{outcome:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_saga_that_is_over_refuses_to_merge_before_it_asks() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "orders").await;
    let opened = connection
        .saga_open(&SagaOptions::new("alice", 4_000_000_000_000))
        .await
        .expect("the saga opens");
    let saga = opened.entry.id;
    connection.saga_abort(saga).await.expect("abort");

    let error = connection
        .saga_commit(saga, vec![], vec![])
        .await
        .expect_err("an aborted saga has nothing to merge");
    assert!(matches!(error, PeerError::Saga(_)), "unexpected: {error}");
}
