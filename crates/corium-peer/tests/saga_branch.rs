//! Tier-2 reads and step transacting against a live transactor: a saga's
//! branch as an ordinary connection (ADR-0023).

use std::net::TcpListener;
use std::time::Duration;

use corium_core::{Keyword, Value};
use corium_db::Db;
use corium_peer::saga::SagaOptions;
use corium_peer::{Admin, ConnectConfig, Connection};
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
    config.owner = "saga-branch-test".into();
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
                  :db/cardinality :db.cardinality/one}",
            ),
        )
        .await
        .expect("create db");
    Connection::connect(ConnectConfig::new(endpoint, db))
        .await
        .expect("connect")
}

fn status_of(db: &Db, entity: corium_core::EntityId) -> Option<String> {
    let attribute = db.idents().entid(&Keyword::new(Some("order"), "status"))?;
    match db.values(entity, attribute).first() {
        Some(Value::Str(text)) => Some(text.to_string()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_branch_is_an_ordinary_connection_to_partial_progress() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "orders").await;
    let placed = connection
        .transact(forms("{:db/id \"o\" :order/status \"placed\"}"))
        .await
        .expect("an order exists before the saga");
    let order = placed.tempids["o"];

    let opened = connection
        .saga_open(&SagaOptions::new("alice", 4_000_000_000_000).describing("fulfilment"))
        .await
        .expect("the saga opens");
    let saga = opened.entry.id;
    assert!(
        !opened.entry.grants.is_empty(),
        "opening leases the branch an entity-id block"
    );

    let branch = connection
        .saga_branch(saga)
        .await
        .expect("the branch is there to read");
    assert_eq!(branch.saga_id(), saga);
    assert_eq!(
        branch.basis_t(),
        u64::try_from(opened.entry.basis_t.expect("a basis")).expect("a basis fits")
    );
    assert_eq!(
        status_of(&branch.db(), order).as_deref(),
        Some("placed"),
        "a branch starts as the parent as of t₀"
    );

    // Steps are ordinary transactions, and they land only on the branch.
    let step = branch
        .step(forms(
            "[:db/add #eid ORDER :order/status \"picked\"]"
                .replace("ORDER", &order.raw().to_string())
                .as_str(),
        ))
        .await
        .expect("a step");
    assert!(step.basis_t > branch.basis_t());
    let drafted = branch
        .step(forms("{:db/id \"line\" :order/note \"one widget\"}"))
        .await
        .expect("a step creating novelty");
    let line = drafted.tempids["line"];
    assert!(
        opened.entry.grants[0].holds(line),
        "branch novelty takes ids from the leased block"
    );

    let view = branch.sync().await.expect("branch sync");
    assert_eq!(status_of(&view, order).as_deref(), Some("picked"));
    let parent = connection.sync().await.expect("parent sync");
    assert_eq!(
        status_of(&parent, order).as_deref(),
        Some("placed"),
        "canonical state sees nothing until the merge"
    );

    // Below t₀ a branch answers exactly what the parent answers: the same
    // history, shared rather than copied.
    assert_eq!(
        status_of(&view.as_of(branch.basis_t()), order).as_deref(),
        Some("placed")
    );

    // The step grain the design promises auditors is the branch's own log.
    let steps = branch.steps();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].t, branch.basis_t() + 1);
}

/// An aborted saga's branch is retained, not destroyed with the flip: it is
/// the record of what the workflow did outside the database and still has to
/// unwind. What it stops being is a branch to *work* on.
#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_sagas_branch_is_readable_but_takes_no_more_steps() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "orders").await;
    let opened = connection
        .saga_open(&SagaOptions::new("alice", 4_000_000_000_000))
        .await
        .expect("the saga opens");
    let saga = opened.entry.id;
    let branch = connection
        .saga_branch(saga)
        .await
        .expect("the branch opens");
    branch
        .step(forms("{:db/id \"order\" :order/status \"draft\"}"))
        .await
        .expect("a step lands");

    let aborted = connection.saga_abort(saga).await.expect("abort");
    assert!(!aborted.compensated);
    assert!(aborted.on_abort_error.is_none());

    let branch = connection
        .saga_branch(saga)
        .await
        .expect("a retained branch still opens");
    branch.sync().await.expect("sync");
    assert_eq!(branch.steps().len(), 1, "the step history is still there");

    let error = branch
        .step(forms("{:db/id \"order\" :order/status \"placed\"}"))
        .await
        .expect_err("an aborted saga takes no more steps");
    assert!(error.to_string().contains("aborted"), "unexpected: {error}");
}
