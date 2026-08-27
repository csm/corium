//! The saga registry driven end to end against a live transactor: open,
//! extend, reserve, record external compensation, abort (ADR-0023).

use std::net::TcpListener;
use std::time::Duration;

use corium_core::{EntityId, Keyword, Partition};
use corium_db::saga::SagaStatus;
use corium_peer::saga::{CompensationRecord, SagaOptions, compensation_for, format_saga_id};
use corium_peer::{Admin, ConnectConfig, Connection, PeerError};
use corium_protocol::authz::Guard;
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
    config.owner = "saga-test".into();
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

async fn connect(endpoint: &str, db: &str) -> Connection {
    let mut admin = Admin::connect(endpoint, None, None).await.expect("admin");
    admin.create_database(db, &[]).await.expect("create db");
    Connection::connect(ConnectConfig::new(endpoint, db))
        .await
        .expect("connect")
}

fn entity(sequence: u64) -> EntityId {
    EntityId::new(Partition::User as u32, sequence)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_saga_opens_extends_reserves_and_aborts() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "sagas").await;

    let options = SagaOptions::new("alice", 4_000_000_000_000)
        .describing("quarterly reconciliation")
        .declaring([entity(9_000)]);
    let opened = connection
        .saga_open(&options)
        .await
        .expect("the saga opens");
    let id = opened.entry.id;
    assert_eq!(opened.entry.status, Some(SagaStatus::Open));
    assert_eq!(opened.entry.owner.as_deref(), Some("alice"));
    assert_eq!(
        opened.entry.description.as_deref(),
        Some("quarterly reconciliation")
    );
    assert_eq!(opened.entry.footprint, vec![entity(9_000)]);
    assert_eq!(opened.entry.expires_at, Some(4_000_000_000_000));
    // The branch will be rooted where the opener was looking.
    assert_eq!(
        opened.entry.basis_t,
        Some(i64::try_from(opened.tx.basis_before).expect("a basis fits"))
    );

    // Opening the same id twice is refused: an id names one saga for the
    // life of the database.
    let again = connection
        .saga_open(&options.clone().with_id(id))
        .await
        .expect_err("an id is not reusable");
    assert!(matches!(again, PeerError::Saga(_)));

    let extended = connection
        .saga_extend(id, 5_000_000_000_000)
        .await
        .expect("the owner extends the deadline");
    assert_eq!(extended.entry.expires_at, Some(5_000_000_000_000));

    let reserved = connection
        .saga_reserve(id, [entity(9_000), entity(9_001)])
        .await
        .expect("an unsealed set grows");
    assert_eq!(reserved.entry.reserves, vec![entity(9_000), entity(9_001)]);

    // The registry is queryable data: every surface sees the same entry.
    let listed = connection.sagas().await.expect("the registry lists");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
    assert_eq!(
        connection
            .saga(id)
            .await
            .expect("the entry reads back")
            .map(|entry| entry.status),
        Some(Some(SagaStatus::Open))
    );

    let aborted = connection.saga_abort(id).await.expect("the owner aborts");
    assert_eq!(aborted.entry.status, Some(SagaStatus::Aborted));

    // A second abort is not a no-op success: the saga already finished.
    let error = connection
        .saga_abort(id)
        .await
        .expect_err("an aborted saga cannot abort again");
    match error {
        PeerError::Saga(message) => {
            assert!(
                message.contains(&format_saga_id(id)) && message.contains("aborted"),
                "unhelpful message: {message}"
            );
        }
        other => panic!("expected a saga error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sealed_saga_refuses_to_widen_its_reservations() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "sealed").await;

    let opened = connection
        .saga_open(
            &SagaOptions::new("alice", 4_000_000_000_000)
                .reserving([entity(9_000)])
                .sealed(),
        )
        .await
        .expect("a sealed saga opens");
    assert!(opened.entry.sealed);

    let error = connection
        .saga_reserve(opened.entry.id, [entity(9_001)])
        .await
        .expect_err("a sealed set is fixed at open");
    assert!(matches!(error, PeerError::Saga(_)));
    assert_eq!(
        connection
            .saga(opened.entry.id)
            .await
            .expect("the entry survives")
            .expect("the saga is there")
            .reserves,
        vec![entity(9_000)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_compensation_ledger_is_keyed_and_outlives_the_saga() {
    let (endpoint, _stop, _dir) = start_transactor().await;
    let connection = connect(&endpoint, "ledger").await;

    let id = connection
        .saga_open(&SagaOptions::new("orchestrator", 4_000_000_000_000))
        .await
        .expect("the saga opens")
        .entry
        .id;
    connection.saga_abort(id).await.expect("the saga aborts");

    let pending = Keyword::new(Some("db.saga.compensation.status"), "pending");
    let done = Keyword::new(Some("db.saga.compensation.status"), "completed");

    let recorded = connection
        .saga_compensate(
            id,
            &CompensationRecord::new("refund:1234")
                .with_status(pending.clone())
                .with_detail("issuing refund"),
        )
        .await
        .expect("an aborted saga still takes ledger entries");
    assert_eq!(recorded.entry.compensations.len(), 1);
    assert_eq!(
        compensation_for(&recorded.entry, "refund:1234").and_then(|entry| entry.status.clone()),
        Some(pending)
    );

    // Retrying the same compensation updates its entry rather than adding a
    // second row for the same external effect.
    let updated = connection
        .saga_compensate(
            id,
            &CompensationRecord::new("refund:1234")
                .with_status(done.clone())
                .completed_at(1_700_000_000_000),
        )
        .await
        .expect("the entry updates in place");
    assert_eq!(updated.entry.compensations.len(), 1);
    let entry = compensation_for(&updated.entry, "refund:1234").expect("the entry is keyed");
    assert_eq!(entry.status.clone(), Some(done));
    assert_eq!(entry.completed_at, Some(1_700_000_000_000));
    assert_eq!(entry.detail.as_deref(), Some("issuing refund"));
    assert_eq!(updated.entry.status, Some(SagaStatus::Aborted));
}
