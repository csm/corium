//! End-to-end tests driving the fluent client against a real transactor,
//! through both the local peer library and a remote peer server over gRPC.
//!
//! The same fluent query/pull/datoms/transact code runs against each backend
//! and the results are asserted for parity, proving the two surfaces agree.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use corium_client::pull::Pull;
use corium_client::query::{Query, attr, data, gte, var};
use corium_client::tx::{EntityMap, TxBuilder, lookup, tempid};
use corium_client::{Args, Index, LocalPeer, Peer, RemotePeer};
use corium_peer::server::{PeerServerConfig, serve as serve_peer};
use corium_peer::{Admin, ConnectConfig, Connection};
use corium_protocol::auth::StaticToken;
use corium_query::edn::read_all;
use corium_transactor::node::{NodeConfig, TransactorNode};

const SCHEMA: &str = r"
{:db/ident :person/name
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/unique :db.unique/identity}
{:db/ident :person/age
 :db/valueType :db.type/long
 :db/cardinality :db.cardinality/one}
{:db/ident :person/friend
 :db/valueType :db.type/ref
 :db/cardinality :db.cardinality/many}
";

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
    config.owner = "e2e".into();
    config.lease_ttl_ms = 600_000;
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    let node = TransactorNode::open(config).await.expect("open node");
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().expect("addr");
    let auth = Arc::new(StaticToken::new(None));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(corium_transactor::server::serve(
        node,
        addr,
        auth,
        None,
        async move {
            let _ = stop_rx.await;
        },
    ));
    let endpoint = format!("http://{addr}");
    // Wait for readiness.
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

fn schema_forms() -> Vec<corium_query::edn::Edn> {
    read_all(SCHEMA).expect("schema parses")
}

/// Loads Alice (39) and Bob (40, friend of Alice) through the fluent tx
/// builder; returns the basis after the first (Alice-only) transaction.
async fn load_people(peer: &impl Peer) -> u64 {
    let first = peer
        .transact(
            TxBuilder::new()
                .entity(
                    EntityMap::with_id(tempid("alice"))
                        .set("person/name", "Alice")
                        .set("person/age", 39_i64),
                )
                .build(),
        )
        .await
        .expect("first tx");

    peer.transact(
        TxBuilder::new()
            .entity(
                EntityMap::with_id(tempid("bob"))
                    .set("person/name", "Bob")
                    .set("person/age", 40_i64),
            )
            .add(
                tempid("bob"),
                "person/friend",
                lookup("person/name", "Alice"),
            )
            .build(),
    )
    .await
    .expect("second tx");

    first.basis_t
}

/// Runs the shared fluent read workload against a database value and returns
/// the observations, so the two backends can be compared for parity.
async fn observe(db: &corium_client::Db) -> Observations {
    // Names of people at least 40, via a predicate and a scalar input.
    let adults = Query::find_coll(var("name"))
        .in_scalar(var("min"))
        .where_(data(var("e"), attr("person/name"), var("name")))
        .and(data(var("e"), attr("person/age"), var("age")))
        .and(gte(var("age"), var("min")));
    let adults = db
        .query(&adults, Args::new().scalar(40_i64))
        .await
        .expect("adults query")
        .values_as::<String>()
        .expect("names");

    // Pull Alice by lookup ref.
    let pulled = db
        .pull(
            &Pull::new().attr("person/name").attr("person/age"),
            lookup("person/name", "Alice"),
        )
        .await
        .expect("pull alice");

    // Count of :person/age datoms via the attribute index (AEVT is always
    // covered; AVET only covers indexed/unique attributes).
    let age_datoms = db
        .datoms(Index::Aevt, vec![attr("person/age")], 0)
        .await
        .expect("age datoms")
        .len();

    let stats = db.stats().await.expect("stats");

    Observations {
        adults,
        pulled: pulled.to_string(),
        age_datoms,
        basis_t: stats.basis_t,
        datoms: stats.datoms,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Observations {
    adults: Vec<String>,
    pulled: String,
    age_datoms: usize,
    basis_t: u64,
    datoms: u64,
}

#[tokio::test]
async fn local_and_remote_backends_agree() {
    let (endpoint, stop, _dir) = start_transactor().await;
    Admin::connect(&endpoint, None, None)
        .await
        .expect("admin")
        .create_database("people", &schema_forms())
        .await
        .expect("create db");

    // --- Local peer: query directly from storage. ---
    let local = LocalPeer::connect(ConnectConfig::new(&endpoint, "people"))
        .await
        .expect("local connect");
    let first_basis = load_people(&local).await;
    let local_db = local.sync().await.expect("local sync");
    let local_obs = observe(&local_db).await;

    assert_eq!(local_obs.adults, vec!["Bob".to_string()]);
    assert!(local_obs.pulled.contains(":person/name \"Alice\""));
    assert!(local_obs.pulled.contains(":person/age 39"));
    assert_eq!(local_obs.age_datoms, 2);

    // Time travel: as-of the first transaction only Alice exists.
    let as_of_first = local_db.as_of(first_basis);
    let early = observe(&as_of_first).await;
    assert_eq!(early.age_datoms, 1, "only Alice existed at the first basis");
    assert!(early.adults.is_empty(), "Alice (39) is not an adult at 40+");

    // --- Remote peer: same surface over the peer-server gRPC. ---
    let hosted = Arc::new(
        Connection::connect(ConnectConfig::new(&endpoint, "people"))
            .await
            .expect("hosted connection"),
    );
    // Ensure the hosted peer has caught up before serving.
    hosted.sync().await.expect("hosted sync");
    let peer_addr: std::net::SocketAddr =
        format!("127.0.0.1:{}", free_port()).parse().expect("addr");
    let (peer_stop_tx, peer_stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve_peer(
        hosted,
        peer_addr,
        Arc::new(StaticToken::new(None)),
        None,
        PeerServerConfig::default(),
        async move {
            let _ = peer_stop_rx.await;
        },
    ));
    let peer_endpoint = format!("http://{peer_addr}");

    // Wait for the peer server to accept requests and report the full basis.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let remote = loop {
        if let Ok(remote) = RemotePeer::connect(&peer_endpoint, "people", None, None).await
            && let Ok(db) = remote.db().await
            && db.basis_t().await.unwrap_or(0) >= local_obs.basis_t
        {
            break remote;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "peer server never ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let remote_db = remote.db().await.expect("remote db");
    let remote_obs = observe(&remote_db).await;

    // The two backends observe the same database.
    assert_eq!(local_obs, remote_obs, "local and remote backends disagree");

    let _ = stop.send(());
    let _ = peer_stop_tx.send(());
}
