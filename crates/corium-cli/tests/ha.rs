//! M7 acceptance battery: real active/standby failover over gRPC.
//!
//! Covers: a standby transactor rejecting work while the active is
//! healthy; kill -9 of the active under load with the standby serving
//! writes within the lease-expiry bound; peers failing over their
//! subscriptions and transactions with zero acked-transaction loss and no
//! duplicates; and lease-holder rediscovery through the root record.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use corium_peer::{Admin, ConnectConfig, Connection};
use corium_query::edn::{Edn, read_one};
use corium_store::{DbRoot, FsStore, RootStore, db_root_name};

const SCHEMA: &str = r"[{:db/ident :k/v :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}]";
const LEASE_TTL_MS: u64 = 1_000;

fn schema_forms() -> Vec<Edn> {
    match read_one(SCHEMA).expect("schema EDN") {
        Edn::Vector(items) => items,
        other => panic!("bad schema {other}"),
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

struct TransactorProc {
    child: Child,
    port: u16,
}

impl TransactorProc {
    /// Spawns an HA transactor advertising its own endpoint.
    fn spawn_ha(data_dir: &Path, port: u16, owner: &str) -> Self {
        let endpoint = format!("http://127.0.0.1:{port}");
        let child = Command::new(env!("CARGO_BIN_EXE_corium"))
            .args([
                "transactor",
                "--data-dir",
                data_dir.to_str().expect("utf8"),
                "--listen",
                &format!("127.0.0.1:{port}"),
                "--owner",
                owner,
                "--ha",
                "--advertise",
                &endpoint,
                "--lease-ttl-ms",
                &LEASE_TTL_MS.to_string(),
                "--index-interval-ms",
                "300",
                "--heartbeat-ms",
                "500",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn transactor");
        Self { child, port }
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn kill9(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    async fn wait_ready(&self) -> Admin {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(mut admin) = Admin::connect(&self.endpoint(), None, None).await
                && admin.list_databases().await.is_ok()
            {
                return admin;
            }
            assert!(
                Instant::now() < deadline,
                "transactor on port {} never became ready",
                self.port
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for TransactorProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn add_value(value: i64) -> Vec<Edn> {
    vec![Edn::Vector(vec![
        Edn::keyword("db/add"),
        Edn::Str("e".into()),
        Edn::keyword("k/v"),
        Edn::Long(value),
    ])]
}

fn long_values(db: &corium_db::Db) -> Vec<i64> {
    db.datoms()
        .into_iter()
        .filter_map(|datom| match datom.v {
            corium_core::Value::Long(v) => Some(v),
            _ => None,
        })
        .collect()
}

async fn read_root(data: &Path, db: &str) -> DbRoot {
    FsStore::open(data.join("store"))
        .expect("open store")
        .get_root(&db_root_name(db))
        .await
        .expect("read root")
        .as_deref()
        .and_then(DbRoot::decode)
        .expect("decodable root")
}

/// Commits `value` exactly once across a failover: safe rejections are
/// retried by the peer library; an ambiguous in-flight failure (the kill
/// window) is resolved by resyncing and checking whether the value already
/// committed before resubmitting — the documented client pattern.
async fn ensure_committed(peer: &Connection, value: i64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match peer.transact(add_value(value)).await {
            Ok(_) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "value {value} never committed: {error}"
                );
                // Wait until the peer is resubscribed and caught up, then
                // check whether the ambiguous attempt actually committed.
                let synced = loop {
                    if let Ok(db) = peer.sync().await {
                        break db;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "peer never resynced after failover"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                };
                if long_values(&synced).contains(&value) {
                    return;
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn standby_serves_writes_after_kill9_with_zero_acked_loss() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let port_a = free_port();
    let port_b = free_port();
    let mut active = TransactorProc::spawn_ha(&data, port_a, "owner-a");
    let mut admin = active.wait_ready().await;
    assert!(
        admin
            .create_database("ha", &schema_forms())
            .await
            .expect("create db")
    );
    let standby = TransactorProc::spawn_ha(&data, port_b, "owner-b");
    let _standby_admin = standby.wait_ready().await;

    // While the active is healthy, the standby refuses subscriptions with
    // a standby rejection rather than serving stale data.
    let refused = Connection::connect(ConnectConfig::new(standby.endpoint(), "ha")).await;
    match refused {
        Err(corium_peer::PeerError::Rpc(status)) => {
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(
                status.message().contains("standby"),
                "unexpected rejection: {}",
                status.message()
            );
        }
        Ok(_) => panic!("standby accepted a subscription while the active is healthy"),
        Err(other) => panic!("unexpected standby rejection: {other}"),
    }

    // The root record names the active as lease holder (peer rediscovery).
    let root = read_root(&data, "ha").await;
    assert_eq!(root.owner, "owner-a");
    assert_eq!(root.owner_endpoint, active.endpoint());

    // Peers configured with the pair; a reader subscribes before the kill.
    let writer = Connection::connect(ConnectConfig::with_failover(
        vec![active.endpoint(), standby.endpoint()],
        "ha",
    ))
    .await
    .expect("writer connects");
    let reader = Connection::connect(ConnectConfig::with_failover(
        vec![active.endpoint(), standby.endpoint()],
        "ha",
    ))
    .await
    .expect("reader connects");

    // Load, killing the active mid-stream.
    let total = 60_i64;
    let kill_after = 20_i64;
    let mut killed_at = None;
    let mut recovered_at = None;
    for value in 0..total {
        if value == kill_after {
            active.kill9();
            killed_at = Some(Instant::now());
        }
        ensure_committed(&writer, value).await;
        if let (Some(kill), None) = (killed_at, recovered_at) {
            recovered_at = Some(kill.elapsed());
        }
    }
    let takeover_latency = recovered_at.expect("recovery observed");
    // The standby polls at ttl/3 and takes over once the lease lapses; the
    // whole outage must stay within the lease-expiry bound plus retry
    // margin (generous for CI, still far below a restart-and-recover).
    assert!(
        takeover_latency < Duration::from_secs(15),
        "takeover took {takeover_latency:?}, beyond the lease-expiry bound"
    );

    // Every value committed exactly once, from a fully synced peer.
    let db = writer.sync().await.expect("sync with new active");
    let mut values = long_values(&db);
    values.sort_unstable();
    assert_eq!(
        values,
        (0..total).collect::<Vec<_>>(),
        "acked transaction lost or duplicated across failover"
    );
    // The merged durable log is gapless.
    let ts: Vec<u64> = db.tx_range(0, None).into_iter().map(|(t, _)| t).collect();
    assert_eq!(ts, (1..=values.len() as u64).collect::<Vec<_>>());

    // The passive reader failed over without losing its subscription.
    let target = db.basis_t();
    let deadline = Instant::now() + Duration::from_secs(20);
    while reader.basis_t() < target {
        assert!(
            Instant::now() < deadline,
            "reader never converged after failover (at {} of {target})",
            reader.basis_t()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut reader_values = long_values(&reader.db());
    reader_values.sort_unstable();
    assert_eq!(reader_values, values, "reader diverged across failover");

    // Lease-holder rediscovery: the root record now names the standby.
    let root = read_root(&data, "ha").await;
    assert_eq!(root.owner, "owner-b", "standby never took the lease over");
    assert_eq!(root.owner_endpoint, standby.endpoint());

    // The new active keeps publishing indexes under its own lease version.
    let published = read_root(&data, "ha").await;
    assert!(published.lease_version >= 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn ha_pair_survives_repeated_failover() {
    // Kill the active twice (the second time the restarted first node has
    // rejoined as standby), proving deposed processes rejoin the pair and
    // the log stays consistent across multiple lease generations.
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let port_a = free_port();
    let port_b = free_port();
    let mut node_a = TransactorProc::spawn_ha(&data, port_a, "owner-a");
    let mut admin = node_a.wait_ready().await;
    admin
        .create_database("gens", &schema_forms())
        .await
        .expect("create db");
    let node_b = TransactorProc::spawn_ha(&data, port_b, "owner-b");
    let _ = node_b.wait_ready().await;

    let peer = Connection::connect(ConnectConfig::with_failover(
        vec![node_a.endpoint(), node_b.endpoint()],
        "gens",
    ))
    .await
    .expect("peer connects");

    for value in 0..10 {
        ensure_committed(&peer, value).await;
    }
    // First failover: B takes over.
    node_a.kill9();
    for value in 10..20 {
        ensure_committed(&peer, value).await;
    }
    assert_eq!(read_root(&data, "gens").await.owner, "owner-b");

    // A rejoins as standby, then B dies: A takes over again.
    let node_a = TransactorProc::spawn_ha(&data, port_a, "owner-a");
    let _ = node_a.wait_ready().await;
    let mut node_b = node_b;
    node_b.kill9();
    for value in 20..30 {
        ensure_committed(&peer, value).await;
    }
    let root = read_root(&data, "gens").await;
    assert_eq!(root.owner, "owner-a", "restarted node never took over");
    assert!(root.lease_version >= 3, "each takeover bumps the fence");

    let db = peer.sync().await.expect("final sync");
    let mut values = long_values(&db);
    values.sort_unstable();
    assert_eq!(
        values,
        (0..30).collect::<Vec<_>>(),
        "loss or duplication across repeated failovers"
    );
    let ts: Vec<u64> = db.tx_range(0, None).into_iter().map(|(t, _)| t).collect();
    assert_eq!(ts, (1..=30).collect::<Vec<_>>(), "gap in the merged log");
}
