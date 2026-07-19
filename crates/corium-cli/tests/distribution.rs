//! M4 acceptance battery: real multi-process integration over gRPC.
//!
//! Covers: N peers converging on every transaction; kill -9 of the
//! transactor mid-load with zero acked-transaction loss; gapless reconnect
//! backfill; deposed-transactor fencing (a paused process cannot publish);
//! TLS and bearer-token auth; and direct segment reads through the peer
//! segment cache.

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use corium_core::IndexOrder;
use corium_peer::segment::SegmentSource;
use corium_peer::{Admin, ConnectConfig, Connection};
use corium_query::edn::{Edn, read_one};
use corium_store::{BlobStore, DbRoot, FsStore, RootStore};

const SCHEMA: &str = r"[{:db/ident :k/v :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}]";

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

/// A transactor child process, killed on drop.
struct TransactorProc {
    child: Child,
    port: u16,
}

impl TransactorProc {
    fn spawn(data_dir: &Path, port: u16, extra: &[&str]) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_corium"))
            .arg("transactor")
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--listen")
            .arg(format!("127.0.0.1:{port}"))
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn transactor");
        Self { child, port }
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn kill9(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn signal(&self, name: &str) {
        let status = Command::new("kill")
            .arg(format!("-{name}"))
            .arg(self.pid().to_string())
            .status()
            .expect("send signal");
        assert!(status.success(), "kill -{name} failed");
    }

    async fn wait_ready(&self) -> Admin {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(mut admin) = Admin::connect(&self.endpoint(), None, None).await {
                if admin.list_databases().await.is_ok() {
                    return admin;
                }
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

#[tokio::test(flavor = "multi_thread")]
async fn n_peers_converge_and_reconnect_backfills_gaplessly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let port = free_port();
    let mut proc = TransactorProc::spawn(&data, port, &["--index-interval-ms", "300"]);
    let mut admin = proc.wait_ready().await;
    assert!(
        admin
            .create_database("converge", &schema_forms())
            .await
            .expect("create db")
    );

    let mut peers = Vec::new();
    for _ in 0..3 {
        peers.push(
            Connection::connect(ConnectConfig::new(proc.endpoint(), "converge"))
                .await
                .expect("peer connects"),
        );
    }
    let mut reports = peers[1].tx_reports();
    for value in 0..30 {
        peers[0].transact(add_value(value)).await.expect("transact");
    }
    // Every peer converges on every transaction.
    for peer in &peers {
        let db = peer.sync().await.expect("sync");
        assert_eq!(db.basis_t(), 30);
        let mut values = long_values(&db);
        values.sort_unstable();
        assert_eq!(values, (0..30).collect::<Vec<_>>());
    }
    // The tx-report queue observed a gapless, ordered t sequence.
    let mut seen = Vec::new();
    while let Ok(report) = reports.try_recv() {
        seen.push(report.t);
    }
    assert_eq!(seen, (1..=30).collect::<Vec<_>>());

    // Kill -9, restart on the same port and directory: peers reconnect,
    // resubscribe from their basis, and the server backfills the gap.
    proc.kill9();
    let proc = TransactorProc::spawn(&data, port, &["--index-interval-ms", "300"]);
    let _admin = proc.wait_ready().await;
    // New transactions committed while peers are still backing off.
    let writer = Connection::connect(ConnectConfig::new(proc.endpoint(), "converge"))
        .await
        .expect("writer reconnects");
    for value in 30..40 {
        writer.transact(add_value(value)).await.expect("transact");
    }
    for peer in &peers {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if peer.basis_t() >= 40 {
                break;
            }
            assert!(Instant::now() < deadline, "peer never caught up");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let db = peer.db();
        let mut values = long_values(&db);
        values.sort_unstable();
        assert_eq!(values, (0..40).collect::<Vec<_>>(), "gap after reconnect");
        // Local record of every t with no holes.
        let ts: Vec<u64> = db.tx_range(0, None).into_iter().map(|(t, _)| t).collect();
        assert_eq!(ts, (1..=40).collect::<Vec<_>>());
    }

    // Segment cache: read the published index root directly from storage and
    // compare its EAVT keys with the converged peer value at that basis.
    let deadline = Instant::now() + Duration::from_secs(20);
    let source = SegmentSource::new(Arc::new(
        FsStore::open(data.join("store")).expect("open store"),
    ));
    let root = loop {
        if let Some(root) = source.index_root("converge").await.expect("root") {
            if root.roots.is_some() && root.index_basis_t >= 40 {
                break root;
            }
        }
        assert!(Instant::now() < deadline, "index never published");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let segment = source
        .segment(&root, IndexOrder::Eavt)
        .await
        .expect("segment loads")
        .expect("segment present");
    let keys = SegmentSource::<FsStore>::segment_keys(&segment).expect("keys decode");
    let expected: Vec<Vec<u8>> = peers[0]
        .db()
        .as_of(root.index_basis_t)
        .datoms()
        .iter()
        .map(|datom| datom.key(IndexOrder::Eavt))
        .collect();
    assert_eq!(keys, expected, "published segment diverges from peer state");
}

#[tokio::test(flavor = "multi_thread")]
async fn kill9_mid_load_loses_no_acked_transaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let port = free_port();
    let mut proc = TransactorProc::spawn(&data, port, &[]);
    let mut admin = proc.wait_ready().await;
    admin
        .create_database("main", &schema_forms())
        .await
        .expect("create db");
    let client = Connection::connect(ConnectConfig::new(proc.endpoint(), "main"))
        .await
        .expect("connect");

    // Load transactions; kill -9 fires mid-stream from another task.
    let killer = {
        let pid = proc.pid();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let _ = Command::new("kill")
                .arg("-KILL")
                .arg(pid.to_string())
                .status();
        })
    };
    let mut acked = Vec::new();
    for value in 0..10_000 {
        let forms = codec_encode(add_value(value));
        match client.transact_raw(forms).await {
            Ok(response) => {
                assert!(response.basis_t > 0);
                acked.push(value);
            }
            Err(_) => break,
        }
    }
    killer.await.expect("killer task");
    proc.kill9();
    assert!(
        !acked.is_empty(),
        "no transaction was acked before the kill"
    );

    // Restart and read back through a fresh peer.
    let proc = TransactorProc::spawn(&data, port, &[]);
    let _admin = proc.wait_ready().await;
    let peer = Connection::connect(ConnectConfig::new(proc.endpoint(), "main"))
        .await
        .expect("reconnect");
    let db = peer.sync().await.expect("sync");
    let committed = long_values(&db);
    let committed_set: BTreeSet<i64> = committed.iter().copied().collect();
    assert_eq!(
        committed.len(),
        committed_set.len(),
        "recovery duplicated a transaction"
    );
    for value in &acked {
        assert!(
            committed_set.contains(value),
            "acked transaction {value} was lost by kill -9"
        );
    }
    // At most one in-flight (durable but unacked) transaction may trail.
    assert!(committed.len() <= acked.len() + 1);
}

fn codec_encode(forms: Vec<Edn>) -> Vec<u8> {
    corium_protocol::codec::encode_edn(&Edn::Vector(forms))
}

#[tokio::test(flavor = "multi_thread")]
async fn deposed_transactor_cannot_publish_or_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let port_a = free_port();
    let flags_a = [
        "--owner",
        "owner-a",
        "--lease-ttl-ms",
        "800",
        "--index-interval-ms",
        "200",
    ];
    let proc_a = TransactorProc::spawn(&data, port_a, &flags_a);
    let mut admin_a = proc_a.wait_ready().await;
    admin_a
        .create_database("fenced", &schema_forms())
        .await
        .expect("create db");
    let client_a = Connection::connect(ConnectConfig::new(proc_a.endpoint(), "fenced"))
        .await
        .expect("connect A");
    for value in 0..3 {
        client_a.transact(add_value(value)).await.expect("transact");
    }

    // Pause A (GC-pause stand-in) and let its lease expire.
    proc_a.signal("STOP");

    let port_b = free_port();
    let proc_b = TransactorProc::spawn(
        &data,
        port_b,
        &[
            "--owner",
            "owner-b",
            "--lease-ttl-ms",
            "800",
            "--lease-wait-ms",
            "20000",
            "--index-interval-ms",
            "200",
        ],
    );
    let _admin_b = proc_b.wait_ready().await;
    let client_b = Connection::connect(ConnectConfig::new(proc_b.endpoint(), "fenced"))
        .await
        .expect("connect B");
    client_b.transact(add_value(100)).await.expect("B commits");

    // Wait for B to publish an index under its (newer) lease version.
    let store = FsStore::open(data.join("store")).expect("open store");
    let deadline = Instant::now() + Duration::from_secs(20);
    let fenced_root = loop {
        let root = store
            .get_root(&corium_store::db_root_name("fenced"))
            .await
            .expect("read root")
            .as_deref()
            .and_then(DbRoot::decode)
            .expect("decodable root");
        if root.roots.is_some() && root.index_basis_t >= 4 {
            break root;
        }
        assert!(Instant::now() < deadline, "B never published");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        fenced_root.lease_version >= 2,
        "B must fence with a newer lease"
    );

    // Wake the deposed transactor. It must refuse to commit and must not
    // regress the published root; the process shuts itself down.
    proc_a.signal("CONT");
    let refused = client_a.transact(add_value(999));
    let refused = tokio::time::timeout(Duration::from_secs(10), refused).await;
    if let Ok(Ok(result)) = refused {
        panic!("deposed transactor acked t={}", result.basis_t);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let root_after = store
        .get_root(&corium_store::db_root_name("fenced"))
        .await
        .expect("read root")
        .as_deref()
        .and_then(DbRoot::decode)
        .expect("decodable root");
    assert!(
        root_after.lease_version >= fenced_root.lease_version
            && root_after.index_basis_t >= fenced_root.index_basis_t,
        "deposed transactor regressed the published root"
    );
    // B remains healthy and its history contains no datoms from A's refused
    // transaction.
    let db = client_b.sync().await.expect("B sync");
    assert!(!long_values(&db).contains(&999));
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_and_bearer_token_auth_guard_every_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    // Self-signed certificate for localhost.
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("generate certificate");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, certified.cert.pem()).expect("write cert");
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).expect("write key");

    let port = free_port();
    let proc = TransactorProc::spawn(
        &data,
        port,
        &[
            "--serve-token",
            "secret-token",
            "--tls-cert",
            cert_path.to_str().expect("utf8"),
            "--tls-key",
            key_path.to_str().expect("utf8"),
        ],
    );
    let endpoint = format!("https://localhost:{port}");
    let tls =
        corium_protocol::auth::client_tls(Some(&cert_path), Some("localhost")).expect("client tls");

    // Correct token over TLS succeeds.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut admin = loop {
        if let Ok(mut admin) =
            Admin::connect(&endpoint, Some("secret-token".into()), Some(tls.clone())).await
        {
            if admin.list_databases().await.is_ok() {
                break admin;
            }
        }
        assert!(Instant::now() < deadline, "TLS transactor never ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        admin
            .create_database("secure", &schema_forms())
            .await
            .expect("authorized create")
    );
    let mut config = ConnectConfig::new(endpoint.clone(), "secure");
    config.token = Some("secret-token".into());
    config.tls = Some(tls.clone());
    let peer = Connection::connect(config)
        .await
        .expect("TLS peer connects");
    peer.transact(add_value(1)).await.expect("TLS transact");

    // Wrong token is rejected.
    let mut admin = Admin::connect(&endpoint, Some("wrong".into()), Some(tls.clone()))
        .await
        .expect("channel still opens");
    let denied = admin.list_databases().await;
    match denied {
        Err(corium_peer::PeerError::Rpc(status)) => {
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }
        other => panic!("wrong token was not rejected: {other:?}"),
    }

    // Plaintext to a TLS port fails outright.
    let plain = Admin::connect(&format!("http://localhost:{port}"), None, None).await;
    if let Ok(mut admin) = plain {
        assert!(admin.list_databases().await.is_err());
    }
    drop(proc);
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_admin_commands_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let port = free_port();
    let proc = TransactorProc::spawn(&data, port, &[]);
    let _admin = proc.wait_ready().await;
    let schema_path = dir.path().join("schema.edn");
    std::fs::write(&schema_path, SCHEMA).expect("write schema");

    let corium = env!("CARGO_BIN_EXE_corium");
    let run = |args: Vec<String>| {
        let output = Command::new(corium)
            .args(&args)
            .output()
            .expect("run corium");
        assert!(
            output.status.success(),
            "corium {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let endpoint = proc.endpoint();
    let created = run(vec![
        "db".into(),
        "create".into(),
        "clidb".into(),
        "--schema".into(),
        schema_path.display().to_string(),
        "--transactor".into(),
        endpoint.clone(),
    ]);
    assert!(created.contains(":created true"), "{created}");
    let listed = run(vec![
        "db".into(),
        "list".into(),
        "--transactor".into(),
        endpoint.clone(),
    ]);
    assert!(listed.contains("clidb"), "{listed}");

    let peer = Connection::connect(ConnectConfig::new(endpoint.clone(), "clidb"))
        .await
        .expect("connect");
    peer.transact(add_value(7)).await.expect("transact");

    let stats = run(vec![
        "db".into(),
        "stats".into(),
        "clidb".into(),
        "--transactor".into(),
        endpoint.clone(),
    ]);
    assert!(
        stats.contains(":basis-t 1") && stats.contains(":datoms 1"),
        "{stats}"
    );

    // Offline log inspection sees the committed record.
    let logged = run(vec![
        "log".into(),
        "--data-dir".into(),
        data.display().to_string(),
        "--db".into(),
        "clidb".into(),
    ]);
    assert!(logged.contains(":t 1"), "{logged}");

    let deleted = run(vec![
        "db".into(),
        "delete".into(),
        "clidb".into(),
        "--transactor".into(),
        endpoint.clone(),
    ]);
    assert!(deleted.contains(":deleted true"), "{deleted}");
    // An explicit zero retention reaches the online GC path as an immediate
    // sweep rather than being confused with the omitted/default window.
    let store = FsStore::open(data.join("store")).expect("open store");
    let orphan = store.put(b"online-gc-orphan").await.expect("put orphan");
    let swept = run(vec![
        "gc".into(),
        "--transactor".into(),
        endpoint,
        "--window".into(),
        "0".into(),
    ]);
    assert!(swept.contains(":swept"), "{swept}");
    assert!(!store.contains(&orphan).await.expect("orphan swept"));
}
