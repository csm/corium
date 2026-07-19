//! Thin-client conformance kit (M4): replays the Datomic-semantics corpus
//! through the public peer-server surface — schema via the catalog service,
//! transactions and queries as composite-encoded EDN over the
//! `PeerServerService` handlers — against a real transactor served over
//! gRPC. The client side performs only what a thin client can: tag
//! rewriting against tempid maps returned by earlier transactions.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use corium_peer::server::{PeerServerConfig, PeerServerSvc, assemble_query_result};
use corium_peer::{Admin, ConnectConfig, Connection};
use corium_protocol::codec;
use corium_protocol::pb;
use corium_protocol::pb::peer_server_server::PeerServer;
use corium_query::ast;
use corium_query::edn::{Edn, read_all};
use corium_transactor::node::{NodeConfig, TransactorNode};
use tokio_stream::StreamExt;
use tonic::Request;

fn kw(text: &str) -> Edn {
    Edn::keyword(text)
}

/// Rewrites `#tempid`/`#tx` tags: inputs get `#eid` tags, expected outputs
/// get plain longs (mirroring the embedded conformance harness).
fn substitute(form: &Edn, tempids: &BTreeMap<String, i64>, output: bool) -> Edn {
    match form {
        Edn::Tagged(tag, value) if tag == "tempid" => {
            let Edn::Str(name) = value.as_ref() else {
                panic!("#tempid requires a string");
            };
            let raw = *tempids
                .get(name)
                .unwrap_or_else(|| panic!("unknown tempid {name}"));
            if output {
                Edn::Long(raw)
            } else {
                Edn::Tagged("eid".into(), Box::new(Edn::Long(raw)))
            }
        }
        Edn::Tagged(tag, value) if output && tag == "tx" => {
            let Edn::Long(t) = value.as_ref() else {
                panic!("#tx requires a long");
            };
            let raw = corium_core::EntityId::new(
                corium_core::Partition::Tx as u32,
                u64::try_from(*t).expect("t"),
            )
            .raw();
            Edn::Long(i64::try_from(raw).expect("fits"))
        }
        Edn::List(items) => Edn::List(
            items
                .iter()
                .map(|item| substitute(item, tempids, output))
                .collect(),
        ),
        Edn::Vector(items) => Edn::Vector(
            items
                .iter()
                .map(|item| substitute(item, tempids, output))
                .collect(),
        ),
        Edn::Set(items) => {
            let mut out: Vec<Edn> = items
                .iter()
                .map(|item| substitute(item, tempids, output))
                .collect();
            out.sort();
            out.dedup();
            Edn::Set(out)
        }
        Edn::Map(pairs) => {
            let mut out: Vec<(Edn, Edn)> = pairs
                .iter()
                .map(|(key, value)| {
                    (
                        substitute(key, tempids, output),
                        substitute(value, tempids, output),
                    )
                })
                .collect();
            out.sort_by(|left, right| left.0.cmp(&right.0));
            Edn::Map(out)
        }
        Edn::Tagged(tag, value) => {
            Edn::Tagged(tag.clone(), Box::new(substitute(value, tempids, output)))
        }
        other => other.clone(),
    }
}

fn normalize(result: &Edn) -> Edn {
    match result {
        Edn::Set(items) => {
            let mut rows = items.clone();
            rows.sort();
            rows.dedup();
            Edn::Vector(rows)
        }
        Edn::Vector(rows) => {
            let mut rows = rows.clone();
            rows.sort();
            Edn::Vector(rows)
        }
        other => other.clone(),
    }
}

fn view_spec(db: &str, view: Option<&Edn>) -> pb::DbViewSpec {
    let view = match view {
        None => None,
        Some(form) if form == &kw("history") => Some(pb::db_view_spec::View::History(true)),
        Some(form) if form == &kw("current") => None,
        Some(Edn::Map(pairs)) => match pairs.as_slice() {
            [(key, Edn::Long(t))] if key == &kw("as-of") => Some(pb::db_view_spec::View::AsOf(
                u64::try_from(*t).expect("as-of t"),
            )),
            [(key, Edn::Long(t))] if key == &kw("since") => Some(pb::db_view_spec::View::Since(
                u64::try_from(*t).expect("since t"),
            )),
            other => panic!("bad view {other:?}"),
        },
        Some(other) => panic!("bad view {other}"),
    };
    pb::DbViewSpec {
        db: db.to_owned(),
        view,
    }
}

struct Kit {
    admin: Admin,
    endpoint: String,
}

impl Kit {
    async fn host(&self, db: &str) -> PeerServerSvc {
        let connection = Connection::connect(ConnectConfig::new(self.endpoint.clone(), db))
            .await
            .expect("peer connects");
        PeerServerSvc::new(Arc::new(connection), PeerServerConfig::default())
    }
}

#[allow(clippy::too_many_lines)]
async fn run_vector(kit: &mut Kit, index: usize, vector: &Edn) {
    let name = vector
        .get(&kw("name"))
        .map_or_else(|| "<unnamed>".to_owned(), ToString::to_string);
    let context = || format!("conformance vector {name}");
    let db_name = format!("vec-{index}");

    // Schema through the catalog service.
    let schema = vector
        .get(&kw("schema"))
        .and_then(Edn::as_seq)
        .unwrap_or(&[])
        .to_vec();
    assert!(
        kit.admin
            .create_database(&db_name, &schema)
            .await
            .unwrap_or_else(|error| panic!("{}: create db: {error}", context())),
        "{}: database existed",
        context()
    );
    let server = kit.host(&db_name).await;

    // Transactions through the peer server's transact proxy, accumulating
    // tempids like any thin client.
    let mut tempids: BTreeMap<String, i64> = BTreeMap::new();
    if let Some(txes) = vector.get(&kw("tx")).and_then(Edn::as_seq) {
        for tx_forms in txes {
            let forms = tx_forms.as_seq().expect("tx must be a vector");
            let rewritten: Vec<Edn> = forms
                .iter()
                .map(|form| substitute(form, &tempids, false))
                .collect();
            let response = server
                .transact(Request::new(pb::TransactRequest {
                    db: db_name.clone(),
                    protocol_version: corium_protocol::PROTOCOL_VERSION,
                    tx_data: codec::encode_edn(&Edn::Vector(rewritten)),
                }))
                .await
                .unwrap_or_else(|error| panic!("{}: transact failed: {error}", context()))
                .into_inner();
            let Edn::Map(pairs) = codec::decode_edn(&response.tempids).expect("tempids decode")
            else {
                panic!("{}: tempids must be a map", context());
            };
            for (key, value) in pairs {
                if let (Edn::Str(temp), Edn::Long(raw)) = (key, value) {
                    tempids.insert(temp, raw);
                }
            }
        }
    }

    let expect_error = vector.get(&kw("expect-error")) == Some(&Edn::Bool(true));
    let expected = vector.get(&kw("expected"));

    // Pull vectors go through the Pull RPC.
    if let Some(pull_spec) = vector.get(&kw("pull")) {
        let eid = substitute(pull_spec.get(&kw("eid")).expect(":eid"), &tempids, false);
        let pattern = pull_spec.get(&kw("pattern")).expect(":pattern");
        let response = server
            .pull(Request::new(pb::PullRequest {
                db: Some(view_spec(&db_name, vector.get(&kw("view")))),
                pattern: codec::encode_edn(pattern),
                eid: codec::encode_edn(&eid),
            }))
            .await;
        match (expect_error, response) {
            (true, Err(_)) => {}
            (true, Ok(response)) => panic!(
                "{}: expected error, got {:?}",
                context(),
                response.into_inner()
            ),
            (false, Err(error)) => panic!("{}: pull failed: {error}", context()),
            (false, Ok(response)) => {
                let result =
                    codec::decode_edn(&response.into_inner().result).expect("result decodes");
                let expected = substitute(expected.expect(":expected"), &tempids, true);
                assert_eq!(result, expected, "{}", context());
            }
        }
        return;
    }

    // Query vectors go through the Query RPC with chunked reassembly.
    let query_form = substitute(vector.get(&kw("query")).expect(":query"), &tempids, false);
    let parsed = ast::parse_query(&query_form);
    let mut dbs = vec![view_spec(&db_name, vector.get(&kw("view")))];
    for extra in vector
        .get(&kw("extra-dbs"))
        .and_then(Edn::as_seq)
        .unwrap_or(&[])
    {
        dbs.push(view_spec(&db_name, Some(extra)));
    }
    let args: Vec<Edn> = vector
        .get(&kw("args"))
        .and_then(Edn::as_seq)
        .unwrap_or(&[])
        .iter()
        .map(|arg| substitute(arg, &tempids, false))
        .collect();
    let response = server
        .query(Request::new(pb::QueryRequest {
            dbs,
            query: codec::encode_edn(&query_form),
            args: codec::encode_edn(&Edn::Vector(args)),
            fuel: 0,
        }))
        .await;
    let mut stream = match response {
        Err(error) => {
            assert!(expect_error, "{}: query failed: {error}", context());
            return;
        }
        Ok(response) => response.into_inner(),
    };
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => chunks.push(chunk),
            Err(error) => {
                assert!(expect_error, "{}: chunk failed: {error}", context());
                return;
            }
        }
    }
    assert!(
        !expect_error,
        "{}: expected an error, got a result",
        context()
    );
    let result = assemble_query_result(&chunks)
        .unwrap_or_else(|error| panic!("{}: bad result stream: {error}", context()));
    let expected = substitute(expected.expect(":expected"), &tempids, true);
    let unordered = matches!(
        parsed.map(|query| query.find),
        Ok(ast::FindSpec::Rel(_) | ast::FindSpec::Coll(_))
    );
    if unordered {
        assert_eq!(
            normalize(&result),
            normalize(&expected),
            "{}: got {result}",
            context()
        );
    } else {
        assert_eq!(result, expected, "{}", context());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn thin_client_conformance_kit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = NodeConfig::new(dir.path().join("data"));
    config.owner = "conformance".into();
    config.lease_ttl_ms = 600_000;
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    let node = TransactorNode::open(config).await.expect("open node");
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let auth = Arc::new(corium_protocol::auth::StaticToken::new(None));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(corium_transactor::server::serve(
        Arc::clone(&node),
        addr,
        auth,
        None,
        async move {
            let _ = stop_rx.await;
        },
    ));
    let endpoint = format!("http://127.0.0.1:{port}");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let admin = loop {
        if let Ok(mut admin) = Admin::connect(&endpoint, None, None).await {
            if admin.list_databases().await.is_ok() {
                break admin;
            }
        }
        assert!(std::time::Instant::now() < deadline, "server never ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let mut kit = Kit { admin, endpoint };

    let corpus = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests/conformance");
    let mut files: Vec<_> = std::fs::read_dir(&corpus)
        .expect("tests/conformance directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "edn"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no conformance files found");
    let mut total = 0;
    for file in files {
        let text = std::fs::read_to_string(&file).expect("readable corpus file");
        let vectors = read_all(&text)
            .unwrap_or_else(|error| panic!("{}: EDN error: {error}", file.display()));
        for vector in vectors {
            run_vector(&mut kit, total, &vector).await;
            total += 1;
        }
    }
    assert!(total >= 150, "conformance kit ran {total} vectors (< 150)");
    println!("thin-client conformance kit: {total} vectors green");
    let _ = stop_tx.send(());
    let _ = server.await;
}
