//! M5 acceptance: the Datomic-semantics conformance corpus re-run driven
//! from Clojurust. Schema installs through the catalog; every transaction,
//! query, pull, and time view is issued by evaluating `corium.api` forms in
//! a real cljrs environment connected (as a peer) to a transactor process
//! served over gRPC. Results must be identical to the embedded M3 harness.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use cljrs_env::env::{Env, GlobalEnv};
use cljrs_value::Value;
use corium_cljrs::convert;
use corium_peer::Admin;
use corium_query::ast;
use std::fmt::Write as _;

use corium_query::edn::{Edn, read_all};
use corium_transactor::node::{NodeConfig, TransactorNode};

fn kw(text: &str) -> Edn {
    Edn::keyword(text)
}

/// Rewrites `#tempid`/`#tx` tags: inputs get `#eid` tags, expected outputs
/// get plain longs (mirroring the embedded and thin-client harnesses).
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

/// Normalizes a relation result for order-insensitive comparison.
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

/// The cljrs client: evaluates source in a `corium.api`-enabled
/// environment; every result converts to boundary EDN before the next
/// evaluation (cljrs values must not be held across evals — GC).
struct Client {
    globals: Arc<GlobalEnv>,
}

impl Client {
    fn eval(&self, source: &str) -> Result<Value, String> {
        let mut parser = cljrs_reader::Parser::new(source.to_owned(), "<driver>".to_owned());
        let forms = parser.parse_all().map_err(|error| format!("{error:?}"))?;
        let mut env = Env::new(Arc::clone(&self.globals), "user");
        let mut last = Value::Nil;
        for form in &forms {
            let _frame = cljrs_gc::push_alloc_frame();
            last =
                cljrs_interp::eval::eval(form, &mut env).map_err(|error| format!("{error:?}"))?;
        }
        Ok(last)
    }

    fn eval_edn(&self, source: &str) -> Result<Edn, String> {
        let value = self.eval(source)?;
        convert::to_edn(&value).map_err(|error| error.to_string())
    }

    /// Interns EDN data as a `user` var so generated forms can reference it.
    fn intern(&self, name: &str, form: &Edn) {
        let _frame = cljrs_gc::push_alloc_frame();
        self.globals
            .intern("user", name.into(), convert::from_edn(form));
    }
}

fn view_expr(base: &str, view: Option<&Edn>) -> String {
    match view {
        None => base.to_owned(),
        Some(form) if form == &kw("history") => format!("(corium.api/history {base})"),
        Some(form) if form == &kw("current") => base.to_owned(),
        Some(Edn::Map(pairs)) => match pairs.as_slice() {
            [(key, Edn::Long(t))] if key == &kw("as-of") => {
                format!("(corium.api/as-of {base} {t})")
            }
            [(key, Edn::Long(t))] if key == &kw("since") => {
                format!("(corium.api/since {base} {t})")
            }
            other => panic!("bad view {other:?}"),
        },
        Some(other) => panic!("bad view {other}"),
    }
}

#[allow(clippy::too_many_lines)]
fn run_vector(client: &Client, endpoint_port: u16, index: usize, vector: &Edn) {
    let name = vector
        .get(&kw("name"))
        .map_or_else(|| "<unnamed>".to_owned(), ToString::to_string);
    let context = || format!("conformance vector {name}");
    let db_name = format!("vec-{index}");

    // Connect as a peer, through cljrs.
    client
        .eval(&format!(
            "(def conn (corium.api/connect \"corium://127.0.0.1:{endpoint_port}/{db_name}\"))"
        ))
        .unwrap_or_else(|error| panic!("{}: connect: {error}", context()));

    // Transactions through (corium.api/transact conn …), accumulating
    // tempids like any client.
    let mut tempids: BTreeMap<String, i64> = BTreeMap::new();
    if let Some(txes) = vector.get(&kw("tx")).and_then(Edn::as_seq) {
        for tx_forms in txes {
            let forms = tx_forms.as_seq().expect("tx must be a vector");
            let rewritten = Edn::Vector(
                forms
                    .iter()
                    .map(|form| substitute(form, &tempids, false))
                    .collect(),
            );
            client.intern("tx-data", &rewritten);
            let allocated = client
                .eval_edn("(:tempids (corium.api/transact conn tx-data))")
                .unwrap_or_else(|error| panic!("{}: transact: {error}", context()));
            let Edn::Map(pairs) = allocated else {
                panic!("{}: tempids must be a map", context());
            };
            for (key, value) in pairs {
                if let (Edn::Str(temp), Edn::Long(raw)) = (key, value) {
                    tempids.insert(temp, raw);
                }
            }
        }
    }

    // Time view over the peer-local database value.
    let dbexpr = view_expr("(corium.api/db conn)", vector.get(&kw("view")));
    client
        .eval(&format!("(def dbv {dbexpr})"))
        .unwrap_or_else(|error| panic!("{}: view: {error}", context()));

    let expect_error = vector.get(&kw("expect-error")) == Some(&Edn::Bool(true));
    let expected = vector.get(&kw("expected"));

    // Pull vectors.
    if let Some(pull_spec) = vector.get(&kw("pull")) {
        let eid = substitute(pull_spec.get(&kw("eid")).expect(":eid"), &tempids, false);
        client.intern("eidv", &eid);
        client.intern(
            "patternv",
            &substitute(
                pull_spec.get(&kw("pattern")).expect(":pattern"),
                &tempids,
                false,
            ),
        );
        let result = client.eval_edn("(corium.api/pull dbv patternv eidv)");
        match (expect_error, result) {
            (true, Err(_)) => {}
            (true, Ok(result)) => panic!("{}: expected error, got {result}", context()),
            (false, Err(error)) => panic!("{}: pull failed: {error}", context()),
            (false, Ok(result)) => {
                let expected = substitute(expected.expect(":expected"), &tempids, true);
                assert_eq!(result, expected, "{}", context());
            }
        }
        return;
    }

    // Query vectors: extra database views bind positionally after `$`.
    let query_form = substitute(vector.get(&kw("query")).expect(":query"), &tempids, false);
    client.intern("qv", &query_form);
    let mut call = String::from("(corium.api/q qv dbv");
    for (i, extra) in vector
        .get(&kw("extra-dbs"))
        .and_then(Edn::as_seq)
        .unwrap_or(&[])
        .iter()
        .enumerate()
    {
        let expr = view_expr("(corium.api/db conn)", Some(extra));
        client
            .eval(&format!("(def dbx{i} {expr})"))
            .unwrap_or_else(|error| panic!("{}: extra db: {error}", context()));
        let _ = write!(call, " dbx{i}");
    }
    for (i, arg) in vector
        .get(&kw("args"))
        .and_then(Edn::as_seq)
        .unwrap_or(&[])
        .iter()
        .enumerate()
    {
        client.intern(&format!("argv{i}"), &substitute(arg, &tempids, false));
        let _ = write!(call, " argv{i}");
    }
    call.push(')');

    let result = client.eval_edn(&call);
    match (expect_error, result) {
        (true, Err(_)) => {}
        (true, Ok(result)) => panic!("{}: expected error, got {result}", context()),
        (false, Err(error)) => panic!("{}: query failed: {error}", context()),
        (false, Ok(result)) => {
            let expected = substitute(expected.expect(":expected"), &tempids, true);
            let unordered = matches!(
                ast::parse_query(&query_form).map(|query| query.find),
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
    }
}

#[test]
fn conformance_corpus_from_cljrs() {
    // Transactor process served over gRPC on a background runtime.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = NodeConfig::new(dir.path().join("data"));
    config.owner = "cljrs-conformance".into();
    config.lease_ttl_ms = 600_000;
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    let node = TransactorNode::open(config).expect("open node");
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let auth = Arc::new(corium_protocol::auth::StaticToken::new(None));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = runtime.spawn(corium_transactor::server::serve(
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
    let mut admin = loop {
        let attempt = runtime.block_on(async {
            let mut admin = Admin::connect(&endpoint, None, None).await.ok()?;
            admin.list_databases().await.ok()?;
            Some(admin)
        });
        if let Some(admin) = attempt {
            break admin;
        }
        assert!(std::time::Instant::now() < deadline, "server never ready");
        std::thread::sleep(Duration::from_millis(50));
    };

    // This thread hosts the cljrs isolate that drives the whole corpus.
    let _mutator = cljrs_gc::register_mutator();
    let client = Client {
        globals: corium_cljrs::api::client_env(runtime.handle()),
    };

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
            let schema = vector
                .get(&kw("schema"))
                .and_then(Edn::as_seq)
                .unwrap_or(&[])
                .to_vec();
            let created = runtime
                .block_on(admin.create_database(&format!("vec-{total}"), &schema))
                .unwrap_or_else(|error| panic!("create db vec-{total}: {error}"));
            assert!(created, "database vec-{total} existed");
            run_vector(&client, port, total, &vector);
            total += 1;
        }
    }
    assert!(
        total >= 150,
        "cljrs conformance ran {total} vectors (< 150)"
    );
    println!("cljrs conformance corpus: {total} vectors green");
    // The 194 peer connections in the cljrs environment keep subscribe
    // streams open, so a graceful drain would never finish: signal shutdown,
    // abort the accept loop, and drop the runtime without waiting.
    let _ = stop_tx.send(());
    server.abort();
    runtime.shutdown_background();
}
