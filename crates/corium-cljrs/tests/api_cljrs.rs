//! `corium.api` surface coverage (M5): connect, transact, db, entity,
//! datoms, tx-range, tx-report-queue, and sync — all driven by evaluating
//! cljrs forms against a live transactor.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use cljrs_env::env::{Env, GlobalEnv};
use cljrs_value::Value;
use corium_cljrs::convert;
use corium_peer::Admin;
use corium_query::edn::{Edn, read_one};
use corium_transactor::node::{NodeConfig, TransactorNode};

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

    fn eval_edn(&self, source: &str) -> Edn {
        let value = self.eval(source).unwrap_or_else(|error| panic!("{error}"));
        convert::to_edn(&value).unwrap_or_else(|error| panic!("{error}"))
    }
}

const SCHEMA: &str = r"
[{:db/ident :person/name :db/valueType :db.type/string :db/unique :db.unique/identity}
 {:db/ident :person/age :db/valueType :db.type/long}
 {:db/ident :person/langs :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}]
";

#[test]
#[allow(clippy::too_many_lines)]
fn api_surface_from_cljrs() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = NodeConfig::new(dir.path().join("data"));
    config.owner = "cljrs-api".into();
    config.lease_ttl_ms = 600_000;
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    let node = runtime
        .block_on(TransactorNode::open(config))
        .expect("open node");
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let auth = Arc::new(corium_protocol::auth::StaticToken::new(None));
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
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
    loop {
        let ready = runtime.block_on(async {
            let mut admin = Admin::connect(&endpoint, None, None).await.ok()?;
            let schema = read_one(SCHEMA).expect("schema").as_seq()?.to_vec();
            admin.create_database("people", &schema).await.ok()
        });
        if ready.is_some() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "server never ready");
        std::thread::sleep(Duration::from_millis(50));
    }

    let _mutator = cljrs_gc::register_mutator();
    let client = Client {
        globals: corium_cljrs::api::client_env(runtime.handle()),
    };

    // connect + tx-report-queue before transacting (so the queue sees it).
    client
        .eval(&format!(
            "(def conn (d/connect \"corium://127.0.0.1:{port}/people\"))"
        ))
        .expect("connect");
    client
        .eval("(def queue (d/tx-report-queue conn))")
        .expect("queue");

    // transact via the d alias; result carries tempids and basis.
    let result = client.eval_edn(
        r#"(let [r (d/transact conn [{:db/id "rich" :person/name "Rich" :person/age 42
                                      :person/langs [:clojure :rust :java]}])]
             [(count (:tempids r)) (pos? (:basis-t r))])"#,
    );
    assert_eq!(result, read_one("[1 true]").expect("edn"));

    // The queue delivered the report.
    let report = client.eval_edn("(select-keys (d/take-report queue 5000) [:t])");
    let Edn::Map(pairs) = &report else {
        panic!("report must be a map, got {report}");
    };
    assert!(!pairs.is_empty(), "report must carry :t");

    // Entity view (eager map) via lookup ref.
    let entity = client.eval_edn(
        "(select-keys (d/entity (d/db conn) [:person/name \"Rich\"]) [:person/age :person/langs])",
    );
    assert_eq!(
        entity,
        read_one("{:person/age 42 :person/langs #{:clojure :rust :java}}").expect("edn")
    );

    // Query through q; pull through pull.
    let ages = client.eval_edn("(d/q '[:find ?a . :where [?e :person/age ?a]] (d/db conn))");
    assert_eq!(ages, Edn::Long(42));
    let pulled =
        client.eval_edn("(d/pull (d/db conn) [:person/name :person/age] [:person/name \"Rich\"])");
    assert_eq!(
        pulled,
        read_one("{:person/name \"Rich\" :person/age 42}").expect("edn")
    );

    // Time model: as-of before the transaction is empty; history sees both.
    client
        .eval("(d/transact conn [[:db/add [:person/name \"Rich\"] :person/age 43]])")
        .expect("update");
    let baseline =
        client.eval_edn("(d/q '[:find ?a . :where [?e :person/age ?a]] (d/as-of (d/db conn) 1))");
    assert_eq!(baseline, Edn::Long(42));
    let history = client.eval_edn(
        "(count (d/q '[:find ?a ?added :where [?e :person/age ?a _ ?added]]
                     (d/history (d/db conn))))",
    );
    assert_eq!(history, Edn::Long(3), "42 added, 42 retracted, 43 added");

    // sync returns a database at the transactor's basis.
    let synced = client.eval_edn("(d/basis-t (d/sync conn))");
    assert_eq!(synced, Edn::Long(2));

    // tx-range covers both transactions with their datoms.
    let range = client.eval_edn("(mapv :t (d/tx-range conn 1 nil))");
    assert_eq!(range, read_one("[1 2]").expect("edn"));

    // Direct index access.
    let datoms = client.eval_edn(
        "(count (filterv (fn [[e a v tx added]] (= a :person/age))
                         (d/datoms (d/db conn) :eavt)))",
    );
    assert_eq!(datoms, Edn::Long(1));

    server.abort();
    runtime.shutdown_background();
}
