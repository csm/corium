//! Database-function acceptance battery on the built-in `cljrs-tx` runtime
//! (feature `cljrs`): a cas-like function, an invariant-enforcing function,
//! recursive expansion, and clean aborts on budget exhaustion — all against
//! the embedded transaction pipeline, plus the transactor-node wiring.
//!
//! Ported from `corium-cljrs`'s sandbox battery; the functions are the same
//! sources, exercising the `corium.api` read host-API (`q`, `entity`) over
//! the database token instead of a live db handle.

#![cfg(feature = "cljrs")]

use std::sync::Arc;
use std::time::Duration;

use cljrs_tx::TxError;
use corium_core::KeywordInterner;
use corium_db::{Db, Idents};
use corium_log::MemoryLog;
use corium_protocol::schemaform::schema_from_edn;
use corium_protocol::txforms::tx_items_from_edn;
use corium_query::edn::{Edn, read_one};
use corium_transactor::EmbeddedTransactor;
use corium_transactor::txfn::{DbFnBudget, DbFnError, DbFnExpander};

/// Schema: account attributes plus the `:db/ident` + `:db/fn` pair that
/// makes function entities addressable by keyword.
const SCHEMA: &str = r"
[{:db/ident :db/ident :db/valueType :db.type/keyword :db/unique :db.unique/identity}
 {:db/ident :db/fn :db/valueType :db.type/string}
 {:db/ident :acct/name :db/valueType :db.type/string :db/unique :db.unique/identity}
 {:db/ident :acct/balance :db/valueType :db.type/long}]
";

struct Fixture {
    transactor: EmbeddedTransactor,
    expander: DbFnExpander,
    idents: Idents,
}

impl Fixture {
    fn new(budget: DbFnBudget) -> Self {
        let Edn::Vector(forms) = read_one(SCHEMA).expect("schema edn") else {
            panic!("schema must be a vector");
        };
        let (schema, idents) = schema_from_edn(&forms).expect("schema");
        let base = Db::new(schema).with_naming(idents.clone(), KeywordInterner::default());
        let transactor = EmbeddedTransactor::recover_from(base, Arc::new(MemoryLog::default()))
            .expect("recover");
        Self {
            transactor,
            expander: DbFnExpander::new(budget).with_max_depth(4),
            idents,
        }
    }

    /// Expands db functions, converts, and commits one transaction.
    fn transact(&self, text: &str) -> Result<corium_transactor::TxReport, DbFnError> {
        let Edn::Vector(forms) = read_one(text).expect("tx edn") else {
            panic!("tx text must be a vector");
        };
        let db = self.transactor.db();
        let expanded = self.expander.expand(&db, forms)?;
        let mut interner = db.interner().clone();
        let items = tx_items_from_edn(&db, &mut interner, &expanded)
            .unwrap_or_else(|error| panic!("tx forms: {error}"));
        self.transactor.update_naming(self.idents.clone(), interner);
        Ok(self.transactor.transact(items).expect("transact"))
    }

    fn balance(&self, name: &str) -> Option<i64> {
        let db = self.transactor.db();
        let name_attr = db
            .idents()
            .entid(&corium_core::Keyword::parse("acct/name"))?;
        let bal_attr = db
            .idents()
            .entid(&corium_core::Keyword::parse("acct/balance"))?;
        let e = db.lookup(name_attr, &corium_core::Value::Str(name.into()))?;
        match db.values(e, bal_attr).into_iter().next() {
            Some(corium_core::Value::Long(n)) => Some(n),
            _ => None,
        }
    }
}

fn budget() -> DbFnBudget {
    DbFnBudget {
        fuel: 100_000,
        ..DbFnBudget::default()
    }
}

/// A cas-like function: swaps the balance only when the current value
/// matches, aborting the transaction otherwise.
const CAS_FN: &str = r#"
"(fn [db acct old new]
   (let [e (ffirst (corium.api/q (quote [:find ?e :in $ ?n :where [?e :acct/name ?n]]) db acct))
         cur (:acct/balance (corium.api/entity db e))]
     (if (= cur old)
       [[:db/add e :acct/balance new]]
       (throw (ex-info \"balance changed\" {:expected old :actual cur})))))"
"#;

fn escaped(source_literal: &str) -> Edn {
    read_one(source_literal).expect("source string literal")
}

fn eval_failure(error: &DbFnError) -> Option<&str> {
    match error {
        DbFnError::Execution(TxError::Evaluation(text)) => Some(text),
        _ => None,
    }
}

#[test]
fn cas_like_function_swaps_and_rejects() {
    let fixture = Fixture::new(budget());
    fixture
        .transact(r#"[{:db/id "a" :acct/name "alice" :acct/balance 100}]"#)
        .expect("seed");
    let source = escaped(CAS_FN);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/cas :db/fn {source}}}]"#
        ))
        .expect("install fn");

    // Matching old value: swap applies.
    fixture
        .transact(r#"[[:acct/cas "alice" 100 150]]"#)
        .expect("cas succeeds");
    assert_eq!(fixture.balance("alice"), Some(150));

    // Stale old value: the function throws and the transaction aborts.
    let error = fixture
        .transact(r#"[[:acct/cas "alice" 100 999]]"#)
        .expect_err("stale cas must abort");
    assert!(
        eval_failure(&error).is_some_and(|text| text.contains("balance changed")),
        "got {error:?}"
    );
    assert_eq!(fixture.balance("alice"), Some(150));
}

/// An invariant function: withdrawals may never drive a balance negative.
const WITHDRAW_FN: &str = r#"
"(fn [db acct amount]
   (let [e (ffirst (corium.api/q (quote [:find ?e :in $ ?n :where [?e :acct/name ?n]]) db acct))
         cur (:acct/balance (corium.api/entity db e))
         next (- cur amount)]
     (if (neg? next)
       (throw (ex-info \"insufficient funds\" {:balance cur :requested amount}))
       [[:db/add e :acct/balance next]])))"
"#;

#[test]
fn invariant_function_enforces_non_negative_balance() {
    let fixture = Fixture::new(budget());
    fixture
        .transact(r#"[{:db/id "a" :acct/name "bob" :acct/balance 40}]"#)
        .expect("seed");
    let source = escaped(WITHDRAW_FN);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/withdraw :db/fn {source}}}]"#
        ))
        .expect("install fn");

    fixture
        .transact(r#"[[:acct/withdraw "bob" 25]]"#)
        .expect("withdrawal within balance");
    assert_eq!(fixture.balance("bob"), Some(15));

    let error = fixture
        .transact(r#"[[:acct/withdraw "bob" 100]]"#)
        .expect_err("overdraft must abort");
    assert!(
        eval_failure(&error).is_some_and(|text| text.contains("insufficient")),
        "got {error:?}"
    );
    assert_eq!(fixture.balance("bob"), Some(15));
}

#[test]
fn functions_expand_recursively_through_other_functions() {
    let fixture = Fixture::new(budget());
    fixture
        .transact(r#"[{:db/id "a" :acct/name "carol" :acct/balance 10}]"#)
        .expect("seed");
    let inner = escaped(
        r#""(fn [db acct amount]
             (let [e (ffirst (corium.api/q (quote [:find ?e :in $ ?n :where [?e :acct/name ?n]]) db acct))
                   cur (:acct/balance (corium.api/entity db e))]
               [[:db/add e :acct/balance (+ cur amount)]]))""#,
    );
    // The outer function returns an invocation of the inner one.
    let outer = escaped(r#""(fn [db acct] [[:acct/deposit acct 5]])""#);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f1" :db/ident :acct/deposit :db/fn {inner}}}
                {{:db/id "f2" :db/ident :acct/bonus :db/fn {outer}}}]"#
        ))
        .expect("install fns");

    fixture
        .transact(r#"[[:acct/bonus "carol"]]"#)
        .expect("recursive expansion");
    assert_eq!(fixture.balance("carol"), Some(15));
}

#[test]
fn runaway_recursive_expansion_aborts_cleanly() {
    let fixture = Fixture::new(budget());
    let source = escaped(r#""(fn [db] [[:acct/loop]])""#);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/loop :db/fn {source}}}]"#
        ))
        .expect("install fn");
    let error = fixture
        .transact("[[:acct/loop]]")
        .expect_err("self-recursive expansion must abort");
    assert!(matches!(error, DbFnError::Recursion(_)), "got {error:?}");
    // The pipeline still accepts ordinary transactions afterwards.
    fixture
        .transact(r#"[{:db/id "a" :acct/name "dave" :acct/balance 1}]"#)
        .expect("pipeline survives");
    assert_eq!(fixture.balance("dave"), Some(1));
}

#[test]
fn fuel_exhaustion_aborts_the_transaction() {
    let fixture = Fixture::new(DbFnBudget {
        fuel: 200,
        ..DbFnBudget::default()
    });
    let source = escaped(r#""(fn [db] ((fn boom [n] (boom (inc n))) 0))""#);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/burn :db/fn {source}}}]"#
        ))
        .expect("install fn");
    let error = fixture
        .transact("[[:acct/burn]]")
        .expect_err("fuel exhaustion must abort");
    assert!(
        matches!(
            error,
            DbFnError::Execution(TxError::GasExhausted | TxError::DepthExceeded)
        ),
        "got {error:?}"
    );
    // Clean abort: no partial datoms, next transaction fine.
    fixture
        .transact(r#"[{:db/id "a" :acct/name "erin" :acct/balance 5}]"#)
        .expect("pipeline survives");
    assert_eq!(fixture.balance("erin"), Some(5));
}

#[test]
fn call_depth_exhaustion_aborts_the_transaction() {
    let fixture = Fixture::new(DbFnBudget {
        call_depth: 64,
        ..DbFnBudget::default()
    });
    let source = escaped(r#""(fn [db] ((fn boom [n] (boom (inc n))) 0))""#);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/deep :db/fn {source}}}]"#
        ))
        .expect("install fn");
    let error = fixture
        .transact("[[:acct/deep]]")
        .expect_err("depth exhaustion must abort");
    assert!(
        matches!(error, DbFnError::Execution(TxError::DepthExceeded)),
        "got {error:?}"
    );
}

#[test]
fn memory_exhaustion_aborts_the_transaction() {
    let fixture = Fixture::new(DbFnBudget {
        memory_bytes: 256 * 1024,
        fuel: 10_000_000,
        ..DbFnBudget::default()
    });
    let source = escaped(r#""(fn [db] (vec (range 100000)))""#);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/flood :db/fn {source}}}]"#
        ))
        .expect("install fn");
    let error = fixture
        .transact("[[:acct/flood]]")
        .expect_err("memory exhaustion must abort");
    assert!(
        matches!(error, DbFnError::Execution(TxError::MemoryExhausted { .. })),
        "got {error:?}"
    );
    // Clean abort: the next transaction is fine.
    fixture
        .transact(r#"[{:db/id "a" :acct/name "faye" :acct/balance 2}]"#)
        .expect("pipeline survives");
    assert_eq!(fixture.balance("faye"), Some(2));
}

#[test]
fn functions_returning_non_tx_data_abort() {
    let fixture = Fixture::new(budget());
    let source = escaped(r#""(fn [db] 42)""#);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/bad :db/fn {source}}}]"#
        ))
        .expect("install fn");
    let error = fixture
        .transact("[[:acct/bad]]")
        .expect_err("non-tx-data result must abort");
    assert!(matches!(error, DbFnError::BadResult(_)), "got {error:?}");
}

#[test]
fn side_effects_are_forbidden() {
    let fixture = Fixture::new(budget());
    let source = escaped(r#""(fn [db] (spit \"/tmp/corium-txfn-test\" \"no\"))""#);
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/leak :db/fn {source}}}]"#
        ))
        .expect("install fn");
    let error = fixture
        .transact("[[:acct/leak]]")
        .expect_err("effectful function must abort");
    assert!(
        matches!(&error, DbFnError::Execution(TxError::ForbiddenEffect(name)) if name == "spit"),
        "got {error:?}"
    );
}

#[test]
fn basis_t_and_as_of_read_the_token_view() {
    let fixture = Fixture::new(budget());
    fixture
        .transact(r#"[{:db/id "a" :acct/name "gus" :acct/balance 1}]"#)
        .expect("seed");
    // A function that proves the db token answers basis-t.
    let source = escaped(
        r#""(fn [db acct]
             (let [t (corium.api/basis-t db)
                   e (ffirst (corium.api/q (quote [:find ?e :in $ ?n :where [?e :acct/name ?n]]) db acct))]
               [[:db/add e :acct/balance t]]))""#,
    );
    fixture
        .transact(&format!(
            r#"[{{:db/id "f" :db/ident :acct/stamp :db/fn {source}}}]"#
        ))
        .expect("install fn");
    fixture
        .transact(r#"[[:acct/stamp "gus"]]"#)
        .expect("stamp basis");
    let stamped = fixture.balance("gus").expect("balance");
    assert!(stamped > 0, "basis-t must be positive, got {stamped}");
}

/// The default expander wired through a `TransactorNode` (the process path,
/// exercising the feature-default `NodeConfig` wiring).
#[tokio::test(flavor = "multi_thread")]
async fn node_expands_db_functions_by_default() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = corium_transactor::node::NodeConfig::new(dir.path().join("data"));
    config.owner = "txfn-test".into();
    config.lease_ttl_ms = 600_000;
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    assert!(
        config.tx_fn_expander.is_some(),
        "feature cljrs must wire a default expander"
    );
    let node = corium_transactor::node::TransactorNode::open(config)
        .await
        .expect("open node");

    let encode =
        |text: &str| corium_protocol::codec::encode_edn(&read_one(text).expect("edn vector"));
    node.create_db("accounts", &encode(SCHEMA))
        .await
        .expect("create db");
    node.transact(
        "accounts",
        &encode(r#"[{:db/id "a" :acct/name "zoe" :acct/balance 10}]"#),
    )
    .await
    .expect("seed");
    let source = escaped(CAS_FN);
    node.transact(
        "accounts",
        &encode(&format!(
            r#"[{{:db/id "f" :db/ident :acct/cas :db/fn {source}}}]"#
        )),
    )
    .await
    .expect("install fn");
    node.transact("accounts", &encode(r#"[[:acct/cas "zoe" 10 20]]"#))
        .await
        .expect("cas through the node");
    let error = node
        .transact("accounts", &encode(r#"[[:acct/cas "zoe" 10 99]]"#))
        .await
        .expect_err("stale cas through the node");
    assert!(error.to_string().contains("balance changed"), "got {error}");
    let db = node.db_state("accounts").await.expect("db state").db();
    let name_attr = db
        .idents()
        .entid(&corium_core::Keyword::parse("acct/name"))
        .expect("attr");
    let bal_attr = db
        .idents()
        .entid(&corium_core::Keyword::parse("acct/balance"))
        .expect("attr");
    let e = db
        .lookup(name_attr, &corium_core::Value::Str("zoe".into()))
        .expect("entity");
    assert_eq!(db.values(e, bal_attr), vec![corium_core::Value::Long(20)]);
}
