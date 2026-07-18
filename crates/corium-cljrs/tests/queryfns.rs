//! Query fn/pred clause resolution through the sandbox seam (M5): names
//! registered with [`corium_cljrs::query::QueryFns`] resolve in call
//! clauses, run under the sandbox budget, and unknown names still produce
//! the engine's canonical unsupported error.

use std::time::Duration;

use corium_cljrs::query::QueryFns;
use corium_cljrs::sandbox::SandboxBudget;
use corium_core::{
    Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Partition, Schema, Value,
    ValueType,
};
use corium_db::{Db, Idents};
use corium_query::edn::read_one;
use corium_query::{ExecOptions, QInput, QueryError, run};

fn test_db() -> Db {
    let attr = EntityId::new(Partition::Db as u32, 100);
    let mut schema = Schema::default();
    schema.insert(Attribute {
        id: attr,
        value_type: ValueType::Long,
        cardinality: Cardinality::One,
        unique: None,
        is_component: false,
        indexed: false,
        no_history: false,
    });
    let mut idents = Idents::default();
    idents.insert(Keyword::parse("num/value"), attr);
    let db = Db::new(schema).with_naming(idents, KeywordInterner::default());
    let datoms: Vec<Datom> = (0..5)
        .map(|n| Datom {
            e: EntityId::new(Partition::User as u32, 1000 + n),
            a: attr,
            v: Value::Long(i64::try_from(n).expect("fits")),
            tx: EntityId::new(Partition::Tx as u32, 1),
            added: true,
        })
        .collect();
    db.with_transaction(1, &datoms)
}

fn budget() -> SandboxBudget {
    SandboxBudget {
        fuel: 100_000,
        deadline: Duration::from_secs(2),
        ..SandboxBudget::default()
    }
}

#[test]
fn sandbox_predicate_filters_frames() {
    let db = test_db();
    let fns = QueryFns::new(budget());
    fns.register("user/big?", "(fn [n] (>= n 3))");
    let query = read_one("[:find ?v :where [?e :num/value ?v] [(user/big? ?v)]]").expect("query");
    let parsed = corium_query::ast::parse_query(&query).expect("parse");
    let options = ExecOptions {
        extern_call: Some(fns.extern_call(&db)),
        ..ExecOptions::default()
    };
    let (result, _) = run(&parsed, &[QInput::Db(&db)], options).expect("run");
    assert_eq!(result, read_one("[[3] [4]]").expect("expected"));
}

#[test]
fn sandbox_function_binds_scalars() {
    let db = test_db();
    let fns = QueryFns::new(budget());
    fns.register("user/squared", "(fn [n] (* n n))");
    let query = read_one("[:find ?v ?sq :where [?e :num/value ?v] [(user/squared ?v) ?sq]]")
        .expect("query");
    let parsed = corium_query::ast::parse_query(&query).expect("parse");
    let options = ExecOptions {
        extern_call: Some(fns.extern_call(&db)),
        ..ExecOptions::default()
    };
    let (result, _) = run(&parsed, &[QInput::Db(&db)], options).expect("run");
    assert_eq!(
        result,
        read_one("[[0 0] [1 1] [2 4] [3 9] [4 16]]").expect("expected")
    );
}

#[test]
fn unknown_names_keep_the_canonical_error() {
    let db = test_db();
    let fns = QueryFns::new(budget());
    let query =
        read_one("[:find ?v :where [?e :num/value ?v] [(user/mystery ?v)]]").expect("query");
    let parsed = corium_query::ast::parse_query(&query).expect("parse");
    let options = ExecOptions {
        extern_call: Some(fns.extern_call(&db)),
        ..ExecOptions::default()
    };
    let error = run(&parsed, &[QInput::Db(&db)], options).expect_err("must fail");
    assert!(matches!(error, QueryError::Unsupported(_)), "got {error:?}");
}

#[test]
fn sandbox_failures_abort_the_query() {
    let db = test_db();
    let fns = QueryFns::new(SandboxBudget {
        fuel: 50,
        deadline: Duration::from_secs(2),
        ..SandboxBudget::default()
    });
    fns.register("user/burn", "(fn [n] ((fn boom [i] (boom (inc i))) n))");
    let query = read_one("[:find ?v :where [?e :num/value ?v] [(user/burn ?v)]]").expect("query");
    let parsed = corium_query::ast::parse_query(&query).expect("parse");
    let options = ExecOptions {
        extern_call: Some(fns.extern_call(&db)),
        ..ExecOptions::default()
    };
    let error = run(&parsed, &[QInput::Db(&db)], options).expect_err("must fail");
    assert!(
        matches!(&error, QueryError::Unsupported(text) if text.contains("fuel")),
        "got {error:?}"
    );
}
