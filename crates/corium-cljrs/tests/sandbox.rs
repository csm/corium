//! Sandbox acceptance battery (M5): compilation, in-sandbox read API,
//! fuel/allocation/deadline budgets, and escape attempts — every escape
//! must fail safely (a clean error, a still-usable sandbox).

use std::time::Duration;

use corium_cljrs::sandbox::{Sandbox, SandboxBudget, SandboxError};
use corium_core::{
    Attribute, Cardinality, EntityId, KeywordInterner, Partition, Schema, ValueType,
};
use corium_db::{Db, Idents};
use corium_query::edn::{Edn, read_one};

fn attr(seq: u64, value_type: ValueType) -> Attribute {
    Attribute {
        id: EntityId::new(Partition::Db as u32, seq),
        value_type,
        cardinality: Cardinality::One,
        unique: None,
        is_component: false,
        indexed: false,
        no_history: false,
    }
}

/// A tiny database: one entity with `:acct/balance 10`.
fn test_db() -> Db {
    let mut schema = Schema::default();
    schema.insert(attr(100, ValueType::Long));
    let mut idents = Idents::default();
    idents.insert(corium_core::Keyword::parse("acct/balance"), attr_id());
    let db = Db::new(schema).with_naming(idents, KeywordInterner::default());
    let e = EntityId::new(Partition::User as u32, 1000);
    db.with_transaction(
        1,
        &[corium_core::Datom {
            e,
            a: attr_id(),
            v: corium_core::Value::Long(10),
            tx: EntityId::new(Partition::Tx as u32, 1),
            added: true,
        }],
    )
}

fn attr_id() -> EntityId {
    EntityId::new(Partition::Db as u32, 100)
}

fn entity_long() -> i64 {
    i64::try_from(EntityId::new(Partition::User as u32, 1000).raw()).expect("fits")
}

fn budget() -> SandboxBudget {
    SandboxBudget {
        fuel: 100_000,
        max_alloc_bytes: 32 * 1024 * 1024,
        deadline: Duration::from_secs(2),
        ..SandboxBudget::default()
    }
}

#[test]
fn evaluates_pure_functions() {
    let sandbox = Sandbox::new();
    let result = sandbox
        .invoke(
            "(fn [a b] (+ a (* 2 b)))",
            None,
            vec![Edn::Long(3), Edn::Long(4)],
            budget(),
        )
        .expect("invoke");
    assert_eq!(result, Edn::Long(11));
}

#[test]
fn compile_cache_reuses_functions() {
    let sandbox = Sandbox::new();
    for n in 0..3 {
        let result = sandbox
            .invoke("(fn [n] (inc n))", None, vec![Edn::Long(n)], budget())
            .expect("invoke");
        assert_eq!(result, Edn::Long(n + 1));
    }
}

#[test]
fn collections_round_trip() {
    let sandbox = Sandbox::new();
    let result = sandbox
        .invoke(
            "(fn [m] {:doubled (mapv #(* 2 %) (:xs m)) :name (:name m)})",
            None,
            vec![read_one("{:xs [1 2 3] :name \"corium\"}").expect("edn")],
            budget(),
        )
        .expect("invoke");
    assert_eq!(
        result,
        read_one("{:doubled [2 4 6] :name \"corium\"}").expect("edn")
    );
}

#[test]
fn reads_database_through_sandbox_api() {
    let sandbox = Sandbox::new();
    let db = test_db();
    let source = "(fn [db e] (:acct/balance (corium.api/entity db e)))";
    let result = sandbox
        .invoke(source, Some(db), vec![Edn::Long(entity_long())], budget())
        .expect("invoke");
    assert_eq!(result, Edn::Long(10));
}

#[test]
fn queries_database_through_sandbox_api() {
    let sandbox = Sandbox::new();
    let db = test_db();
    let source = "(fn [db] (corium.api/q (quote [:find ?b . :where [?e :acct/balance ?b]]) db))";
    let result = sandbox
        .invoke(source, Some(db), vec![], budget())
        .expect("invoke");
    assert_eq!(result, Edn::Long(10));
}

#[test]
fn returns_tx_data_shapes() {
    let sandbox = Sandbox::new();
    let db = test_db();
    let source = "(fn [db e] [[:db/add e :acct/balance 42]])";
    let result = sandbox
        .invoke(source, Some(db), vec![Edn::Long(entity_long())], budget())
        .expect("invoke");
    let expected = Edn::Vector(vec![Edn::Vector(vec![
        Edn::keyword("db/add"),
        Edn::Long(entity_long()),
        Edn::keyword("acct/balance"),
        Edn::Long(42),
    ])]);
    assert_eq!(result, expected);
}

#[test]
fn fuel_exhaustion_aborts_cleanly() {
    let sandbox = Sandbox::new();
    let tight = SandboxBudget {
        fuel: 500,
        ..budget()
    };
    let source = "(fn [n] ((fn boom [i] (boom (inc i))) n))";
    let error = sandbox
        .invoke(source, None, vec![Edn::Long(0)], tight)
        .expect_err("must exhaust fuel");
    assert_eq!(error, SandboxError::FuelExhausted);
    // The sandbox remains usable on the same worker.
    let result = sandbox
        .invoke("(fn [] :ok)", None, vec![], budget())
        .expect("invoke after exhaustion");
    assert_eq!(result, Edn::keyword("ok"));
}

#[test]
fn allocation_cap_aborts_cleanly() {
    let sandbox = Sandbox::new();
    let tight = SandboxBudget {
        fuel: u64::MAX / 2,
        max_alloc_bytes: 1024 * 1024,
        deadline: Duration::from_secs(10),
        ..SandboxBudget::default()
    };
    let source = "(fn [] (count (vec (range 10000000))))";
    let error = sandbox
        .invoke(source, None, vec![], tight)
        .expect_err("must exceed allocation cap");
    assert!(
        matches!(
            error,
            SandboxError::AllocExceeded | SandboxError::FuelExhausted
        ),
        "got {error:?}"
    );
}

#[test]
fn unbounded_special_form_loop_hits_watchdog() {
    let sandbox = Sandbox::new();
    let tight = SandboxBudget {
        deadline: Duration::from_millis(300),
        ..budget()
    };
    let error = sandbox
        .invoke("(fn [] (loop [] (recur)))", None, vec![], tight)
        .expect_err("must hit the watchdog");
    assert_eq!(error, SandboxError::Deadline);
    assert!(sandbox.abandoned_worker());
    // A fresh worker serves the next invocation.
    let result = sandbox
        .invoke("(fn [] 7)", None, vec![], budget())
        .expect("invoke after abandonment");
    assert_eq!(result, Edn::Long(7));
}

#[test]
fn fn_calling_loop_hits_fuel_not_watchdog() {
    let sandbox = Sandbox::new();
    let tight = SandboxBudget {
        fuel: 10_000,
        ..budget()
    };
    // The loop body applies a cljrs function every iteration, so the fuel
    // hook sees it. (Native builtins alone are billed by the deadline.)
    let error = sandbox
        .invoke(
            "(fn [] (let [f (fn [x] (inc x))] (loop [n 0] (recur (f n)))))",
            None,
            vec![],
            tight,
        )
        .expect_err("must exhaust fuel");
    assert_eq!(error, SandboxError::FuelExhausted);
}

// ── Escape attempts: every one must fail safely ──────────────────────────────

fn assert_escape_fails(source: &str) {
    let sandbox = Sandbox::new();
    let error = sandbox
        .invoke(source, None, vec![], budget())
        .expect_err(&format!("{source} must fail"));
    assert!(
        !matches!(error, SandboxError::Deadline),
        "{source} should fail fast, got {error:?}"
    );
    // The sandbox stays healthy afterwards.
    let result = sandbox
        .invoke("(fn [] :alive)", None, vec![], budget())
        .expect("sandbox survives escape attempt");
    assert_eq!(result, Edn::keyword("alive"));
}

#[test]
fn io_escapes_fail_safely() {
    assert_escape_fails("(fn [] (slurp \"/etc/passwd\"))");
    assert_escape_fails("(fn [] (spit \"/tmp/x\" \"data\"))");
    assert_escape_fails("(fn [] (println \"leak\"))");
    assert_escape_fails("(fn [] (load-file \"/etc/passwd\"))");
    assert_escape_fails("(fn [] (read-string \"(+ 1 2)\"))");
}

#[test]
fn namespace_escapes_fail_safely() {
    assert_escape_fails("(fn [] (require 'clojure.java.io))");
    assert_escape_fails("(fn [] (in-ns 'evil))");
    assert_escape_fails("(fn [] (def leaked 1))");
    assert_escape_fails("(fn [] (intern 'user 'x 1))");
    assert_escape_fails("(fn [] (resolve 'slurp))");
}

#[test]
fn interop_escapes_fail_safely() {
    assert_escape_fails("(fn [] (new java.io.File \"/\"))");
    assert_escape_fails("(fn [] (System/getProperty \"user.home\"))");
    assert_escape_fails("(fn [] (var slurp))");
}

#[test]
fn state_and_nondeterminism_fail_safely() {
    assert_escape_fails("(fn [] (swap! (atom 0) inc))");
    assert_escape_fails("(fn [] (rand))");
    assert_escape_fails("(fn [] (random-uuid))");
    assert_escape_fails("(fn [] @(future 1))");
    assert_escape_fails("(fn [] (nanotime))");
}

#[test]
fn rejects_non_function_sources() {
    let sandbox = Sandbox::new();
    let error = sandbox
        .invoke("(+ 1 2)", None, vec![], budget())
        .expect_err("non-fn source");
    assert!(matches!(error, SandboxError::Compile(_)), "got {error:?}");
    let error = sandbox
        .invoke("(fn [] 1) (fn [] 2)", None, vec![], budget())
        .expect_err("two forms");
    assert!(matches!(error, SandboxError::Compile(_)), "got {error:?}");
}
