//! Planner guarantees: a bound attribute never full-scans, selectivity
//! ordering favors rare attributes, and fuel bounds runaway queries.

use corium_core::{
    Attribute, Cardinality, Datom, EntityId, IndexOrder, Keyword, KeywordInterner, Partition,
    Schema, Value, ValueType,
};
use corium_db::{Db, Idents};
use corium_query::ast::parse_query;
use corium_query::edn::read_one;
use corium_query::plan::choose_index;
use corium_query::{ExecOptions, QInput, QueryError, run};

fn attr_id(n: u64) -> EntityId {
    EntityId::new(Partition::Db as u32, n)
}

/// A database with one common attribute (1000 datoms) and one rare
/// attribute (5 datoms).
fn skewed_db() -> Db {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    for (id, ident, indexed) in [(1, "t/common", false), (2, "t/rare", true)] {
        schema.insert(Attribute {
            id: attr_id(id),
            value_type: ValueType::Long,
            cardinality: Cardinality::One,
            unique: None,
            is_component: false,
            indexed,
            no_history: false,
        });
        idents.insert(Keyword::parse(ident), attr_id(id));
    }
    let mut datoms = Vec::new();
    for n in 0..1000_u64 {
        datoms.push(Datom {
            e: EntityId::new(Partition::User as u32, n),
            a: attr_id(1),
            v: Value::Long(i64::try_from(n).expect("fits")),
            tx: EntityId::new(Partition::Tx as u32, 1),
            added: true,
        });
    }
    for n in 0..5_u64 {
        datoms.push(Datom {
            e: EntityId::new(Partition::User as u32, n * 100),
            a: attr_id(2),
            v: Value::Long(i64::try_from(n).expect("fits")),
            tx: EntityId::new(Partition::Tx as u32, 1),
            added: true,
        });
    }
    Db::new(schema)
        .with_naming(idents, KeywordInterner::default())
        .with_transaction(1, &datoms)
}

#[test]
fn bound_attribute_never_full_scans() {
    // Exhaustive over every boundness combination: whenever `a` is bound the
    // chosen scan must have a non-empty prefix.
    for mask in 0..32_u8 {
        let (e, a, v, avet_ok, v_is_ref) = (
            mask & 1 != 0,
            mask & 2 != 0,
            mask & 4 != 0,
            mask & 8 != 0,
            mask & 16 != 0,
        );
        let choice = choose_index(e, a, v, avet_ok, v_is_ref);
        if a {
            assert!(
                choice.prefix_len >= 1,
                "bound a must not full-scan: {choice:?}"
            );
            // VAET is only acceptable when the attribute is in its prefix.
            assert!(
                choice.order != IndexOrder::Vaet || choice.prefix_len >= 2,
                "bound a picks an index whose prefix covers it: {choice:?}"
            );
        }
        if e {
            assert_eq!(choice.order, IndexOrder::Eavt);
            assert!(choice.prefix_len >= 1);
        }
    }
}

#[test]
fn rare_attribute_scan_touches_only_its_datoms() {
    let db = skewed_db();
    let query = parse_query(&read_one("[:find ?e ?v :where [?e :t/rare ?v]]").expect("edn"))
        .expect("parse");
    let (result, report) = run(&query, &[QInput::Db(&db)], ExecOptions::default()).expect("run");
    assert!(matches!(
        result,
        corium_query::edn::Edn::Vector(rows) if rows.len() == 5
    ));
    assert!(
        report.datoms_scanned <= 5,
        "scanned {} datoms for a 5-datom attribute",
        report.datoms_scanned
    );
}

#[test]
fn selectivity_ordering_starts_from_the_rare_pattern() {
    let db = skewed_db();
    // Textually the common pattern comes first; statistics must reorder so
    // the rare pattern seeds the join and the common one is an EAVT lookup.
    let query = parse_query(
        &read_one("[:find ?e ?c :where [?e :t/common ?c] [?e :t/rare ?r]]").expect("edn"),
    )
    .expect("parse");
    let (_, report) = run(&query, &[QInput::Db(&db)], ExecOptions::default()).expect("run");
    assert!(
        report.datoms_scanned <= 5 + 5 * 2,
        "expected rare-first ordering, scanned {}",
        report.datoms_scanned
    );
}

#[test]
fn fuel_bounds_runaway_scans() {
    let db = skewed_db();
    let query = parse_query(&read_one("[:find ?e ?v :where [?e :t/common ?v]]").expect("edn"))
        .expect("parse");
    let result = run(&query, &[QInput::Db(&db)], ExecOptions { fuel: Some(10) });
    assert_eq!(result.unwrap_err(), QueryError::FuelExhausted);
}
