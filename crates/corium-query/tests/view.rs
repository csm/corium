//! What a reader under a policy view sees — and, more importantly, what it
//! cannot learn by asking sideways.
//!
//! Hiding an attribute in the scan is only half the job: the engine has
//! several paths that reach the index directly (`get-else`, `missing?`,
//! lookup refs, reverse-ref pulls, the entity API). Each of those is an
//! existence oracle over exactly the attribute the view withholds, so each
//! one is exercised here.

use std::sync::Arc;

use corium_core::{
    Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Partition, Schema, Unique,
    Value, ValueType,
};
use corium_db::read::{AttrVisibility, ReadContext};
use corium_db::{Db, Idents};
use corium_query::edn::{Edn, read_one};
use corium_query::{Entity, ExecOptions, QInput, pull_with, run};

const NAME: u64 = 100;
const SSN: u64 = 101;
const MANAGER: u64 = 102;

fn attr_id(n: u64) -> EntityId {
    EntityId::new(Partition::Db as u32, n)
}

fn person(n: u64) -> EntityId {
    EntityId::new(Partition::User as u32, n)
}

/// Alice(1) and Bob(2) both have a name and an SSN; Bob reports to Alice.
fn fixture() -> Db {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    let interner = KeywordInterner::default();
    let mut install = |id: u64, ident: &str, value_type: ValueType, unique: Option<Unique>| {
        schema.insert(Attribute {
            id: attr_id(id),
            value_type,
            cardinality: Cardinality::One,
            unique,
            is_component: false,
            indexed: true,
            no_history: false,
        });
        idents.insert(Keyword::parse(ident), attr_id(id));
    };
    install(NAME, "person/name", ValueType::Str, Some(Unique::Identity));
    install(SSN, "person/ssn", ValueType::Str, Some(Unique::Identity));
    install(MANAGER, "person/manager", ValueType::Ref, None);

    let datom = |e: EntityId, a: u64, v: Value| Datom {
        e,
        a: attr_id(a),
        v,
        tx: EntityId::new(Partition::Tx as u32, 1),
        added: true,
    };
    let datoms = vec![
        datom(person(1), NAME, Value::Str("alice".into())),
        datom(person(1), SSN, Value::Str("111-11-1111".into())),
        datom(person(2), NAME, Value::Str("bob".into())),
        datom(person(2), SSN, Value::Str("222-22-2222".into())),
        datom(person(2), MANAGER, Value::Ref(person(1))),
    ];
    Db::new(schema)
        .with_naming(idents, interner)
        .with_transaction(1, &datoms)
}

/// A reader who may not see `:person/ssn` or `:person/manager`.
fn restricted() -> ReadContext {
    ReadContext::open().with_visibility(Arc::new(AttrVisibility::hiding([
        attr_id(SSN),
        attr_id(MANAGER),
    ])))
}

fn query(db: &Db, text: &str, read: &ReadContext) -> Result<Edn, corium_query::QueryError> {
    let parsed = corium_query::QueryCache::new()
        .parse(&read_one(text).expect("query parses"))
        .expect("query is well formed");
    run(
        &parsed,
        &[QInput::Db(db)],
        ExecOptions {
            read: read.clone(),
            ..ExecOptions::default()
        },
    )
    .map(|(result, _)| result)
}

fn rows(edn: &Edn) -> Vec<Edn> {
    match edn {
        Edn::Vector(items) => items.clone(),
        other => panic!("expected a relation, got {other}"),
    }
}

#[test]
fn a_hidden_attribute_never_binds_in_a_scan() {
    let db = fixture();
    let text = "[:find ?ssn :where [?e :person/ssn ?ssn]]";
    assert_eq!(
        rows(&query(&db, text, &ReadContext::open()).expect("unrestricted read")).len(),
        2,
        "the facts are there for a reader with full visibility"
    );
    assert!(
        rows(&query(&db, text, &restricted()).expect("restricted read")).is_empty(),
        "a hidden attribute yields no rows at all"
    );
}

#[test]
fn a_hidden_attribute_is_absent_from_an_unbound_attribute_scan() {
    let db = fixture();
    // The attribute position is a variable, so this scan sees every attribute
    // the reader is allowed to see and must not tour around the view.
    let result =
        query(&db, "[:find ?a :where [?e ?a ?v]]", &restricted()).expect("restricted read");
    let hidden = Edn::Long(i64::try_from(attr_id(SSN).raw()).expect("id fits"));
    assert!(
        !rows(&result).iter().any(|row| match row {
            Edn::Vector(cells) => cells.first() == Some(&hidden),
            _ => false,
        }),
        "the hidden attribute id must not appear: {result}"
    );
}

#[test]
fn get_else_falls_back_to_its_default_for_a_hidden_attribute() {
    let db = fixture();
    let text = "[:find ?ssn
                 :where [?e :person/name \"alice\"]
                        [(get-else $ ?e :person/ssn \"none\") ?ssn]]";
    assert_eq!(
        rows(&query(&db, text, &ReadContext::open()).expect("unrestricted read")),
        vec![Edn::Vector(vec![Edn::Str("111-11-1111".into())])]
    );
    assert_eq!(
        rows(&query(&db, text, &restricted()).expect("restricted read")),
        vec![Edn::Vector(vec![Edn::Str("none".into())])],
        "a hidden attribute reads as absent rather than leaking through get-else"
    );
}

#[test]
fn missing_reports_a_hidden_attribute_as_missing() {
    let db = fixture();
    let text = "[:find ?name
                 :where [?e :person/name ?name] [(missing? $ ?e :person/ssn)]]";
    assert!(
        rows(&query(&db, text, &ReadContext::open()).expect("unrestricted read")).is_empty(),
        "everyone has an SSN, so nothing is missing for a full reader"
    );
    assert_eq!(
        rows(&query(&db, text, &restricted()).expect("restricted read")).len(),
        2,
        "missing? must not become an existence oracle over a hidden attribute"
    );
}

#[test]
fn a_lookup_ref_over_a_hidden_attribute_does_not_resolve() {
    let db = fixture();
    let text = "[:find ?name
                 :where [[:person/ssn \"111-11-1111\"] :person/name ?name]]";
    assert_eq!(
        rows(&query(&db, text, &ReadContext::open()).expect("unrestricted read")),
        vec![Edn::Vector(vec![Edn::Str("alice".into())])]
    );
    assert!(
        rows(&query(&db, text, &restricted()).expect("restricted read")).is_empty(),
        "whether a lookup ref resolves reports whether the value exists"
    );
}

#[test]
fn a_lookup_ref_input_over_a_hidden_attribute_is_refused() {
    let db = fixture();
    let parsed = corium_query::QueryCache::new()
        .parse(&read_one("[:find ?name :in $ ?e :where [?e :person/name ?name]]").expect("parses"))
        .expect("well formed");
    let lookup = read_one("[:person/ssn \"111-11-1111\"]").expect("parses");
    let refused = run(
        &parsed,
        &[QInput::Db(&db), QInput::Edn(lookup)],
        ExecOptions {
            read: restricted(),
            ..ExecOptions::default()
        },
    )
    .expect_err("a hidden lookup ref must not resolve an input");
    assert!(
        refused.to_string().contains("cannot see"),
        "unexpected error: {refused}"
    );
}

#[test]
fn pull_omits_hidden_attributes_including_under_a_wildcard() {
    let db = fixture();
    let pattern = read_one("[*]").expect("pattern parses");
    let full = pull_with(&db, &pattern, person(2), &ReadContext::open()).expect("full pull");
    assert!(full.to_string().contains("person/ssn"), "{full}");

    let hidden = pull_with(&db, &pattern, person(2), &restricted()).expect("restricted pull");
    assert!(
        hidden.to_string().contains("person/name"),
        "visible attributes still come back: {hidden}"
    );
    assert!(
        !hidden.to_string().contains("person/ssn"),
        "a hidden attribute is absent from a wildcard pull: {hidden}"
    );

    let explicit = read_one("[:person/ssn]").expect("pattern parses");
    let named = pull_with(&db, &explicit, person(2), &restricted()).expect("restricted pull");
    assert!(
        !named.to_string().contains("222-22-2222"),
        "naming the attribute explicitly does not reveal it: {named}"
    );
}

#[test]
fn a_hidden_reference_does_not_point_back_either() {
    let db = fixture();
    let pattern = read_one("[:person/name {:person/_manager [:person/name]}]").expect("parses");
    let full = pull_with(&db, &pattern, person(1), &ReadContext::open()).expect("full pull");
    assert!(
        full.to_string().contains("bob"),
        "Bob reports to Alice: {full}"
    );

    let hidden = pull_with(&db, &pattern, person(1), &restricted()).expect("restricted pull");
    assert!(
        !hidden.to_string().contains("bob"),
        "a reverse ref over a hidden attribute reports nothing: {hidden}"
    );
}

#[test]
fn the_entity_api_hides_keys_and_reverse_navigation() {
    let db = fixture();
    let read = restricted();

    let open = Entity::new(&db, person(2));
    assert!(open.keys().contains(&attr_id(SSN)));
    assert_eq!(
        Entity::new(&db, person(1)).reverse(attr_id(MANAGER)).len(),
        1
    );

    let entity = Entity::new(&db, person(2)).with_read(&read);
    assert!(
        !entity.keys().contains(&attr_id(SSN)),
        "the key list is itself information"
    );
    assert!(entity.keys().contains(&attr_id(NAME)));
    assert!(entity.get(attr_id(SSN)).is_empty());
    assert!(
        Entity::new(&db, person(1))
            .with_read(&read)
            .reverse(attr_id(MANAGER))
            .is_empty()
    );
}
