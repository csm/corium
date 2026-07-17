//! End-to-end engine tests over a hand-built database value.

use corium_core::{
    Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Partition, Schema, Unique,
    Value, ValueType,
};
use corium_db::{Db, Idents};
use corium_query::edn::{Edn, read_one};
use corium_query::{Entity, QInput, QueryCache, pull, q_str};

fn attr_id(n: u64) -> EntityId {
    EntityId::new(Partition::Db as u32, n)
}

fn person(n: u64) -> EntityId {
    EntityId::new(Partition::User as u32, n)
}

fn tx(t: u64) -> EntityId {
    EntityId::new(Partition::Tx as u32, t)
}

const NAME: u64 = 100;
const AGE: u64 = 101;
const FRIEND: u64 = 102;
const PET: u64 = 103;
const PET_NAME: u64 = 104;
const TAG: u64 = 105;

#[allow(clippy::too_many_lines)]
fn fixture() -> Db {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    let mut interner = KeywordInterner::default();
    let mut install = |id: u64,
                       ident: &str,
                       value_type: ValueType,
                       cardinality: Cardinality,
                       unique: Option<Unique>,
                       is_component: bool| {
        schema.insert(Attribute {
            id: attr_id(id),
            value_type,
            cardinality,
            unique,
            is_component,
            indexed: true,
            no_history: false,
        });
        idents.insert(Keyword::parse(ident), attr_id(id));
    };
    install(
        NAME,
        "person/name",
        ValueType::Str,
        Cardinality::One,
        Some(Unique::Identity),
        false,
    );
    install(
        AGE,
        "person/age",
        ValueType::Long,
        Cardinality::One,
        None,
        false,
    );
    install(
        FRIEND,
        "person/friend",
        ValueType::Ref,
        Cardinality::Many,
        None,
        false,
    );
    install(
        PET,
        "person/pet",
        ValueType::Ref,
        Cardinality::One,
        None,
        true,
    );
    install(
        PET_NAME,
        "pet/name",
        ValueType::Str,
        Cardinality::One,
        None,
        false,
    );
    install(
        TAG,
        "person/tag",
        ValueType::Keyword,
        Cardinality::Many,
        None,
        false,
    );
    let kw_vip = interner.intern(Keyword::parse("tag/vip"));

    let mut datoms = Vec::new();
    let mut add = |e: EntityId, a: u64, v: Value, t: u64| {
        datoms.push((
            t,
            Datom {
                e,
                a: attr_id(a),
                v,
                tx: tx(t),
                added: true,
            },
        ));
    };
    // t=1: alice(1), bob(2), carol(3), dan(4); pet rex(10)
    add(person(1), NAME, Value::Str("alice".into()), 1);
    add(person(1), AGE, Value::Long(30), 1);
    add(person(2), NAME, Value::Str("bob".into()), 1);
    add(person(2), AGE, Value::Long(25), 1);
    add(person(3), NAME, Value::Str("carol".into()), 1);
    add(person(3), AGE, Value::Long(35), 1);
    add(person(4), NAME, Value::Str("dan".into()), 1);
    add(person(1), FRIEND, Value::Ref(person(2)), 1);
    add(person(2), FRIEND, Value::Ref(person(3)), 1);
    add(person(1), PET, Value::Ref(person(10)), 1);
    add(person(10), PET_NAME, Value::Str("rex".into()), 1);
    add(person(1), TAG, Value::Keyword(kw_vip), 1);
    // t=2: bob's age changes 25 -> 26 (retract + assert)
    let mut db = Db::new(schema).with_naming(idents, interner);
    let first: Vec<Datom> = datoms
        .iter()
        .filter(|(t, _)| *t == 1)
        .map(|(_, d)| d.clone())
        .collect();
    db = db.with_transaction(1, &first);
    let second = vec![
        Datom {
            e: person(2),
            a: attr_id(AGE),
            v: Value::Long(25),
            tx: tx(2),
            added: false,
        },
        Datom {
            e: person(2),
            a: attr_id(AGE),
            v: Value::Long(26),
            tx: tx(2),
            added: true,
        },
    ];
    db.with_transaction(2, &second)
}

fn rows(edn: &Edn) -> Vec<Edn> {
    match edn {
        Edn::Vector(items) => items.clone(),
        other => panic!("expected relation, got {other}"),
    }
}

#[test]
fn pattern_join_and_predicate() {
    let db = fixture();
    let result = q_str(
        "[:find ?name ?age
          :where [?e :person/name ?name] [?e :person/age ?age] [(>= ?age 30)]]",
        &[QInput::Db(&db)],
    )
    .expect("query");
    let result_rows = rows(&result);
    assert_eq!(result_rows.len(), 2);
    assert!(result.to_string().contains("alice"));
    assert!(result.to_string().contains("carol"));
}

#[test]
fn ref_join_across_entities() {
    let db = fixture();
    let result = q_str(
        "[:find ?fname
          :where [?e :person/name \"alice\"] [?e :person/friend ?f] [?f :person/name ?fname]]",
        &[QInput::Db(&db)],
    )
    .expect("query");
    assert_eq!(
        rows(&result),
        vec![Edn::Vector(vec![Edn::Str("bob".into())])]
    );
}

#[test]
fn scalar_collection_and_tuple_finds() {
    let db = fixture();
    let scalar = q_str(
        "[:find ?age . :where [?e :person/name \"bob\"] [?e :person/age ?age]]",
        &[QInput::Db(&db)],
    )
    .expect("scalar");
    assert_eq!(scalar, Edn::Long(26));
    let coll = q_str(
        "[:find [?name ...] :where [?e :person/name ?name]]",
        &[QInput::Db(&db)],
    )
    .expect("coll");
    assert_eq!(rows(&coll).len(), 4);
    let tuple = q_str(
        "[:find [?name ?age] :where [?e :person/name ?name] [?e :person/age ?age]]",
        &[QInput::Db(&db)],
    )
    .expect("tuple");
    assert!(matches!(tuple, Edn::Vector(items) if items.len() == 2));
}

#[test]
fn inputs_scalar_collection_relation() {
    let db = fixture();
    let by_name = q_str(
        "[:find ?age . :in $ ?name :where [?e :person/name ?name] [?e :person/age ?age]]",
        &[QInput::Db(&db), QInput::Edn(Edn::Str("carol".into()))],
    )
    .expect("scalar input");
    assert_eq!(by_name, Edn::Long(35));
    let names = read_one("[\"alice\" \"bob\"]").expect("edn");
    let coll = q_str(
        "[:find ?name ?age :in $ [?name ...]
          :where [?e :person/name ?name] [?e :person/age ?age]]",
        &[QInput::Db(&db), QInput::Edn(names)],
    )
    .expect("coll input");
    assert_eq!(rows(&coll).len(), 2);
}

#[test]
fn not_and_or_clauses() {
    let db = fixture();
    let not_result = q_str(
        "[:find ?name :where [?e :person/name ?name] (not [?e :person/age _])]",
        &[QInput::Db(&db)],
    )
    .expect("not");
    assert_eq!(
        rows(&not_result),
        vec![Edn::Vector(vec![Edn::Str("dan".into())])]
    );
    let or_result = q_str(
        "[:find ?name
          :where [?e :person/name ?name]
                 (or [?e :person/age 30] [?e :person/age 35])]",
        &[QInput::Db(&db)],
    )
    .expect("or");
    assert_eq!(rows(&or_result).len(), 2);
}

#[test]
fn function_clause_binds_output() {
    let db = fixture();
    let result = q_str(
        "[:find ?name ?next
          :where [?e :person/name ?name] [?e :person/age ?age] [(inc ?age) ?next] [(= ?next 27)]]",
        &[QInput::Db(&db)],
    )
    .expect("fn");
    assert_eq!(
        rows(&result),
        vec![Edn::Vector(vec![Edn::Str("bob".into()), Edn::Long(27)])]
    );
}

#[test]
fn get_else_and_missing() {
    let db = fixture();
    let result = q_str(
        "[:find ?name ?age
          :where [?e :person/name ?name] [(get-else $ ?e :person/age -1) ?age]]",
        &[QInput::Db(&db)],
    )
    .expect("get-else");
    assert!(result.to_string().contains("-1"));
    let missing = q_str(
        "[:find ?name :where [?e :person/name ?name] [(missing? $ ?e :person/age)]]",
        &[QInput::Db(&db)],
    )
    .expect("missing?");
    assert_eq!(
        rows(&missing),
        vec![Edn::Vector(vec![Edn::Str("dan".into())])]
    );
}

#[test]
fn keyword_values_match() {
    let db = fixture();
    let result = q_str(
        "[:find ?name :where [?e :person/tag :tag/vip] [?e :person/name ?name]]",
        &[QInput::Db(&db)],
    )
    .expect("kw");
    assert_eq!(
        rows(&result),
        vec![Edn::Vector(vec![Edn::Str("alice".into())])]
    );
}

#[test]
fn aggregates_group_correctly() {
    let db = fixture();
    let result = q_str(
        "[:find (count ?e) . :where [?e :person/name _]]",
        &[QInput::Db(&db)],
    )
    .expect("count");
    assert_eq!(result, Edn::Long(4));
    let sums = q_str(
        "[:find (sum ?age) (avg ?age) (min ?age) (max ?age) (median ?age)
          :with ?e
          :where [?e :person/age ?age]]",
        &[QInput::Db(&db)],
    )
    .expect("sums");
    let row = rows(&sums)[0].clone();
    assert_eq!(
        row,
        Edn::Vector(vec![
            Edn::Long(91),
            read_one("30.333333333333332").expect("avg"),
            Edn::Long(26),
            Edn::Long(35),
            Edn::Long(30),
        ])
    );
}

#[test]
fn recursive_rules_find_transitive_friends() {
    let db = fixture();
    let rules = read_one(
        "[[(reachable ?a ?b) [?a :person/friend ?b]]
          [(reachable ?a ?b) [?a :person/friend ?x] (reachable ?x ?b)]]",
    )
    .expect("rules");
    let result = q_str(
        "[:find ?name
          :in $ %
          :where [?a :person/name \"alice\"] (reachable ?a ?b) [?b :person/name ?name]]",
        &[QInput::Db(&db), QInput::Edn(rules)],
    )
    .expect("rules query");
    let mut names: Vec<String> = rows(&result).iter().map(std::string::ToString::to_string).collect();
    names.sort();
    assert_eq!(names.len(), 2);
    assert!(names[0].contains("bob"));
    assert!(names[1].contains("carol") || names[0].contains("carol"));
}

#[test]
fn time_views_answer_history_queries() {
    let db = fixture();
    let as_of = db.as_of(1);
    let old_age = q_str(
        "[:find ?age . :where [?e :person/name \"bob\"] [?e :person/age ?age]]",
        &[QInput::Db(&as_of)],
    )
    .expect("as-of");
    assert_eq!(old_age, Edn::Long(25));
    let history = db.history();
    let audit = q_str(
        "[:find ?age ?added
          :where [?e :person/name \"bob\"] [?e :person/age ?age ?tx ?added]]",
        &[QInput::Db(&history)],
    )
    .expect("history");
    assert_eq!(rows(&audit).len(), 3);
    let since = db.since(1);
    let recent = q_str(
        "[:find ?age . :where [?e :person/age ?age]]",
        &[QInput::Db(&since)],
    )
    .expect("since");
    assert_eq!(recent, Edn::Long(26));
}

#[test]
fn pull_standalone_and_in_find() {
    let db = fixture();
    let alice = db
        .lookup(attr_id(NAME), &Value::Str("alice".into()))
        .expect("alice");
    let pulled = pull(
        &db,
        &read_one("[:person/name {:person/pet [:pet/name]} {:person/friend 2}]").expect("pattern"),
        alice,
    )
    .expect("pull");
    let text = pulled.to_string();
    assert!(text.contains("alice"));
    assert!(text.contains("rex"));
    assert!(text.contains("bob"));
    let in_find = q_str(
        "[:find (pull ?e [:person/name :person/age]) .
          :where [?e :person/name \"carol\"]]",
        &[QInput::Db(&db)],
    )
    .expect("pull in find");
    assert!(in_find.to_string().contains("carol"));
}

#[test]
fn pull_wildcard_recurses_components() {
    let db = fixture();
    let alice = db
        .lookup(attr_id(NAME), &Value::Str("alice".into()))
        .expect("alice");
    let pulled = pull(&db, &read_one("[*]").expect("pattern"), alice).expect("pull");
    // The component pet is pulled as a full map, not as {:db/id …} only.
    assert!(pulled.to_string().contains("rex"));
}

#[test]
fn entity_api_navigates_lazily() {
    let db = fixture();
    let alice = db
        .lookup(attr_id(NAME), &Value::Str("alice".into()))
        .expect("alice");
    let entity = Entity::new(&db, alice);
    assert_eq!(entity.get(attr_id(AGE)), vec![Value::Long(30)]);
    assert_eq!(
        entity.get_kw(&Keyword::parse("person/name")),
        vec![Value::Str("alice".into())]
    );
    let friends = entity.refs(attr_id(FRIEND));
    assert_eq!(friends.len(), 1);
    assert_eq!(
        friends[0].get(attr_id(NAME)),
        vec![Value::Str("bob".into())]
    );
    let reverse = friends[0].reverse(attr_id(FRIEND));
    assert_eq!(reverse.len(), 1);
    assert_eq!(reverse[0].id(), alice);
}

#[test]
fn query_cache_reuses_parses() {
    let cache = QueryCache::new();
    let form = read_one("[:find ?e :where [?e :person/name _]]").expect("edn");
    let first = cache.parse(&form).expect("parse");
    let second = cache.parse(&form).expect("parse");
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    assert_eq!(cache.len(), 1);
}

#[test]
fn lookup_ref_in_pattern_entity_position() {
    let db = fixture();
    let result = q_str(
        "[:find ?age . :where [[:person/name \"alice\"] :person/age ?age]]",
        &[QInput::Db(&db)],
    )
    .expect("lookup ref");
    assert_eq!(result, Edn::Long(30));
}

#[test]
fn tx_range_groups_recorded_datoms() {
    let db = fixture();
    let ranged = db.tx_range(2, None);
    assert_eq!(ranged.len(), 1);
    assert_eq!(ranged[0].1.len(), 2);
}
