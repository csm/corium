//! Golden diff, digest, and classification tests for the schema-update
//! planner (`docs/design/schema-migrations.md`).

use corium_core::migration::{
    AckCode, BlockedReason, ChangeKind, ExecutionClass, InstalledAttribute, InstalledSchema,
    Observation, ObservationValue, PlanStep, Risk, SchemaPlan, SchemaProperty,
};
use corium_core::{Datom, EntityId, KeywordInterner, Partition, Value};
use corium_db::{Db, Idents};
use corium_forms::desired::DesiredSchema;
use corium_forms::planner::{PlanError, PlanOptions, plan, plan_against};
use corium_forms::schemaform::schema_from_edn;
use corium_query::edn::read_all;

/// Builds a database whose installed schema is the given EDN, with no data.
fn database(schema_edn: &str) -> Db {
    let forms = read_all(schema_edn).expect("installed schema parses");
    let (schema, idents) = schema_from_edn(&forms).expect("installed schema installs");
    Db::new(schema).with_naming(idents, KeywordInterner::default())
}

fn desired(schema_edn: &str) -> DesiredSchema {
    DesiredSchema::from_edn(&read_all(schema_edn).expect("desired schema parses"))
        .expect("desired schema normalizes")
}

fn options() -> PlanOptions {
    PlanOptions::new("people")
}

fn attr(db: &Db, ident: &str) -> EntityId {
    db.idents()
        .entid(&corium_core::Keyword::parse(ident.trim_start_matches(':')))
        .unwrap_or_else(|| panic!("{ident} is installed"))
}

fn datom(e: u64, a: EntityId, v: Value, t: u64, added: bool) -> Datom {
    Datom {
        e: EntityId::new(Partition::User as u32, e),
        a,
        v,
        tx: EntityId::new(Partition::Tx as u32, t),
        added,
    }
}

fn step<'a>(plan: &'a SchemaPlan, key: &str) -> &'a PlanStep {
    plan.steps
        .iter()
        .find(|step| step.key == key)
        .unwrap_or_else(|| {
            panic!(
                "{key} is planned; planned steps: {:?}",
                plan.steps.iter().map(|step| &step.key).collect::<Vec<_>>()
            )
        })
}

fn observation(step: &PlanStep, label: &str) -> ObservationValue {
    step.observations
        .iter()
        .find(|observation| observation.label == label)
        .unwrap_or_else(|| panic!("{label} is observed for {}", step.key))
        .value
        .clone()
}

fn count(step: &PlanStep, label: &str) -> u64 {
    match observation(step, label) {
        ObservationValue::Count(count) => count,
        other => panic!("{label} is an exact count, got {other:?}"),
    }
}

const BASE: &str = r"
{:db/ident :person/email :db/valueType :db.type/string}
{:db/ident :person/tags :db/valueType :db.type/keyword}
{:db/ident :person/address :db/valueType :db.type/ref}
{:db/ident :person/age :db/valueType :db.type/long}
";

#[test]
fn an_unchanged_schema_plans_nothing() {
    let db = database(BASE);
    let plan = plan(&desired(BASE), &db, &options()).expect("plans");
    assert!(!plan.has_changes());
    assert!(plan.unmanaged.is_empty());
    assert_eq!(plan.basis_t, db.basis_t());
    assert_eq!(plan.schema_generation, None);
}

#[test]
fn a_new_attribute_is_additive() {
    let db = database(BASE);
    let plan = plan(
        &desired(&format!(
            "{BASE} {{:db/ident :person/nickname :db/valueType :db.type/string
                       :db/unique :db.unique/identity}}"
        )),
        &db,
        &options(),
    )
    .expect("plans");
    let added = step(&plan, ":person/nickname#add");
    assert_eq!(added.kind, ChangeKind::Add);
    assert_eq!(added.class, ExecutionClass::Additive);
    assert_eq!(added.risk, Risk::Low);
    assert!(added.acks.is_empty());
    assert!(added.blocked.is_none());
    // No fact can exist yet, so an index or constraint costs nothing.
    assert!(added.observations.is_empty());
    assert_eq!(
        added.desired.as_deref(),
        Some("string cardinality-one unique-identity")
    );
    assert_eq!(plan.required_classes().len(), 0);
}

#[test]
fn widening_cardinality_is_additive_and_narrowing_is_not() {
    let db = database(BASE).with_transaction_at(
        1,
        1_000,
        &[datom(
            1,
            attr(&database(BASE), ":person/tags"),
            Value::Keyword(0),
            1,
            true,
        )],
    );
    let widened = plan(
        &desired(
            r"
        {:db/ident :person/email :db/valueType :db.type/string}
        {:db/ident :person/tags :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
        {:db/ident :person/address :db/valueType :db.type/ref}
        {:db/ident :person/age :db/valueType :db.type/long}
        ",
        ),
        &db,
        &options(),
    )
    .expect("plans");
    let step = step(&widened, ":person/tags#cardinality");
    assert_eq!(step.class, ExecutionClass::Additive);
    assert_eq!(step.summary, "cardinality one -> many");
    assert_eq!(count(step, "current-datoms"), 1);
}

#[test]
fn narrowing_cardinality_validates_when_nothing_conflicts_and_blocks_when_it_does() {
    let many = r"
    {:db/ident :person/tags :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
    ";
    let one = r"{:db/ident :person/tags :db/valueType :db.type/keyword}";
    let empty = database(many);
    let tags = attr(&empty, ":person/tags");

    let clean = empty.clone().with_transaction_at(
        1,
        1_000,
        &[
            datom(1, tags, Value::Keyword(1), 1, true),
            datom(2, tags, Value::Keyword(2), 1, true),
        ],
    );
    let planned = plan(&desired(one), &clean, &options()).expect("plans");
    let narrowed = step(&planned, ":person/tags#cardinality");
    assert_eq!(narrowed.class, ExecutionClass::ValidateReindex);
    assert_eq!(narrowed.risk, Risk::Medium);
    assert!(narrowed.blocked.is_none());
    assert_eq!(count(narrowed, "conflicting-entities"), 0);

    let conflicting = empty.with_transaction_at(
        1,
        1_000,
        &[
            datom(1, tags, Value::Keyword(1), 1, true),
            datom(1, tags, Value::Keyword(2), 1, true),
            datom(2, tags, Value::Keyword(2), 1, true),
        ],
    );
    let planned = plan(&desired(one), &conflicting, &options()).expect("plans");
    let narrowed = step(&planned, ":person/tags#cardinality");
    assert_eq!(narrowed.class, ExecutionClass::Rewrite);
    assert_eq!(narrowed.risk, Risk::High);
    assert_eq!(narrowed.blocked, Some(BlockedReason::CardinalityConflicts));
    assert_eq!(count(narrowed, "conflicting-entities"), 1);
    assert_eq!(count(narrowed, "conflicting-datoms"), 2);
    assert!(matches!(
        observation(narrowed, "conflicting-entity-sample"),
        ObservationValue::Entities(sample) if sample.len() == 1
    ));
}

#[test]
fn adding_uniqueness_reports_duplicates_and_orders_the_backfill_first() {
    let plain = r"{:db/ident :person/email :db/valueType :db.type/string}";
    let unique = r"
    {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity}
    ";
    let empty = database(plain);
    let email = attr(&empty, ":person/email");

    let clean = empty.clone().with_transaction_at(
        1,
        1_000,
        &[
            datom(1, email, Value::Str("a@example.com".into()), 1, true),
            datom(2, email, Value::Str("b@example.com".into()), 1, true),
        ],
    );
    let planned = plan(&desired(unique), &clean, &options()).expect("plans");
    let constraint = step(&planned, ":person/email#unique");
    let index = step(&planned, ":person/email#index");
    assert_eq!(constraint.class, ExecutionClass::ValidateReindex);
    assert!(constraint.blocked.is_none());
    assert_eq!(count(constraint, "duplicate-values"), 0);
    // Uniqueness may not activate before its covering index is ready.
    assert_eq!(constraint.depends_on, vec![index.key.clone()]);
    assert_eq!(count(index, "avet-backfill-datoms"), 2);

    let duplicated = empty.with_transaction_at(
        1,
        1_000,
        &[
            datom(1, email, Value::Str("same@example.com".into()), 1, true),
            datom(2, email, Value::Str("same@example.com".into()), 1, true),
        ],
    );
    let planned = plan(&desired(unique), &duplicated, &options()).expect("plans");
    let constraint = step(&planned, ":person/email#unique");
    assert_eq!(constraint.blocked, Some(BlockedReason::UniqueDuplicates));
    assert_eq!(constraint.risk, Risk::High);
    assert_eq!(count(constraint, "duplicate-values"), 1);
    assert_eq!(count(constraint, "duplicate-entities"), 2);
}

#[test]
fn changing_the_uniqueness_mode_needs_an_acknowledgement() {
    let identity = r"
    {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity}
    ";
    let value = r"
    {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/value}
    ";
    let db = database(identity);
    let planned = plan(&desired(value), &db, &options()).expect("plans");
    let changed = step(&planned, ":person/email#unique");
    assert_eq!(changed.class, ExecutionClass::ValidateReindex);
    assert_eq!(changed.risk, Risk::High);
    assert_eq!(changed.acks, vec![AckCode::UniqueModeChange]);
    assert_eq!(changed.summary, "unique identity -> value");
    // Coverage does not change, so no index step is planned.
    assert_eq!(planned.steps.len(), 1);
}

#[test]
fn dropping_uniqueness_rebuilds_only_when_coverage_is_no_longer_requested() {
    let unique = r"
    {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity}
    ";
    let db = database(unique);
    let dropped = plan(
        &desired(r"{:db/ident :person/email :db/valueType :db.type/string}"),
        &db,
        &options(),
    )
    .expect("plans");
    assert!(
        step(&dropped, ":person/email#unique")
            .notes
            .iter()
            .any(|note| note.contains("no longer requested"))
    );
    assert_eq!(
        step(&dropped, ":person/email#index").desired.as_deref(),
        Some("false")
    );

    let kept = plan(
        &desired(r"{:db/ident :person/email :db/valueType :db.type/string :db/index true}"),
        &db,
        &options(),
    )
    .expect("plans");
    assert!(
        step(&kept, ":person/email#unique")
            .notes
            .iter()
            .any(|note| note.contains("still requested"))
    );
    // Coverage is unchanged, so only the constraint changes.
    assert_eq!(kept.steps.len(), 1);
}

#[test]
fn component_and_no_history_toggles_carry_their_stable_codes() {
    let db = database(BASE);
    let address = attr(&db, ":person/address");
    let db = db.with_transaction_at(
        1,
        1_000,
        &[datom(
            1,
            address,
            Value::Ref(EntityId::new(Partition::User as u32, 2)),
            1,
            true,
        )],
    );
    let planned = plan(
        &desired(
            r"
        {:db/ident :person/email :db/valueType :db.type/string}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        {:db/ident :person/address :db/valueType :db.type/ref :db/isComponent true}
        {:db/ident :person/age :db/valueType :db.type/long :db/noHistory true}
        ",
        ),
        &db,
        &options(),
    )
    .expect("plans");

    let component = step(&planned, ":person/address#component");
    assert_eq!(component.class, ExecutionClass::ValidateReindex);
    assert_eq!(component.risk, Risk::High);
    assert_eq!(component.acks, vec![AckCode::ComponentEnable]);
    assert_eq!(count(component, "live-refs"), 1);

    let no_history = step(&planned, ":person/age#no-history");
    assert_eq!(no_history.class, ExecutionClass::Additive);
    assert_eq!(no_history.acks, vec![AckCode::NoHistoryEnable]);
    assert!(
        no_history
            .notes
            .iter()
            .any(|note| note.contains("forward-only"))
    );

    assert_eq!(
        planned.required_acks().into_iter().collect::<Vec<_>>(),
        vec![AckCode::ComponentEnable, AckCode::NoHistoryEnable]
    );
}

#[test]
fn disabling_no_history_states_the_interval_it_cannot_reconstruct() {
    let db = database(r"{:db/ident :person/age :db/valueType :db.type/long :db/noHistory true}")
        .with_transaction_at(7, 7_000, &[]);
    let planned = plan(
        &desired(r"{:db/ident :person/age :db/valueType :db.type/long}"),
        &db,
        &options(),
    )
    .expect("plans");
    let step = step(&planned, ":person/age#no-history");
    assert_eq!(step.acks, vec![AckCode::NoHistoryDisable]);
    assert!(
        step.notes.iter().any(|note| note.contains("basis 7")),
        "{step:?}"
    );
    // `:db/noHistory` means the exact historical count cannot be produced.
    assert!(matches!(
        observation(step, "recorded-history-datoms"),
        ObservationValue::Unavailable(_)
    ));
}

#[test]
fn a_value_type_change_is_destructive_and_blocks_the_rest_of_its_attribute() {
    let db = database(BASE);
    let age = attr(&db, ":person/age");
    let db = db.with_transaction_at(1, 1_000, &[datom(1, age, Value::Long(41), 1, true)]);
    let planned = plan(
        &desired(
            r"
        {:db/ident :person/email :db/valueType :db.type/string}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        {:db/ident :person/address :db/valueType :db.type/ref}
        {:db/ident :person/age :db/valueType :db.type/string :db/index true}
        ",
        ),
        &db,
        &options(),
    )
    .expect("plans");

    let mutation = step(&planned, ":person/age#value-type");
    assert_eq!(mutation.class, ExecutionClass::Destructive);
    assert_eq!(mutation.blocked, Some(BlockedReason::ValueTypeMutation));
    assert!(!mutation.class.executable());
    assert_eq!(mutation.summary, "long -> string");
    assert_eq!(count(mutation, "current-datoms"), 1);
    assert!(
        mutation
            .recipe
            .iter()
            .any(|line| line.contains(":person/age-string"))
    );
    // The other difference on the same attribute is still classified, and is
    // explicitly downstream of the change that can never run.
    let index = step(&planned, ":person/age#index");
    assert_eq!(index.depends_on, vec![mutation.key.clone()]);
    assert_eq!(planned.blocked_steps().count(), 1);
}

#[test]
fn absent_attributes_are_unmanaged_until_prune_retires_them() {
    let db = database(BASE);
    let email = attr(&db, ":person/email");
    let db = db.with_transaction_at(
        1,
        1_000,
        &[datom(1, email, Value::Str("a@example.com".into()), 1, true)],
    );
    let partial = desired(r"{:db/ident :person/tags :db/valueType :db.type/keyword}");

    let reported = plan(&partial, &db, &options()).expect("plans");
    assert!(!reported.has_changes());
    assert_eq!(
        reported.unmanaged,
        vec![
            ":person/address".to_owned(),
            ":person/age".to_owned(),
            ":person/email".to_owned()
        ]
    );

    let pruned = plan(&partial, &db, &options().with_prune(true)).expect("plans");
    assert!(pruned.unmanaged.is_empty());
    let retire_live = step(&pruned, ":person/email#retire");
    assert_eq!(retire_live.class, ExecutionClass::ValidateReindex);
    assert_eq!(retire_live.risk, Risk::High);
    assert_eq!(retire_live.acks, vec![AckCode::RetireLiveAttribute]);
    assert_eq!(count(retire_live, "current-datoms"), 1);
    assert!(retire_live.blocked.is_none());
    // An attribute with no live fact still retires, but acknowledges nothing.
    assert!(step(&pruned, ":person/age#retire").acks.is_empty());
    assert_eq!(pruned.steps.len(), 3);
}

#[test]
fn engine_attributes_are_never_managed_by_a_file() {
    let db = database(BASE);
    // `:db/txInstant` is installed in every database but belongs to the engine.
    assert!(
        !plan(&desired(BASE), &db, &options().with_prune(true))
            .expect("plans")
            .steps
            .iter()
            .any(|step| step.ident == ":db/txInstant")
    );
    let error = plan(
        &desired(r"{:db/ident :db/txInstant :db/valueType :db.type/string}"),
        &db,
        &options(),
    )
    .expect_err("engine attributes are refused");
    assert_eq!(
        error,
        PlanError::EngineAttribute(":db/txInstant".to_owned())
    );
}

#[test]
fn one_attribute_can_need_several_independent_changes() {
    let db = database(r"{:db/ident :person/email :db/valueType :db.type/string}");
    let planned = plan(
        &desired(
            r"
        {:db/ident :person/email :db/valueType :db.type/string
         :db/cardinality :db.cardinality/many :db/unique :db.unique/value
         :db/isComponent true :db/noHistory true}
        ",
        ),
        &db,
        &options(),
    )
    .expect("plans");
    let keys: Vec<&str> = planned.steps.iter().map(|step| step.key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            ":person/email#cardinality",
            ":person/email#component",
            ":person/email#index",
            ":person/email#no-history",
            ":person/email#unique",
        ]
    );
    // Deterministic ordering: keys sort, so two runs agree.
    let again = plan(
        &desired(
            r"
        {:db/ident :person/email :db/valueType :db.type/string
         :db/cardinality :db.cardinality/many :db/unique :db.unique/value
         :db/isComponent true :db/noHistory true}
        ",
        ),
        &db,
        &options(),
    )
    .expect("plans");
    assert_eq!(planned, again);
    assert_eq!(planned.digest(), again.digest());
}

#[test]
fn data_drift_leaves_an_additive_plan_valid_but_schema_drift_does_not() {
    let db = database(BASE);
    let email = attr(&db, ":person/email");
    let addition = desired(&format!(
        "{BASE} {{:db/ident :person/nickname :db/valueType :db.type/string}}"
    ));

    let before = plan(&addition, &db, &options()).expect("plans");
    let busier = db.with_transaction_at(
        1,
        1_000,
        &[datom(1, email, Value::Str("a@example.com".into()), 1, true)],
    );
    let after = plan(&addition, &busier, &options()).expect("plans");
    assert_ne!(before.basis_t, after.basis_t);
    assert_eq!(before.digest(), after.digest());
    assert_eq!(before.installed_fingerprint, after.installed_fingerprint);

    // A schema change moves both the fingerprint and the digest.
    let widened = database(&format!(
        "{BASE} {{:db/ident :person/other :db/valueType :db.type/string}}"
    ));
    let drifted = plan(&addition, &widened, &options()).expect("plans");
    assert_ne!(before.installed_fingerprint, drifted.installed_fingerprint);
    assert_ne!(before.digest(), drifted.digest());
}

#[test]
fn prune_mode_is_part_of_the_plan_digest() {
    let db = database(BASE);
    let partial = desired(r"{:db/ident :person/tags :db/valueType :db.type/keyword}");
    let reported = plan(&partial, &db, &options()).expect("plans");
    let pruned = plan(&partial, &db, &options().with_prune(true)).expect("plans");
    assert_ne!(reported.digest(), pruned.digest());
}

#[test]
fn a_snapshot_backed_value_says_its_history_counts_are_a_floor() {
    let complete = database(BASE);
    let email = attr(&complete, ":person/email");
    let complete = complete.with_transaction_at(
        1,
        1_000,
        &[datom(1, email, Value::Str("a@example.com".into()), 1, true)],
    );
    let forms = read_all(BASE).expect("schema parses");
    let (schema, idents) = schema_from_edn(&forms).expect("schema installs");
    let snapshot = Db::from_current_snapshot(
        complete.basis_t(),
        schema,
        idents,
        KeywordInterner::default(),
        complete.datoms(),
    );
    let partial = desired(r"{:db/ident :person/tags :db/valueType :db.type/keyword}");
    let planned = plan(&partial, &snapshot, &options().with_prune(true)).expect("plans");
    assert!(
        planned
            .notes
            .iter()
            .any(|note| note.contains("floor rather than an exact answer")),
        "{:?}",
        planned.notes
    );
    assert!(matches!(
        observation(
            step(&planned, ":person/email#retire"),
            "recorded-history-datoms"
        ),
        ObservationValue::Unavailable(_)
    ));
}

#[test]
fn documentation_is_reported_as_untracked_rather_than_planned() {
    let db = database(BASE);
    let planned = plan(
        &desired(
            r#"
        {:db/ident :person/email :db/valueType :db.type/string :db/doc "primary contact"}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        {:db/ident :person/address :db/valueType :db.type/ref}
        {:db/ident :person/age :db/valueType :db.type/long}
        "#,
        ),
        &db,
        &options(),
    )
    .expect("plans");
    assert!(!planned.has_changes());
    assert!(
        planned
            .notes
            .iter()
            .any(|note| note.contains(":db/doc differences are not planned")),
        "{:?}",
        planned.notes
    );
    // The documentation is still part of the desired digest, so a plan made
    // from a file with docs is not the plan made from one without.
    let without = plan(&desired(BASE), &db, &options()).expect("plans");
    assert_ne!(planned.desired_digest, without.desired_digest);
    assert_ne!(planned.digest(), without.digest());
}

#[test]
fn every_property_difference_is_classified_exactly_once() {
    // One installed attribute, one desired attribute differing in every
    // property the model has.
    let db =
        database(r"{:db/ident :a/b :db/valueType :db.type/long :db/unique :db.unique/identity}");
    let planned = plan(
        &desired(
            r"
        {:db/ident :a/b :db/valueType :db.type/ref :db/cardinality :db.cardinality/many
         :db/isComponent true :db/noHistory true}
        ",
        ),
        &db,
        &options(),
    )
    .expect("plans");
    let mut kinds: Vec<ChangeKind> = planned.steps.iter().map(|step| step.kind).collect();
    kinds.sort();
    assert_eq!(
        kinds,
        vec![
            ChangeKind::Property(SchemaProperty::ValueType),
            ChangeKind::Property(SchemaProperty::Cardinality),
            ChangeKind::Property(SchemaProperty::Unique),
            ChangeKind::Property(SchemaProperty::Index),
            ChangeKind::Property(SchemaProperty::Component),
            ChangeKind::Property(SchemaProperty::NoHistory),
        ]
    );
    // Every step key is unique, so no difference is planned twice.
    let mut keys: Vec<&String> = planned.steps.iter().map(|step| &step.key).collect();
    keys.sort();
    let unique_keys = keys.len();
    keys.dedup();
    assert_eq!(keys.len(), unique_keys);
}

#[test]
fn observations_are_advisory_and_never_reach_the_digest() {
    let db = database(BASE);
    let tags = attr(&db, ":person/tags");
    let many = desired(
        r"
    {:db/ident :person/email :db/valueType :db.type/string}
    {:db/ident :person/tags :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
    {:db/ident :person/address :db/valueType :db.type/ref}
    {:db/ident :person/age :db/valueType :db.type/long}
    ",
    );
    let empty = plan(&many, &db, &options()).expect("plans");
    let loaded = plan(
        &many,
        &db.with_transaction_at(
            1,
            1_000,
            &[
                datom(1, tags, Value::Keyword(1), 1, true),
                datom(2, tags, Value::Keyword(2), 1, true),
            ],
        ),
        &options(),
    )
    .expect("plans");
    assert_eq!(
        count(step(&empty, ":person/tags#cardinality"), "current-datoms"),
        0
    );
    assert_eq!(
        count(step(&loaded, ":person/tags#cardinality"), "current-datoms"),
        2
    );
    assert_eq!(empty.digest(), loaded.digest());
}

#[test]
fn plans_render_their_observations_for_review() {
    assert_eq!(
        Observation::count("current-datoms", 12_204)
            .value
            .to_string(),
        "12,204"
    );
    assert_eq!(ObservationValue::Estimate(1_000).to_string(), "~1,000");
    assert_eq!(
        ObservationValue::Entities(vec![EntityId::new(Partition::User as u32, 7)]).to_string(),
        "7"
    );
    assert!(
        ObservationValue::Unavailable("no history".to_owned())
            .to_string()
            .starts_with("unavailable")
    );
}

#[test]
fn idents_match_exactly_and_a_rename_is_never_inferred() {
    let db = database(r"{:db/ident :person/email :db/valueType :db.type/string}");
    let planned = plan(
        &desired(r"{:db/ident :person/e-mail :db/valueType :db.type/string}"),
        &db,
        &options().with_prune(true),
    )
    .expect("plans");
    let keys: Vec<&str> = planned.steps.iter().map(|step| step.key.as_str()).collect();
    assert_eq!(keys, vec![":person/e-mail#add", ":person/email#retire"]);
}

#[test]
fn an_unnamed_installed_attribute_is_never_managed() {
    // An attribute with no ident cannot be named by a file, so it is neither
    // planned nor reported as unmanaged — but it is still fingerprinted.
    let forms = read_all(BASE).expect("schema parses");
    let (mut schema, idents) = schema_from_edn(&forms).expect("schema installs");
    schema.insert(corium_db::attribute(
        900,
        corium_core::ValueType::Str,
        corium_core::Cardinality::One,
        None,
    ));
    let db = Db::new(schema).with_naming(idents, KeywordInterner::default());
    let planned = plan(&desired(BASE), &db, &options().with_prune(true)).expect("plans");
    assert!(!planned.has_changes());
    assert!(planned.unmanaged.is_empty());
    assert_ne!(
        planned.installed_fingerprint,
        plan(&desired(BASE), &database(BASE), &options())
            .expect("plans")
            .installed_fingerprint
    );
}

#[test]
fn a_documented_installed_schema_plans_documentation_changes() {
    // Documentation is not recorded by a `Db` today. A caller that can read it
    // supplies the installed state instead, and the same planner diffs it.
    let db = database(r"{:db/ident :person/email :db/valueType :db.type/string}");
    let installed = corium_forms::planner::installed_schema(&db);
    let mut documented: Vec<InstalledAttribute> = installed.iter().cloned().collect();
    for attribute in &mut documented {
        if attribute.ident == ":person/email" {
            attribute.doc = Some("old text".to_owned());
        }
    }
    let installed = InstalledSchema::new(documented)
        .with_doc_tracking(true)
        .with_generation(Some(3));

    let planned = plan_against(
        &desired(r#"{:db/ident :person/email :db/valueType :db.type/string :db/doc "new text"}"#),
        &db,
        &installed,
        &options(),
    )
    .expect("plans");
    assert_eq!(planned.schema_generation, Some(3));
    let doc = step(&planned, ":person/email#doc");
    assert_eq!(doc.class, ExecutionClass::Additive);
    assert_eq!(doc.risk, Risk::Low);
    assert_eq!(doc.installed.as_deref(), Some("old text"));
    assert_eq!(doc.desired.as_deref(), Some("new text"));
    assert_eq!(doc.summary, "doc replaced");
    assert!(doc.acks.is_empty());
    assert!(
        !planned
            .notes
            .iter()
            .any(|note| note.contains(":db/doc differences are not planned"))
    );

    // Retracting documentation and adding it are the same metadata-only class.
    let retracted = plan_against(
        &desired(r"{:db/ident :person/email :db/valueType :db.type/string}"),
        &db,
        &installed,
        &options(),
    )
    .expect("plans");
    assert_eq!(
        step(&retracted, ":person/email#doc").summary,
        "doc retracted"
    );
}

#[test]
fn an_ever_protected_attribute_can_never_gain_index_or_unique_coverage() {
    let db = database(r"{:db/ident :person/email :db/valueType :db.type/string}");
    let mut attrs: Vec<InstalledAttribute> = corium_forms::planner::installed_schema(&db)
        .iter()
        .cloned()
        .collect();
    for attribute in &mut attrs {
        if attribute.ident == ":person/email" {
            attribute.ever_protected = true;
        }
    }
    let installed = InstalledSchema::new(attrs);

    let indexed = plan_against(
        &desired(r"{:db/ident :person/email :db/valueType :db.type/string :db/index true}"),
        &db,
        &installed,
        &options(),
    )
    .expect("plans");
    assert_eq!(
        step(&indexed, ":person/email#index").blocked,
        Some(BlockedReason::EverProtected)
    );
    assert_eq!(indexed.blocked_steps().count(), 1);

    let unique = plan_against(
        &desired(
            r"{:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/value}",
        ),
        &db,
        &installed,
        &options(),
    )
    .expect("plans");
    assert_eq!(
        step(&unique, ":person/email#unique").blocked,
        Some(BlockedReason::EverProtected)
    );
    assert_eq!(step(&unique, ":person/email#unique").risk, Risk::High);
    // The prohibition is permanent, so no allowance or acknowledgement can
    // enable either step.
    assert_eq!(unique.blocked_steps().count(), 2);
}

#[test]
fn idents_registry_is_read_for_attribute_ids() {
    // Attribute ids come from the installed database, never from file order.
    let db = database(BASE);
    let installed = corium_forms::planner::installed_schema(&db);
    let email = installed.get(":person/email").expect("installed");
    assert_eq!(email.id, attr(&db, ":person/email"));
    assert!(!email.engine);
    assert!(
        installed
            .get(":db/txInstant")
            .expect("engine attribute is installed")
            .engine
    );
    let _ = Idents::default();
}
