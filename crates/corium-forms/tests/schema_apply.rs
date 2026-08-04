//! Applying a schema plan: the plan compiles to datoms, the datoms install the
//! desired schema, and re-applying the same file changes nothing
//! (`docs/design/schema-migrations.md`).
//!
//! These tests exercise the whole loop in one process — plan, compile, commit,
//! replan — because that loop is the actual contract. A compiler that emitted
//! plausible datoms which did not derive back to the desired schema would pass
//! any test that only inspected its output.

use corium_core::{
    Cardinality, EntityId, Keyword, KeywordInterner, Partition, Unique, Value, ValueType,
};
use corium_db::{Db, bootstrap};
use corium_forms::apply::{ApplyError, AuditRecord, audit_datoms, compile};
use corium_forms::desired::DesiredSchema;
use corium_forms::planner::{PlanOptions, installed_schema, plan};
use corium_forms::schemaform::schema_from_edn;
use corium_query::edn::read_all;

/// A database and the keyword interner its values are named through.
struct Fixture {
    db: Db,
    interner: KeywordInterner,
    t: u64,
}

impl Fixture {
    fn new(schema_edn: &str) -> Self {
        let forms = read_all(schema_edn).expect("installed schema parses");
        let (schema, idents) = schema_from_edn(&forms).expect("installed schema installs");
        let interner = KeywordInterner::default();
        Self {
            db: Db::new(schema).with_naming(idents, interner.clone()),
            interner,
            t: 0,
        }
    }

    /// Plans `file` against the current value and commits the result.
    ///
    /// Returns the number of datoms the schema transaction carried.
    fn update(&mut self, file: &str) -> usize {
        let desired = DesiredSchema::from_edn(&read_all(file).expect("desired parses"))
            .expect("desired normalizes");
        let installed = installed_schema(&self.db);
        let planned = plan(&desired, &self.db, &PlanOptions::new("people")).expect("plans");
        self.t += 1;
        let tx = EntityId::new(Partition::Tx as u32, self.t);
        let transaction = compile(
            &planned,
            &desired,
            &self.db,
            &installed,
            &mut self.interner,
            tx,
        )
        .expect("the plan compiles");
        if transaction.is_empty() {
            self.t -= 1;
            return 0;
        }
        let mut datoms = transaction.datoms;
        datoms.extend(audit_datoms(
            &planned,
            &AuditRecord {
                requester: "alice".to_owned(),
                tool: "corium-test".to_owned(),
            },
            tx,
        ));
        let count = datoms.len();
        // Re-attach the interner: keywords minted while compiling must be
        // durable before the datoms that name them, exactly as the transactor
        // publishes naming before it appends.
        self.db = self
            .db
            .clone()
            .with_naming(self.db.idents().clone(), self.interner.clone())
            .with_transaction_at(self.t, 1_000 * self.t.cast_signed(), &datoms);
        count
    }

    fn attr(&self, ident: &str) -> EntityId {
        self.db
            .idents()
            .entid(&Keyword::parse(ident.trim_start_matches(':')))
            .unwrap_or_else(|| panic!("{ident} is installed"))
    }
}

const BASE: &str = r"
{:db/ident :person/email :db/valueType :db.type/string}
{:db/ident :person/tags :db/valueType :db.type/keyword}
";

#[test]
fn a_new_attribute_is_installed_and_immediately_usable() {
    let mut fixture = Fixture::new(BASE);
    let before = fixture.db.schema_generation();
    fixture.update(&format!(
        "{BASE}
         {{:db/ident :person/nickname :db/valueType :db.type/string
           :db/doc \"what they go by\"}}"
    ));

    let nickname = fixture.attr(":person/nickname");
    let installed = fixture.db.schema().get(nickname).expect("installed");
    assert_eq!(installed.value_type, ValueType::Str);
    assert_eq!(installed.cardinality, Cardinality::One);
    assert_eq!(fixture.db.schema().doc(nickname), Some("what they go by"));
    // The id is allocated above the durable maximum, never reused.
    assert!(nickname.sequence() > fixture.attr(":person/tags").sequence());
    assert_eq!(
        fixture.db.schema_generation(),
        before + 1,
        "a schema change advances the generation"
    );
}

#[test]
fn applying_the_same_file_twice_changes_nothing() {
    let file = format!(
        "{BASE}
         {{:db/ident :person/nickname :db/valueType :db.type/string}}"
    );
    let mut fixture = Fixture::new(BASE);
    assert!(fixture.update(&file) > 0, "the first apply installs it");
    let basis = fixture.db.basis_t();
    let generation = fixture.db.schema_generation();

    assert_eq!(
        fixture.update(&file),
        0,
        "re-applying an already installed file compiles to nothing"
    );
    assert_eq!(fixture.db.basis_t(), basis);
    assert_eq!(fixture.db.schema_generation(), generation);
}

#[test]
fn every_property_change_round_trips_through_the_log() {
    let mut fixture = Fixture::new(
        r#"
        {:db/ident :person/email :db/valueType :db.type/string :db/doc "old"}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        {:db/ident :person/address :db/valueType :db.type/ref}
        {:db/ident :person/legacy :db/valueType :db.type/long :db/index true}
        "#,
    );
    fixture.update(
        r#"
        {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity
         :db/doc "primary contact"}
        {:db/ident :person/tags :db/valueType :db.type/keyword
         :db/cardinality :db.cardinality/many}
        {:db/ident :person/address :db/valueType :db.type/ref :db/isComponent true}
        {:db/ident :person/legacy :db/valueType :db.type/long :db/noHistory true}
        "#,
    );

    let schema = fixture.db.schema();
    let email = schema.get(fixture.attr(":person/email")).expect("email");
    assert_eq!(email.unique, Some(Unique::Identity));
    assert!(email.indexed, "uniqueness implies AVET coverage");
    assert_eq!(
        fixture.db.schema().doc(fixture.attr(":person/email")),
        Some("primary contact"),
        "documentation is replaced, not duplicated"
    );
    assert_eq!(
        schema
            .get(fixture.attr(":person/tags"))
            .expect("tags")
            .cardinality,
        Cardinality::Many
    );
    assert!(
        schema
            .get(fixture.attr(":person/address"))
            .expect("address")
            .is_component
    );
    let legacy = schema.get(fixture.attr(":person/legacy")).expect("legacy");
    // Index coverage the creation seed granted is turned off by asserting
    // `false`, even though no datom exists to retract.
    assert!(!legacy.indexed);
    assert!(legacy.no_history);

    // A replan against the updated database finds nothing left to do.
    let desired = DesiredSchema::from_edn(
        &read_all(
            r#"
        {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity
         :db/doc "primary contact"}
        {:db/ident :person/tags :db/valueType :db.type/keyword
         :db/cardinality :db.cardinality/many}
        {:db/ident :person/address :db/valueType :db.type/ref :db/isComponent true}
        {:db/ident :person/legacy :db/valueType :db.type/long :db/noHistory true}
        "#,
        )
        .expect("parses"),
    )
    .expect("normalizes");
    let replan = plan(&desired, &fixture.db, &PlanOptions::new("people")).expect("plans");
    assert!(!replan.has_changes(), "{:?}", replan.steps);
}

#[test]
fn granting_index_coverage_indexes_the_facts_already_stored() {
    let mut fixture = Fixture::new(BASE);
    let email = fixture.attr(":person/email");
    let alice = EntityId::new(Partition::User as u32, 1_000);
    // A fact stored while the attribute had no AVET coverage.
    fixture.t += 1;
    fixture.db = fixture.db.with_transaction_at(
        fixture.t,
        1_000,
        &[corium_core::Datom {
            e: alice,
            a: email,
            v: Value::Str("a@example.test".into()),
            tx: EntityId::new(Partition::Tx as u32, fixture.t),
            added: true,
        }],
    );
    assert_eq!(avet_entries(&fixture.db, email), 0);
    // Force the parent value to build its indexes, so the schema transaction
    // below has a fold it could wrongly extend.
    assert_eq!(fixture.db.datoms().len(), 2);

    fixture.update(
        r"
        {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        ",
    );
    // The pre-change fact is covered. Extending the parent's fold under the
    // parent's schema would have indexed only what arrived afterwards, leaving
    // a partial AVET that reads as authoritative.
    assert_eq!(
        avet_entries(&fixture.db, email),
        1,
        "AVET covers the whole population, not just post-change datoms"
    );
    assert_eq!(
        fixture
            .db
            .lookup(email, &Value::Str("a@example.test".into())),
        Some(alice)
    );
}

/// How many AVET entries the value holds for one attribute.
fn avet_entries(db: &Db, a: EntityId) -> usize {
    db.datoms_at(corium_core::IndexOrder::Avet)
        .filter(|datom| datom.a == a)
        .count()
}

#[test]
fn retirement_keeps_the_attribute_readable() {
    let mut fixture = Fixture::new(BASE);
    let email = fixture.attr(":person/email");
    let desired = DesiredSchema::from_edn(
        &read_all("{:db/ident :person/tags :db/valueType :db.type/keyword}").expect("parses"),
    )
    .expect("normalizes");
    let installed = installed_schema(&fixture.db);
    let planned = plan(
        &desired,
        &fixture.db,
        &PlanOptions::new("people").with_prune(true),
    )
    .expect("plans");
    let tx = EntityId::new(Partition::Tx as u32, 1);
    let transaction = compile(
        &planned,
        &desired,
        &fixture.db,
        &installed,
        &mut fixture.interner,
        tx,
    )
    .expect("compiles");
    fixture.db = fixture
        .db
        .with_transaction_at(1, 1_000, &transaction.datoms);

    assert!(fixture.db.schema().is_retired(email));
    assert!(
        fixture.db.schema().get(email).is_some(),
        "the metadata stays readable at every later basis"
    );
    assert_eq!(
        fixture.db.idents().ident(email).map(ToString::to_string),
        Some(":person/email".to_owned()),
        "the ident still resolves"
    );
}

#[test]
fn a_blocked_step_never_compiles() {
    let fixture = Fixture::new(BASE);
    let desired = DesiredSchema::from_edn(
        &read_all(
            r"
        {:db/ident :person/email :db/valueType :db.type/long}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        ",
        )
        .expect("parses"),
    )
    .expect("normalizes");
    let installed = installed_schema(&fixture.db);
    let planned = plan(&desired, &fixture.db, &PlanOptions::new("people")).expect("plans");
    let mut interner = fixture.interner.clone();
    let error = compile(
        &planned,
        &desired,
        &fixture.db,
        &installed,
        &mut interner,
        EntityId::new(Partition::Tx as u32, 1),
    )
    .expect_err("a destructive change is never compiled");
    assert!(matches!(error, ApplyError::NotExecutable { .. }), "{error}");
}

#[test]
fn the_audit_trail_records_digests_and_acknowledgements() {
    let mut fixture = Fixture::new(BASE);
    fixture.update(&format!(
        "{BASE}
         {{:db/ident :person/address :db/valueType :db.type/ref :db/isComponent true}}"
    ));
    let tx = EntityId::new(Partition::Tx as u32, 1);
    let audited: Vec<_> = fixture
        .db
        .datoms()
        .into_iter()
        .filter(|datom| datom.e == tx && bootstrap::is_audit_attribute(datom.a))
        .collect();
    assert!(
        audited
            .iter()
            .any(|datom| datom.a == bootstrap::UPDATE_REQUESTER
                && datom.v == Value::Str("alice".into())),
        "the requester is recorded"
    );
    assert!(
        audited
            .iter()
            .any(|datom| datom.a == bootstrap::UPDATE_PLAN_DIGEST),
        "the plan digest is recorded"
    );
    assert!(
        audited
            .iter()
            .any(|datom| datom.a == bootstrap::UPDATE_CLASS
                && datom.v == Value::Str("additive".into())),
        "the execution class is recorded; audited: {audited:?}"
    );
}

/// A database whose `:protect/pii` class is installed at creation.
const PROTECTED_BASE: &str = r#"
{:db.protect/ident :protect/pii :db.protect/key "file:/etc/corium/pii.key"}
{:db/ident :person/ssn :db/valueType :db.type/string}
{:db/ident :person/tags :db/valueType :db.type/keyword}
"#;

#[test]
fn protecting_an_attribute_appends_to_its_timeline() {
    let mut fixture = Fixture::new(PROTECTED_BASE);
    let ssn = fixture.attr(":person/ssn");
    assert!(fixture.db.schema().protection(ssn).is_empty());

    fixture.update(
        r"
        {:db/ident :person/ssn :db/valueType :db.type/string :db/protection :protect/pii}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        ",
    );

    let class = fixture.attr(":protect/pii");
    let timeline = fixture.db.schema().protection(ssn);
    assert_eq!(timeline.entries(), &[(1, Some(class))]);
    assert!(fixture.db.schema().is_protected(ssn));
    assert_eq!(
        fixture
            .db
            .schema()
            .protection_class(ssn)
            .map(|class| class.key_id.as_str()),
        Some("file:/etc/corium/pii.key"),
        "the class resolves through the class table"
    );

    // Unprotecting appends a closing entry rather than erasing the first one:
    // the values sealed under the class are still sealed, and still have to be
    // retractable by naming the form they were asserted in.
    fixture.update(
        r"
        {:db/ident :person/ssn :db/valueType :db.type/string}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        ",
    );
    let timeline = fixture.db.schema().protection(ssn);
    assert_eq!(timeline.entries(), &[(1, Some(class)), (2, None)]);
    assert!(!fixture.db.schema().is_protected(ssn));
    assert!(
        timeline.ever_protected(),
        "the attribute stays permanently ineligible for index and unique coverage"
    );
    assert_eq!(timeline.at(1), Some(class), "history keeps the old form");
}

#[test]
fn a_protected_attribute_is_never_planned_into_avet() {
    let mut fixture = Fixture::new(PROTECTED_BASE);
    fixture.update(
        r"
        {:db/ident :person/ssn :db/valueType :db.type/string :db/protection :protect/pii}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        ",
    );
    // Having been protected, the attribute can never gain value-ordered
    // coverage, even after it is unprotected again.
    let desired = DesiredSchema::from_edn(
        &read_all(
            r"
        {:db/ident :person/ssn :db/valueType :db.type/string :db/index true}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        ",
        )
        .expect("parses"),
    )
    .expect("normalizes");
    let planned = plan(&desired, &fixture.db, &PlanOptions::new("people")).expect("plans");
    let blocked: Vec<_> = planned
        .blocked_steps()
        .map(|step| step.key.as_str())
        .collect();
    assert!(
        blocked.contains(&":person/ssn#index"),
        "granting AVET to an ever-protected attribute is blocked; blocked: {blocked:?}"
    );

    let installed = installed_schema(&fixture.db);
    let mut interner = fixture.interner.clone();
    compile(
        &planned,
        &desired,
        &fixture.db,
        &installed,
        &mut interner,
        EntityId::new(Partition::Tx as u32, 9),
    )
    .expect_err("a blocked step never compiles");
}

#[test]
fn naming_a_class_the_database_does_not_have_is_refused() {
    let fixture = Fixture::new(BASE);
    let desired = DesiredSchema::from_edn(
        &read_all(
            r"
        {:db/ident :person/email :db/valueType :db.type/string :db/protection :protect/ghost}
        {:db/ident :person/tags :db/valueType :db.type/keyword}
        ",
        )
        .expect("parses"),
    )
    .expect("normalizes");
    let installed = installed_schema(&fixture.db);
    let planned = plan(&desired, &fixture.db, &PlanOptions::new("people")).expect("plans");
    let mut interner = fixture.interner.clone();
    let error = compile(
        &planned,
        &desired,
        &fixture.db,
        &installed,
        &mut interner,
        EntityId::new(Partition::Tx as u32, 1),
    )
    .expect_err("an unknown class cannot be installed");
    assert!(
        matches!(error, ApplyError::UnknownProtectionClass { .. }),
        "{error}"
    );
}

#[test]
fn the_normalized_schema_round_trips_through_edn() {
    let source = r#"
        {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity
         :db/doc "primary contact"}
        {:db/ident :person/tags :db/valueType :db.type/keyword
         :db/cardinality :db.cardinality/many :db/noHistory true}
        {:db/ident :person/address :db/valueType :db.type/ref :db/isComponent true}
        {:db/ident :person/ssn :db/valueType :db.type/string :db/protection :protect/pii}
    "#;
    let desired = DesiredSchema::from_edn(&read_all(source).expect("parses")).expect("normalizes");
    let round_tripped =
        DesiredSchema::from_edn(&desired.to_edn()).expect("the rendered form normalizes");
    // Equal values, so the transactor computes the digest the client printed.
    assert_eq!(desired, round_tripped);
    assert_eq!(desired.digest(), round_tripped.digest());
}
