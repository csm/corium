//! Transaction model and validation tests.

use corium_core::{Cardinality, EntityId, Partition, Schema, Unique, Value, ValueType};
use corium_db::{Db, attribute, bootstrap};
use corium_tx::{EntityRef, TX_TEMPID, TxError, TxItem, TxOp, prepare};
use std::sync::Arc;

fn fixture() -> (Db, EntityId, EntityId) {
    let name = EntityId::new(Partition::Db as u32, 100);
    let email = EntityId::new(Partition::Db as u32, 101);
    let mut schema = Schema::default();
    schema.insert(attribute(100, ValueType::Str, Cardinality::One, None));
    schema.insert(attribute(
        101,
        ValueType::Str,
        Cardinality::One,
        Some(Unique::Identity),
    ));
    (Db::new(schema), name, email)
}

#[test]
fn tempid_upsert_cardinality_and_cas_follow_model() {
    let (empty, name, email) = fixture();
    let tx1 = EntityId::new(Partition::Tx as u32, 1);
    let first = prepare(
        &empty,
        [
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("alice".into()),
                email,
                Value::Str(Arc::from("a@example.test")),
            )),
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("alice".into()),
                name,
                Value::Str(Arc::from("Alice")),
            )),
        ],
        tx1,
        1_000,
    )
    .expect("prepare first transaction");
    let alice = first.tempids["alice"];
    let db = empty.with_transaction(1, &first.datoms);
    let second = prepare(
        &db,
        [
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("same-person".into()),
                email,
                Value::Str(Arc::from("a@example.test")),
            )),
            TxItem::Op(TxOp::Cas(
                EntityRef::Temp("same-person".into()),
                name,
                Some(Value::Str(Arc::from("Alice"))),
                Value::Str(Arc::from("Alicia")),
            )),
        ],
        EntityId::new(Partition::Tx as u32, 2),
        1_001,
    )
    .expect("upsert and cas");
    assert_eq!(second.tempids["same-person"], alice);
    let db = db.with_transaction(2, &second.datoms);
    assert_eq!(
        db.values(alice, name),
        vec![Value::Str(Arc::from("Alicia"))]
    );
    assert_eq!(db.stats().datoms, 2);
}

#[test]
fn rejects_wrong_types_and_unique_conflicts() {
    let (empty, _name, email) = fixture();
    let tx = EntityId::new(Partition::Tx as u32, 1);
    assert_eq!(
        prepare(
            &empty,
            [TxItem::Op(TxOp::Add(
                EntityRef::Temp("x".into()),
                email,
                Value::Long(1)
            ))],
            tx,
            1_000
        ),
        Err(TxError::TypeMismatch(email))
    );
    let result = prepare(
        &empty,
        [
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("x".into()),
                email,
                Value::Str(Arc::from("same")),
            )),
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("y".into()),
                email,
                Value::Str(Arc::from("same")),
            )),
        ],
        tx,
        1_000,
    );
    assert_eq!(result, Err(TxError::UniqueConflict));
}

#[test]
fn reasserting_present_and_retracting_absent_facts_are_no_ops() {
    let (empty, name, _email) = fixture();
    let e = EntityId::new(Partition::User as u32, 1_000);
    let first = prepare(
        &empty,
        [TxItem::Op(TxOp::Add(
            EntityRef::Id(e),
            name,
            Value::Str(Arc::from("Alice")),
        ))],
        EntityId::new(Partition::Tx as u32, 1),
        1_000,
    )
    .expect("prepare first transaction");
    let db = empty.with_transaction(1, &first.datoms);
    let second = prepare(
        &db,
        [
            TxItem::Op(TxOp::Add(
                EntityRef::Id(e),
                name,
                Value::Str(Arc::from("Alice")),
            )),
            TxItem::Op(TxOp::Retract(
                EntityRef::Id(e),
                name,
                Value::Str(Arc::from("Bob")),
            )),
        ],
        EntityId::new(Partition::Tx as u32, 2),
        1_001,
    )
    .expect("no-op transaction");
    assert_eq!(second.datoms, vec![]);
}

#[test]
fn retract_entity_removes_components_and_incoming_refs() {
    let name = EntityId::new(Partition::Db as u32, 100);
    let child = EntityId::new(Partition::Db as u32, 102);
    let friend = EntityId::new(Partition::Db as u32, 103);
    let mut schema = Schema::default();
    schema.insert(attribute(100, ValueType::Str, Cardinality::One, None));
    schema.insert(corium_core::Attribute {
        is_component: true,
        ..attribute(102, ValueType::Ref, Cardinality::One, None)
    });
    schema.insert(attribute(103, ValueType::Ref, Cardinality::One, None));
    let empty = Db::new(schema);
    let tx1 = EntityId::new(Partition::Tx as u32, 1);
    let first = prepare(
        &empty,
        [
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("parent".into()),
                name,
                Value::Str(Arc::from("parent")),
            )),
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("kid".into()),
                name,
                Value::Str(Arc::from("kid")),
            )),
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("other".into()),
                name,
                Value::Str(Arc::from("other")),
            )),
        ],
        tx1,
        1_000,
    )
    .expect("create entities");
    let parent = first.tempids["parent"];
    let kid = first.tempids["kid"];
    let other = first.tempids["other"];
    let db = empty.with_transaction(1, &first.datoms);
    let second = prepare(
        &db,
        [
            TxItem::Op(TxOp::Add(EntityRef::Id(parent), child, Value::Ref(kid))),
            TxItem::Op(TxOp::Add(EntityRef::Id(other), friend, Value::Ref(kid))),
        ],
        EntityId::new(Partition::Tx as u32, 2),
        1_003,
    )
    .expect("link entities");
    let db = db.with_transaction(2, &second.datoms);
    let third = prepare(
        &db,
        [TxItem::Op(TxOp::RetractEntity(EntityRef::Id(parent)))],
        EntityId::new(Partition::Tx as u32, 3),
        1_003,
    )
    .expect("retract parent");
    let db = db.with_transaction(3, &third.datoms);
    // The component child and every reference to it are gone; only the
    // unrelated entity's own fact survives.
    let remaining = db.datoms();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].e, other);
    assert_eq!(remaining[0].a, name);
    assert!(db.values(kid, name).is_empty());
    assert!(db.values(other, friend).is_empty());
}

#[test]
fn the_reserved_tempid_names_the_transaction_entity() {
    // Transaction metadata is asserted against the transaction being built,
    // whose entity id the caller cannot know: the reserved tempid stands in
    // for it and resolves without allocating a user entity.
    let (empty, name, _) = fixture();
    let tx = EntityId::new(Partition::Tx as u32, 1);
    let prepared = prepare(
        &empty,
        [
            TxItem::Op(TxOp::Add(
                EntityRef::Temp("alice".into()),
                name,
                Value::Str(Arc::from("Alice")),
            )),
            TxItem::Op(TxOp::Add(
                EntityRef::Temp(TX_TEMPID.into()),
                name,
                Value::Str(Arc::from("nightly import")),
            )),
        ],
        tx,
        1_000,
    )
    .expect("prepare with transaction metadata");

    assert_eq!(prepared.tempids[TX_TEMPID], tx);
    let metadata: Vec<_> = prepared
        .datoms
        .iter()
        .filter(|datom| datom.e == tx)
        .collect();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].v, Value::Str(Arc::from("nightly import")));
    // The metadata datom belongs to the transaction it describes.
    assert_eq!(metadata[0].tx, tx);
    // The reserved tempid consumed no user id: `alice` still got the first.
    assert_eq!(prepared.tempids["alice"].sequence(), 1_000);
}

#[test]
fn the_reserved_tempid_does_not_upsert_through_identity_attributes() {
    // `datomic.tx` is not an ordinary tempid: a unique-identity assertion
    // unifies an ordinary tempid with the entity already holding that value,
    // but transaction metadata must stay on the transaction entity — so the
    // same assertion resolves to the transaction, and a value another entity
    // already owns is the ordinary uniqueness conflict rather than a silent
    // retarget.
    let (empty, _, email) = fixture();
    let first = prepare(
        &empty,
        [TxItem::Op(TxOp::Add(
            EntityRef::Temp("alice".into()),
            email,
            Value::Str(Arc::from("a@example.test")),
        ))],
        EntityId::new(Partition::Tx as u32, 1),
        1_000,
    )
    .expect("prepare first transaction");
    let alice = first.tempids["alice"];
    let db = empty.with_transaction(1, &first.datoms);

    let tx = EntityId::new(Partition::Tx as u32, 2);
    let fresh = prepare(
        &db,
        [TxItem::Op(TxOp::Add(
            EntityRef::Temp(TX_TEMPID.into()),
            email,
            Value::Str(Arc::from("importer@example.test")),
        ))],
        tx,
        1_001,
    )
    .expect("prepare transaction metadata");
    assert_eq!(fresh.tempids[TX_TEMPID], tx);
    assert_ne!(fresh.tempids[TX_TEMPID], alice);
    assert!(fresh.datoms.iter().all(|datom| datom.e == tx));

    let taken = prepare(
        &db,
        [TxItem::Op(TxOp::Add(
            EntityRef::Temp(TX_TEMPID.into()),
            email,
            Value::Str(Arc::from("a@example.test")),
        ))],
        tx,
        1_001,
    );
    assert_eq!(taken, Err(TxError::UniqueConflict));
}

#[test]
fn transaction_instant_can_only_be_added_once_to_the_current_transaction() {
    let (empty, _, _) = fixture();
    let tx = EntityId::new(Partition::Tx as u32, 1);
    let user = EntityId::new(Partition::User as u32, 1_000);

    let on_user = prepare(
        &empty,
        [TxItem::Op(TxOp::Add(
            EntityRef::Id(user),
            bootstrap::TX_INSTANT,
            Value::Instant(1_000),
        ))],
        tx,
        1_000,
    );
    assert_eq!(on_user, Err(TxError::InvalidTxInstantOperation));

    let twice = prepare(
        &empty,
        [
            TxItem::Op(TxOp::Add(
                EntityRef::Temp(TX_TEMPID.into()),
                bootstrap::TX_INSTANT,
                Value::Instant(2_000),
            )),
            TxItem::Op(TxOp::Add(
                EntityRef::Temp(TX_TEMPID.into()),
                bootstrap::TX_INSTANT,
                Value::Instant(1_000),
            )),
        ],
        tx,
        1_000,
    );
    assert_eq!(twice, Err(TxError::InvalidTxInstantOperation));
}

#[test]
fn transaction_instants_cannot_be_changed_or_retracted() {
    let (empty, _, _) = fixture();
    let old_tx = EntityId::new(Partition::Tx as u32, 1);
    let db = empty.with_transaction_at(1, 1_000, &[]);
    let tx = EntityId::new(Partition::Tx as u32, 2);

    let retract = prepare(
        &db,
        [TxItem::Op(TxOp::Retract(
            EntityRef::Id(old_tx),
            bootstrap::TX_INSTANT,
            Value::Instant(1_000),
        ))],
        tx,
        1_000,
    );
    assert_eq!(retract, Err(TxError::InvalidTxInstantOperation));

    let cas = prepare(
        &db,
        [TxItem::Op(TxOp::Cas(
            EntityRef::Id(old_tx),
            bootstrap::TX_INSTANT,
            Some(Value::Instant(1_000)),
            Value::Instant(2_000),
        ))],
        tx,
        1_000,
    );
    assert_eq!(cas, Err(TxError::InvalidTxInstantOperation));

    let retract_entity = prepare(
        &db,
        [TxItem::Op(TxOp::RetractEntity(EntityRef::Id(old_tx)))],
        tx,
        1_000,
    );
    assert_eq!(retract_entity, Err(TxError::InvalidTxInstantOperation));
}

/// A database whose `:person/ssn` is protected by one class from `t = 0`.
fn protected_fixture() -> (Db, EntityId, EntityId, EntityId) {
    use corium_core::{
        LegacyPlaintextPolicy, MissingKeyPolicy, ProtectionClass, ProtectionScope,
        ProtectionTimeline, SealAlgorithm,
    };
    let class = EntityId::new(Partition::Db as u32, 100);
    let ssn = EntityId::new(Partition::Db as u32, 101);
    let name = EntityId::new(Partition::Db as u32, 102);
    let mut schema = Schema::default();
    schema.insert(attribute(101, ValueType::Str, Cardinality::One, None));
    schema.insert(attribute(102, ValueType::Str, Cardinality::One, None));
    schema.insert_class(ProtectionClass {
        id: class,
        key_id: "file:/etc/corium/pii.key".into(),
        algorithm: SealAlgorithm::Aes256GcmSiv,
        scope: ProtectionScope::Attribute,
        padding: None,
        on_missing_key: MissingKeyPolicy::Redact,
        legacy_plaintext: LegacyPlaintextPolicy::Redact,
        current_epoch: 2,
    });
    schema.set_protection(ssn, ProtectionTimeline::protected_from(0, class));
    (Db::new(schema), class, ssn, name)
}

fn sealed(class: EntityId, epoch: u32, body: &[u8]) -> Value {
    Value::Sealed(corium_core::Sealed {
        class,
        epoch,
        vtype: ValueType::Str,
        body: Arc::from(body),
    })
}

#[test]
fn a_keyless_transactor_enforces_the_form_the_attribute_has_now() {
    let (db, class, ssn, name) = protected_fixture();
    let tx = EntityId::new(Partition::Tx as u32, 1);
    let alice = EntityRef::Id(EntityId::new(Partition::User as u32, 1_000));
    let assert = |value: Value| {
        prepare(
            &db,
            [TxItem::Op(TxOp::Add(alice.clone(), ssn, value))],
            tx,
            1_000,
        )
    };

    // Plaintext into a protected attribute: a client that does not know about
    // protection cannot write through it by accident.
    assert_eq!(
        assert(Value::Str(Arc::from("123-45-6789")))
            .expect_err("plaintext must be refused")
            .to_string(),
        format!("attribute {ssn} is protected; assertions must be sealed")
    );

    // The current class at the current epoch is what an assertion must use.
    assert!(assert(sealed(class, 2, b"body")).is_ok());
    assert_eq!(
        assert(sealed(class, 1, b"body")).expect_err("stale epoch must be refused"),
        TxError::StaleProtectionForm {
            attr: ssn,
            expected_class: class,
            expected_epoch: 2,
            class,
            epoch: 1,
        }
    );
    let other = EntityId::new(Partition::Db as u32, 900);
    assert_eq!(
        assert(sealed(other, 2, b"body")).expect_err("foreign class must be refused"),
        TxError::StaleProtectionForm {
            attr: ssn,
            expected_class: class,
            expected_epoch: 2,
            class: other,
            epoch: 2,
        }
    );

    // A sealed value where the schema says plaintext is equally wrong.
    assert_eq!(
        prepare(
            &db,
            [TxItem::Op(TxOp::Add(
                alice,
                name,
                sealed(class, 2, b"body")
            ))],
            tx,
            1_000,
        )
        .expect_err("sealed into an unprotected attribute must be refused"),
        TxError::UnexpectedProtection(name)
    );
}

#[test]
fn a_sealed_fact_is_retracted_and_swapped_bytewise() {
    let (db, class, ssn, _) = protected_fixture();
    let alice = EntityId::new(Partition::User as u32, 1_000);
    let value = sealed(class, 2, b"the-ciphertext");
    let first = prepare(
        &db,
        [TxItem::Op(TxOp::Add(
            EntityRef::Id(alice),
            ssn,
            value.clone(),
        ))],
        EntityId::new(Partition::Tx as u32, 1),
        1_000,
    )
    .expect("assert a sealed fact");
    let db = db.with_transaction(1, &first.datoms);

    // Retraction pairs bytewise, with no key anywhere in sight.
    let retracted = prepare(
        &db,
        [TxItem::Op(TxOp::Retract(
            EntityRef::Id(alice),
            ssn,
            value.clone(),
        ))],
        EntityId::new(Partition::Tx as u32, 2),
        1_001,
    )
    .expect("retract it");
    assert_eq!(retracted.datoms.len(), 1);
    assert!(!retracted.datoms[0].added);
    assert_eq!(retracted.datoms[0].v, value);

    // :db/cas compares two sealed values byte for byte, which works precisely
    // because sealing is deterministic.
    let swapped = prepare(
        &db,
        [TxItem::Op(TxOp::Cas(
            EntityRef::Id(alice),
            ssn,
            Some(value.clone()),
            sealed(class, 2, b"another-ciphertext"),
        ))],
        EntityId::new(Partition::Tx as u32, 2),
        1_001,
    )
    .expect("swap it");
    assert_eq!(swapped.datoms.len(), 2);
    assert_eq!(
        prepare(
            &db,
            [TxItem::Op(TxOp::Cas(
                EntityRef::Id(alice),
                ssn,
                Some(sealed(class, 2, b"not-the-stored-bytes")),
                sealed(class, 2, b"new"),
            ))],
            EntityId::new(Partition::Tx as u32, 2),
            1_001,
        )
        .expect_err("a mismatched old value must fail"),
        TxError::CasFailed
    );
}

#[test]
fn a_retraction_may_use_any_form_the_attribute_has_ever_had() {
    let (db, class, ssn, _) = protected_fixture();
    let alice = EntityRef::Id(EntityId::new(Partition::User as u32, 1_000));
    let retract = |value: Value| {
        prepare(
            &db,
            [TxItem::Op(TxOp::Retract(alice.clone(), ssn, value))],
            EntityId::new(Partition::Tx as u32, 1),
            1_000,
        )
    };
    // Plaintext asserted before the attribute was protected, and a value
    // sealed under an older epoch, are both forms the fact may have.
    assert!(retract(Value::Str(Arc::from("123-45-6789"))).is_ok());
    assert!(retract(sealed(class, 1, b"older-epoch")).is_ok());
    // A class the attribute was never sealed under names a form it never had.
    let other = EntityId::new(Partition::Db as u32, 900);
    assert_eq!(
        retract(sealed(other, 2, b"body")).expect_err("foreign class must be refused"),
        TxError::ForeignProtectionClass {
            attr: ssn,
            class: other,
        }
    );
}

#[test]
fn ordinary_transactions_cannot_write_the_schema_vocabulary() {
    let (db, name, _) = fixture();
    let tx = EntityId::new(Partition::Tx as u32, 1);
    let attribute = EntityRef::Id(name);
    // Every operation shape is refused, so there is no back door into schema
    // metadata through a retraction or a compare-and-swap.
    for (written, op) in [
        (
            bootstrap::DOC,
            TxOp::Add(
                attribute.clone(),
                bootstrap::DOC,
                Value::Str(Arc::from("smuggled")),
            ),
        ),
        (
            bootstrap::DOC,
            TxOp::Retract(
                attribute.clone(),
                bootstrap::DOC,
                Value::Str(Arc::from("smuggled")),
            ),
        ),
        (
            bootstrap::INDEX,
            TxOp::Cas(attribute.clone(), bootstrap::INDEX, None, Value::Bool(true)),
        ),
    ] {
        assert_eq!(
            prepare(&db, [TxItem::Op(op)], tx, 1_000)
                .expect_err("schema metadata is not ordinary data"),
            TxError::SchemaAttribute(written),
        );
    }
}

#[test]
fn a_retired_attribute_refuses_assertions_and_keeps_retractions() {
    let (base, name, _) = fixture();
    let alice = EntityRef::Id(EntityId::new(Partition::User as u32, 1_000));
    let tx = EntityId::new(Partition::Tx as u32, 1);
    let stored = prepare(
        &base,
        [TxItem::Op(TxOp::Add(
            alice.clone(),
            name,
            Value::Str(Arc::from("Alice")),
        ))],
        tx,
        1_000,
    )
    .expect("the attribute still accepts facts");
    let db = base.with_transaction(1, &stored.datoms);

    let mut retired = db.schema().clone();
    retired.set_retired(name, true);
    let db = Db::new(retired).with_transaction(1, &stored.datoms);

    assert_eq!(
        prepare(
            &db,
            [TxItem::Op(TxOp::Add(
                alice.clone(),
                name,
                Value::Str(Arc::from("Alicia")),
            ))],
            EntityId::new(Partition::Tx as u32, 2),
            1_001,
        )
        .expect_err("a retired attribute refuses new assertions"),
        TxError::RetiredAttribute(name),
    );
    // The facts it already holds stay readable and stay retractable.
    let retraction = prepare(
        &db,
        [TxItem::Op(TxOp::Retract(
            alice,
            name,
            Value::Str(Arc::from("Alice")),
        ))],
        EntityId::new(Partition::Tx as u32, 2),
        1_001,
    )
    .expect("retracting an existing fact stays legal");
    assert_eq!(retraction.datoms.len(), 1);
}
