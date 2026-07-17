//! Transaction model and validation tests.

use corium_core::{Cardinality, EntityId, Partition, Schema, Unique, Value, ValueType};
use corium_db::{Db, attribute};
use corium_tx::{EntityRef, TxError, TxItem, TxOp, prepare};
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
