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
