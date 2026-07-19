//! Embedded pipeline, indexing, and crash-recovery tests.

use corium_core::{Cardinality, EntityId, Partition, Schema, Value, ValueType};
use corium_db::attribute;
use corium_log::FileLog;
use corium_store::{BlobStore, FsStore, RootStore};
use corium_transactor::EmbeddedTransactor;
use corium_tx::{EntityRef, TxItem, TxOp};
use std::{sync::Arc, thread};
fn schema() -> (Schema, EntityId) {
    let a = EntityId::new(Partition::Db as u32, 100);
    let mut schema = Schema::default();
    schema.insert(attribute(100, ValueType::Long, Cardinality::One, None));
    (schema, a)
}
#[tokio::test]
async fn durable_ack_recovers_once_and_publishes_concurrent_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let log = Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log"));
    let tx = Arc::new(EmbeddedTransactor::recover(schema.clone(), log).expect("recover"));
    let report_rx = tx.subscribe();
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("e".into()),
        a,
        Value::Long(1),
    ))])
    .expect("durable transaction");
    assert_eq!(report_rx.recv().expect("report").db_after.basis_t(), 1);
    let store = Arc::new(FsStore::open(dir.path().join("store")).expect("store"));
    let writer = {
        let tx = Arc::clone(&tx);
        thread::spawn(move || {
            tx.transact([TxItem::Op(TxOp::Add(
                EntityRef::Temp("other".into()),
                a,
                Value::Long(2),
            ))])
            .expect("concurrent transaction")
        })
    };
    let published = tx
        .publish_indexes(&*store, "db:main", 1)
        .await
        .expect("publish indexes");
    writer.join().expect("writer");
    assert!(published.index_basis_t == 1 || published.index_basis_t == 2);
    for root in &published.roots.clone().expect("roots published") {
        assert!(store.contains(root).await.expect("root blob exists"));
    }
    drop(tx);
    let recovered = EmbeddedTransactor::recover(
        schema,
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("reopen log")),
    )
    .expect("crash recovery");
    assert_eq!(recovered.db().basis_t(), 2);
    assert_eq!(recovered.db().stats().datoms, 2);
}

#[test]
fn recovery_never_reuses_retracted_entity_ids() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let log = Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log"));
    let tx = EmbeddedTransactor::recover(schema.clone(), log).expect("recover");
    let first = tx
        .transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp("e".into()),
            a,
            Value::Long(1),
        ))])
        .expect("create")
        .tx
        .tempids["e"];
    tx.transact([TxItem::Op(TxOp::RetractEntity(EntityRef::Id(first)))])
        .expect("retract entity");
    drop(tx);
    let recovered = EmbeddedTransactor::recover(
        schema,
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("reopen log")),
    )
    .expect("recover after restart");
    let second = recovered
        .transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp("f".into()),
            a,
            Value::Long(2),
        ))])
        .expect("create after recovery")
        .tx
        .tempids["f"];
    assert!(
        second.sequence() > first.sequence(),
        "id {} reused after recovery (first allocation was {})",
        second.sequence(),
        first.sequence()
    );
}

#[tokio::test]
async fn stale_publisher_cannot_regress_published_root() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let store = FsStore::open(dir.path().join("store")).expect("store");
    let fresh = EmbeddedTransactor::recover(
        schema.clone(),
        Arc::new(FileLog::open(dir.path().join("fresh.log")).expect("log")),
    )
    .expect("recover fresh");
    for value in [1, 2] {
        fresh
            .transact([TxItem::Op(TxOp::Add(
                EntityRef::Temp("e".into()),
                a,
                Value::Long(value),
            ))])
            .expect("transact");
    }
    let published = fresh
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("publish fresh");
    assert_eq!(published.index_basis_t, 2);
    let stale = EmbeddedTransactor::recover(
        schema,
        Arc::new(FileLog::open(dir.path().join("stale.log")).expect("log")),
    )
    .expect("recover stale");
    stale
        .transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp("e".into()),
            a,
            Value::Long(9),
        ))])
        .expect("transact");
    stale
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("stale publish is a no-op");
    let root = store
        .get_root("db:main")
        .await
        .expect("read root")
        .expect("root set");
    let decoded = corium_transactor::DbRoot::decode(&root).expect("decodable root");
    assert_eq!(
        decoded.index_basis_t, 2,
        "stale publisher regressed the root to an older basis"
    );
}

#[tokio::test]
async fn deposed_lease_version_cannot_publish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let store = FsStore::open(dir.path().join("store")).expect("store");
    let tx = EmbeddedTransactor::recover(
        schema,
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log")),
    )
    .expect("recover");
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("e".into()),
        a,
        Value::Long(1),
    ))])
    .expect("transact");
    tx.publish_indexes(&store, "db:main", 2)
        .await
        .expect("current lease publishes");
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("f".into()),
        a,
        Value::Long(2),
    ))])
    .expect("transact again");
    let error = tx
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect_err("deposed lease version must not publish");
    assert!(matches!(
        error,
        corium_transactor::TransactError::Deposed { published: 2 }
    ));
}
