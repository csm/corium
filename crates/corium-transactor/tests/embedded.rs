//! Embedded pipeline, indexing, and crash-recovery tests.

use corium_core::{Cardinality, EntityId, Partition, Schema, Value, ValueType};
use corium_db::attribute;
use corium_log::{FileLog, TransactionLog};
use corium_store::{BlobId, BlobStore, FsStore, RootStore};
use corium_transactor::EmbeddedTransactor;
use corium_tx::{EntityRef, TxItem, TxOp};
use std::collections::HashSet;
use std::{sync::Arc, thread};
use tokio_stream::StreamExt;
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

async fn blob_ids(store: &FsStore) -> HashSet<BlobId> {
    let mut ids = HashSet::new();
    let mut stream = store.list().await.expect("list blobs");
    while let Some(id) = stream.next().await {
        ids.insert(id.expect("blob id"));
    }
    ids
}

#[tokio::test]
async fn republication_uploads_only_the_chunks_a_change_touches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let store = FsStore::open(dir.path().join("store")).expect("store");
    // Enough datoms that every covering index spans several leaf chunks
    // (content-defined boundaries average one per ~2k keys). The load goes
    // straight into the durable log — this test is about publication, and
    // per-item transaction validation over a database this size would
    // dominate its runtime.
    let log = Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log"));
    let datoms: Vec<_> = (0u64..30_000)
        .map(|n| corium_core::Datom {
            e: EntityId::new(Partition::User as u32, corium_db::FIRST_USER_ID + n),
            a,
            v: Value::Long(i64::try_from(n).expect("small value")),
            tx: EntityId::new(Partition::Tx as u32, 1),
            added: true,
        })
        .collect();
    log.append(&corium_log::TxRecord {
        t: 1,
        tx_instant: 1,
        datoms,
    })
    .expect("bulk log append");
    let tx = EmbeddedTransactor::recover(schema, log).expect("recover");
    tx.publish_indexes(&store, "db:main", 1)
        .await
        .expect("first publish");
    let before = blob_ids(&store).await;
    assert!(
        before.len() >= 24,
        "expected several chunks per index, found {} blobs",
        before.len()
    );

    // One appended datom (largest entity id and value, so it lands in the
    // tail chunk of every order) must not re-upload the settled chunks.
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("tail".into()),
        a,
        Value::Long(1_000_000),
    ))])
    .expect("tail transact");
    tx.publish_indexes(&store, "db:main", 1)
        .await
        .expect("second publish");
    let after = blob_ids(&store).await;
    let fresh = after.difference(&before).count();
    assert!(fresh >= 4, "each index publishes a new manifest");
    assert!(
        fresh <= 12,
        "appending one datom re-uploaded {fresh} blobs of {} (expected only \
         each index's manifest and tail chunk)",
        after.len()
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
