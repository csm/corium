//! Embedded pipeline, indexing, and crash-recovery tests.

use corium_core::{
    Cardinality, Datom, EntityId, IndexOrder, KeywordInterner, Partition, Schema, Value, ValueType,
};
use corium_db::{Db, Idents, attribute};
use corium_log::{FileLog, LogError, MemoryLog, TransactionLog, TxRecord};
use corium_store::{BlobId, BlobStore, DbRoot, FsStore, RootStore};
use corium_transactor::EmbeddedTransactor;
use corium_tx::{EntityRef, TxItem, TxOp};
use std::collections::HashSet;
use std::{sync::Arc, thread};
use tokio_stream::StreamExt;

#[derive(Default)]
struct DelayedAsyncLog {
    inner: MemoryLog,
    append_started: tokio::sync::Notify,
    release_append: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl TransactionLog for DelayedAsyncLog {
    fn append(&self, _record: &TxRecord) -> Result<(), LogError> {
        Err(LogError::AsyncOnly)
    }

    async fn append_async(&self, record: &TxRecord) -> Result<(), LogError> {
        self.append_started.notify_one();
        self.release_append.notified().await;
        self.inner.append(record)
    }

    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        self.inner.tx_range(start, end)
    }
}

/// Materializes the current value at a published index root the way a
/// transactor recovering from the index root does: read the EAVT snapshot,
/// decode its keys back to datoms.
async fn load_index_root_snapshot(store: &FsStore, root: &DbRoot, schema: Schema) -> Db {
    use corium_store::{decode_index_manifest, decode_segment_keys, is_index_manifest};
    let eavt = &root.roots.as_ref().expect("published roots")[IndexOrder::Eavt as usize];
    let blob = store
        .get(eavt)
        .await
        .expect("get eavt")
        .expect("eavt present");
    let keys = if is_index_manifest(&blob) {
        let mut keys = Vec::new();
        for child in decode_index_manifest(&blob).expect("manifest") {
            let chunk = store.get(&child).await.expect("get chunk").expect("chunk");
            keys.extend(decode_segment_keys(&chunk).expect("chunk keys"));
        }
        keys
    } else {
        decode_segment_keys(&blob).expect("flat keys")
    };
    let datoms = keys
        .iter()
        .map(|key| Datom::from_key(IndexOrder::Eavt, key).expect("decode datom"))
        .collect();
    Db::from_current_snapshot(
        root.index_basis_t,
        schema,
        Idents::default(),
        KeywordInterner::default(),
        datoms,
    )
}
fn schema() -> (Schema, EntityId) {
    let a = EntityId::new(Partition::Db as u32, 100);
    let mut schema = Schema::default();
    schema.insert(attribute(100, ValueType::Long, Cardinality::One, None));
    (schema, a)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_durable_append_does_not_hold_database_state_lock() {
    let (schema, a) = schema();
    let delayed = Arc::new(DelayedAsyncLog::default());
    let log: Arc<dyn TransactionLog> = delayed.clone();
    let tx = Arc::new(
        EmbeddedTransactor::recover_from_async(Db::new(schema), log)
            .await
            .expect("recover"),
    );
    let writer = {
        let tx = Arc::clone(&tx);
        tokio::spawn(async move {
            tx.transact_async([TxItem::Op(TxOp::Add(
                EntityRef::Temp("e".into()),
                a,
                Value::Long(1),
            ))])
            .await
        })
    };
    delayed.append_started.notified().await;

    // Snapshot readers must remain responsive while storage durability is
    // deliberately paused. This is the lock inversion that deadlocked the
    // node when the native log synchronously re-entered Tokio.
    let reader = {
        let tx = Arc::clone(&tx);
        tokio::task::spawn_blocking(move || tx.db().basis_t())
    };
    let read = tokio::time::timeout(std::time::Duration::from_millis(250), reader).await;
    delayed.release_append.notify_one();
    let report = writer
        .await
        .expect("writer task")
        .expect("durable transaction");

    assert_eq!(
        read.expect("snapshot read blocked on storage I/O")
            .expect("reader task"),
        0
    );
    assert_eq!(report.db_after.basis_t(), 1);
    assert_eq!(tx.db().basis_t(), 1);
}
#[tokio::test]
async fn durable_ack_recovers_once_and_publishes_concurrent_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let log: Arc<dyn TransactionLog> =
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log"));
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
    let log: Arc<dyn TransactionLog> =
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log"));
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
    let log: Arc<dyn TransactionLog> =
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log"));
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

#[tokio::test]
async fn index_root_recovery_matches_full_log_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let store = FsStore::open(dir.path().join("store")).expect("store");
    let log: Arc<dyn TransactionLog> =
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log"));
    let tx = EmbeddedTransactor::recover(schema.clone(), Arc::clone(&log)).expect("recover");
    for value in 1..=3 {
        tx.transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp(format!("e{value}")),
            a,
            Value::Long(value),
        ))])
        .expect("transact head");
    }
    // Publish a snapshot mid-history, then commit a tail past it.
    let root = tx
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("publish");
    assert_eq!(root.index_basis_t, 3);
    for value in 4..=6 {
        tx.transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp(format!("e{value}")),
            a,
            Value::Long(value),
        ))])
        .expect("transact tail");
    }
    drop(tx);

    // Recovering from the index root replays only the (3, 6] tail.
    let snapshot = load_index_root_snapshot(&store, &root, schema.clone()).await;
    let from_index = EmbeddedTransactor::recover_from_snapshot(
        snapshot,
        root.next_entity_id,
        root.last_tx_instant,
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("reopen log")),
    )
    .expect("index-root recovery");
    // Full-log replay is the reference: the two must agree on the current value.
    let from_log = EmbeddedTransactor::recover(
        schema,
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("reopen log")),
    )
    .expect("full replay");
    assert_eq!(from_index.db().basis_t(), from_log.db().basis_t());
    assert_eq!(from_index.db().basis_t(), 6);
    assert_eq!(
        from_index.db().datoms(),
        from_log.db().datoms(),
        "index-root recovery must reconstruct the same current value as full replay"
    );
}

#[tokio::test]
async fn index_root_recovery_does_not_reuse_ids_retracted_before_the_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let store = FsStore::open(dir.path().join("store")).expect("store");
    let log: Arc<dyn TransactionLog> =
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("log"));
    let tx = EmbeddedTransactor::recover(schema.clone(), Arc::clone(&log)).expect("recover");
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("keep".into()),
        a,
        Value::Long(1),
    ))])
    .expect("create survivor");
    // The highest-numbered entity is fully retracted *before* the snapshot,
    // so it leaves no live datom for the EAVT snapshot to carry — only the
    // persisted allocator high-water records that its id was ever used.
    let doomed = tx
        .transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp("doomed".into()),
            a,
            Value::Long(2),
        ))])
        .expect("create doomed")
        .tx
        .tempids["doomed"];
    tx.transact([TxItem::Op(TxOp::RetractEntity(EntityRef::Id(doomed)))])
        .expect("retract doomed");
    let root = tx
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("publish");
    assert!(
        root.next_entity_id > doomed.sequence(),
        "published high-water must be past the retracted id"
    );
    drop(tx);

    // Recover from the index root with an empty tail: only the persisted
    // high-water stands between allocation and reusing `doomed`'s id.
    let snapshot = load_index_root_snapshot(&store, &root, schema.clone()).await;
    assert!(
        snapshot.datoms().iter().all(|datom| datom.e != doomed),
        "snapshot must not carry the fully retracted entity"
    );
    let recovered = EmbeddedTransactor::recover_from_snapshot(
        snapshot,
        root.next_entity_id,
        root.last_tx_instant,
        Arc::new(FileLog::open(dir.path().join("tx.log")).expect("reopen log")),
    )
    .expect("index-root recovery");
    let fresh = recovered
        .transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp("fresh".into()),
            a,
            Value::Long(3),
        ))])
        .expect("allocate after recovery")
        .tx
        .tempids["fresh"];
    assert!(
        fresh.sequence() > doomed.sequence(),
        "id {} reused after index-root recovery (retracted id was {})",
        fresh.sequence(),
        doomed.sequence()
    );
}
