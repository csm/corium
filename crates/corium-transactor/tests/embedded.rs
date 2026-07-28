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
    // Two asserted values plus each transaction's own `:db/txInstant`.
    assert_eq!(recovered.db().stats().datoms, 4);
    // Replay reconstructs the transaction-time correspondence from the log.
    assert!(recovered.db().tx_instant(1) < recovered.db().tx_instant(2));
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

/// Loads a database of `count` datoms straight from the durable log.
///
/// The load bypasses the transaction pipeline on purpose: these tests are
/// about publication, and per-item validation over a database this size would
/// dominate their runtime.
fn bulk_loaded(
    dir: &std::path::Path,
    schema: Schema,
    a: EntityId,
    count: u64,
) -> EmbeddedTransactor {
    let log: Arc<dyn TransactionLog> = Arc::new(FileLog::open(dir.join("tx.log")).expect("log"));
    let datoms: Vec<_> = (0..count)
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
    EmbeddedTransactor::recover(schema, log).expect("recover")
}

async fn index_chunks(store: &impl BlobStore, root: &DbRoot) -> Vec<Vec<BlobId>> {
    use corium_store::decode_index_manifest;
    let mut per_index = Vec::new();
    for id in root.roots.as_ref().expect("published roots") {
        let blob = store
            .get(id)
            .await
            .expect("get manifest")
            .expect("manifest");
        per_index.push(decode_index_manifest(&blob).expect("manifest decodes"));
    }
    per_index
}

/// A blob store that counts how many blobs each publication touches, so a
/// test can tell "folded the tail in" apart from "rebuilt and re-uploaded
/// only what changed" — both leave the same bytes in the store.
///
/// It can also stand in for a second publisher, replacing the root behind a
/// publication's back at a chosen point in that publication's own reads.
#[derive(Default)]
struct CountingStore {
    inner: corium_store::MemoryStore,
    touched: std::sync::atomic::AtomicUsize,
    touched_ids: std::sync::Mutex<HashSet<BlobId>>,
    root_reads: std::sync::atomic::AtomicUsize,
    /// Root bytes to install just before the root read with this ordinal,
    /// counting from the arming call.
    ambush: std::sync::Mutex<Option<(usize, Vec<u8>)>>,
}

impl CountingStore {
    /// Blob reads, writes, and presence probes since the last call.
    fn take_touched(&self) -> usize {
        self.touched_ids.lock().expect("touched ids").clear();
        self.touched.swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    /// The blobs named since the last [`Self::take_touched`]. A publication
    /// that rebuilds an index names every one of its chunks, if only to probe
    /// whether the store already has it; one that carries a chunk over never
    /// mentions it.
    fn touched_ids(&self) -> HashSet<BlobId> {
        self.touched_ids.lock().expect("touched ids").clone()
    }

    fn touch(&self) {
        self.touched
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn touch_id(&self, id: &BlobId) {
        self.touch();
        self.touched_ids
            .lock()
            .expect("touched ids")
            .insert(id.clone());
    }

    /// Arms a one-shot root replacement to land just before the `nth` root
    /// read from now — a second publisher winning the record while this
    /// process is midway through a publication.
    fn ambush_root_read(&self, nth: usize, root: &DbRoot) {
        self.root_reads
            .store(0, std::sync::atomic::Ordering::SeqCst);
        *self.ambush.lock().expect("ambush") = Some((nth, root.encode()));
    }

    /// Fires the armed replacement when this read is its target.
    async fn maybe_ambush(&self, name: &str) {
        let read = self
            .root_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let due = {
            let mut ambush = self.ambush.lock().expect("ambush");
            match ambush.as_ref() {
                Some((nth, _)) if *nth == read => ambush.take().map(|(_, bytes)| bytes),
                _ => None,
            }
        };
        if let Some(bytes) = due {
            let current = self.inner.get_root(name).await.expect("read root");
            self.inner
                .cas_root(name, current.as_deref(), &bytes)
                .await
                .expect("ambush root");
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for CountingStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, corium_store::StoreError> {
        self.touch_id(&corium_store::digest(bytes));
        self.inner.put(bytes).await
    }
    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, corium_store::StoreError> {
        self.touch_id(id);
        self.inner.get(id).await
    }
    async fn contains(&self, id: &BlobId) -> Result<bool, corium_store::StoreError> {
        self.touch_id(id);
        self.inner.contains(id).await
    }
    async fn delete(&self, id: &BlobId) -> Result<(), corium_store::StoreError> {
        self.inner.delete(id).await
    }
    async fn list(&self) -> Result<corium_store::BlobIdStream, corium_store::StoreError> {
        self.inner.list().await
    }
}

#[async_trait::async_trait]
impl RootStore for CountingStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, corium_store::StoreError> {
        self.maybe_ambush(name).await;
        self.inner.get_root(name).await
    }
    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), corium_store::StoreError> {
        self.inner.cas_root(name, expected, new).await
    }
    async fn delete_root(&self, name: &str) -> Result<(), corium_store::StoreError> {
        self.inner.delete_root(name).await
    }
    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, corium_store::StoreError> {
        self.inner.list_roots(prefix).await
    }
}

/// Blobs the first publication of a `count`-datom database stages, and the
/// most any later single-commit pass stages.
async fn publication_cost(dir: &std::path::Path, count: u64) -> (usize, usize) {
    let (schema, a) = schema();
    let store = CountingStore::default();
    let tx = bulk_loaded(dir, schema, a, count);
    tx.publish_indexes(&store, "db:main", 1)
        .await
        .expect("first publish");
    let full = store.take_touched();

    let mut incremental = 0;
    for value in 0..3 {
        tx.transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp(format!("e{value}")),
            a,
            Value::Long(1_000_000 + value),
        ))])
        .expect("transact");
        tx.publish_indexes(&store, "db:main", 1)
            .await
            .expect("incremental publish");
        incremental = incremental.max(store.take_touched());
    }

    // Republishing an unchanged basis must stage nothing new, and must leave
    // the next pass still able to fold rather than rebuild.
    tx.publish_indexes(&store, "db:main", 1)
        .await
        .expect("idempotent publish");
    let idle = store.take_touched();
    assert!(idle <= 8, "a no-op pass staged {idle} blobs");
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("after-idle".into()),
        a,
        Value::Long(2_000_000),
    ))])
    .expect("transact");
    tx.publish_indexes(&store, "db:main", 1)
        .await
        .expect("publish after idle pass");
    let after_idle = store.take_touched();
    assert!(
        after_idle <= incremental,
        "a no-op pass cost the next one its incremental basis ({after_idle} \
         blobs against {incremental})"
    );
    (full, incremental)
}

#[tokio::test]
async fn losing_the_root_mid_pass_rebuilds_rather_than_republishing_reused_chunks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let store = CountingStore::default();
    let tx = bulk_loaded(dir.path(), schema, a, 30_000);
    let published = tx
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("first publish");

    // Every comparison below is against the chunks of the publication
    // immediately before it, since each commit does replace a few of them.
    let mut settled: HashSet<BlobId> = index_chunks(&store, &published)
        .await
        .concat()
        .into_iter()
        .collect();
    store.take_touched();

    // Baseline: an undisturbed pass carries those chunks over, so it never
    // names them — that is exactly the reuse the race has to disarm.
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("first".into()),
        a,
        Value::Long(1_000_000),
    ))])
    .expect("transact");
    let undisturbed = tx
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("undisturbed publish");
    let touched = store.touched_ids();
    let after: HashSet<BlobId> = index_chunks(&store, &undisturbed)
        .await
        .concat()
        .into_iter()
        .collect();
    // Chunks in both publications are the ones a fold carries over by id; a
    // commit does replace a few, and those are not part of the comparison.
    let carried = &settled & &after;
    assert!(!carried.is_empty(), "nothing was carried across the commit");
    assert!(
        (&touched & &carried).is_empty(),
        "an undisturbed pass named {} of the {} chunks it carried over; it is \
         not reusing them and the race below proves nothing",
        (&touched & &carried).len(),
        carried.len()
    );
    settled = after;
    store.take_touched();

    // Now a second publisher replaces the root after this pass has checked it
    // and decided its cached chunks are reusable, but before the pass writes
    // over it. That root's index state no longer names those chunks, so a
    // sweep between the two publications is free to delete them — carrying
    // their blob ids into a new manifest would leave the live root pointing at
    // nothing. The root this pass would install carries the newer basis, so
    // the CAS alone would happily accept it. Root reads from here: 1 is the
    // pass's own check, 2 is the one inside the root CAS.
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("second".into()),
        a,
        Value::Long(2_000_000),
    ))])
    .expect("transact");
    let interloper = DbRoot {
        index_basis_t: published.index_basis_t.saturating_sub(1),
        roots: Some(std::array::from_fn(|slot| {
            corium_store::digest(format!("another publisher's index {slot}").as_bytes())
        })),
        ..published.clone()
    };
    store.ambush_root_read(2, &interloper);
    let raced = tx
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("publish under a racing publisher");
    assert!(raced.index_basis_t > published.index_basis_t);

    // Having lost the root that vouched for them, the pass must republish
    // every chunk it would have carried over from its own bytes rather than by
    // id — which means naming each one, if only to find the store already has
    // it.
    let touched = store.touched_ids();
    let chunks = index_chunks(&store, &raced).await.concat();
    let carried = &settled & &chunks.iter().cloned().collect::<HashSet<_>>();
    assert!(!carried.is_empty(), "nothing was carried across the commit");
    assert_eq!(
        (&touched & &carried).len(),
        carried.len(),
        "a pass that lost the root named only {} of the {} chunks it had \
         carried over; it republished ids it could no longer vouch for",
        (&touched & &carried).len(),
        carried.len()
    );
    // And the root it did publish must be fully dereferenceable.
    for chunk in chunks {
        assert!(
            store.contains(&chunk).await.expect("probe chunk"),
            "published root references a chunk that is not in the store"
        );
    }
}

#[tokio::test]
async fn indexing_a_tail_costs_the_tail_and_not_the_database() {
    let small = tempfile::tempdir().expect("tempdir");
    let large = tempfile::tempdir().expect("tempdir");
    let (small_full, small_incremental) = publication_cost(small.path(), 30_000).await;
    let (large_full, large_incremental) = publication_cost(large.path(), 90_000).await;

    // A rebuild stages every chunk of every index, so its cost grows with the
    // database. Folding a one-datom tail into the last publication stages only
    // the leaves that datom rebuilt, so its cost does not.
    assert!(
        large_full > small_full,
        "tripling the database did not make a full publication stage more \
         blobs ({small_full} then {large_full}) — the measurement is wrong"
    );
    assert_eq!(
        small_incremental, large_incremental,
        "an incremental pass staged {small_incremental} blobs on a 30k-datom \
         database and {large_incremental} on a 90k-datom one; publication \
         still scales with the database"
    );
    assert!(
        large_incremental * 2 < large_full,
        "an incremental pass staged {large_incremental} of the {large_full} \
         blobs a full publication does; publication fell back to a rebuild"
    );
}

#[tokio::test]
async fn republication_uploads_only_the_chunks_a_change_touches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let store = FsStore::open(dir.path().join("store")).expect("store");
    // Enough datoms that the full covering indexes span several leaf chunks
    // (content-defined boundaries average one per ~2k keys).
    let tx = bulk_loaded(dir.path(), schema, a, 30_000);
    let first = tx
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("first publish");
    let before = blob_ids(&store).await;
    let chunks_before = index_chunks(&store, &first).await;
    assert!(
        chunks_before[IndexOrder::Eavt as usize].len() >= 4,
        "expected EAVT to span several chunks, found {}",
        chunks_before[IndexOrder::Eavt as usize].len()
    );

    // Republishing at an unchanged basis must upload nothing at all.
    tx.publish_indexes(&store, "db:main", 1)
        .await
        .expect("idempotent publish");
    assert_eq!(blob_ids(&store).await, before, "a no-op pass wrote blobs");

    // One appended datom (largest entity id and value, so it lands in the
    // tail chunk of every order) must not re-upload the settled chunks.
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp("tail".into()),
        a,
        Value::Long(1_000_000),
    ))])
    .expect("tail transact");
    let second = tx
        .publish_indexes(&store, "db:main", 1)
        .await
        .expect("second publish");
    let after = blob_ids(&store).await;
    let fresh = after.difference(&before).count();
    // Each index re-uploads its manifest plus the chunks the transaction
    // dirtied: the appended datom's tail chunk, and the chunk holding the
    // transaction partition, where the commit's own `:db/txInstant` lands.
    // Both regions grow at their own tail, so this stays O(1) per commit.
    assert!(
        fresh <= 12,
        "appending one datom re-uploaded {fresh} blobs of {} (expected only \
         each index's manifest and the chunks it appends to)",
        after.len()
    );
    let chunks_after = index_chunks(&store, &second).await;
    let eavt = IndexOrder::Eavt as usize;
    let carried = chunks_after[eavt]
        .iter()
        .filter(|id| chunks_before[eavt].contains(id))
        .count();
    assert!(
        carried >= chunks_before[eavt].len() - 2,
        "EAVT carried only {carried} of {} chunks across an append",
        chunks_before[eavt].len()
    );
}

#[tokio::test]
async fn incremental_publication_matches_a_rebuild_from_scratch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (schema, a) = schema();
    let store = FsStore::open(dir.path().join("store")).expect("store");
    let tx = bulk_loaded(dir.path(), schema, a, 8_000);
    let base = tx
        .publish_indexes(&store, "db:incremental", 1)
        .await
        .expect("base publish");

    // A tail that asserts, supersedes a cardinality-one value, and retracts
    // an entity outright — every shape the current-value fold has to handle.
    let kept = tx
        .transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp("kept".into()),
            a,
            Value::Long(7),
        ))])
        .expect("assert")
        .tx
        .tempids["kept"];
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Id(kept),
        a,
        Value::Long(8),
    ))])
    .expect("supersede");
    let doomed = tx
        .transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp("doomed".into()),
            a,
            Value::Long(9),
        ))])
        .expect("assert doomed")
        .tx
        .tempids["doomed"];
    tx.transact([TxItem::Op(TxOp::RetractEntity(EntityRef::Id(doomed)))])
        .expect("retract entity");
    let incremental = tx
        .publish_indexes(&store, "db:incremental", 1)
        .await
        .expect("incremental publish");
    assert!(incremental.index_basis_t > base.index_basis_t);

    // A transactor that has never published anything rebuilds every index
    // from its own in-memory covering indexes. The two paths must agree
    // exactly — same chunk boundaries, same blob ids, same manifests.
    let cold = EmbeddedTransactor::recover_from_snapshot(
        tx.db(),
        incremental.next_entity_id,
        incremental.last_tx_instant,
        Arc::new(corium_log::MemoryLog::default()),
    )
    .expect("cold transactor");
    let rebuilt = cold
        .publish_indexes(&store, "db:rebuilt", 1)
        .await
        .expect("rebuild publish");
    assert_eq!(
        rebuilt.roots, incremental.roots,
        "folding the tail in published different indexes than a full rebuild"
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
    // `:db/txInstant` datoms are live facts, so the published snapshot carries
    // transaction time for the history it covers, not just for the tail.
    for t in 1..=6 {
        assert_eq!(
            from_index.db().tx_instant(t),
            from_log.db().tx_instant(t),
            "snapshot recovery lost the instant of transaction {t}"
        );
    }
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

#[test]
fn every_commit_stamps_a_queryable_transaction_instant() {
    let (schema, a) = schema();
    let log: Arc<dyn TransactionLog> = Arc::new(MemoryLog::default());
    let tx = EmbeddedTransactor::recover(schema, Arc::clone(&log)).expect("recover");
    for value in 1..=3 {
        tx.transact([TxItem::Op(TxOp::Add(
            EntityRef::Temp(format!("e{value}")),
            a,
            Value::Long(value),
        ))])
        .expect("transact");
    }
    let db = tx.db();

    // The commit's own datom is in the value, in the report, and in the log.
    let stamped: Vec<_> = db
        .datoms_for_attribute(corium_db::bootstrap::TX_INSTANT)
        .map(|datom| (datom.e, datom.v.clone()))
        .collect();
    assert_eq!(stamped.len(), 3);
    for (t, (entity, value)) in (1..=3).zip(&stamped) {
        assert_eq!(*entity, EntityId::new(Partition::Tx as u32, t));
        assert_eq!(*value, Value::Instant(db.tx_instant(t).expect("instant")));
    }
    // Monotone, so instants order transactions exactly as `t` does.
    assert!(db.tx_instant(1) < db.tx_instant(2) && db.tx_instant(2) < db.tx_instant(3));
    for record in log.replay().expect("replay") {
        assert_eq!(
            corium_db::bootstrap::asserted_instant(record.t, &record.datoms),
            Some(record.tx_instant),
            "the log record carries the instant as a datom too"
        );
    }

    // Wall clock names the same views as the basis does.
    let second = db.tx_instant(2).expect("instant");
    assert_eq!(db.as_of_instant(second).datoms(), db.as_of(2).datoms());
    assert_eq!(db.since_instant(second).datoms(), db.since(2).datoms());
}

#[test]
fn transaction_data_can_supply_its_own_instant_but_not_move_time_backwards() {
    let (schema, a) = schema();
    let log: Arc<dyn TransactionLog> = Arc::new(MemoryLog::default());
    let tx = EmbeddedTransactor::recover(schema, log).expect("recover");
    let backdated = 1_000_000_000_000;
    let report = tx
        .transact([
            TxItem::Op(TxOp::Add(EntityRef::Temp("e".into()), a, Value::Long(1))),
            TxItem::Op(TxOp::Add(
                EntityRef::Temp(corium_tx::TX_TEMPID.into()),
                corium_db::bootstrap::TX_INSTANT,
                Value::Instant(backdated),
            )),
        ])
        .expect("import with a supplied instant");
    assert_eq!(report.tx_instant, backdated);
    assert_eq!(tx.db().tx_instant(1), Some(backdated));

    // The next commit's own instant is later than the supplied one, and a
    // second import may not rewind past it.
    let rewound = tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp(corium_tx::TX_TEMPID.into()),
        corium_db::bootstrap::TX_INSTANT,
        Value::Instant(backdated - 1),
    ))]);
    assert!(
        matches!(
            rewound,
            Err(corium_transactor::TransactError::Tx(
                corium_tx::TxError::TxInstantNotMonotonic { .. }
            ))
        ),
        "expected a monotonicity rejection, got {rewound:?}"
    );
    assert_eq!(tx.db().basis_t(), 1, "the rejected commit left no trace");
}

#[test]
fn replaying_a_log_written_without_instant_datoms_still_resolves_instants() {
    // Records appended before Corium recorded `:db/txInstant` as a datom carry
    // the instant only as a record field; replay must reconstruct the datom so
    // an old database gains instant-named views without a log rewrite.
    let (schema, a) = schema();
    let log: Arc<dyn TransactionLog> = Arc::new(MemoryLog::default());
    for t in 1..=2 {
        log.append(&TxRecord {
            t,
            tx_instant: i64::try_from(t * 1_000).expect("small instant"),
            datoms: vec![Datom {
                e: EntityId::new(Partition::User as u32, corium_db::FIRST_USER_ID + t),
                a,
                v: Value::Long(i64::try_from(t).expect("small value")),
                tx: EntityId::new(Partition::Tx as u32, t),
                added: true,
            }],
        })
        .expect("append legacy record");
    }
    let db = EmbeddedTransactor::recover(schema, log)
        .expect("recover")
        .db();

    assert_eq!(db.tx_instant(1), Some(1_000));
    assert_eq!(db.t_at_instant(1_999), 1);
    assert_eq!(
        db.datoms_for_attribute(corium_db::bootstrap::TX_INSTANT)
            .count(),
        2
    );
}

#[test]
fn replay_prefers_a_materialized_instant_over_conflicting_log_metadata() {
    let (schema, _) = schema();
    let log: Arc<dyn TransactionLog> = Arc::new(MemoryLog::default());
    log.append(&TxRecord {
        t: 1,
        tx_instant: 5_000,
        datoms: vec![corium_db::bootstrap::tx_instant_datom(1, 1_000)],
    })
    .expect("append record");
    let tx = EmbeddedTransactor::recover(schema, log).expect("recover");

    assert_eq!(tx.db().tx_instant(1), Some(1_000));
    tx.transact([TxItem::Op(TxOp::Add(
        EntityRef::Temp(corium_tx::TX_TEMPID.into()),
        corium_db::bootstrap::TX_INSTANT,
        Value::Instant(2_000),
    ))])
    .expect("the materialized datom is the recovered high-water mark");
}
