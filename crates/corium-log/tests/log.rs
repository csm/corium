//! Durable log conformance tests.

use corium_core::{Datom, EntityId, Value};
use corium_log::{FileLog, MemLogRegistry, TransactionLog, TxRecord, VersionedLog};
use std::io::Write;
fn record(t: u64) -> TxRecord {
    let signed_t = i64::try_from(t).expect("test transaction fits i64");
    TxRecord {
        t,
        tx_instant: 100 + signed_t,
        datoms: vec![Datom {
            e: EntityId::from_raw(t),
            a: EntityId::from_raw(2),
            v: Value::Long(signed_t),
            tx: EntityId::from_raw(100 + t),
            added: true,
        }],
    }
}

/// A record whose value string is `bytes` long, for driving the chunked
/// native log past its per-chunk size cap.
fn big_record(t: u64, bytes: usize) -> TxRecord {
    let signed_t = i64::try_from(t).expect("test transaction fits i64");
    TxRecord {
        t,
        tx_instant: 100 + signed_t,
        datoms: vec![Datom {
            e: EntityId::from_raw(t),
            a: EntityId::from_raw(2),
            v: Value::Str("x".repeat(bytes).into()),
            tx: EntityId::from_raw(100 + t),
            added: true,
        }],
    }
}
#[test]
fn filesystem_log_replays_and_ranges_after_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let log = FileLog::open(&path).expect("open");
    log.append(&record(1)).expect("append 1");
    log.append(&record(2)).expect("append 2");
    drop(log);
    let log = FileLog::open(path).expect("reopen");
    assert_eq!(log.replay().expect("replay"), vec![record(1), record(2)]);
    assert_eq!(log.tx_range(2, Some(3)).expect("range"), vec![record(2)]);
}

#[test]
fn torn_tail_from_crash_is_dropped_and_log_stays_appendable() {
    use std::{fs::OpenOptions, io::Write};
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let log = FileLog::open(&path).expect("open");
    log.append(&record(1)).expect("append 1");
    log.append(&record(2)).expect("append 2");
    drop(log);
    // Simulate a crash mid-append: a full length prefix promising more
    // payload bytes than were flushed.
    let mut file = OpenOptions::new().append(true).open(&path).expect("file");
    file.write_all(&100_u64.to_be_bytes()).expect("torn length");
    file.write_all(&[0xAB; 5]).expect("torn payload");
    drop(file);
    let log = FileLog::open(&path).expect("reopen tolerates torn tail");
    assert_eq!(log.replay().expect("replay"), vec![record(1), record(2)]);
    log.append(&record(3)).expect("append after truncation");
    drop(log);
    // A partial length prefix is likewise dropped.
    let mut file = OpenOptions::new().append(true).open(&path).expect("file");
    file.write_all(&[0x01; 3]).expect("torn prefix");
    drop(file);
    let log = FileLog::open(&path).expect("reopen tolerates torn prefix");
    assert_eq!(
        log.replay().expect("replay"),
        vec![record(1), record(2), record(3)]
    );
}

#[test]
fn mem_registry_shares_records_across_reopens_and_ranges() {
    let registry = MemLogRegistry::new();
    assert!(!registry.exists("db"));
    let log = registry.open("db", 1);
    log.append(&record(1)).expect("append 1");
    log.append(&record(2)).expect("append 2");
    assert!(registry.exists("db"));

    // Reopening the same name reaches the same records (recovery within a
    // process), and appends continue past the replayed tail.
    let reopened = registry.open("db", 1);
    assert_eq!(
        reopened.replay().expect("replay"),
        vec![record(1), record(2)]
    );
    reopened.append(&record(3)).expect("append 3");
    assert_eq!(log.tx_range(2, Some(3)).expect("range"), vec![record(2)]);

    // A clone of the registry shares storage; delete_all clears it.
    let shared = registry.clone();
    shared.delete_all("db");
    assert!(!registry.exists("db"));
    assert!(registry.open("db", 1).replay().expect("empty").is_empty());
}

#[test]
fn mem_versioned_log_applies_the_takeover_cutoff() {
    let registry = MemLogRegistry::new();
    let old = registry.open("db", 1);
    old.append(&record(1)).expect("append 1");
    // Takeover under version 2 replays t=1 and commits its own t=2.
    let new = registry.open("db", 2);
    new.append(&record(2)).expect("new owner's t=2");
    // The deposed writer's stale append under the older version must lose.
    let mut stale = record(2);
    stale.tx_instant = 999;
    old.append(&stale).expect("stale append is dead");
    assert_eq!(new.replay().expect("replay"), vec![record(1), record(2)]);
}

#[test]
fn versioned_log_merges_files_in_lease_version_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    // History under lease version 1, takeover continues under version 2.
    let v1 = VersionedLog::open(dir.path(), "db", 1).expect("open v1");
    v1.append(&record(1)).expect("append 1");
    v1.append(&record(2)).expect("append 2");
    let v2 = VersionedLog::open(dir.path(), "db", 2).expect("open v2");
    v2.append(&record(3))
        .expect("append continues past replayed tail");
    assert_eq!(
        v2.replay().expect("replay"),
        vec![record(1), record(2), record(3)]
    );
    assert_eq!(v2.tx_range(2, Some(3)).expect("range"), vec![record(2)]);
}

#[test]
fn takeover_cutoff_discards_a_deposed_writers_stale_append() {
    let dir = tempfile::tempdir().expect("tempdir");
    let old = VersionedLog::open(dir.path(), "db", 1).expect("open v1");
    old.append(&record(1)).expect("append 1");
    // Takeover: version 2 replays t=1 and commits its own t=2.
    let new = VersionedLog::open(dir.path(), "db", 2).expect("open v2");
    new.append(&record(2)).expect("new owner's t=2");
    // The deposed writer's in-flight append lands in its own version file
    // with the same t; readers must prefer the newer lease's record.
    let mut stale = record(2);
    stale.tx_instant = 999;
    old.append(&stale)
        .expect("stale append is durable but dead");
    let merged = VersionedLog::open_read_only(dir.path(), "db")
        .expect("read only")
        .replay()
        .expect("replay");
    assert_eq!(merged, vec![record(1), record(2)]);
}

#[test]
fn plain_log_file_reads_as_version_zero_history() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Pre-HA deployments wrote a single unversioned file.
    let legacy = FileLog::open(dir.path().join("db.log")).expect("legacy");
    legacy.append(&record(1)).expect("append");
    let log = VersionedLog::open(dir.path(), "db", 3).expect("open versioned");
    log.append(&record(2)).expect("append continues");
    assert_eq!(log.replay().expect("replay"), vec![record(1), record(2)]);
}

#[test]
fn versioned_log_survives_torn_tail_in_an_older_version_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let old = VersionedLog::open(dir.path(), "db", 1).expect("open v1");
    old.append(&record(1)).expect("append");
    // Crash mid-append: a torn record at the old file's tail.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(dir.path().join("db.v1.log"))
        .expect("open raw");
    Write::write_all(&mut file, &[0, 0, 0, 0, 0, 0, 0, 99, 1, 2, 3]).expect("torn bytes");
    drop(file);
    let new = VersionedLog::open(dir.path(), "db", 2).expect("takeover open");
    assert_eq!(new.replay().expect("replay"), vec![record(1)]);
    new.append(&record(2)).expect("append past torn tail");
    assert_eq!(new.replay().expect("replay"), vec![record(1), record(2)]);
}

type ObjectMap = std::sync::Mutex<std::collections::BTreeMap<(String, u64, u64), Vec<u8>>>;

#[derive(Default)]
struct TestNativeStorage {
    /// Per-transaction record objects, keyed `(name, version, t)`.
    records: ObjectMap,
    /// Legacy chunk objects, keyed `(name, version, chunk)`.
    legacy: ObjectMap,
}

impl TestNativeStorage {
    /// Seeds a legacy chunk object as an older binary would have written it,
    /// for exercising read-only backward compatibility.
    fn seed_legacy_chunk(&self, name: &str, version: u64, chunk: u64, bytes: Vec<u8>) {
        self.legacy
            .lock()
            .expect("lock")
            .insert((name.to_owned(), version, chunk), bytes);
    }
}

#[async_trait::async_trait]
impl corium_log::NativeLogStorage for TestNativeStorage {
    async fn put_record(
        &self,
        name: &str,
        version: u64,
        t: u64,
        bytes: &[u8],
    ) -> Result<bool, corium_log::LogError> {
        let mut guard = self.records.lock().expect("lock");
        let key = (name.to_owned(), version, t);
        if guard.contains_key(&key) {
            return Ok(false);
        }
        guard.insert(key, bytes.to_vec());
        Ok(true)
    }

    async fn read_record(
        &self,
        name: &str,
        version: u64,
        t: u64,
    ) -> Result<Option<Vec<u8>>, corium_log::LogError> {
        Ok(self
            .records
            .lock()
            .expect("lock")
            .get(&(name.to_owned(), version, t))
            .cloned())
    }

    async fn list_records(&self, name: &str) -> Result<Vec<(u64, u64)>, corium_log::LogError> {
        Ok(self
            .records
            .lock()
            .expect("lock")
            .keys()
            .filter_map(|(record_name, version, t)| {
                (record_name == name).then_some((*version, *t))
            })
            .collect())
    }

    async fn read_legacy_chunk(
        &self,
        name: &str,
        version: u64,
        chunk: u64,
    ) -> Result<Option<Vec<u8>>, corium_log::LogError> {
        Ok(self
            .legacy
            .lock()
            .expect("lock")
            .get(&(name.to_owned(), version, chunk))
            .cloned())
    }

    async fn list_legacy_chunks(
        &self,
        name: &str,
    ) -> Result<Vec<(u64, u64)>, corium_log::LogError> {
        Ok(self
            .legacy
            .lock()
            .expect("lock")
            .keys()
            .filter_map(|(record_name, version, chunk)| {
                (record_name == name).then_some((*version, *chunk))
            })
            .collect())
    }

    async fn delete_all(&self, name: &str) -> Result<(), corium_log::LogError> {
        self.records
            .lock()
            .expect("lock")
            .retain(|(record_name, _, _), _| record_name != name);
        self.legacy
            .lock()
            .expect("lock")
            .retain(|(record_name, _, _), _| record_name != name);
        Ok(())
    }
}

/// Wraps a native storage and counts how many times each version object is
/// read, so a test can assert appends do not re-read the whole log.
#[derive(Default)]
struct CountingNativeStorage {
    inner: TestNativeStorage,
    reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl corium_log::NativeLogStorage for CountingNativeStorage {
    async fn put_record(
        &self,
        name: &str,
        version: u64,
        t: u64,
        bytes: &[u8],
    ) -> Result<bool, corium_log::LogError> {
        self.inner.put_record(name, version, t, bytes).await
    }

    async fn read_record(
        &self,
        name: &str,
        version: u64,
        t: u64,
    ) -> Result<Option<Vec<u8>>, corium_log::LogError> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.read_record(name, version, t).await
    }

    async fn list_records(&self, name: &str) -> Result<Vec<(u64, u64)>, corium_log::LogError> {
        self.inner.list_records(name).await
    }

    async fn read_legacy_chunk(
        &self,
        name: &str,
        version: u64,
        chunk: u64,
    ) -> Result<Option<Vec<u8>>, corium_log::LogError> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.read_legacy_chunk(name, version, chunk).await
    }

    async fn list_legacy_chunks(
        &self,
        name: &str,
    ) -> Result<Vec<(u64, u64)>, corium_log::LogError> {
        self.inner.list_legacy_chunks(name).await
    }

    async fn delete_all(&self, name: &str) -> Result<(), corium_log::LogError> {
        self.inner.delete_all(name).await
    }
}

#[tokio::test]
async fn native_versioned_log_append_does_not_reread_the_whole_log() {
    use std::sync::atomic::Ordering;
    let storage = std::sync::Arc::new(CountingNativeStorage::default());
    let log = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 1)
        .await
        .expect("open");
    // Opening reads the tail to establish the next `t`.
    let opened_reads = storage.reads.load(Ordering::Relaxed);
    for t in 1..=64 {
        log.append_async(&record(t)).await.expect("append");
    }
    // Every append is a single create-only write of its own object: it reads
    // nothing, so per-transaction cost never grows with the history (the old
    // quadratic write path re-read and re-copied the whole log each append).
    assert_eq!(
        storage.reads.load(Ordering::Relaxed),
        opened_reads,
        "appends must not read any log object"
    );
    // The cached appends are durable and replay in order.
    let replayed = log.replay_async().await.expect("replay");
    assert_eq!(replayed.len(), 64);
    assert_eq!(replayed.first().expect("first").t, 1);
    assert_eq!(replayed.last().expect("last").t, 64);
    // A fresh open recovers exactly the same durable history from the store.
    let reopened = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 1)
        .await
        .expect("reopen");
    assert_eq!(reopened.replay_async().await.expect("replay"), replayed);
}

#[tokio::test]
async fn native_versioned_log_writes_one_object_per_record() {
    use corium_log::NativeLogStorage;
    let storage = std::sync::Arc::new(TestNativeStorage::default());
    let log = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 1)
        .await
        .expect("open");
    for t in 1..=6 {
        log.append_async(&record(t)).await.expect("append");
    }
    // One object per transaction — no chunking, whatever the record size.
    let records = storage.list_records("db").await.expect("records");
    assert_eq!(records.len(), 6);
    let replayed = log.replay_async().await.expect("replay");
    assert_eq!(replayed.len(), 6);
    assert!(replayed.iter().zip(1..).all(|(record, t)| record.t == t));
    // A large record still fits: there is no size cap to cross.
    let reopened = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 1)
        .await
        .expect("reopen");
    reopened
        .append_async(&big_record(7, 512 * 1024))
        .await
        .expect("append 7");
    let replayed = reopened.replay_async().await.expect("replay after reopen");
    assert_eq!(replayed.len(), 7);
    assert_eq!(replayed.last().expect("last").t, 7);
}

#[tokio::test]
async fn native_versioned_log_replays_legacy_chunks_then_appends_records() {
    use corium_log::NativeLogStorage;
    // An older binary wrote records 1..=3 as a single legacy chunk 0 under
    // version 1, then record 4 into a rolled chunk 1 — the pre-per-record
    // layout, several framed records packed per chunk object.
    let storage = std::sync::Arc::new(TestNativeStorage::default());
    let mut chunk0 = Vec::new();
    for t in 1..=3 {
        corium_log::append_framed_record(&mut chunk0, &record(t)).expect("frame");
    }
    storage.seed_legacy_chunk("db", 1, 0, chunk0);
    let mut chunk1 = Vec::new();
    corium_log::append_framed_record(&mut chunk1, &record(4)).expect("frame");
    storage.seed_legacy_chunk("db", 1, 1, chunk1);

    // The upgraded binary opens under a fresh lease version and replays the
    // legacy log read-only.
    let log = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 2)
        .await
        .expect("open");
    assert_eq!(
        log.replay_async().await.expect("replay legacy"),
        (1..=4).map(record).collect::<Vec<_>>()
    );

    // It continues appending in the per-record layout under its own version;
    // the merged history stays contiguous across the format boundary.
    log.append_async(&record(5)).await.expect("append 5");
    log.append_async(&record(6)).await.expect("append 6");
    assert_eq!(
        log.replay_async().await.expect("replay merged"),
        (1..=6).map(record).collect::<Vec<_>>()
    );
    // The new records are per-record objects; the legacy chunks are untouched.
    assert_eq!(storage.list_records("db").await.expect("records").len(), 2);
    assert_eq!(
        storage.list_legacy_chunks("db").await.expect("chunks").len(),
        2
    );
}

#[tokio::test]
async fn native_versioned_log_uses_store_versions_and_takeover_cutoff() {
    let storage = std::sync::Arc::new(TestNativeStorage::default());
    let v1 = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 1)
        .await
        .expect("open v1");
    v1.append_async(&record(1)).await.expect("append 1");
    v1.append_async(&record(2)).await.expect("append 2");

    let v2 = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 2)
        .await
        .expect("open v2");
    v2.append_async(&record(3)).await.expect("append 3");
    v1.append_async(&record(3)).await.expect("stale append");
    v1.append_async(&record(4)).await.expect("stale append 4");

    assert_eq!(
        v2.replay_async().await.expect("replay"),
        vec![record(1), record(2), record(3)]
    );
}
