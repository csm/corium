//! Durable log conformance tests.

use corium_core::{Datom, EntityId, Value};
use corium_crypt::SecretKey;
use corium_log::{
    FileLog, LogCipher, LogError, MemLogRegistry, TransactionLog, TxRecord, VersionedLog,
    append_framed_record, decode_framed_records,
};
use std::io::Write;
use std::sync::Arc;

// Frame header (8) + transaction number (8) + final byte of tx_instant (7).
const TX_INSTANT_LOW_BYTE_OFFSET: usize = 8 + 8 + 7;

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

/// A record whose value string is `bytes` long, for exercising chunked logs.
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

/// Encodes the length-only frame emitted before checksums were introduced.
fn legacy_frame(record: &TxRecord) -> Vec<u8> {
    let mut frame = Vec::new();
    append_framed_record(&mut frame, record).expect("frame");
    let encoded_len = u64::from_be_bytes(frame[..8].try_into().expect("length"));
    assert_ne!(encoded_len >> 63, 0, "new frames must carry the format bit");
    let payload_len = encoded_len & !(1_u64 << 63);
    frame[..8].copy_from_slice(&payload_len.to_be_bytes());
    frame.truncate(8 + usize::try_from(payload_len).expect("payload length"));
    frame
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
fn filesystem_replay_streams_large_ranges_in_bounded_chunks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let log = FileLog::open(path).expect("open");
    let records = vec![
        big_record(1, 2 * 1024 * 1024),
        big_record(2, 2 * 1024 * 1024),
        big_record(3, 2 * 1024 * 1024),
    ];
    for record in &records {
        log.append(record).expect("append");
    }
    assert_eq!(log.replay().expect("chunked replay"), records);
}

#[cfg(unix)]
#[test]
fn filesystem_log_uses_its_cached_file_handle_after_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let moved = dir.path().join("moved.log");
    let log = FileLog::open(&path).expect("open");
    log.append(&record(1)).expect("append 1");

    // Renaming an open file leaves the descriptor attached to the same inode
    // on Unix. Both range reads and appends should use that descriptor rather
    // than trying to reopen the original path.
    std::fs::rename(&path, &moved).expect("rename open log");
    assert_eq!(log.tx_range(1, Some(2)).expect("range"), vec![record(1)]);
    log.append(&record(2)).expect("append 2");
    assert!(!path.exists());
    drop(log);

    assert_eq!(
        FileLog::open(moved)
            .expect("reopen moved log")
            .replay()
            .expect("replay"),
        vec![record(1), record(2)]
    );
}

#[cfg(unix)]
#[test]
fn filesystem_range_reads_only_the_indexed_byte_span() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let log = FileLog::open(&path).expect("open");
    log.append(&record(1)).expect("append 1");
    log.append(&record(2)).expect("append 2");
    log.append(&record(3)).expect("append 3");

    // Corrupt a same-length byte in the first frame after open. A tail range
    // uses its indexed offset and does not revisit or decode that prefix.
    let mut bytes = std::fs::read(&path).expect("read log");
    bytes[TX_INSTANT_LOW_BYTE_OFFSET] ^= 1;
    std::fs::write(&path, bytes).expect("rewrite corrupt prefix");
    assert_eq!(log.tx_range(3, None).expect("tail range"), vec![record(3)]);
    assert!(matches!(log.replay(), Err(LogError::Corrupt)));
}

#[test]
fn filesystem_log_detects_a_fully_written_corrupt_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let log = FileLog::open(&path).expect("open");
    log.append(&record(1)).expect("append");
    drop(log);

    // Change only the low byte of tx_instant. The payload remains structurally
    // valid and would decode without an integrity check.
    let mut bytes = std::fs::read(&path).expect("read log");
    bytes[TX_INSTANT_LOW_BYTE_OFFSET] ^= 1;
    std::fs::write(&path, bytes).expect("rewrite corrupt log");

    assert!(matches!(FileLog::open(&path), Err(LogError::Corrupt)));
}

#[test]
fn filesystem_log_reads_legacy_frames_then_appends_checksummed_frames() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    std::fs::write(&path, legacy_frame(&record(1))).expect("seed legacy log");

    let log = FileLog::open(&path).expect("open legacy log");
    assert_eq!(log.replay().expect("legacy replay"), vec![record(1)]);
    log.append(&record(2)).expect("append checksummed record");
    drop(log);

    let log = FileLog::open(&path).expect("reopen mixed log");
    assert_eq!(
        log.replay().expect("mixed replay"),
        vec![record(1), record(2)]
    );
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
fn torn_checksum_is_dropped_with_its_unacked_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let log = FileLog::open(&path).expect("open");
    log.append(&record(1)).expect("append 1");
    log.append(&record(2)).expect("append 2");
    drop(log);

    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open raw");
    file.set_len(file.metadata().expect("metadata").len() - 2)
        .expect("tear checksum");
    drop(file);

    let log = FileLog::open(&path).expect("drop torn checksummed frame");
    assert_eq!(log.replay().expect("replay"), vec![record(1)]);
    log.append(&record(2)).expect("append replacement");
}

#[test]
fn framed_records_verify_checksums_and_accept_legacy_frames() {
    let mut checksummed = Vec::new();
    append_framed_record(&mut checksummed, &record(1)).expect("frame");
    assert_eq!(
        decode_framed_records(&checksummed).expect("decode"),
        vec![record(1)]
    );

    // As in the filesystem test, this mutation leaves a decodable payload.
    checksummed[8 + 15] ^= 1;
    assert!(matches!(
        decode_framed_records(&checksummed),
        Err(LogError::Corrupt)
    ));
    assert_eq!(
        decode_framed_records(&legacy_frame(&record(1))).expect("decode legacy"),
        vec![record(1)]
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
fn versioned_read_handle_indexes_only_new_file_tails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let v1 = VersionedLog::open(dir.path(), "db", 1).expect("open v1");
    v1.append(&record(1)).expect("append 1");
    let reader = VersionedLog::open_read_only(dir.path(), "db").expect("open reader");

    // An already-open reader extends the cached index for an existing file.
    v1.append(&record(2)).expect("append 2");
    assert_eq!(reader.tx_range(2, None).expect("v1 tail"), vec![record(2)]);

    // A lease takeover adds one descriptor/index for the new segment without
    // reopening or rescanning the earlier version.
    let v2 = VersionedLog::open(dir.path(), "db", 2).expect("open v2");
    v2.append(&record(3)).expect("append 3");
    assert_eq!(reader.tx_range(3, None).expect("v2 tail"), vec![record(3)]);
}

#[test]
fn stale_same_version_handle_never_truncates_an_acknowledged_append() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = VersionedLog::open(dir.path(), "db", 1).expect("open first");
    first.append(&record(1)).expect("append 1");
    let stale = VersionedLog::open(dir.path(), "db", 1).expect("open stale");

    first.append(&record(2)).expect("append acknowledged t=2");
    let mut replacement = record(2);
    replacement.tx_instant = 999;
    assert!(matches!(stale.append(&replacement), Err(LogError::Corrupt)));
    drop((first, stale));

    assert_eq!(
        VersionedLog::open_read_only(dir.path(), "db")
            .expect("reopen")
            .replay()
            .expect("replay"),
        vec![record(1), record(2)]
    );
}

#[test]
fn cutoff_dead_suffix_may_have_a_gap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let v1 = VersionedLog::open(dir.path(), "db", 1).expect("open v1");
    for t in 1..=3 {
        v1.append(&record(t)).expect("append v1");
    }
    drop(v1);

    let v2 = VersionedLog::open(dir.path(), "db", 2).expect("open v2");
    v2.append(&record(4)).expect("append 4");
    v2.append(&record(5)).expect("append 5");
    drop(v2);

    // Reopening the deposed version derives merged next_t=6, leaving a gap
    // after its local t=3. That suffix is dead below v2's cutoff and must not
    // make the otherwise contiguous merged log unreadable.
    let deposed = VersionedLog::open(dir.path(), "db", 1).expect("reopen v1");
    deposed.append(&record(6)).expect("append dead t=6");
    drop(deposed);

    assert_eq!(
        VersionedLog::open_read_only(dir.path(), "db")
            .expect("open merged")
            .replay()
            .expect("replay"),
        (1..=5).map(record).collect::<Vec<_>>()
    );
}

#[test]
fn takeover_cutoff_discards_a_deposed_writers_stale_append() {
    let dir = tempfile::tempdir().expect("tempdir");
    let old = VersionedLog::open(dir.path(), "db", 1).expect("open v1");
    old.append(&record(1)).expect("append 1");
    // Takeover: version 2 replays t=1 and commits its own t=2.
    let new = VersionedLog::open(dir.path(), "db", 2).expect("open v2");
    new.append(&record(2)).expect("new owner's t=2");
    // A range refresh can discover the successor, but must not advance the
    // deposed writer's local append position: an already in-flight t=2 still
    // has to land in v1 so the takeover cutoff can discard it.
    assert_eq!(
        old.replay().expect("old reader refresh"),
        vec![record(1), record(2)]
    );
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
    async fn put_batch(
        &self,
        name: &str,
        version: u64,
        records: &[(u64, Vec<u8>)],
    ) -> Result<bool, corium_log::LogError> {
        let Some((last_t, _)) = records.last() else {
            return Ok(true);
        };
        let mut guard = self.records.lock().expect("lock");
        let key = (name.to_owned(), version, *last_t);
        if guard.contains_key(&key) {
            return Ok(false);
        }
        let mut bytes = Vec::new();
        for (_, framed) in records {
            bytes.extend_from_slice(framed);
        }
        guard.insert(key, bytes);
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
            .filter_map(|(record_name, version, t)| (record_name == name).then_some((*version, *t)))
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
    async fn put_batch(
        &self,
        name: &str,
        version: u64,
        records: &[(u64, Vec<u8>)],
    ) -> Result<bool, corium_log::LogError> {
        self.inner.put_batch(name, version, records).await
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
async fn native_read_only_open_defers_the_scan_and_rejects_appends() {
    use std::sync::atomic::Ordering;

    let storage = std::sync::Arc::new(CountingNativeStorage::default());
    let writer = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 1)
        .await
        .expect("writer");
    writer.append_async(&record(1)).await.expect("append");

    let reads = storage.reads.load(Ordering::Relaxed);
    let reader =
        corium_log::NativeVersionedLog::open_read_only(std::sync::Arc::clone(&storage), "db");
    assert_eq!(
        storage.reads.load(Ordering::Relaxed),
        reads,
        "read-only open should not initialize writer state"
    );
    assert!(matches!(
        reader.append_async(&record(2)).await,
        Err(corium_log::LogError::Native(message)) if message.contains("read-only")
    ));
    assert_eq!(
        reader.replay_async().await.expect("replay"),
        vec![record(1)]
    );
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
async fn native_versioned_log_batches_a_run_into_one_object() {
    use corium_log::NativeLogStorage;
    let storage = std::sync::Arc::new(TestNativeStorage::default());
    let log = corium_log::NativeVersionedLog::open(std::sync::Arc::clone(&storage), "db", 1)
        .await
        .expect("open");
    // A batch of four is written as one object, keyed by its last `t`.
    let batch: Vec<_> = (1..=4).map(record).collect();
    log.append_batch_async(&batch).await.expect("append batch");
    assert_eq!(
        storage.list_records("db").await.expect("records"),
        vec![(1, 4)]
    );
    // A single append and a second batch continue the contiguous run.
    log.append_async(&record(5)).await.expect("append 5");
    log.append_batch_async(&[record(6), record(7)])
        .await
        .expect("append batch 2");
    assert_eq!(
        log.replay_async().await.expect("replay"),
        (1..=7).map(record).collect::<Vec<_>>()
    );
    // One object per write: the two batches (keyed 4 and 7) and the single (5).
    let mut keys = storage.list_records("db").await.expect("records");
    keys.sort_unstable();
    assert_eq!(keys, vec![(1, 4), (1, 5), (1, 7)]);
    // A batch that does not start at the next `t` is rejected.
    assert!(
        log.append_batch_async(&[record(9), record(10)])
            .await
            .is_err()
    );
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
        chunk0.extend_from_slice(&legacy_frame(&record(t)));
    }
    storage.seed_legacy_chunk("db", 1, 0, chunk0);
    let chunk1 = legacy_frame(&record(4));
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
        storage
            .list_legacy_chunks("db")
            .await
            .expect("chunks")
            .len(),
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

fn cipher(lineage: &str) -> Arc<LogCipher> {
    Arc::new(LogCipher::with_key(lineage, 1, SecretKey::new([0x5a; 32])))
}

/// The sentinel a `--storage-key` deployment must never find in its data
/// directory. `record`'s datoms carry longs, so tests that scan for plaintext
/// use this string value instead.
const SENTINEL: &str = "salary-is-140000";

fn sentinel_record(t: u64) -> TxRecord {
    let signed_t = i64::try_from(t).expect("test transaction fits i64");
    TxRecord {
        t,
        tx_instant: 100 + signed_t,
        datoms: vec![Datom {
            e: EntityId::from_raw(t),
            a: EntityId::from_raw(2),
            v: Value::Str(SENTINEL.into()),
            tx: EntityId::from_raw(100 + t),
            added: true,
        }],
    }
}

#[test]
fn sealed_filesystem_log_round_trips_and_stores_no_plaintext() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let log = FileLog::open_sealed(&path, cipher("people")).expect("open");
    log.append(&sentinel_record(1)).expect("append 1");
    log.append(&sentinel_record(2)).expect("append 2");

    let bytes = std::fs::read(&path).expect("read log");
    assert!(
        !bytes
            .windows(SENTINEL.len())
            .any(|window| window == SENTINEL.as_bytes()),
        "sealed log must not hold plaintext datom values"
    );

    let reopened = FileLog::open_sealed(&path, cipher("people")).expect("reopen");
    assert_eq!(
        reopened.replay().expect("replay"),
        vec![sentinel_record(1), sentinel_record(2)]
    );
    assert_eq!(
        reopened.tx_range(2, None).expect("range"),
        vec![sentinel_record(2)]
    );
    reopened.append(&sentinel_record(3)).expect("append 3");
    assert_eq!(reopened.replay().expect("replay").len(), 3);
}

#[test]
fn sealed_log_refuses_the_wrong_key_and_the_wrong_database() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("transactions.log");
    let log = FileLog::open_sealed(&path, cipher("people")).expect("open");
    log.append(&record(1)).expect("append 1");

    let other_key = Arc::new(LogCipher::with_key("people", 1, SecretKey::new([1; 32])));
    assert!(matches!(
        FileLog::open_sealed(&path, other_key),
        Err(LogError::Crypt(_))
    ));
    assert!(matches!(
        FileLog::open_sealed(&path, cipher("payroll")),
        Err(LogError::Crypt(_))
    ));
    // An epoch this process cannot resolve is named, not guessed at.
    let future_epoch =
        Arc::new(LogCipher::new("people", 2, [(2, SecretKey::new([2; 32]))]).expect("cipher"));
    assert!(matches!(
        FileLog::open_sealed(&path, future_epoch),
        Err(LogError::MissingKeyEpoch(1))
    ));
}

#[test]
fn encryption_state_mismatches_fail_loudly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sealed_path = dir.path().join("sealed.log");
    FileLog::open_sealed(&sealed_path, cipher("people"))
        .expect("open sealed")
        .append(&record(1))
        .expect("append");
    assert!(matches!(
        FileLog::open(&sealed_path),
        Err(LogError::Encrypted)
    ));

    let plain_path = dir.path().join("plain.log");
    FileLog::open(&plain_path)
        .expect("open plain")
        .append(&record(1))
        .expect("append");
    assert!(matches!(
        FileLog::open_sealed(&plain_path, cipher("people")),
        Err(LogError::Unencrypted)
    ));
}

#[test]
fn sealed_records_are_bound_to_their_version_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let v1 = VersionedLog::open_sealed(dir.path(), "db", 1, cipher("db")).expect("open v1");
    v1.append(&record(1)).expect("append 1");
    let v2 = VersionedLog::open_sealed(dir.path(), "db", 2, cipher("db")).expect("open v2");
    v2.append(&record(2)).expect("append 2");
    assert_eq!(
        VersionedLog::open_read_only_sealed(dir.path(), "db", cipher("db"))
            .expect("reader")
            .replay()
            .expect("replay"),
        vec![record(1), record(2)]
    );

    // Copying a frame into another lease version's file cannot smuggle a
    // record past the takeover cutoff: the version is authenticated.
    let frame = std::fs::read(dir.path().join("db.v1.log")).expect("read v1");
    let mut forged = std::fs::read(dir.path().join("db.v2.log")).expect("read v2");
    forged.extend_from_slice(&frame);
    std::fs::write(dir.path().join("db.v2.log"), &forged).expect("write v2");
    assert!(matches!(
        VersionedLog::open_read_only_sealed(dir.path(), "db", cipher("db")),
        Err(LogError::Crypt(_))
    ));
}

#[test]
fn sealed_frames_keep_the_cleartext_framing_contract() {
    let mut sealed = Vec::new();
    corium_log::append_framed_record_sealed(&mut sealed, &record(1), Some(&cipher("db")), 0)
        .expect("frame");
    // Framing is unchanged: high-bit length word, payload, trailing CRC32C.
    let encoded_len = u64::from_be_bytes(sealed[..8].try_into().expect("length"));
    assert_ne!(encoded_len >> 63, 0);
    let payload_len = usize::try_from(encoded_len & !(1_u64 << 63)).expect("payload length");
    assert_eq!(sealed.len(), 8 + payload_len + 4);

    assert_eq!(
        corium_log::decode_framed_records_sealed(&sealed, Some(&cipher("db")), 0).expect("decode"),
        vec![record(1)]
    );
    // A CRC32C failure is still detected without any key.
    let last = sealed.len() - 1;
    sealed[last] ^= 1;
    assert!(matches!(
        corium_log::decode_framed_records_sealed(&sealed, Some(&cipher("db")), 0),
        Err(LogError::Corrupt)
    ));
}

#[tokio::test]
async fn sealed_native_log_round_trips_across_versions() {
    use corium_log::NativeLogStorage;

    let storage = Arc::new(TestNativeStorage::default());
    let v1 =
        corium_log::NativeVersionedLog::open_sealed(Arc::clone(&storage), "db", 1, cipher("db"))
            .await
            .expect("open v1");
    v1.append_async(&sentinel_record(1))
        .await
        .expect("append 1");
    v1.append_async(&sentinel_record(2))
        .await
        .expect("append 2");

    for (_, t) in NativeLogStorage::list_records(storage.as_ref(), "db")
        .await
        .expect("records")
    {
        let bytes = NativeLogStorage::read_record(storage.as_ref(), "db", 1, t)
            .await
            .expect("read")
            .expect("present");
        assert!(
            !bytes
                .windows(SENTINEL.len())
                .any(|window| window == SENTINEL.as_bytes())
        );
    }

    let v2 =
        corium_log::NativeVersionedLog::open_sealed(Arc::clone(&storage), "db", 2, cipher("db"))
            .await
            .expect("open v2");
    v2.append_async(&sentinel_record(3))
        .await
        .expect("append 3");
    assert_eq!(
        v2.replay_async().await.expect("replay"),
        (1..=3).map(sentinel_record).collect::<Vec<_>>()
    );
}
