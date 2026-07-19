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
