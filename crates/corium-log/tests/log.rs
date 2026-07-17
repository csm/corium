//! Durable log conformance tests.

use corium_core::{Datom, EntityId, Value};
use corium_log::{FileLog, TransactionLog, TxRecord};
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
