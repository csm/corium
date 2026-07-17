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
