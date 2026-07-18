//! Lightweight process metrics with Prometheus text exposition.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Transactor process counters and gauges.
#[derive(Default)]
pub struct Metrics {
    tx_total: AtomicU64,
    tx_failed: AtomicU64,
    tx_latency_micros: AtomicU64,
    tx_queue_depth: AtomicU64,
    index_runs: AtomicU64,
    index_latency_micros: AtomicU64,
    gc_runs: AtomicU64,
    gc_swept: AtomicU64,
    gc_retained: AtomicU64,
}

/// Point-in-time counters used by the Status RPC.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Snapshot {
    /// Transactions attempted.
    pub tx_total: u64,
    /// Transactions that failed.
    pub tx_failed: u64,
    /// Requests currently waiting for the commit lock.
    pub queue_depth: u64,
    /// Completed indexing runs.
    pub index_runs: u64,
    /// Completed garbage-collection runs.
    pub gc_runs: u64,
    /// Blobs deleted by GC.
    pub gc_swept: u64,
}

impl Metrics {
    /// Returns a consistent-enough point-in-time counter snapshot.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            tx_total: self.tx_total.load(Ordering::Relaxed),
            tx_failed: self.tx_failed.load(Ordering::Relaxed),
            queue_depth: self.tx_queue_depth.load(Ordering::Relaxed),
            index_runs: self.index_runs.load(Ordering::Relaxed),
            gc_runs: self.gc_runs.load(Ordering::Relaxed),
            gc_swept: self.gc_swept.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn queue_enter(&self) {
        self.tx_queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn queue_leave(&self) {
        self.tx_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn record_tx(&self, elapsed: Duration, success: bool) {
        self.tx_total.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.tx_failed.fetch_add(1, Ordering::Relaxed);
        }
        self.tx_latency_micros.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_index(&self, elapsed: Duration) {
        self.index_runs.fetch_add(1, Ordering::Relaxed);
        self.index_latency_micros.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_gc(&self, swept: u64, retained: u64) {
        self.gc_runs.fetch_add(1, Ordering::Relaxed);
        self.gc_swept.fetch_add(swept, Ordering::Relaxed);
        self.gc_retained.fetch_add(retained, Ordering::Relaxed);
    }

    /// Renders Prometheus's text exposition format.
    #[must_use]
    pub fn prometheus(&self) -> String {
        let tx_total = self.tx_total.load(Ordering::Relaxed);
        let tx_failed = self.tx_failed.load(Ordering::Relaxed);
        let tx_micros = self.tx_latency_micros.load(Ordering::Relaxed);
        let queue = self.tx_queue_depth.load(Ordering::Relaxed);
        let index_runs = self.index_runs.load(Ordering::Relaxed);
        let index_micros = self.index_latency_micros.load(Ordering::Relaxed);
        let gc_runs = self.gc_runs.load(Ordering::Relaxed);
        let gc_swept = self.gc_swept.load(Ordering::Relaxed);
        let gc_retained = self.gc_retained.load(Ordering::Relaxed);
        let (tx_seconds, tx_subseconds) = (tx_micros / 1_000_000, tx_micros % 1_000_000);
        let (index_seconds, index_subseconds) =
            (index_micros / 1_000_000, index_micros % 1_000_000);
        format!(
            "# TYPE corium_transactor_transactions_total counter\n\
corium_transactor_transactions_total {tx_total}\n\
# TYPE corium_transactor_transaction_failures_total counter\n\
corium_transactor_transaction_failures_total {tx_failed}\n\
# TYPE corium_transactor_transaction_latency_seconds summary\n\
corium_transactor_transaction_latency_seconds_count {tx_total}\n\
corium_transactor_transaction_latency_seconds_sum {tx_seconds}.{tx_subseconds:06}\n\
# TYPE corium_transactor_queue_depth gauge\n\
corium_transactor_queue_depth {queue}\n\
# TYPE corium_transactor_index_duration_seconds summary\n\
corium_transactor_index_duration_seconds_count {index_runs}\n\
corium_transactor_index_duration_seconds_sum {index_seconds}.{index_subseconds:06}\n\
# TYPE corium_transactor_gc_runs_total counter\n\
corium_transactor_gc_runs_total {gc_runs}\n\
corium_transactor_gc_swept_blobs_total {gc_swept}\n\
corium_transactor_gc_retained_blobs_total {gc_retained}\n",
        )
    }
}
