//! Peer-server Prometheus metrics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Query counters recorded by a peer server.
#[derive(Default)]
pub struct Metrics {
    queries: AtomicU64,
    query_latency_micros: AtomicU64,
    query_fuel: AtomicU64,
}

impl Metrics {
    pub(crate) fn record_query(&self, elapsed: Duration, fuel: usize) {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.query_latency_micros.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.query_fuel
            .fetch_add(u64::try_from(fuel).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// Renders Prometheus's text exposition format.
    #[must_use]
    pub fn prometheus(&self) -> String {
        let queries = self.queries.load(Ordering::Relaxed);
        let latency = self.query_latency_micros.load(Ordering::Relaxed);
        let fuel = self.query_fuel.load(Ordering::Relaxed);
        let (seconds, subseconds) = (latency / 1_000_000, latency % 1_000_000);
        format!(
            "# TYPE corium_peer_queries_total counter\n\
corium_peer_queries_total {queries}\n\
# TYPE corium_peer_query_latency_seconds summary\n\
corium_peer_query_latency_seconds_count {queries}\n\
corium_peer_query_latency_seconds_sum {seconds}.{subseconds:06}\n\
# TYPE corium_peer_query_fuel_spent_total counter\n\
corium_peer_query_fuel_spent_total {fuel}\n",
        )
    }
}
