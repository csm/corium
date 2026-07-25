//! Peer-server Prometheus metrics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::SegmentCacheMetricsHandle;

/// Query counters recorded by a peer server.
pub struct Metrics {
    queries: AtomicU64,
    query_latency_micros: AtomicU64,
    query_fuel: AtomicU64,
    segment_cache: Option<SegmentCacheMetricsHandle>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Metrics {
    pub(crate) fn new(segment_cache: Option<SegmentCacheMetricsHandle>) -> Self {
        Self {
            queries: AtomicU64::new(0),
            query_latency_micros: AtomicU64::new(0),
            query_fuel: AtomicU64::new(0),
            segment_cache,
        }
    }

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
    #[allow(clippy::too_many_lines)] // Keep the metric families together so labels remain auditable.
    pub fn prometheus(&self) -> String {
        let queries = self.queries.load(Ordering::Relaxed);
        let latency = self.query_latency_micros.load(Ordering::Relaxed);
        let fuel = self.query_fuel.load(Ordering::Relaxed);
        let (seconds, subseconds) = (latency / 1_000_000, latency % 1_000_000);
        let mut output = format!(
            "# TYPE corium_peer_queries_total counter\n\
corium_peer_queries_total {queries}\n\
# TYPE corium_peer_query_latency_seconds summary\n\
corium_peer_query_latency_seconds_count {queries}\n\
corium_peer_query_latency_seconds_sum {seconds}.{subseconds:06}\n\
# TYPE corium_peer_query_fuel_spent_total counter\n\
corium_peer_query_fuel_spent_total {fuel}\n",
        );
        if let Some(cache) = &self.segment_cache {
            use std::fmt::Write as _;
            let metric = &cache.metrics;
            let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
            writeln!(
                output,
                "# TYPE corium_peer_segment_cache_requests_total counter"
            )
            .unwrap();
            for (result, tier, value) in [
                ("hit", "memory", load(&metric.memory_hits)),
                ("miss", "memory", load(&metric.memory_misses)),
                ("hit", "disk", load(&metric.disk_hits)),
                ("miss", "disk", load(&metric.disk_misses)),
            ] {
                writeln!(output, "corium_peer_segment_cache_requests_total{{result=\"{result}\",tier=\"{tier}\"}} {value}").unwrap();
            }
            writeln!(
                output,
                "# TYPE corium_peer_segment_cache_native_fetches_total counter"
            )
            .unwrap();
            for (result, value) in [
                ("found", load(&metric.native_found)),
                ("not_found", load(&metric.native_not_found)),
                ("error", load(&metric.native_errors)),
            ] {
                writeln!(
                    output,
                    "corium_peer_segment_cache_native_fetches_total{{result=\"{result}\"}} {value}"
                )
                .unwrap();
            }
            writeln!(
                output,
                "# TYPE corium_peer_segment_cache_bytes_read_total counter"
            )
            .unwrap();
            for (source, value) in [
                ("memory", load(&metric.bytes_memory)),
                ("disk", load(&metric.bytes_disk)),
                ("native", load(&metric.bytes_native)),
            ] {
                writeln!(
                    output,
                    "corium_peer_segment_cache_bytes_read_total{{source=\"{source}\"}} {value}"
                )
                .unwrap();
            }
            writeln!(
                output,
                "# TYPE corium_peer_segment_cache_admissions_total counter"
            )
            .unwrap();
            for (result, value) in [
                ("admitted", load(&metric.admissions)),
                ("too_large", load(&metric.too_large)),
                ("io_error", load(&metric.admission_errors)),
            ] {
                writeln!(
                    output,
                    "corium_peer_segment_cache_admissions_total{{result=\"{result}\"}} {value}"
                )
                .unwrap();
            }
            for (name, value) in [
                ("evictions_total", load(&metric.evictions)),
                ("evicted_bytes_total", load(&metric.evicted_bytes)),
                ("corruptions_total", load(&metric.corruptions)),
                ("coalesced_waiters_total", load(&metric.coalesced_waiters)),
            ] {
                writeln!(output, "# TYPE corium_peer_segment_cache_{name} counter\ncorium_peer_segment_cache_{name} {value}").unwrap();
            }
            for (name, memory, disk) in [
                (
                    "used_bytes",
                    load(&metric.memory_bytes),
                    load(&metric.disk_bytes),
                ),
                (
                    "entries",
                    load(&metric.memory_entries),
                    load(&metric.disk_entries),
                ),
                ("capacity_bytes", cache.memory_capacity, cache.disk_capacity),
            ] {
                writeln!(output, "# TYPE corium_peer_segment_cache_{name} gauge").unwrap();
                writeln!(
                    output,
                    "corium_peer_segment_cache_{name}{{tier=\"memory\"}} {memory}"
                )
                .unwrap();
                writeln!(
                    output,
                    "corium_peer_segment_cache_{name}{{tier=\"disk\"}} {disk}"
                )
                .unwrap();
            }
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_store::SegmentCacheMetrics;
    use std::sync::Arc;

    #[test]
    fn prometheus_includes_bounded_segment_cache_series() {
        let cache = Arc::new(SegmentCacheMetrics::default());
        cache.disk_hits.store(2, Ordering::Relaxed);
        cache.native_found.store(1, Ordering::Relaxed);
        cache.disk_bytes.store(99, Ordering::Relaxed);
        let metrics = Metrics::new(Some(SegmentCacheMetricsHandle {
            metrics: cache,
            disk_capacity: 1024,
            memory_capacity: 64,
        }));
        let output = metrics.prometheus();
        assert!(
            output.contains(
                "corium_peer_segment_cache_requests_total{result=\"hit\",tier=\"disk\"} 2"
            )
        );
        assert!(
            output.contains("corium_peer_segment_cache_native_fetches_total{result=\"found\"} 1")
        );
        assert!(output.contains("corium_peer_segment_cache_used_bytes{tier=\"disk\"} 99"));
        assert!(output.contains("corium_peer_segment_cache_capacity_bytes{tier=\"memory\"} 64"));
        assert!(!output.contains("directory"));
    }
}
