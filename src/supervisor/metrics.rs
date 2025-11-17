//! Supervisor metrics aggregation and reporting.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

use crate::clock::{Duration, Timestamp};

/// Aggregated metrics from supervisor's periodic collection.
///
/// Metrics are stored atomically for lock-free concurrent access.
/// The supervisor aggregates worker metrics every 5 seconds and computes
/// EWMA values for stability.
#[derive(Debug)]
pub(crate) struct SupervisorMetrics {
    // Aggregate totals
    total_commands: AtomicU64,
    total_queries: AtomicU64,

    // Command metrics
    avg_sustained_tps_bits: AtomicU64,       // f64 as bits
    peak_tps_bits: AtomicU64,                // f64 as bits
    avg_command_saturation_bits: AtomicU64,  // f64 as bits (0.0-1.0)
    avg_command_duration_micros: AtomicU64,  // Duration as micros

    // Query metrics
    avg_query_duration_micros: AtomicU64,  // Duration as micros
    query_worker_count: AtomicU8,

    // Timestamp tracking
    last_aggregation: AtomicU64,  // Timestamp as micros
}

impl Default for SupervisorMetrics {
    fn default() -> Self {
        Self {
            total_commands: AtomicU64::new(0),
            total_queries: AtomicU64::new(0),
            avg_sustained_tps_bits: AtomicU64::new(0),
            peak_tps_bits: AtomicU64::new(0),
            avg_command_saturation_bits: AtomicU64::new(0),
            avg_command_duration_micros: AtomicU64::new(0),
            avg_query_duration_micros: AtomicU64::new(0),
            query_worker_count: AtomicU8::new(0),
            last_aggregation: AtomicU64::new(Timestamp::now().as_micros()),
        }
    }
}

impl SupervisorMetrics {
    // Command metrics accessors

    pub(crate) fn fetch_total_commands(&self) -> u64 {
        self.total_commands.load(Ordering::Relaxed)
    }

    pub(crate) fn put_total_commands(&self, commands: u64) {
        self.total_commands.store(commands, Ordering::Relaxed);
    }

    pub(crate) fn fetch_avg_sustained_tps(&self) -> f64 {
        let bits = self.avg_sustained_tps_bits.load(Ordering::Relaxed);
        f64::from_bits(bits)
    }

    pub(crate) fn put_avg_sustained_tps(&self, tps: f64) {
        self.avg_sustained_tps_bits
            .store(tps.to_bits(), Ordering::Relaxed);
    }

    pub(crate) fn fetch_peak_tps(&self) -> f64 {
        let bits = self.peak_tps_bits.load(Ordering::Relaxed);
        f64::from_bits(bits)
    }

    pub(crate) fn put_peak_tps(&self, tps: f64) {
        self.peak_tps_bits.store(tps.to_bits(), Ordering::Relaxed);
    }

    pub(crate) fn fetch_avg_command_saturation(&self) -> f64 {
        let bits = self.avg_command_saturation_bits.load(Ordering::Relaxed);
        f64::from_bits(bits)
    }

    pub(crate) fn put_avg_command_saturation(&self, saturation: f64) {
        self.avg_command_saturation_bits
            .store(saturation.to_bits(), Ordering::Relaxed);
    }

    pub(crate) fn fetch_avg_command_duration(&self) -> Duration {
        Duration::load_atomic(&self.avg_command_duration_micros, Ordering::Relaxed)
    }

    pub(crate) fn put_avg_command_duration(&self, duration: Duration) {
        duration.store_atomic(&self.avg_command_duration_micros, Ordering::Relaxed);
    }

    // Query metrics accessors

    pub(crate) fn fetch_total_queries(&self) -> u64 {
        self.total_queries.load(Ordering::Relaxed)
    }

    pub(crate) fn put_total_queries(&self, queries: u64) {
        self.total_queries.store(queries, Ordering::Relaxed);
    }

    pub(crate) fn fetch_avg_query_duration(&self) -> Duration {
        Duration::load_atomic(&self.avg_query_duration_micros, Ordering::Relaxed)
    }

    pub(crate) fn put_avg_query_duration(&self, duration: Duration) {
        duration.store_atomic(&self.avg_query_duration_micros, Ordering::Relaxed);
    }

    pub(crate) fn fetch_query_worker_count(&self) -> u8 {
        self.query_worker_count.load(Ordering::Relaxed)
    }

    pub(crate) fn put_query_worker_count(&self, count: u8) {
        self.query_worker_count.store(count, Ordering::Relaxed);
    }

    // Timestamp accessors

    pub(crate) fn fetch_last_aggregation(&self) -> Timestamp {
        Timestamp::load_atomic(&self.last_aggregation, Ordering::Relaxed)
    }

    pub(crate) fn put_last_aggregation(&self, ts: Timestamp) {
        ts.store_atomic(&self.last_aggregation, Ordering::Relaxed);
    }

    // Reporting methods

    /// Generate detailed human-readable metrics report.
    pub(crate) fn detailed_report(&self) -> String {
        let sustained_tps = self.fetch_avg_sustained_tps();
        let peak_tps = self.fetch_peak_tps();
        let avg_duration = self.fetch_avg_command_duration();
        let total_commands = self.fetch_total_commands();
        let command_saturation = self.fetch_avg_command_saturation() * 100.0;
        let total_queries = self.fetch_total_queries();
        let query_workers = self.fetch_query_worker_count();
        let query_duration = self.fetch_avg_query_duration();

        format!(
            "Supervisor Metrics Report\n\
            ========================\n\
            \n\
            Commands:\n\
            ---------\n\
            Total:                {}\n\
            Sustained TPS:        {:.0}\n\
            Peak TPS:             {:.0}\n\
            Transaction Latency:  {}\n\
            Channel Saturation:   {:.1}%\n\
            \n\
            Queries:\n\
            --------\n\
            Total:                {}\n\
            Workers:              {}\n\
            Avg Query Duration:   {}",
            total_commands,
            sustained_tps,
            peak_tps,
            avg_duration,
            command_saturation,
            total_queries,
            query_workers,
            query_duration,
        )
    }

    /// Generate parseable key-value metrics report.
    pub(crate) fn kvp_report(&self) -> String {
        let sustained_tps = self.fetch_avg_sustained_tps();
        let peak_tps = self.fetch_peak_tps();
        let avg_duration = self.fetch_avg_command_duration();
        let total_commands = self.fetch_total_commands();
        let command_saturation = self.fetch_avg_command_saturation();
        let total_queries = self.fetch_total_queries();
        let query_workers = self.fetch_query_worker_count();

        format!(
            "total_commands={}\n\
            sustained_tps={:.0}\n\
            peak_tps={:.0}\n\
            avg_duration_micros={}\n\
            command_saturation={:.4}\n\
            total_queries={}\n\
            query_workers={}",
            total_commands,
            sustained_tps,
            peak_tps,
            avg_duration.as_micros(),
            command_saturation,
            total_queries,
            query_workers
        )
    }
}

/// Display implementation for inline metrics logging.
impl std::fmt::Display for SupervisorMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sustained_tps = self.fetch_avg_sustained_tps();
        let avg_duration = self.fetch_avg_command_duration();
        let total_commands = self.fetch_total_commands();
        let command_saturation = self.fetch_avg_command_saturation() * 100.0;
        let total_queries = self.fetch_total_queries();
        let query_workers = self.fetch_query_worker_count();

        write!(
            f,
            "Commands: {} @ {:.0} TPS ({}) ({:.2}% sat) | Queries: {} ({} workers)",
            total_commands, sustained_tps, avg_duration, command_saturation, total_queries, query_workers
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_default_initialization() {
        let metrics = SupervisorMetrics::default();

        assert_eq!(metrics.fetch_total_commands(), 0);
        assert_eq!(metrics.fetch_total_queries(), 0);
        assert_eq!(metrics.fetch_avg_sustained_tps(), 0.0);
        assert_eq!(metrics.fetch_peak_tps(), 0.0);
        assert_eq!(metrics.fetch_avg_command_saturation(), 0.0);
        assert!(metrics.fetch_avg_command_duration().is_zero());
        assert!(metrics.fetch_avg_query_duration().is_zero());
        assert_eq!(metrics.fetch_query_worker_count(), 0);
    }

    #[test]
    fn metrics_command_totals() {
        let metrics = SupervisorMetrics::default();

        metrics.put_total_commands(100);
        assert_eq!(metrics.fetch_total_commands(), 100);

        metrics.put_total_commands(250);
        assert_eq!(metrics.fetch_total_commands(), 250);
    }

    #[test]
    fn metrics_tps_tracking() {
        let metrics = SupervisorMetrics::default();

        metrics.put_avg_sustained_tps(150.5);
        assert_eq!(metrics.fetch_avg_sustained_tps(), 150.5);

        metrics.put_peak_tps(200.0);
        assert_eq!(metrics.fetch_peak_tps(), 200.0);
    }

    #[test]
    fn metrics_saturation_tracking() {
        let metrics = SupervisorMetrics::default();

        metrics.put_avg_command_saturation(0.75);
        assert_eq!(metrics.fetch_avg_command_saturation(), 0.75);
    }

    #[test]
    fn metrics_duration_tracking() {
        let metrics = SupervisorMetrics::default();

        let duration = Duration::from_micros(50000); // 50ms = 50000μs
        metrics.put_avg_command_duration(duration);
        assert_eq!(metrics.fetch_avg_command_duration(), duration);

        let query_duration = Duration::from_micros(500);
        metrics.put_avg_query_duration(query_duration);
        assert_eq!(metrics.fetch_avg_query_duration(), query_duration);
    }

    #[test]
    fn metrics_query_workers() {
        let metrics = SupervisorMetrics::default();

        metrics.put_query_worker_count(4);
        assert_eq!(metrics.fetch_query_worker_count(), 4);
    }

    #[test]
    fn metrics_timestamp_tracking() {
        let metrics = SupervisorMetrics::default();

        let ts = Timestamp::now();
        metrics.put_last_aggregation(ts);
        assert_eq!(metrics.fetch_last_aggregation(), ts);
    }

    #[test]
    fn metrics_detailed_report_format() {
        let metrics = SupervisorMetrics::default();

        metrics.put_total_commands(1000);
        metrics.put_avg_sustained_tps(150.5);
        metrics.put_avg_command_duration(Duration::from_micros(500));

        let report = metrics.detailed_report();

        assert!(report.contains("Total:                1000"));
        assert!(report.contains("Sustained TPS:        150"));
        assert!(report.contains("500μs"));
    }

    #[test]
    fn metrics_kvp_report_format() {
        let metrics = SupervisorMetrics::default();

        metrics.put_total_commands(1000);
        metrics.put_avg_sustained_tps(150.5);

        let report = metrics.kvp_report();

        assert!(report.contains("total_commands=1000"));
        assert!(report.contains("sustained_tps=150"));
    }

    #[test]
    fn metrics_display_implementation() {
        let metrics = SupervisorMetrics::default();

        metrics.put_total_commands(1000);
        metrics.put_avg_sustained_tps(150.0);
        metrics.put_avg_command_duration(Duration::from_micros(500));

        let display = format!("{}", metrics);

        assert!(display.contains("1000"));
        assert!(display.contains("150"));
        assert!(display.contains("500μs"));
    }
}
