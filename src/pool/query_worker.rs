//! Query worker implementation (one of N R/O workers).
//!
//! Processes read-only operations without transaction overhead.
//! Tracks metrics with EWMA smoothing for latency monitoring.

use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use super::worker::{SupervisedWorker, Worker, WorkerRunner};
use crate::channel::{QueryReceiver, QueryRequest};
use crate::clock::Duration;
use crate::error::Result;
use crate::handler::DomainHandler;

// --- TYPES ---

/// Metrics tracked by query worker with EWMA duration smoothing.
#[derive(Debug)]
pub(crate) struct QueryMetrics {
    queries_processed: AtomicU64,
    avg_query_duration_micros: AtomicU64,
    last_query_duration_micros: AtomicU64,
}

/// Type alias for supervised query worker.
pub(crate) type SupervisedQueryWorker = SupervisedWorker<QueryMetrics>;

/// Query worker handles read-only operations without transactions.
#[derive(Debug)]
pub(crate) struct QueryWorker<H: DomainHandler> {
    pub(crate) worker: Worker<H>,
    pub(crate) metrics: Arc<QueryMetrics>,
}

// --- IMPLEMENTATIONS ---

impl QueryMetrics {
    /// Create new query metrics initialized to zero.
    pub(crate) fn new() -> Self {
        Self {
            queries_processed: AtomicU64::new(0),
            avg_query_duration_micros: AtomicU64::new(0),
            last_query_duration_micros: AtomicU64::new(0),
        }
    }

    /// Get total queries processed.
    pub(crate) fn fetch_queries_processed(&self) -> u64 {
        self.queries_processed.load(Relaxed)
    }

    /// Get average query duration (EWMA) in microseconds.
    pub(crate) fn fetch_avg_query_duration_micros(&self) -> u64 {
        self.avg_query_duration_micros.load(Relaxed)
    }

    /// Get last query duration in microseconds.
    pub(crate) fn fetch_last_query_duration_micros(&self) -> u64 {
        self.last_query_duration_micros.load(Relaxed)
    }

    /// Record query duration with EWMA smoothing.
    ///
    /// Uses exponentially weighted moving average with alpha=0.2
    /// (20% new value, 80% historical average) to dampen outliers.
    #[inline]
    fn record_query_duration(&self, duration: Duration) {
        let duration_micros = duration.as_micros();

        // Store last duration
        self.last_query_duration_micros
            .store(duration_micros, Relaxed);

        // Update EWMA with alpha=0.2
        let old_avg = self.avg_query_duration_micros.load(Relaxed);
        let new_avg = match old_avg {
            0 => duration_micros,
            _ => {
                // EWMA: new_avg = (duration * 0.2) + (old_avg * 0.8)
                // Simplified: (duration / 5) + (old_avg * 4 / 5)
                (duration_micros / 5) + (old_avg * 4 / 5)
            }
        };

        self.avg_query_duration_micros.store(new_avg, Relaxed);
    }
}

#[async_trait]
impl<H: DomainHandler> WorkerRunner<H> for QueryWorker<H> {
    type Receiver = QueryReceiver<H>;

    async fn run(mut self, mut receiver: Self::Receiver) -> Result<()> {
        while let Some(request) = receiver.recv().await {
            let _ = self.process_query(request);
        }
        Ok(())
    }
}

impl<H: DomainHandler> QueryWorker<H> {
    /// Process a single query without transaction overhead
    ///
    /// Records metrics for duration tracking with EWMA smoothing
    fn process_query(&mut self, request: QueryRequest<H>) -> Result<()> {
        // Check if client still interested before expensive worker
        if request.response.is_closed() {
            return Ok(());
        }

        // Measure query execution duration
        let (result, duration) = Duration::measure(|| {
            self.worker
                .handler
                .handle_query(&self.worker.connection, request.payload)
        });

        // Send response (defensive send - gracefully handles disconnected client)
        let _ = request.response.send(result);

        // Record metrics
        self.metrics.queries_processed.fetch_add(1, Relaxed);
        self.metrics.record_query_duration(duration);

        Ok(())
    }
}

// --- TESTS ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_metrics_new_initializes_to_zero() {
        let metrics = QueryMetrics::new();
        assert_eq!(metrics.fetch_queries_processed(), 0);
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 0);
        assert_eq!(metrics.fetch_last_query_duration_micros(), 0);
    }

    #[test]
    fn query_metrics_fetch_queries_processed() {
        let metrics = QueryMetrics::new();
        metrics.queries_processed.fetch_add(1, Relaxed);
        assert_eq!(metrics.fetch_queries_processed(), 1);
        metrics.queries_processed.fetch_add(5, Relaxed);
        assert_eq!(metrics.fetch_queries_processed(), 6);
    }

    #[test]
    fn query_metrics_first_duration_sets_average() {
        let metrics = QueryMetrics::new();
        let duration = Duration::from_micros(1000);
        metrics.record_query_duration(duration);

        assert_eq!(metrics.fetch_last_query_duration_micros(), 1000);
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 1000);
    }

    #[test]
    fn query_metrics_ewma_smoothing() {
        let metrics = QueryMetrics::new();

        // First duration: 1000 → avg = 1000
        metrics.record_query_duration(Duration::from_micros(1000));
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 1000);

        // Second duration: 2000
        // EWMA = (2000 / 5) + (1000 * 4 / 5) = 400 + 800 = 1200
        metrics.record_query_duration(Duration::from_micros(2000));
        assert_eq!(metrics.fetch_last_query_duration_micros(), 2000);
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 1200);

        // Third duration: 2000
        // EWMA = (2000 / 5) + (1200 * 4 / 5) = 400 + 960 = 1360
        metrics.record_query_duration(Duration::from_micros(2000));
        assert_eq!(metrics.fetch_last_query_duration_micros(), 2000);
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 1360);
    }

    #[test]
    fn query_metrics_ewma_dampens_outliers() {
        let metrics = QueryMetrics::new();

        // Establish baseline: 100μs
        metrics.record_query_duration(Duration::from_micros(100));
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 100);

        // Outlier spike: 10000μs
        // EWMA = (10000 / 5) + (100 * 4 / 5) = 2000 + 80 = 2080
        metrics.record_query_duration(Duration::from_micros(10000));
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 2080);

        // Return to normal: 100μs
        // EWMA = (100 / 5) + (2080 * 4 / 5) = 20 + 1664 = 1684
        metrics.record_query_duration(Duration::from_micros(100));
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 1684);

        // Recover: 100μs
        // EWMA = (100 / 5) + (1684 * 4 / 5) = 20 + 1347 = 1367
        metrics.record_query_duration(Duration::from_micros(100));
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 1367);
    }

    #[test]
    fn query_metrics_concurrent_increments() {
        let metrics = Arc::new(QueryMetrics::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let m = Arc::clone(&metrics);
            let handle = std::thread::spawn(move || {
                m.queries_processed.fetch_add(1, Relaxed);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(metrics.fetch_queries_processed(), 10);
    }

    #[test]
    fn query_metrics_last_duration_updates_independently() {
        let metrics = QueryMetrics::new();

        let d1 = Duration::from_micros(100);
        metrics.record_query_duration(d1);
        assert_eq!(metrics.fetch_last_query_duration_micros(), 100);

        let d2 = Duration::from_micros(500);
        metrics.record_query_duration(d2);
        assert_eq!(metrics.fetch_last_query_duration_micros(), 500);
        // EWMA = (500 / 5) + (100 * 4 / 5) = 100 + 80 = 180
        assert_eq!(metrics.fetch_avg_query_duration_micros(), 180);
    }
}
