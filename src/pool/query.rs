//! Query worker implementation (one of N R/O workers).
//!
//! Processes read-only operations without transaction overhead.
//! Tracks metrics with EWMA smoothing for latency monitoring.

use async_trait::async_trait;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

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
