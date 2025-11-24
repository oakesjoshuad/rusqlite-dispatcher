//! Query worker implementation (one of N R/O workers).
//!
//! Processes read-only operations without transaction overhead.
//! Emits metrics via tracing for external collection.

use super::worker::{Worker, WorkerRunner};
use crate::Request;
use crate::channel::{QueryReceiver, QueryRequest};
use crate::error::Result;
use crate::handler::DomainHandler;

// --- TYPES ---

/// Query worker handles read-only operations without transactions.
#[derive(Debug)]
pub(crate) struct QueryWorker<H: DomainHandler> {
    pub(crate) worker: Worker<H>,
}

impl<H: DomainHandler> WorkerRunner<H> for QueryWorker<H> {
    type Receiver = QueryReceiver<H>;

    fn run(mut self, receiver: Self::Receiver) -> Result<()> {
        while let Ok(request) = receiver.recv() {
            let _ = self.process_query(request);
        }
        // Channel closed - graceful shutdown
        Ok(())
    }
}

impl<H: DomainHandler> QueryWorker<H> {
    /// Process a single query without transaction overhead
    ///
    /// Emits metrics via tracing for external collection
    fn process_query(&mut self, request: QueryRequest<H>) -> Result<()> {
        let Request {
            payload: query,
            response_tx,
        } = request;

        // Measure query execution duration
        let start = std::time::Instant::now();
        let result = self
            .worker
            .handler
            .handle_query(&self.worker.connection, query);
        let duration_micros = start.elapsed().as_micros() as u64;

        // Emit tracing event for external metrics collection
        tracing::info!(
            worker_id = %self.worker.worker_id,
            worker_type = ?self.worker.worker_type,
            operation = "query",
            duration_micros = duration_micros,
            success = result.is_ok(),
            "dispatcher_operation_completed"
        );

        // Send response back (defensive send - gracefully handles disconnected client)
        let _ = response_tx.send(result.map_err(|e| e.into()));

        Ok(())
    }
}

// --- TESTS ---
