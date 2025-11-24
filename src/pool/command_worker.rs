//! Command worker implementation (single R/W worker).
//!
//! Processes write operations with automatic transaction management.
//! Tracks metrics for throughput and latency monitoring.

use super::worker::{Worker, WorkerRunner};
use crate::channel::{CommandReceiver, CommandRequest};
use crate::error::Result;
use crate::handler::DomainHandler;

// --- TYPES ---

/// Command worker handles write operations with transaction management.
#[derive(Debug)]
pub(crate) struct CommandWorker<H: DomainHandler> {
    pub(crate) worker: Worker<H>,
}

impl<H: DomainHandler> WorkerRunner<H> for CommandWorker<H> {
    type Receiver = CommandReceiver<H>;

    fn run(mut self, receiver: Self::Receiver) -> Result<()> {
        while let Ok(request) = receiver.recv() {
            let _ = self.process_command(request);
        }
        Ok(())
    }
}

impl<H: DomainHandler> CommandWorker<H> {
    /// Process a single command with transaction management
    ///
    /// Automatically commits on success, rolls back on handler error.
    /// Records metrics for duration and throughput tracking.
    fn process_command(&mut self, request: CommandRequest<H>) -> Result<()> {
        // Destructure command request to avoid partial move issues
        let crate::Request {
            payload: command,
            response_tx,
        } = request;

        // Measure command execution duration
        let start = std::time::Instant::now();
        let result = {
            let tx = self.worker.connection.transaction()?;

            match self.worker.handler.handle_command(&tx, command) {
                Ok(()) => {
                    tx.commit()?;
                    Ok(())
                }
                Err(handler_err) => {
                    tx.rollback()?;
                    Err(handler_err)
                }
            }
        };
        let duration_micros = start.elapsed().as_micros() as u64;

        // Emit tracing event for external metrics collection
        tracing::info!(
            worker_id = %self.worker.worker_id,
            worker_type = ?self.worker.worker_type,
            operation = "command",
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
