//! Command worker implementation (single R/W worker).
//!
//! Processes write operations with automatic transaction management.
//! Tracks metrics for throughput and latency monitoring.

use async_trait::async_trait;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use super::worker::{SupervisedWorker, Worker, WorkerRunner};
use crate::channel::{CommandReceiver, CommandRequest};
use crate::clock::Duration;
use crate::error::Result;
use crate::handler::DomainHandler;

// --- TYPES ---

/// Metrics tracked by command worker.
#[derive(Debug, Default)]
pub(crate) struct CommandMetrics {
    commands_processed: AtomicU64,
    last_command_duration_micros: AtomicU64,
}

/// Type alias for supervised command worker.
pub(crate) type SupervisedCommandWorker = SupervisedWorker<CommandMetrics>;

/// Command worker handles write operations with transaction management.
#[derive(Debug)]
pub(crate) struct CommandWorker<H: DomainHandler> {
    pub(crate) worker: Worker<H>,
    pub(crate) metrics: Arc<CommandMetrics>,
}

impl CommandMetrics {
    /// Get total commands processed
    pub(crate) fn fetch_commands_processed(&self) -> u64 {
        self.commands_processed.load(Relaxed)
    }

    /// Increment commands processed counter
    #[inline]
    fn increment_commands_processed(&self) {
        self.commands_processed.fetch_add(1, Relaxed);
    }

    /// Get last command duration in microseconds
    pub(crate) fn fetch_last_command_duration_micros(&self) -> u64 {
        self.last_command_duration_micros.load(Relaxed)
    }

    /// Record command duration
    #[inline]
    fn record_command_duration(&self, duration: Duration) {
        duration.store_atomic(&self.last_command_duration_micros, Relaxed);
    }
}

#[async_trait]
impl<H: DomainHandler> WorkerRunner<H> for CommandWorker<H> {
    type Receiver = CommandReceiver<H>;

    async fn run(mut self, mut receiver: Self::Receiver) -> Result<()> {
        while let Some(command) = receiver.recv().await {
            let _ = self.process_command(command);
        }
        Ok(())
    }
}

impl<H: DomainHandler> CommandWorker<H> {
    /// Process a single command with transaction management
    ///
    /// Automatically commits on success, rolls back on handler error.
    /// Records metrics for duration and throughput tracking.
    fn process_command(&mut self, command: CommandRequest<H>) -> Result<()> {
        // Check if client still interested before expensive work
        if command.response.is_closed() {
            return Ok(());
        }

        // Measure command execution duration
        let (result, duration) = Duration::measure(|| {
            let tx = self.worker.connection.transaction()?;

            match self.worker.handler.handle_command(&tx, command.payload) {
                Ok(()) => {
                    tx.commit()?;
                    Ok(())
                }
                Err(handler_err) => {
                    tx.rollback()?;
                    Err(handler_err)
                }
            }
        });

        // Send response (defensive send - gracefully handles disconnected client)
        match result {
            Ok(()) => {
                let _ = command.response.send(Ok(()));

                // Record successful command metrics
                self.metrics.increment_commands_processed();
                self.metrics.record_command_duration(duration);
            }
            Err(handler_err) => {
                let _ = command.response.send(Err(handler_err));
            }
        }

        Ok(())
    }
}

// --- TESTS ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_metrics_default_initializes_to_zero() {
        let metrics = CommandMetrics::default();
        assert_eq!(metrics.fetch_commands_processed(), 0);
        assert_eq!(metrics.fetch_last_command_duration_micros(), 0);
    }

    #[test]
    fn command_metrics_increment_commands_processed() {
        let metrics = CommandMetrics::default();
        metrics.increment_commands_processed();
        assert_eq!(metrics.fetch_commands_processed(), 1);
        metrics.increment_commands_processed();
        assert_eq!(metrics.fetch_commands_processed(), 2);
    }

    #[test]
    fn command_metrics_record_command_duration() {
        let metrics = CommandMetrics::default();
        let duration = Duration::from_micros(1000);
        metrics.record_command_duration(duration);
        assert_eq!(metrics.fetch_last_command_duration_micros(), 1000);
    }

    #[test]
    fn command_metrics_multiple_durations() {
        let metrics = CommandMetrics::default();

        let d1 = Duration::from_micros(100);
        metrics.record_command_duration(d1);
        assert_eq!(metrics.fetch_last_command_duration_micros(), 100);

        let d2 = Duration::from_micros(200);
        metrics.record_command_duration(d2);
        assert_eq!(metrics.fetch_last_command_duration_micros(), 200);
    }

    #[test]
    fn command_metrics_concurrent_increments() {
        let metrics = Arc::new(CommandMetrics::default());
        let mut handles = vec![];

        for _ in 0..10 {
            let m = Arc::clone(&metrics);
            let handle = std::thread::spawn(move || {
                m.increment_commands_processed();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(metrics.fetch_commands_processed(), 10);
    }
}
