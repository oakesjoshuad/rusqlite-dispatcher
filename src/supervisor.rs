//! Supervisor coordinates worker lifecycle and metrics aggregation.

// Submodule declarations
mod metrics;

// Re-exports from submodules
pub(crate) use metrics::SupervisorMetrics;

// External imports
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

#[cfg(feature = "trace")]
use tracing::{debug, error, info, instrument};

use crate::channel::{CommandSender, ControlReceiver, ControlSender};
use crate::clock::{Duration, Timestamp};
use crate::error::{Error, Result};
use crate::handler::DomainHandler;
use crate::pool::{QueryDispatchHandle, QueryWorkerPool, SupervisedCommandWorker, Worker};

const EWMA_ALPHA: f64 = 0.4;

type SupervisorJoinHandle = tokio::task::JoinHandle<()>;

// --- TYPES ---

/// Supervisor configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorConfig {
    /// Number of query workers to spawn.
    pub query_workers: usize,
    /// Maximum query workers (reserved for future dynamic scaling).
    pub max_query_workers: usize,
    /// Channel capacity for backpressure.
    pub channel_size: usize,
    /// Control channel capacity for shutdown signaling.
    pub control_channel_size: usize,
}

/// Supervisor manages worker lifecycle and metrics aggregation.
pub(crate) struct Supervisor<H: DomainHandler> {
    command_sender: CommandSender<H>,
    query_pool: QueryWorkerPool<H>,
    command_worker: SupervisedCommandWorker,
    metrics: Arc<SupervisorMetrics>,
}

/// Cloneable handle to supervisor for dispatcher.
///
/// Contains only cloneable parts (senders, dispatch handle, metrics) to enable
/// `Dispatcher` to be `Clone`. Shutdown is handled via Arc<Mutex<Option<JoinHandle>>>
/// to allow exactly one shutdown call.
#[derive(Clone, Debug)]
pub(crate) struct SupervisorHandle<H: DomainHandler> {
    pub(crate) command_sender: CommandSender<H>,
    pub(crate) query_dispatch: QueryDispatchHandle<H>,
    pub(crate) control_sender: ControlSender,
    pub(crate) shutdown: Arc<Mutex<Option<SupervisorJoinHandle>>>,
    pub(crate) metrics: Arc<SupervisorMetrics>,
}

// --- IMPLEMENTATIONS ---

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            query_workers: 4,
            max_query_workers: 8,
            channel_size: 100,
            control_channel_size: 100,
        }
    }
}

impl<H: DomainHandler> Supervisor<H> {
    /// Calculate EWMA (Exponentially Weighted Moving Average).
    ///
    /// Uses alpha=0.4 (40% new value, 60% historical average).
    /// Returns current value if this is the first measurement.
    #[inline]
    fn calculate_ewma(current: f64, previous: f64, is_first: bool) -> f64 {
        match is_first {
            true => current,
            false => (EWMA_ALPHA * current) + ((1.0 - EWMA_ALPHA) * previous),
        }
    }

    /// Spawn supervisor with workers and return handle.
    #[cfg_attr(feature = "trace", instrument(skip(handler), fields(
        db_path = %db_path,
        query_workers = config.query_workers,
        channel_size = config.channel_size,
        control_channel_size = config.control_channel_size
    )))]
    pub(crate) async fn spawn(
        db_path: &str,
        handler: H,
        config: &SupervisorConfig,
    ) -> Result<SupervisorHandle<H>> {
        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(config.control_channel_size);

        // Spawn single command worker
        let (command_worker, command_sender) =
            Worker::spawn_command_worker(db_path, handler.clone(), config.channel_size).await?;

        // Create query worker pool
        let mut query_pool = QueryWorkerPool::new();
        for _ in 0..config.query_workers {
            let (query_worker, query_sender) =
                Worker::spawn_query_worker(db_path, handler.clone(), config.channel_size).await?;
            query_pool.add_worker(query_worker, query_sender);
        }

        let metrics = Arc::new(SupervisorMetrics::default());

        // Extract cloneable dispatch handle before moving pool
        let query_dispatch = query_pool.dispatch_handle();

        let supervisor = Self {
            command_sender: command_sender.clone(),
            query_pool,
            command_worker,
            metrics: metrics.clone(),
        };

        // Spawn supervisor background task
        let join_handle = tokio::spawn(async move { supervisor.run(control_receiver).await });

        let supervisor_handle = SupervisorHandle {
            command_sender,
            query_dispatch,
            control_sender,
            shutdown: Arc::new(Mutex::new(Some(join_handle))),
            metrics,
        };

        #[cfg(feature = "trace")]
        info!("Supervisor spawned successfully");

        Ok(supervisor_handle)
    }

    /// Run supervisor background loop with periodic metrics aggregation.
    async fn run(self, mut receiver: ControlReceiver) {
        #[cfg(feature = "trace")]
        info!("Supervisor background task starting");

        let mut aggregate_interval = tokio::time::interval(std::time::Duration::from_secs(4));

        loop {
            tokio::select! {
                signal = receiver.recv() => {
                    if signal.is_none() {
                        #[cfg(feature = "trace")]
                        info!("Shutdown signal received");
                        break;
                    }
                }
                _ = aggregate_interval.tick() => {
                    self.aggregate_metrics();
                }
            }
        }

        #[cfg(feature = "trace")]
        info!("Supervisor shutting down workers");

        self.shutdown().await
    }

    /// Aggregate metrics from all workers.
    fn aggregate_metrics(&self) {
        let now = Timestamp::now();
        let time_delta_secs = self.time_delta_secs(now);

        self.aggregate_command_metrics(time_delta_secs);
        self.aggregate_command_saturation();
        self.aggregate_query_metrics();

        self.metrics.put_last_aggregation(now);

        #[cfg(feature = "trace")]
        debug!(
            total_commands = self.metrics.fetch_total_commands(),
            sustained_tps = self.metrics.fetch_avg_sustained_tps(),
            command_saturation = self.metrics.fetch_avg_command_saturation(),
            total_queries = self.metrics.fetch_total_queries(),
            query_workers = self.metrics.fetch_query_worker_count(),
            "Metrics aggregated"
        );
    }

    /// Calculate time delta in seconds since last aggregation.
    fn time_delta_secs(&self, now: Timestamp) -> f64 {
        let last_aggregate = self.metrics.fetch_last_aggregation();
        let time_delta = now - last_aggregate;
        time_delta.as_secs_f64()
    }

    /// Aggregate command metrics (TPS, duration, saturation).
    fn aggregate_command_metrics(&self, time_delta_secs: f64) {
        // Read current command count from worker
        let current_commands = self.command_worker.metrics.fetch_commands_processed();

        // Read last total from supervisor metrics
        let last_total = self.metrics.fetch_total_commands();

        // Calculate commands processed since last tick
        let commands_delta = current_commands - last_total;

        if commands_delta > 0 {
            // Calculate sustained TPS
            let measured_tps = commands_delta as f64 / time_delta_secs;

            let last_tps = self.metrics.fetch_avg_sustained_tps();
            let ewma_tps = Self::calculate_ewma(measured_tps, last_tps, last_tps == 0.0);

            self.metrics.put_avg_sustained_tps(ewma_tps);

            // Track peak (no EWMA - pure high water mark)
            let current_peak = self.metrics.fetch_peak_tps();
            if measured_tps > current_peak {
                self.metrics.put_peak_tps(measured_tps);
            }
        }

        // Copy worker's transaction duration
        let current_duration_micros = self
            .command_worker
            .metrics
            .fetch_last_command_duration_micros();
        let last_avg_duration = self.metrics.fetch_avg_command_duration();

        let ewma_duration = match last_avg_duration.is_zero() {
            true => Duration::from_micros(current_duration_micros),
            false => {
                let ewma = Self::calculate_ewma(
                    current_duration_micros as f64,
                    last_avg_duration.as_micros() as f64,
                    false,
                );
                Duration::from_micros(ewma as u64)
            }
        };

        self.metrics.put_avg_command_duration(ewma_duration);

        // Store total commands
        self.metrics.put_total_commands(current_commands);
    }

    /// Aggregate command worker channel saturation.
    fn aggregate_command_saturation(&self) {
        let available = self.command_sender.capacity();
        let max_capacity = self.command_sender.max_capacity();
        let queued = (max_capacity - available) as u64;
        let current_saturation = queued as f64 / max_capacity as f64;

        let last_saturation = self.metrics.fetch_avg_command_saturation();
        let ewma_saturation =
            Self::calculate_ewma(current_saturation, last_saturation, last_saturation == 0.0);

        self.metrics.put_avg_command_saturation(ewma_saturation);
    }

    /// Aggregate query metrics from pool.
    fn aggregate_query_metrics(&self) {
        let query_metrics = self.query_pool.aggregate_metrics();

        self.metrics.put_total_queries(query_metrics.total_queries);
        self.metrics
            .put_avg_query_duration(Duration::from_micros(query_metrics.avg_duration_micros));
        self.metrics
            .put_query_worker_count(query_metrics.worker_count);
    }

    /// Shutdown all workers gracefully.
    async fn shutdown(self) {
        drop(self.command_sender);

        // Shutdown query pool
        self.query_pool.shutdown().await;

        // Shutdown command worker
        match self.command_worker.join_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                #[cfg(feature = "trace")]
                error!(
                    worker_id = %self.command_worker.worker_id,
                    ?err,
                    "Command worker error during shutdown"
                );

                #[cfg(not(feature = "trace"))]
                eprintln!(
                    "Command worker {} error during shutdown: {:?}",
                    self.command_worker.worker_id, err
                );
            }
            Err(err) => {
                #[cfg(feature = "trace")]
                error!(
                    worker_id = %self.command_worker.worker_id,
                    ?err,
                    "Command worker panicked during shutdown"
                );

                #[cfg(not(feature = "trace"))]
                eprintln!(
                    "Command worker {} panicked during shutdown: {:?}",
                    self.command_worker.worker_id, err
                );
            }
        }
    }
}

impl<H: DomainHandler> SupervisorHandle<H> {
    /// Shutdown supervisor and await completion.
    ///
    /// Can only be called once. Subsequent calls will return an error.
    /// Consumes the join handle from the Arc<Mutex<Option<>>>.
    pub(crate) async fn shutdown(self) -> Result<()> {
        // Signal shutdown
        drop(self.control_sender);
        drop(self.command_sender);

        // Take join handle (only works once)
        let join_handle = self
            .shutdown
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Error::Internal("Supervisor already shut down".to_string()))?;

        match join_handle.await {
            Ok(()) => Ok(()),
            Err(err) => {
                #[cfg(feature = "trace")]
                error!(?err, "Supervisor panicked during shutdown");

                #[cfg(not(feature = "trace"))]
                eprintln!("Supervisor panicked during shutdown: {:?}", err);

                Err(Error::Internal(format!("Supervisor panic: {}", err)))
            }
        }
    }
}
