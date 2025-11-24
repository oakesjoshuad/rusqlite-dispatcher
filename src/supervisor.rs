//! Supervisor coordinates worker lifecycle.

// External imports
use crossbeam_channel::bounded as channel;
use serde::{Deserialize, Serialize};

use crate::channel::{CommandSender, QuerySender};
use crate::error::{Error, Result};
use crate::handler::DomainHandler;
use crate::pool::{SupervisedWorker, Worker};

//type SupervisorJoinHandle = tokio::task::JoinHandle<()>;

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
}

/// Supervisor manages worker lifecycle.
#[derive(Debug)]
pub(crate) struct Supervisor<H: DomainHandler> {
    command_worker: SupervisedWorker,
    query_workers: Vec<SupervisedWorker>,
    pub(crate) command_sender: CommandSender<H>,
    pub(crate) query_sender: QuerySender<H>,
}

// --- IMPLEMENTATIONS ---

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            query_workers: 1,
            max_query_workers: 8,
            channel_size: 10_000,
        }
    }
}

impl<H: DomainHandler> Supervisor<H>
where
    H: Clone,
{
    /// Spawn supervisor with workers.
    pub(crate) fn spawn(db_path: &str, handler: H, config: &SupervisorConfig) -> Result<Self> {
        // Create command channel - crossbeam_channel as channel.
        let (command_sender, command_receiver) = channel(config.channel_size);
        // Spawn single command worker
        let command_worker =
            Worker::spawn_command_worker(db_path, handler.clone(), command_receiver.clone())?;

        // Create query worker pool
        //
        // Create query channel - crossbeam_channel as channel.
        let (query_sender, query_receiver) = channel(config.channel_size);
        // Spawn N workers
        let mut query_workers = Vec::with_capacity(config.query_workers);
        for _ in 0..config.query_workers {
            let query_worker =
                Worker::spawn_query_worker(db_path, handler.clone(), query_receiver.clone())?;
            query_workers.push(query_worker);
        }

        let supervisor = Self {
            command_worker,
            query_workers,
            command_sender,
            query_sender,
        };

        Ok(supervisor)
    }

    /// Shutdown all workers gracefully.
    pub(crate) fn shutdown(self) -> Result<()> {
        // Drop senders to signal workers
        drop(self.command_sender);
        drop(self.query_sender);

        let _ = self
            .command_worker
            .join_handle
            .join()
            .map_err(|err| Error::Internal(format!("Command worker panicked: {err:?}")))?;

        for worker in self.query_workers {
            let _ = worker
                .join_handle
                .join()
                .map_err(|err| Error::Internal(format!("Query worker panicked: {err:?}")))?;
        }

        Ok(())
    }
}
