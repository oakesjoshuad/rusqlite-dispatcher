//! Dispatcher provides public API for command and query execution.
//!
//! Sync core implementation using crossbeam channels:
//! - MPSC for commands (single writer, multiple producers)
//! - MPMC for queries (multiple readers, multiple producers)

use crossbeam_channel::TrySendError;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::handler::DomainHandler;
use crate::request::Request;
use crate::supervisor::{Supervisor, SupervisorConfig};

/// Dispatcher configuration.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DispatcherConfig {
    /// Supervisor configuration.
    pub supervisor: SupervisorConfig,
}

/// Supervisor handle providing operational interface for command/query execution.
///
/// Contains only the cloneable channel senders needed for work distribution.
/// Separate from Dispatcher to cleanly separate operational vs lifecycle concerns.
#[derive(Clone, Debug)]
pub struct SupervisorHandle<H: DomainHandler> {
    pub(crate) command_sender: crate::channel::CommandSender<H>,
    pub(crate) query_sender: crate::channel::QuerySender<H>,
}

// TODO: generate updated documentation
#[derive(Debug)]
pub struct Dispatcher<H: DomainHandler> {
    supervisor: Supervisor<H>,
    handle: SupervisorHandle<H>,
}

impl<H: DomainHandler> Dispatcher<H>
where
    H: Clone,
{
    /// Create new dispatcher with worker pools.
    ///
    /// Spawns supervisor and workers, returns dispatcher that owns the supervisor lifecycle.
    /// Use `.handle()` to get a cloneable handle for passing to repositories/services.
    pub fn new(db_path: &str, handler: H, config: DispatcherConfig) -> Result<Self> {
        let supervisor = Supervisor::spawn(db_path, handler, &config.supervisor)?;

        let handle = SupervisorHandle {
            command_sender: supervisor.command_sender.clone(),
            query_sender: supervisor.query_sender.clone(),
        };

        Ok(Self { supervisor, handle })
    }

    /// Get a cloneable handle for work distribution.
    ///
    /// The handle contains channel senders for executing commands and queries.
    /// It can be cloned and distributed to services/repositories without affecting
    /// dispatcher's lifecycle management capabilities.
    pub fn handle(&self) -> SupervisorHandle<H> {
        self.handle.clone()
    }

    /// Shutdown dispatcher gracefully.
    ///
    /// Drops channel senders to signal workers to stop, then joins worker threads.
    /// Returns error if any worker panicked during shutdown.
    pub fn shutdown(self) -> Result<()> {
        // Drop handle first - its senders must be dropped before workers can exit
        drop(self.handle);
        self.supervisor.shutdown()
    }

    /// TODO: generate updated documentation
    #[cfg(not(feature = "tokio"))]
    pub fn execute_command(&self, command: H::Command) -> Result<()> {
        let (request, response_rx) = Request::new(command);

        match self.handle.command_sender.try_send(request) {
            Ok(()) => match response_rx.recv() {
                Ok(Ok(())) => Ok(()),
                Ok(Err(db_err)) => Err(db_err),
                Err(_recv_err) => Err(Error::Internal("Worker died".to_string())),
            },
            Err(TrySendError::Full(_)) => Err(Error::CommandWorkerBusy),
            Err(TrySendError::Disconnected(_)) => {
                Err(Error::Internal("Workers disconnected".to_string()))
            }
        }
    }

    /// TODO: generate updated documentation
    #[cfg(feature = "tokio")]
    pub async fn execute_command(&self, command: H::Command) -> Result<()> {
        let (request, response_rx) = Request::new(command);

        match self.handle.command_sender.try_send(request) {
            Ok(()) => {
                // Blocking receive
                match response_rx.recv().await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(db_err)) => Err(db_err.into()),
                    Err(_recv_err) => Err(Error::Internal("Worker died".to_string())),
                }
            }
            Err(TrySendError::Full(_)) => Err(Error::CommandWorkerBusy),
            Err(TrySendError::Disconnected(_)) => {
                Err(Error::Internal("Workers disconnected".to_string()))
            }
        }
    }

    /// TODO: generate updated documentation
    #[cfg(not(feature = "tokio"))]
    pub fn execute_query(&self, query: H::Query) -> Result<H::QueryResponse> {
        let (request, response_rx) = Request::new(query);

        match self.handle.query_sender.try_send(request) {
            Ok(()) => {
                // Blocking receive
                match response_rx.recv() {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(db_err)) => Err(db_err),
                    Err(_recv_err) => Err(Error::Internal("Worker died".to_string())),
                }
            }
            Err(TrySendError::Full(_)) => Err(Error::QueryWorkersBusy),
            Err(TrySendError::Disconnected(_)) => {
                Err(Error::Internal("Workers disconnected".to_string()))
            }
        }
    }

    /// TODO: generate updated documentation
    #[cfg(feature = "tokio")]
    pub async fn execute_query(&self, query: H::Query) -> Result<H::QueryResponse> {
        let (request, response_rx) = Request::new(query);

        match self.handle.query_sender.try_send(request) {
            Ok(()) => {
                // Blocking receive
                match response_rx.recv().await {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(db_err)) => Err(db_err),
                    Err(_recv_err) => Err(Error::Internal("Worker died".to_string())),
                }
            }
            Err(TrySendError::Full(_)) => Err(Error::QueryWorkersBusy),
            Err(TrySendError::Disconnected(_)) => {
                Err(Error::Internal("Workers disconnected".to_string()))
            }
        }
    }
}
