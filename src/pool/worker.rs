//! Base worker implementation with database connection management.
//!
//! Defines the core Worker<H> type and traits for worker execution.
//! Connection setup handles SQLite pragmas for performance and concurrency.

use std::sync::Arc;

use async_trait::async_trait;

use super::metadata::{WorkerId, WorkerJoinHandle, WorkerType};
use crate::Result;
use crate::handler::DomainHandler;

// --- TRAITS ---

/// Worker execution trait for CommandWorker and QueryWorker.
///
/// Defines the async run loop that processes requests from a channel
/// until the sender is dropped (graceful shutdown signal).
#[async_trait]
pub(crate) trait WorkerRunner<H: DomainHandler> {
    /// Channel receiver type for this worker.
    type Receiver;

    /// Run the worker loop until channel is closed.
    async fn run(mut self, receiver: Self::Receiver) -> Result<()>;
}

// --- TYPES ---

type WorkerConnection = rusqlite::Connection;

/// Base worker struct containing database connection and handler.
///
/// This is an internal implementation detail. Workers are spawned via
/// the static methods and return supervised handles.
#[derive(Debug)]
pub(crate) struct Worker<H: DomainHandler> {
    pub(crate) connection: WorkerConnection,
    pub(crate) handler: H,
}

/// Supervised worker wrapper providing metrics and lifecycle management.
///
/// Wraps a worker with its associated metrics and task join handle.
/// Type parameter M is typically CommandMetrics or QueryMetrics.
#[derive(Debug)]
pub(crate) struct SupervisedWorker<M> {
    pub(crate) worker_id: WorkerId,
    pub(crate) metrics: Arc<M>,
    pub(crate) join_handle: WorkerJoinHandle,
}

// --- IMPLEMENTATIONS ---

impl<H: DomainHandler> Worker<H> {
    /// Spawn a command worker (single R/W worker).
    ///
    /// Creates a connection with read-write access, WAL mode, and optimized
    /// pragmas. Returns a supervised handle and channel sender.
    ///
    /// # Arguments
    ///
    /// * `db_path` - SQLite database path (supports URI)
    /// * `handler` - Domain handler implementation
    /// * `channel_size` - Bounded channel capacity for backpressure
    pub(crate) async fn spawn_command_worker(
        db_path: &str,
        handler: H,
        channel_size: usize,
    ) -> Result<(
        super::command::SupervisedCommandWorker,
        crate::channel::CommandSender<H>,
    )> {
        todo!("Implement command worker spawning")
    }

    /// Spawn a query worker (one of N R/O workers).
    ///
    /// Creates a connection with read-only access and optimized cache settings.
    /// Returns a supervised handle and channel sender.
    ///
    /// # Arguments
    ///
    /// * `db_path` - SQLite database path (supports URI)
    /// * `handler` - Domain handler implementation
    /// * `channel_size` - Bounded channel capacity for backpressure
    pub(crate) async fn spawn_query_worker(
        db_path: &str,
        handler: H,
        channel_size: usize,
    ) -> Result<(
        super::query::SupervisedQueryWorker,
        crate::channel::QuerySender<H>,
    )> {
        todo!("Implement query worker spawning")
    }

    /// Create database connection with worker-type-specific configuration.
    ///
    /// # Command Worker Configuration
    ///
    /// - Read-write access
    /// - WAL journal mode for concurrent reads
    /// - NORMAL synchronous mode for performance
    /// - 64MB cache size
    /// - NO_MUTEX flag (single-threaded worker)
    ///
    /// # Query Worker Configuration
    ///
    /// - Read-only access
    /// - 32MB cache size
    /// - NO_MUTEX flag (single-threaded worker)
    fn create_connection(worker_type: WorkerType, db_path: &str) -> Result<WorkerConnection> {
        let flags = match worker_type {
            WorkerType::Command => {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            }
            WorkerType::Query => {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            }
        };

        let connection = rusqlite::Connection::open_with_flags(db_path, flags)?;

        match worker_type {
            WorkerType::Command => {
                // Enable WAL mode for concurrent readers
                connection.pragma_update(None, "journal_mode", "WAL")?;
                // NORMAL sync is safe with WAL and much faster
                connection.pragma_update(None, "synchronous", "NORMAL")?;
                // 64MB cache (negative = KB)
                connection.pragma_update(None, "cache_size", "-64000")?;
            }
            WorkerType::Query => {
                // 32MB cache for read-only worker
                connection.pragma_update(None, "cache_size", "-32000")?;
            }
        }

        Ok(connection)
    }
}
