//! Base worker implementation with database connection management.
//!
//! Defines the core Worker<H> type and traits for worker execution.
//! Connection setup handles SQLite pragmas for performance and concurrency.

use std::sync::Arc;

use async_trait::async_trait;

use super::command_worker::{CommandMetrics, CommandWorker};
use super::metadata::{WorkerId, WorkerJoinHandle, WorkerType};
use super::query_worker::{QueryMetrics, QueryWorker};
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
        super::command_worker::SupervisedCommandWorker,
        crate::channel::CommandSender<H>,
    )> {
        let connection = Self::create_connection(WorkerType::Command, db_path)?;
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);
        let metrics = Arc::new(CommandMetrics::default());
        let worker_id = ulid::Ulid::new();

        let worker = Worker {
            connection,
            handler,
        };

        let command_worker = CommandWorker {
            worker,
            metrics: Arc::clone(&metrics),
        };

        let join_handle = tokio::spawn(async move { command_worker.run(rx).await });

        let supervised = SupervisedWorker {
            worker_id,
            metrics,
            join_handle,
        };

        Ok((supervised, tx))
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
        super::query_worker::SupervisedQueryWorker,
        crate::channel::QuerySender<H>,
    )> {
        let connection = Self::create_connection(WorkerType::Query, db_path)?;
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);
        let metrics = Arc::new(QueryMetrics::new());
        let worker_id = ulid::Ulid::new();

        let worker = Worker {
            connection,
            handler,
        };

        let query_worker = QueryWorker {
            worker,
            metrics: Arc::clone(&metrics),
        };

        let join_handle = tokio::spawn(async move { query_worker.run(rx).await });

        let supervised = SupervisedWorker {
            worker_id,
            metrics,
            join_handle,
        };

        Ok((supervised, tx))
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

// --- TESTS ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Mock handler for testing
    #[derive(Clone)]
    struct TestHandler {
        call_count: Arc<AtomicUsize>,
    }

    impl DomainHandler for TestHandler {
        type Command = ();
        type Query = ();
        type QueryResponse = u64;

        fn handle_command(
            &mut self,
            _tx: &rusqlite::Transaction,
            _cmd: Self::Command,
        ) -> std::result::Result<(), rusqlite::Error> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn handle_query(
            &self,
            _conn: &rusqlite::Connection,
            _query: Self::Query,
        ) -> std::result::Result<Self::QueryResponse, rusqlite::Error> {
            Ok(42)
        }
    }

    // Helper to create temp database path
    fn temp_db_path() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/test_worker_{}.db", timestamp)
    }

    // --- CREATE_CONNECTION TESTS ---

    #[test]
    fn create_connection_command_opens_read_write() {
        let path = temp_db_path();
        let conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path);
        assert!(conn.is_ok());
        let conn = conn.unwrap();

        // Verify we can write
        let result = conn.execute("CREATE TABLE test (id INTEGER)", []);
        assert!(result.is_ok());

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn create_connection_query_opens_read_only() {
        // First create database with command connection
        let path = temp_db_path();
        let cmd_conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create command connection");
        cmd_conn
            .execute("CREATE TABLE test (id INTEGER)", [])
            .expect("Failed to create table");
        drop(cmd_conn);

        // Now try to write with query connection (should fail)
        let query_conn = Worker::<TestHandler>::create_connection(WorkerType::Query, &path)
            .expect("Failed to create query connection");

        let result = query_conn.execute("INSERT INTO test VALUES (1)", []);
        assert!(result.is_err());

        // But reading should work
        let result = query_conn.query_row("SELECT COUNT(*) FROM test", [], |row| {
            row.get::<_, i64>(0)
        });
        assert!(result.is_ok());

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn create_connection_command_sets_wal_mode() {
        let path = temp_db_path();
        let conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create connection");

        // Query the journal mode
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("Failed to query journal_mode");

        assert_eq!(journal_mode.to_lowercase(), "wal");

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn create_connection_command_sets_synchronous_normal() {
        let path = temp_db_path();
        let conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create connection");

        // Query synchronous mode (0=OFF, 1=NORMAL, 2=FULL)
        let synchronous: i32 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("Failed to query synchronous");

        assert_eq!(synchronous, 1); // NORMAL

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn create_connection_command_sets_cache_size_64mb() {
        let path = temp_db_path();
        let conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create connection");

        // Query cache size (negative = KB)
        let cache_size: i64 = conn
            .query_row("PRAGMA cache_size", [], |row| row.get(0))
            .expect("Failed to query cache_size");

        // -64000 KB = 64MB
        assert_eq!(cache_size, -64000);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn create_connection_query_sets_cache_size_32mb() {
        let path = temp_db_path();
        // Create database first with command connection
        let cmd_conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create command connection");
        drop(cmd_conn);

        let query_conn = Worker::<TestHandler>::create_connection(WorkerType::Query, &path)
            .expect("Failed to create query connection");

        // Query cache size
        let cache_size: i64 = query_conn
            .query_row("PRAGMA cache_size", [], |row| row.get(0))
            .expect("Failed to query cache_size");

        // -32000 KB = 32MB
        assert_eq!(cache_size, -32000);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn create_connection_invalid_path_fails() {
        let invalid_path = "/nonexistent/path/that/cannot/exist/test.db";
        let result = Worker::<TestHandler>::create_connection(WorkerType::Command, invalid_path);
        assert!(result.is_err());
    }

    // --- SPAWN_COMMAND_WORKER TESTS ---

    #[tokio::test]
    async fn spawn_command_worker_succeeds() {
        let path = temp_db_path();
        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let result = Worker::spawn_command_worker(&path, handler, 10).await;
        assert!(result.is_ok());

        let (supervised, sender) = result.unwrap();
        // Verify we got a sender
        assert!(!sender.is_closed());
        // Verify we got metrics
        assert_eq!(supervised.metrics.fetch_commands_processed(), 0);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn spawn_command_worker_creates_metrics() {
        let path = temp_db_path();
        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let (supervised, _) = Worker::spawn_command_worker(&path, handler, 10)
            .await
            .expect("Failed to spawn");

        // Verify metrics are initialized
        assert_eq!(supervised.metrics.fetch_commands_processed(), 0);
        assert_eq!(supervised.metrics.fetch_last_command_duration_micros(), 0);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn spawn_command_worker_invalid_path_fails() {
        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let result = Worker::spawn_command_worker("/nonexistent/path/test.db", handler, 10).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn spawn_command_worker_generates_unique_ids() {
        let path = temp_db_path();
        let handler1 = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let handler2 = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let (supervised1, _) = Worker::spawn_command_worker(&path, handler1, 10)
            .await
            .expect("Failed to spawn first");

        let (supervised2, _) = Worker::spawn_command_worker(&path, handler2, 10)
            .await
            .expect("Failed to spawn second");

        // Verify worker IDs are different
        assert_ne!(supervised1.worker_id, supervised2.worker_id);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn spawn_command_worker_channel_has_capacity() {
        let path = temp_db_path();
        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let (_, sender) = Worker::spawn_command_worker(&path, handler, 5)
            .await
            .expect("Failed to spawn");

        // Should be able to reserve capacity
        let result = sender.try_reserve();
        assert!(result.is_ok());

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    // --- SPAWN_QUERY_WORKER TESTS ---

    #[tokio::test]
    async fn spawn_query_worker_succeeds() {
        let path = temp_db_path();
        // Create database first
        let cmd_conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create command connection");
        drop(cmd_conn);

        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let result = Worker::spawn_query_worker(&path, handler, 10).await;
        assert!(result.is_ok());

        let (supervised, sender) = result.unwrap();
        assert!(!sender.is_closed());
        assert_eq!(supervised.metrics.fetch_queries_processed(), 0);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn spawn_query_worker_creates_metrics() {
        let path = temp_db_path();
        let cmd_conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create command connection");
        drop(cmd_conn);

        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let (supervised, _) = Worker::spawn_query_worker(&path, handler, 10)
            .await
            .expect("Failed to spawn");

        // Verify QueryMetrics are initialized (different from CommandMetrics)
        assert_eq!(supervised.metrics.fetch_queries_processed(), 0);
        assert_eq!(supervised.metrics.fetch_avg_query_duration_micros(), 0);
        assert_eq!(supervised.metrics.fetch_last_query_duration_micros(), 0);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn spawn_query_worker_generates_unique_ids() {
        let path = temp_db_path();
        let cmd_conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create command connection");
        drop(cmd_conn);

        let handler1 = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let handler2 = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let (supervised1, _) = Worker::spawn_query_worker(&path, handler1, 10)
            .await
            .expect("Failed to spawn first");

        let (supervised2, _) = Worker::spawn_query_worker(&path, handler2, 10)
            .await
            .expect("Failed to spawn second");

        // Verify worker IDs are different
        assert_ne!(supervised1.worker_id, supervised2.worker_id);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn spawn_query_worker_channel_has_capacity() {
        let path = temp_db_path();
        let cmd_conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create command connection");
        drop(cmd_conn);

        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        let (_, sender) = Worker::spawn_query_worker(&path, handler, 5)
            .await
            .expect("Failed to spawn");

        let result = sender.try_reserve();
        assert!(result.is_ok());

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    // --- EDGE CASES ---

    #[test]
    fn multiple_workers_same_database_command_and_query() {
        let path = temp_db_path();

        // Create with command worker
        let cmd_conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create command connection");

        // Create table
        cmd_conn
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", [])
            .expect("Failed to create table");

        drop(cmd_conn);

        // Now open query connection
        let query_conn = Worker::<TestHandler>::create_connection(WorkerType::Query, &path)
            .expect("Failed to create query connection");

        // Should be able to read
        let count: i64 = query_conn
            .query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0))
            .expect("Failed to query");

        assert_eq!(count, 0);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn spawn_workers_with_different_channel_sizes() {
        let path = temp_db_path();
        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };

        // Small channel
        let (_, sender1) = Worker::spawn_command_worker(&path, handler.clone(), 1)
            .await
            .expect("Failed to spawn with size 1");
        assert!(sender1.try_reserve().is_ok());

        // Large channel
        let (_, sender2) = Worker::spawn_command_worker(&path, handler, 1000)
            .await
            .expect("Failed to spawn with size 1000");
        assert!(sender2.try_reserve().is_ok());

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }
}
