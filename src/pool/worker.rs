//! Base worker implementation with database connection management.
//!
//! Defines the core Worker<H> type and traits for worker execution.
//! Connection setup handles SQLite pragmas for performance and concurrency.

use super::command_worker::CommandWorker;
use super::metadata::{WorkerId, WorkerType};
use super::query_worker::QueryWorker;
use crate::Result;
use crate::channel::{CommandReceiver, QueryReceiver};
use crate::handler::DomainHandler;

pub(crate) trait WorkerRunner<H: DomainHandler> {
    /// Channel receiver type for this worker.
    type Receiver;

    /// Run the worker loop until channel is closed.
    fn run(self, receiver: Self::Receiver) -> Result<()>;
}

// --- TYPES ---

type WorkerConnection = rusqlite::Connection;

/// Wrapper around thread JoinHandle for supervised workers.
///
/// Provides consistent API regardless of underlying threading model.
pub(crate) type WorkerJoinHandle = std::thread::JoinHandle<Result<()>>;

/// Base worker struct containing database connection and handler.
///
/// This is an internal implementation detail. Workers are spawned via
/// the static methods and return supervised handles.
#[derive(Debug)]
pub(crate) struct Worker<H: DomainHandler> {
    pub(crate) worker_id: WorkerId,
    pub(crate) worker_type: WorkerType,
    pub(crate) connection: WorkerConnection,
    pub(crate) handler: H,
}

/// Supervised worker wrapper for lifecycle management.
///
/// Wraps a spawned worker task for lifecycle management.
/// Metadata is stored in the Worker itself for use in tracing and monitoring.
#[derive(Debug)]
pub(crate) struct SupervisedWorker {
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
    pub(crate) fn spawn_command_worker(
        db_path: &str,
        handler: H,
        receiver: CommandReceiver<H>,
    ) -> Result<SupervisedWorker>
    where
        H: Clone,
    {
        let connection = Self::create_connection(WorkerType::Command, db_path)?;
        let worker_id = ulid::Ulid::new();

        let worker = Worker {
            worker_id,
            worker_type: WorkerType::Command,
            connection,
            handler,
        };

        let command_worker = CommandWorker { worker };

        let join_handle = std::thread::spawn(move || command_worker.run(receiver));

        let supervised = SupervisedWorker { join_handle };

        Ok(supervised)
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
    pub(crate) fn spawn_query_worker(
        db_path: &str,
        handler: H,
        receiver: QueryReceiver<H>,
    ) -> Result<SupervisedWorker>
    where
        H: Clone,
    {
        let connection = Self::create_connection(WorkerType::Query, db_path)?;
        let worker_id = ulid::Ulid::new();
        let worker = Worker {
            worker_id,
            worker_type: WorkerType::Query,
            connection,
            handler,
        };

        let query_worker = QueryWorker { worker };

        let join_handle = std::thread::spawn(move || query_worker.run(receiver));

        let supervised = SupervisedWorker { join_handle };

        Ok(supervised)
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
                // Enable foreign key constraings (OFF by default in SQLite)
                connection.pragma_update(None, "foreign_keys", "ON")?;
                // Enable WAL mode for concurrent readers
                connection.pragma_update(None, "journal_mode", "WAL")?;
                // NORMAL sync is safe with WAL and much faster
                connection.pragma_update(None, "synchronous", "NORMAL")?;
                // 64MB cache (negative = KB)
                connection.pragma_update(None, "cache_size", "-64000")?;
            }
            WorkerType::Query => {
                // Enable foreign key constraings (OFF by default in SQLite)
                connection.pragma_update(None, "foreign_keys", "ON")?;
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
    use std::sync::Arc;
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
        let result =
            query_conn.query_row("SELECT COUNT(*) FROM test", [], |row| row.get::<_, i64>(0));
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
    fn create_connection_command_sets_foreign_keys() {
        let path = temp_db_path();
        let conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create connection");

        // Query foreign keys setting
        let foreign_keys: i32 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .expect("Failed to query foreign_keys");

        assert_eq!(foreign_keys, 1); // ON

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

    #[test]
    fn spawn_command_worker_succeeds() {
        let path = temp_db_path();
        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let (_tx, rx) =
            crossbeam_channel::bounded::<crate::channel::CommandRequest<TestHandler>>(10);

        let result = Worker::spawn_command_worker(&path, handler, rx);
        assert!(result.is_ok());

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn spawn_command_worker_invalid_path_fails() {
        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let (_tx, rx) =
            crossbeam_channel::bounded::<crate::channel::CommandRequest<TestHandler>>(10);

        let result = Worker::spawn_command_worker("/nonexistent/path/test.db", handler, rx);
        assert!(result.is_err());
    }

    // --- SPAWN_QUERY_WORKER TESTS ---

    #[test]
    fn spawn_query_worker_succeeds() {
        let path = temp_db_path();
        // Create database first
        let _cmd_conn = Worker::<TestHandler>::create_connection(WorkerType::Command, &path)
            .expect("Failed to create command connection");

        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let (_tx, rx) = crossbeam_channel::bounded::<crate::channel::QueryRequest<TestHandler>>(10);

        let result = Worker::spawn_query_worker(&path, handler, rx);
        assert!(result.is_ok());

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn spawn_query_worker_invalid_path_fails() {
        let handler = TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let (_tx, rx) = crossbeam_channel::bounded::<crate::channel::QueryRequest<TestHandler>>(10);

        let result = Worker::spawn_query_worker("/nonexistent/path/test.db", handler, rx);
        assert!(result.is_err());
    }
}
