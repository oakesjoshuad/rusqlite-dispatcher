//! Production-realistic stress tests for rusqlite-dispatcher
//!
//! Run with: cargo test --test stress_tests -- --nocapture
//!
//! These tests simulate real-world production scenarios:
//! - Duration-based stress testing (not just fixed operations)
//! - Concurrent users with realistic workload ratios
//! - Network latency simulation
//! - Backpressure boundary detection
//! - Latency measurement
//!
//! Tracing can be filtered via RUST_LOG environment variable:
//!   RUST_LOG=warn cargo test --test stress_tests -- --nocapture
//!
//! Or programmatically with tracing::Level:
//!   - ERROR: Only errors
//!   - WARN: Warnings and errors
//!   - INFO: Info, warnings, errors (default)
//!   - DEBUG: Debug and above
//!   - TRACE: Everything

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite_dispatcher::{Dispatcher, DispatcherConfig, DomainHandler, Result};

// ============================================================================
// Production Metrics Harness
// ============================================================================

#[derive(Debug)]
struct ProductionMetrics {
    commands_succeeded: AtomicU64,
    commands_backpressure: AtomicU64,
    queries_succeeded: AtomicU64,
    queries_backpressure: AtomicU64,
    min_observed: AtomicU64,
    max_observed: AtomicU64,
    total_command_latency_us: AtomicU64,
    total_query_latency_us: AtomicU64,
}

impl ProductionMetrics {
    fn new() -> Self {
        Self {
            commands_succeeded: AtomicU64::new(0),
            commands_backpressure: AtomicU64::new(0),
            queries_succeeded: AtomicU64::new(0),
            queries_backpressure: AtomicU64::new(0),
            min_observed: AtomicU64::new(u64::MAX),
            max_observed: AtomicU64::new(0),
            total_command_latency_us: AtomicU64::new(0),
            total_query_latency_us: AtomicU64::new(0),
        }
    }

    fn record_command_success(&self, latency_us: u64) {
        self.commands_succeeded.fetch_add(1, Ordering::Relaxed);
        self.total_command_latency_us.fetch_add(latency_us, Ordering::Relaxed);
    }

    fn record_command_backpressure(&self) {
        self.commands_backpressure.fetch_add(1, Ordering::Relaxed);
    }

    fn record_query_success(&self, value: u64, latency_us: u64) {
        self.queries_succeeded.fetch_add(1, Ordering::Relaxed);
        self.total_query_latency_us.fetch_add(latency_us, Ordering::Relaxed);
        self.min_observed.fetch_min(value, Ordering::Relaxed);
        self.max_observed.fetch_max(value, Ordering::Relaxed);
    }

    fn record_query_backpressure(&self) {
        self.queries_backpressure.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self, test_name: &str, duration_secs: f64) {
        let cmd_success = self.commands_succeeded.load(Ordering::Relaxed);
        let cmd_bp = self.commands_backpressure.load(Ordering::Relaxed);
        let qry_success = self.queries_succeeded.load(Ordering::Relaxed);
        let qry_bp = self.queries_backpressure.load(Ordering::Relaxed);

        let total_cmds = cmd_success + cmd_bp;
        let total_queries = qry_success + qry_bp;
        let total_ops = total_cmds + total_queries;

        println!("\n{}", "=".repeat(70));
        println!("Test: {}", test_name);
        println!("{}", "=".repeat(70));

        // Throughput
        println!("\nTHROUGHPUT:");
        println!("  Duration:          {:.2}s", duration_secs);
        println!("  Total Operations:  {}", total_ops);
        println!("  Operations/sec:    {:.0}", total_ops as f64 / duration_secs);
        println!("  Commands/sec:      {:.0}", total_cmds as f64 / duration_secs);
        println!("  Queries/sec:       {:.0}", total_queries as f64 / duration_secs);

        // Workload mix
        let write_pct = if total_ops > 0 {
            (total_cmds as f64 / total_ops as f64) * 100.0
        } else {
            0.0
        };
        println!("\nWORKLOAD MIX:");
        println!("  Commands (writes): {} ({:.1}%)", total_cmds, write_pct);
        println!("  Queries (reads):   {} ({:.1}%)", total_queries, 100.0 - write_pct);

        // Success rates
        let cmd_success_rate = if total_cmds > 0 {
            (cmd_success as f64 / total_cmds as f64) * 100.0
        } else {
            100.0
        };
        let qry_success_rate = if total_queries > 0 {
            (qry_success as f64 / total_queries as f64) * 100.0
        } else {
            100.0
        };
        println!("\nSUCCESS RATES:");
        println!("  Commands: {} ({:.2}%)", cmd_success, cmd_success_rate);
        println!("  Queries:  {} ({:.2}%)", qry_success, qry_success_rate);

        // Backpressure
        let cmd_bp_rate = if total_cmds > 0 {
            (cmd_bp as f64 / total_cmds as f64) * 100.0
        } else {
            0.0
        };
        let qry_bp_rate = if total_queries > 0 {
            (qry_bp as f64 / total_queries as f64) * 100.0
        } else {
            0.0
        };
        println!("\nBACKPRESSURE:");
        println!("  Commands: {} ({:.4}%)", cmd_bp, cmd_bp_rate);
        println!("  Queries:  {} ({:.4}%)", qry_bp, qry_bp_rate);

        // Concurrent progression
        let min_obs = self.min_observed.load(Ordering::Relaxed);
        let max_obs = self.max_observed.load(Ordering::Relaxed);
        if min_obs != u64::MAX && max_obs > 0 {
            let range = max_obs.saturating_sub(min_obs);
            println!("\nCONCURRENT PROGRESSION:");
            println!("  Observed Range: {} - {} (width: {})", min_obs, max_obs, range);
            println!("  (Wide range validates queries ran during writes)");
        }

        // Latency
        let avg_cmd_us = if cmd_success > 0 {
            self.total_command_latency_us.load(Ordering::Relaxed) / cmd_success
        } else {
            0
        };
        let avg_qry_us = if qry_success > 0 {
            self.total_query_latency_us.load(Ordering::Relaxed) / qry_success
        } else {
            0
        };
        println!("\nLATENCY:");
        println!("  Avg Command: {}us ({:.2}ms)", avg_cmd_us, avg_cmd_us as f64 / 1000.0);
        println!("  Avg Query:   {}us ({:.2}ms)", avg_qry_us, avg_qry_us as f64 / 1000.0);

        println!("{}", "=".repeat(70));
    }
}

// ============================================================================
// Test Domain: Counter
// ============================================================================

#[derive(Debug, Clone)]
struct CounterHandler;

#[derive(Debug, Clone)]
enum CounterCommand {
    Initialize,
    Increment,
}

#[derive(Debug, Clone)]
struct GetCount;

impl DomainHandler for CounterHandler {
    type Command = CounterCommand;
    type Query = GetCount;
    type QueryResponse = u64;

    fn handle_command(
        &mut self,
        tx: &rusqlite::Transaction,
        command: Self::Command,
    ) -> std::result::Result<(), rusqlite::Error> {
        match command {
            CounterCommand::Initialize => {
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS counter (value INTEGER PRIMARY KEY)",
                    [],
                )?;
                tx.execute("INSERT OR REPLACE INTO counter VALUES (0)", [])?;
            }
            CounterCommand::Increment => {
                tx.execute("UPDATE counter SET value = value + 1", [])?;
            }
        }
        Ok(())
    }

    fn handle_query(
        &self,
        conn: &rusqlite::Connection,
        _query: Self::Query,
    ) -> std::result::Result<Self::QueryResponse, rusqlite::Error> {
        conn.query_row("SELECT value FROM counter", [], |row| row.get(0))
    }
}

// ============================================================================
// Tracing initialization helper
// ============================================================================

fn init_tracing(level: tracing::Level) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .with_thread_ids(true)
        .try_init();
}

// ============================================================================
// SYNC TESTS (default feature)
// ============================================================================

#[cfg(not(feature = "tokio"))]
mod sync_tests {
    use super::*;
    use std::thread;

    /// Quick sanity test with tracing - verifies basic functionality
    #[test]
    fn test_basic_with_tracing() -> Result<()> {
        init_tracing(tracing::Level::INFO);

        let db_path = "/tmp/test_basic_sync.db";
        let _ = std::fs::remove_file(db_path);

        let dispatcher = Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?;
        dispatcher.execute_command(CounterCommand::Initialize)?;

        for _ in 0..10 {
            dispatcher.execute_command(CounterCommand::Increment)?;
        }

        let count = dispatcher.execute_query(GetCount)?;
        assert_eq!(count, 10);

        dispatcher.shutdown()?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    /// Concurrent access test - multiple threads sharing dispatcher
    ///
    /// Validates thread safety and concurrent execution patterns.
    #[test]
    fn test_concurrent_access() -> Result<()> {
        init_tracing(tracing::Level::WARN); // Reduce noise

        let db_path = "/tmp/test_concurrent_sync.db";
        let _ = std::fs::remove_file(db_path);

        println!("\nConcurrent Access Test (Sync)");
        println!("Threads: 1 writer + 3 readers");

        let dispatcher = Arc::new(Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?);
        dispatcher.execute_command(CounterCommand::Initialize)?;

        let metrics = Arc::new(ProductionMetrics::new());
        let start = Instant::now();

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let d = dispatcher.clone();
                let m = metrics.clone();
                let is_writer = i == 0;

                thread::spawn(move || {
                    for _ in 0..100 {
                        if is_writer {
                            let cmd_start = Instant::now();
                            match d.execute_command(CounterCommand::Increment) {
                                Ok(()) => m.record_command_success(cmd_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::CommandWorkerBusy) => m.record_command_backpressure(),
                                Err(e) => panic!("Command error: {:?}", e),
                            }
                        } else {
                            let qry_start = Instant::now();
                            match d.execute_query(GetCount) {
                                Ok(v) => m.record_query_success(v, qry_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::QueryWorkersBusy) => m.record_query_backpressure(),
                                Err(e) => panic!("Query error: {:?}", e),
                            }
                        }
                        thread::sleep(Duration::from_micros(100));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        metrics.report("Concurrent Access (Sync)", start.elapsed().as_secs_f64());

        let final_count = dispatcher.execute_query(GetCount)?;
        let expected = metrics.commands_succeeded.load(Ordering::Relaxed);
        assert_eq!(final_count, expected, "Final count should match successful commands");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    /// High-load boundary test - finds backpressure limits
    ///
    /// Pushes the system to find where backpressure kicks in.
    #[test]
    fn test_backpressure_boundary() -> Result<()> {
        init_tracing(tracing::Level::WARN);

        let db_path = "/tmp/test_backpressure_sync.db";
        let _ = std::fs::remove_file(db_path);

        println!("\nBackpressure Boundary Test (Sync)");
        println!("Threads: 4 writers (no delay) - finding saturation point");

        let dispatcher = Arc::new(Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?);
        dispatcher.execute_command(CounterCommand::Initialize)?;

        let metrics = Arc::new(ProductionMetrics::new());
        let start = Instant::now();

        // 4 aggressive writers with no delay - should trigger backpressure
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let d = dispatcher.clone();
                let m = metrics.clone();

                thread::spawn(move || {
                    for _ in 0..500 {
                        let cmd_start = Instant::now();
                        match d.execute_command(CounterCommand::Increment) {
                            Ok(()) => m.record_command_success(cmd_start.elapsed().as_micros() as u64),
                            Err(rusqlite_dispatcher::Error::CommandWorkerBusy) => m.record_command_backpressure(),
                            Err(e) => panic!("Command error: {:?}", e),
                        }
                        // No delay - maximum pressure
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        metrics.report("Backpressure Boundary (Sync)", start.elapsed().as_secs_f64());

        let bp = metrics.commands_backpressure.load(Ordering::Relaxed);
        println!("\nBackpressure events: {}", bp);
        if bp > 0 {
            println!("  -> System reached saturation point (expected behavior)");
        } else {
            println!("  -> No backpressure (channel size may be too large for this load)");
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    /// Mixed workload 80/20 - realistic API pattern
    #[test]
    fn test_mixed_workload_80_20() -> Result<()> {
        init_tracing(tracing::Level::WARN);

        let db_path = "/tmp/test_mixed_80_20_sync.db";
        let _ = std::fs::remove_file(db_path);

        const DURATION_SECS: u64 = 5;
        const NUM_WORKERS: usize = 8;

        println!("\nMixed Workload 80/20 Test (Sync)");
        println!("Duration: {}s | Workers: {}", DURATION_SECS, NUM_WORKERS);

        let dispatcher = Arc::new(Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?);
        dispatcher.execute_command(CounterCommand::Initialize)?;

        let metrics = Arc::new(ProductionMetrics::new());
        let test_complete = Arc::new(AtomicBool::new(false));
        let start = Instant::now();

        let handles: Vec<_> = (0..NUM_WORKERS)
            .map(|worker_id| {
                let d = dispatcher.clone();
                let m = metrics.clone();
                let complete = test_complete.clone();

                thread::spawn(move || {
                    let mut op_count = 0u64;
                    while !complete.load(Ordering::Relaxed) {
                        // 80% reads, 20% writes
                        let is_read = (op_count % 5) != 0; // 4 reads per 1 write

                        if is_read {
                            let qry_start = Instant::now();
                            match d.execute_query(GetCount) {
                                Ok(v) => m.record_query_success(v, qry_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::QueryWorkersBusy) => m.record_query_backpressure(),
                                Err(e) => panic!("Worker {} query error: {:?}", worker_id, e),
                            }
                        } else {
                            let cmd_start = Instant::now();
                            match d.execute_command(CounterCommand::Increment) {
                                Ok(()) => m.record_command_success(cmd_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::CommandWorkerBusy) => m.record_command_backpressure(),
                                Err(e) => panic!("Worker {} command error: {:?}", worker_id, e),
                            }
                        }

                        op_count += 1;
                        thread::sleep(Duration::from_millis(1));
                    }
                })
            })
            .collect();

        thread::sleep(Duration::from_secs(DURATION_SECS));
        test_complete.store(true, Ordering::Relaxed);

        for h in handles {
            h.join().unwrap();
        }

        metrics.report("Mixed Workload 80/20 (Sync)", start.elapsed().as_secs_f64());

        // Validate final consistency
        let final_count = dispatcher.execute_query(GetCount)?;
        let expected = metrics.commands_succeeded.load(Ordering::Relaxed);
        assert_eq!(final_count, expected, "Database count should match successful commands");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
}

// ============================================================================
// ASYNC TESTS (tokio feature)
// ============================================================================

#[cfg(feature = "tokio")]
mod async_tests {
    use super::*;

    /// Quick sanity test with tracing
    #[tokio::test]
    async fn test_basic_with_tracing() -> Result<()> {
        init_tracing(tracing::Level::INFO);

        let db_path = "/tmp/test_basic_async.db";
        let _ = std::fs::remove_file(db_path);

        let dispatcher = Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?;
        dispatcher.execute_command(CounterCommand::Initialize).await?;

        for _ in 0..10 {
            dispatcher.execute_command(CounterCommand::Increment).await?;
        }

        let count = dispatcher.execute_query(GetCount).await?;
        assert_eq!(count, 10);

        dispatcher.shutdown()?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    /// Concurrent access test with tokio tasks
    #[tokio::test]
    async fn test_concurrent_access() -> Result<()> {
        init_tracing(tracing::Level::WARN);

        let db_path = "/tmp/test_concurrent_async.db";
        let _ = std::fs::remove_file(db_path);

        println!("\nConcurrent Access Test (Async)");
        println!("Tasks: 1 writer + 3 readers");

        let dispatcher = Arc::new(Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?);
        dispatcher.execute_command(CounterCommand::Initialize).await?;

        let metrics = Arc::new(ProductionMetrics::new());
        let start = Instant::now();

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let d = dispatcher.clone();
                let m = metrics.clone();
                let is_writer = i == 0;

                tokio::spawn(async move {
                    for _ in 0..100 {
                        if is_writer {
                            let cmd_start = Instant::now();
                            match d.execute_command(CounterCommand::Increment).await {
                                Ok(()) => m.record_command_success(cmd_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::CommandWorkerBusy) => m.record_command_backpressure(),
                                Err(e) => panic!("Command error: {:?}", e),
                            }
                        } else {
                            let qry_start = Instant::now();
                            match d.execute_query(GetCount).await {
                                Ok(v) => m.record_query_success(v, qry_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::QueryWorkersBusy) => m.record_query_backpressure(),
                                Err(e) => panic!("Query error: {:?}", e),
                            }
                        }
                        tokio::time::sleep(Duration::from_micros(100)).await;
                    }
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        metrics.report("Concurrent Access (Async)", start.elapsed().as_secs_f64());

        let final_count = dispatcher.execute_query(GetCount).await?;
        let expected = metrics.commands_succeeded.load(Ordering::Relaxed);
        assert_eq!(final_count, expected);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    /// High-load boundary test - finds backpressure limits
    #[tokio::test]
    async fn test_backpressure_boundary() -> Result<()> {
        init_tracing(tracing::Level::WARN);

        let db_path = "/tmp/test_backpressure_async.db";
        let _ = std::fs::remove_file(db_path);

        println!("\nBackpressure Boundary Test (Async)");
        println!("Tasks: 4 writers (no delay) - finding saturation point");

        let dispatcher = Arc::new(Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?);
        dispatcher.execute_command(CounterCommand::Initialize).await?;

        let metrics = Arc::new(ProductionMetrics::new());
        let start = Instant::now();

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let d = dispatcher.clone();
                let m = metrics.clone();

                tokio::spawn(async move {
                    for _ in 0..500 {
                        let cmd_start = Instant::now();
                        match d.execute_command(CounterCommand::Increment).await {
                            Ok(()) => m.record_command_success(cmd_start.elapsed().as_micros() as u64),
                            Err(rusqlite_dispatcher::Error::CommandWorkerBusy) => m.record_command_backpressure(),
                            Err(e) => panic!("Command error: {:?}", e),
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        metrics.report("Backpressure Boundary (Async)", start.elapsed().as_secs_f64());

        let bp = metrics.commands_backpressure.load(Ordering::Relaxed);
        println!("\nBackpressure events: {}", bp);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    /// Mixed workload 80/20 - realistic API pattern
    #[tokio::test]
    async fn test_mixed_workload_80_20() -> Result<()> {
        init_tracing(tracing::Level::WARN);

        let db_path = "/tmp/test_mixed_80_20_async.db";
        let _ = std::fs::remove_file(db_path);

        const DURATION_SECS: u64 = 5;
        const NUM_TASKS: usize = 8;

        println!("\nMixed Workload 80/20 Test (Async)");
        println!("Duration: {}s | Tasks: {}", DURATION_SECS, NUM_TASKS);

        let dispatcher = Arc::new(Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?);
        dispatcher.execute_command(CounterCommand::Initialize).await?;

        let metrics = Arc::new(ProductionMetrics::new());
        let test_complete = Arc::new(AtomicBool::new(false));
        let start = Instant::now();

        let handles: Vec<_> = (0..NUM_TASKS)
            .map(|task_id| {
                let d = dispatcher.clone();
                let m = metrics.clone();
                let complete = test_complete.clone();

                tokio::spawn(async move {
                    let mut op_count = 0u64;
                    while !complete.load(Ordering::Relaxed) {
                        let is_read = (op_count % 5) != 0;

                        if is_read {
                            let qry_start = Instant::now();
                            match d.execute_query(GetCount).await {
                                Ok(v) => m.record_query_success(v, qry_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::QueryWorkersBusy) => m.record_query_backpressure(),
                                Err(e) => panic!("Task {} query error: {:?}", task_id, e),
                            }
                        } else {
                            let cmd_start = Instant::now();
                            match d.execute_command(CounterCommand::Increment).await {
                                Ok(()) => m.record_command_success(cmd_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::CommandWorkerBusy) => m.record_command_backpressure(),
                                Err(e) => panic!("Task {} command error: {:?}", task_id, e),
                            }
                        }

                        op_count += 1;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
            })
            .collect();

        tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
        test_complete.store(true, Ordering::Relaxed);

        for h in handles {
            h.await.unwrap();
        }

        metrics.report("Mixed Workload 80/20 (Async)", start.elapsed().as_secs_f64());

        let final_count = dispatcher.execute_query(GetCount).await?;
        let expected = metrics.commands_succeeded.load(Ordering::Relaxed);
        assert_eq!(final_count, expected);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    /// Duration-based concurrent users test - production-like simulation
    #[tokio::test]
    async fn test_concurrent_users_duration() -> Result<()> {
        init_tracing(tracing::Level::WARN);

        let db_path = "/tmp/test_users_async.db";
        let _ = std::fs::remove_file(db_path);

        const DURATION_SECS: u64 = 10;
        const CONCURRENT_USERS: usize = 20;

        println!("\nConcurrent Users Duration Test (Async)");
        println!("Duration: {}s | Users: {}", DURATION_SECS, CONCURRENT_USERS);

        let dispatcher = Arc::new(Dispatcher::new(db_path, CounterHandler, DispatcherConfig::default())?);
        dispatcher.execute_command(CounterCommand::Initialize).await?;

        let metrics = Arc::new(ProductionMetrics::new());
        let test_complete = Arc::new(AtomicBool::new(false));
        let start = Instant::now();

        let handles: Vec<_> = (0..CONCURRENT_USERS)
            .map(|user_id| {
                let d = dispatcher.clone();
                let m = metrics.clone();
                let complete = test_complete.clone();

                tokio::spawn(async move {
                    while !complete.load(Ordering::Relaxed) {
                        // Simulate network delay
                        tokio::time::sleep(Duration::from_millis(5 + (user_id as u64 % 10))).await;

                        // 80% reads, 20% writes
                        let is_read = user_id % 5 != 0;

                        if is_read {
                            let qry_start = Instant::now();
                            match d.execute_query(GetCount).await {
                                Ok(v) => m.record_query_success(v, qry_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::QueryWorkersBusy) => m.record_query_backpressure(),
                                Err(e) => panic!("User {} error: {:?}", user_id, e),
                            }
                        } else {
                            let cmd_start = Instant::now();
                            match d.execute_command(CounterCommand::Increment).await {
                                Ok(()) => m.record_command_success(cmd_start.elapsed().as_micros() as u64),
                                Err(rusqlite_dispatcher::Error::CommandWorkerBusy) => m.record_command_backpressure(),
                                Err(e) => panic!("User {} error: {:?}", user_id, e),
                            }
                        }
                    }
                })
            })
            .collect();

        // Progress reporting
        let progress_metrics = metrics.clone();
        let progress_complete = test_complete.clone();
        let progress_handle = tokio::spawn(async move {
            let mut last_ops = 0u64;
            while !progress_complete.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let cmds = progress_metrics.commands_succeeded.load(Ordering::Relaxed);
                let queries = progress_metrics.queries_succeeded.load(Ordering::Relaxed);
                let total = cmds + queries;
                let rate = (total - last_ops) / 2;
                println!("  Progress: {} ops ({}/s)", total, rate);
                last_ops = total;
            }
        });

        tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
        test_complete.store(true, Ordering::Relaxed);

        for h in handles {
            h.await.unwrap();
        }
        progress_handle.await.unwrap();

        metrics.report("Concurrent Users Duration (Async)", start.elapsed().as_secs_f64());

        // Validate concurrent progression
        let min = metrics.min_observed.load(Ordering::Relaxed);
        let max = metrics.max_observed.load(Ordering::Relaxed);
        if min != u64::MAX && max > 0 {
            let range = max - min;
            assert!(range > max / 10, "Observation range too narrow - queries may not have run concurrently");
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
}
