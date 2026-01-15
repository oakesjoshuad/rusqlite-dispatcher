//! Advanced stress tests with extended metrics and configurable parameters
//!
//! Features:
//! - Configurable runtime and user counts
//! - Throughput counters (commands/queries per second)
//! - Detailed latency metrics (min/max/avg/percentiles)
//! - Backpressure detection
//! - Workload variation (read/write mixes)
//! - Periodic progress logging for long runs
//! - Per-operation error tracking
//! - Concurrency metrics
//! - Post-test summary report

use rusqlite_dispatcher::{Dispatcher, DispatcherConfig, DomainHandler};
use rusqlite::{Connection, Result as RusqliteResult, Transaction};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use serial_test::serial;

// ============================================================================
// Configuration Constants
// ============================================================================

/// Default runtime for long stress tests (seconds)
const DEFAULT_RUNTIME_SECS: u64 = 120;

/// Default number of simulated users
const DEFAULT_NUM_USERS: usize = 20;

/// Interval for progress logging (seconds)
const PROGRESS_LOG_INTERVAL_SECS: u64 = 10;

// ============================================================================
// Domain Types
// ============================================================================

#[derive(Debug, Clone)]
struct IncrementCommand {
    amount: i64,
}

#[derive(Debug, Clone)]
struct GetCountQuery;

// ============================================================================
// Metrics Collection
// ============================================================================

#[derive(Debug)]
struct LatencyStats {
    samples: Arc<Mutex<Vec<Duration>>>,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            samples: Arc::new(Mutex::new(Vec::with_capacity(1_000_000))),
        }
    }

    fn record(&self, duration: Duration) {
        if let Ok(mut samples) = self.samples.lock() {
            samples.push(duration);
        }
    }

    fn compute_percentile(&self, percentile: f64) -> Duration {
        if let Ok(mut samples) = self.samples.lock() {
            if samples.is_empty() {
                return Duration::ZERO;
            }

            samples.sort_unstable();
            let idx = ((samples.len() as f64) * percentile).ceil() as usize - 1;
            samples[idx.min(samples.len() - 1)]
        } else {
            Duration::ZERO
        }
    }

    fn min(&self) -> Duration {
        self.samples
            .lock()
            .ok()
            .and_then(|s| s.iter().min().copied())
            .unwrap_or(Duration::ZERO)
    }

    fn max(&self) -> Duration {
        self.samples
            .lock()
            .ok()
            .and_then(|s| s.iter().max().copied())
            .unwrap_or(Duration::ZERO)
    }

    fn avg(&self) -> Duration {
        if let Ok(samples) = self.samples.lock() {
            if samples.is_empty() {
                return Duration::ZERO;
            }
            let sum: Duration = samples.iter().copied().sum();
            sum / samples.len() as u32
        } else {
            Duration::ZERO
        }
    }
}

#[derive(Debug)]
struct MetricsCollector {
    // Throughput counters
    total_commands: AtomicUsize,
    total_queries: AtomicUsize,

    // Error tracking
    failed_commands: AtomicUsize,
    failed_queries: AtomicUsize,

    // Backpressure tracking
    backpressure_events: AtomicUsize,

    // Latency tracking
    command_latency: LatencyStats,
    query_latency: LatencyStats,

    // Concurrency tracking
    active_commands: AtomicUsize,
    active_queries: AtomicUsize,
    max_concurrent_commands: AtomicUsize,
    max_concurrent_queries: AtomicUsize,

    // Per-user tracking
    per_user_commands: Vec<AtomicUsize>,
    per_user_queries: Vec<AtomicUsize>,

    // Test configuration
    start_time: Instant,
}

impl MetricsCollector {
    fn new(num_users: usize) -> Arc<Self> {
        Arc::new(Self {
            total_commands: AtomicUsize::new(0),
            total_queries: AtomicUsize::new(0),
            failed_commands: AtomicUsize::new(0),
            failed_queries: AtomicUsize::new(0),
            backpressure_events: AtomicUsize::new(0),
            command_latency: LatencyStats::new(),
            query_latency: LatencyStats::new(),
            active_commands: AtomicUsize::new(0),
            active_queries: AtomicUsize::new(0),
            max_concurrent_commands: AtomicUsize::new(0),
            max_concurrent_queries: AtomicUsize::new(0),
            per_user_commands: (0..num_users).map(|_| AtomicUsize::new(0)).collect(),
            per_user_queries: (0..num_users).map(|_| AtomicUsize::new(0)).collect(),
            start_time: Instant::now(),
        })
    }

    fn record_command_start(&self) {
        let active = self.active_commands.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_concurrent_commands
            .fetch_max(active, Ordering::Relaxed);
    }

    fn record_command_end(&self, user_id: usize, latency: Duration, success: bool) {
        self.active_commands.fetch_sub(1, Ordering::Relaxed);
        if success {
            self.total_commands.fetch_add(1, Ordering::Relaxed);
            self.command_latency.record(latency);
            if let Some(slot) = self.per_user_commands.get(user_id) {
                slot.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.failed_commands.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_query_start(&self) {
        let active = self.active_queries.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_concurrent_queries
            .fetch_max(active, Ordering::Relaxed);
    }

    fn record_query_end(&self, user_id: usize, latency: Duration, success: bool) {
        self.active_queries.fetch_sub(1, Ordering::Relaxed);
        if success {
            self.total_queries.fetch_add(1, Ordering::Relaxed);
            self.query_latency.record(latency);
            if let Some(slot) = self.per_user_queries.get(user_id) {
                slot.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.failed_queries.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_backpressure(&self) {
        self.backpressure_events
            .fetch_add(1, Ordering::Relaxed);
    }

    fn log_progress(&self) {
        let elapsed = self.start_time.elapsed();
        let total_cmds = self.total_commands.load(Ordering::Relaxed);
        let total_queries = self.total_queries.load(Ordering::Relaxed);
        let failed_cmds = self.failed_commands.load(Ordering::Relaxed);
        let failed_queries = self.failed_queries.load(Ordering::Relaxed);
        let active_cmds = self.active_commands.load(Ordering::Relaxed);
        let active_queries = self.active_queries.load(Ordering::Relaxed);

        let cmd_rate = total_cmds as f64 / elapsed.as_secs_f64();
        let query_rate = total_queries as f64 / elapsed.as_secs_f64();

        println!(
            "[{:>6.1}s] Commands: {:>7} ({:>7.1}/s) | Queries: {:>7} ({:>7.1}/s) \
             | Active: C={:>3} Q={:>3} | Errors: C={:>4} Q={:>4}",
            elapsed.as_secs_f64(),
            total_cmds,
            cmd_rate,
            total_queries,
            query_rate,
            active_cmds,
            active_queries,
            failed_cmds,
            failed_queries
        );
    }

    fn print_summary(&self) {
        let elapsed = self.start_time.elapsed();
        let total_cmds = self.total_commands.load(Ordering::Relaxed);
        let total_queries = self.total_queries.load(Ordering::Relaxed);
        let failed_cmds = self.failed_commands.load(Ordering::Relaxed);
        let failed_queries = self.failed_queries.load(Ordering::Relaxed);
        let backpressure = self.backpressure_events.load(Ordering::Relaxed);
        let max_concurrent_cmds = self.max_concurrent_commands.load(Ordering::Relaxed);
        let max_concurrent_queries = self.max_concurrent_queries.load(Ordering::Relaxed);

        let ops = total_cmds + total_queries;
        let dur_s = elapsed.as_secs_f64();

        println!();
        println!("STRESS TEST SUMMARY");
        println!("================================================================================");
        println!("Duration           : {:>8.2} s", dur_s);
        println!("Total commands     : {:>8}   ({:>8.1} /s)", total_cmds, total_cmds as f64 / dur_s);
        println!("Total queries      : {:>8}   ({:>8.1} /s)", total_queries, total_queries as f64 / dur_s);
        println!("Total operations   : {:>8}   ({:>8.1} /s)", ops, ops as f64 / dur_s);
        println!("--------------------------------------------------------------------------------");

        // Latency - Commands
        let c_min = self.command_latency.min().as_secs_f64() * 1000.0;
        let c_max = self.command_latency.max().as_secs_f64() * 1000.0;
        let c_avg = self.command_latency.avg().as_secs_f64() * 1000.0;
        let c_p50 = self.command_latency.compute_percentile(0.50).as_secs_f64() * 1000.0;
        let c_p90 = self.command_latency.compute_percentile(0.90).as_secs_f64() * 1000.0;
        let c_p99 = self.command_latency.compute_percentile(0.99).as_secs_f64() * 1000.0;
        let c_p999 = self.command_latency.compute_percentile(0.999).as_secs_f64() * 1000.0;

        println!("LATENCY - COMMANDS (ms)");
        println!("--------------------------------------------------------------------------------");
        println!("  Metric   Value");
        println!("  Min      {:>8.3}", c_min);
        println!("  Max      {:>8.3}", c_max);
        println!("  Avg      {:>8.3}", c_avg);
        println!("  P50      {:>8.3}", c_p50);
        println!("  P90      {:>8.3}", c_p90);
        println!("  P99      {:>8.3}", c_p99);
        println!("  P99.9    {:>8.3}", c_p999);
        println!("--------------------------------------------------------------------------------");

        // Latency - Queries
        let q_min = self.query_latency.min().as_secs_f64() * 1000.0;
        let q_max = self.query_latency.max().as_secs_f64() * 1000.0;
        let q_avg = self.query_latency.avg().as_secs_f64() * 1000.0;
        let q_p50 = self.query_latency.compute_percentile(0.50).as_secs_f64() * 1000.0;
        let q_p90 = self.query_latency.compute_percentile(0.90).as_secs_f64() * 1000.0;
        let q_p99 = self.query_latency.compute_percentile(0.99).as_secs_f64() * 1000.0;
        let q_p999 = self.query_latency.compute_percentile(0.999).as_secs_f64() * 1000.0;

        println!("LATENCY - QUERIES (ms)");
        println!("--------------------------------------------------------------------------------");
        println!("  Metric   Value");
        println!("  Min      {:>8.3}", q_min);
        println!("  Max      {:>8.3}", q_max);
        println!("  Avg      {:>8.3}", q_avg);
        println!("  P50      {:>8.3}", q_p50);
        println!("  P90      {:>8.3}", q_p90);
        println!("  P99      {:>8.3}", q_p99);
        println!("  P99.9    {:>8.3}", q_p999);
        println!("--------------------------------------------------------------------------------");

        println!("CONCURRENCY");
        println!("--------------------------------------------------------------------------------");
        println!("  Max concurrent commands : {:>4}", max_concurrent_cmds);
        println!("  Max concurrent queries  : {:>4}", max_concurrent_queries);
        println!("--------------------------------------------------------------------------------");

        println!("ERRORS & BACKPRESSURE");
        println!("--------------------------------------------------------------------------------");
        println!("  Failed commands    : {:>8}", failed_cmds);
        println!("  Failed queries     : {:>8}", failed_queries);
        println!("  Backpressure events: {:>8}", backpressure);
        println!("--------------------------------------------------------------------------------");

        println!("PER-USER DISTRIBUTION");
        println!("--------------------------------------------------------------------------------");
        println!("  User   Commands    Queries");
        for (i, (cmds, queries)) in self
            .per_user_commands
            .iter()
            .zip(self.per_user_queries.iter())
            .enumerate()
        {
            let cmd_count = cmds.load(Ordering::Relaxed);
            let query_count = queries.load(Ordering::Relaxed);
            println!("  {:>4}   {:>8}   {:>8}", i, cmd_count, query_count);
        }
        println!("================================================================================");
        println!();
    }
}

// ============================================================================
// Handler Implementation
// ============================================================================

#[derive(Clone)]
struct CounterHandler {
    counter: Arc<AtomicU64>,
}

impl CounterHandler {
    fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get_count(&self) -> i64 {
        self.counter.load(Ordering::SeqCst) as i64
    }
}

impl DomainHandler for CounterHandler {
    type Command = IncrementCommand;
    type Query = GetCountQuery;
    type QueryResponse = i64;

    fn handle_command(
        &mut self,
        _tx: &Transaction,
        cmd: Self::Command,
    ) -> RusqliteResult<()> {
        self.counter.fetch_add(cmd.amount as u64, Ordering::SeqCst);
        Ok(())
    }

    fn handle_query(
        &self,
        _conn: &Connection,
        _query: Self::Query,
    ) -> RusqliteResult<Self::QueryResponse> {
        Ok(self.get_count())
    }
}

// ============================================================================
// Workload Generators
// ============================================================================

enum WorkloadMix {
    /// 80% reads, 20% writes
    ReadHeavy,
    /// 50% reads, 50% writes
    Balanced,
    /// 20% reads, 80% writes
    WriteHeavy,
    /// Burst pattern: alternating heavy write and read phases
    Burst,
}

impl WorkloadMix {
    fn should_write(&self, iteration: usize) -> bool {
        use rand::Rng;
        let mut rng = rand::rng();

        match self {
            WorkloadMix::ReadHeavy => rng.random_ratio(2, 10),
            WorkloadMix::Balanced => rng.random_ratio(5, 10),
            WorkloadMix::WriteHeavy => rng.random_ratio(8, 10),
            WorkloadMix::Burst => {
                // Burst pattern: 100 iterations write, 100 iterations read
                (iteration / 100) % 2 == 0
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            WorkloadMix::ReadHeavy => "Read-heavy (80% queries, 20% commands)",
            WorkloadMix::Balanced => "Balanced (50% queries, 50% commands)",
            WorkloadMix::WriteHeavy => "Write-heavy (20% queries, 80% commands)",
            WorkloadMix::Burst => "Burst (alternating 100-iteration phases)",
        }
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

async fn simulate_user(
    user_id: usize,
    dispatcher: Arc<Dispatcher<CounterHandler>>,
    metrics: Arc<MetricsCollector>,
    workload: WorkloadMix,
    stop_signal: Arc<AtomicBool>,
) {
    let mut iteration = 0usize;

    while !stop_signal.load(Ordering::Relaxed) {
        if workload.should_write(iteration) {
            let start = Instant::now();
            metrics.record_command_start();

            let cmd = IncrementCommand { amount: 1 };
            let result = dispatcher.execute_command(cmd).await;

            let latency = start.elapsed();
            metrics.record_command_end(user_id, latency, result.is_ok());

            if result.is_err() {
                metrics.record_backpressure();
            }
        } else {
            let start = Instant::now();
            metrics.record_query_start();

            let query = GetCountQuery;
            let result = dispatcher.execute_query(query).await;

            let latency = start.elapsed();
            metrics.record_query_end(user_id, latency, result.is_ok());

            if result.is_err() {
                metrics.record_backpressure();
            }
        }

        iteration += 1;

        // Small randomized delay to simulate more realistic user behavior
        let delay_micros = rand::random::<u64>() % 100;
        tokio::time::sleep(Duration::from_micros(delay_micros)).await;
    }
}

async fn progress_logger(
    metrics: Arc<MetricsCollector>,
    stop_signal: Arc<AtomicBool>,
) {
    while !stop_signal.load(Ordering::Relaxed) {
        sleep(Duration::from_secs(PROGRESS_LOG_INTERVAL_SECS)).await;
        if !stop_signal.load(Ordering::Relaxed) {
            metrics.log_progress();
        }
    }
}

// ============================================================================
// Test Functions
// ============================================================================

#[tokio::test]
#[serial]
async fn test_extended_stress_balanced_workload() {
    run_stress_test(
        DEFAULT_NUM_USERS,
        DEFAULT_RUNTIME_SECS,
        WorkloadMix::Balanced,
        "Extended Stress Test - Balanced Workload (50/50)",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_extended_stress_read_heavy() {
    run_stress_test(
        DEFAULT_NUM_USERS,
        DEFAULT_RUNTIME_SECS,
        WorkloadMix::ReadHeavy,
        "Extended Stress Test - Read Heavy (80/20)",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_extended_stress_write_heavy() {
    run_stress_test(
        DEFAULT_NUM_USERS,
        DEFAULT_RUNTIME_SECS,
        WorkloadMix::WriteHeavy,
        "Extended Stress Test - Write Heavy (20/80)",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_extended_stress_burst_pattern() {
    run_stress_test(
        DEFAULT_NUM_USERS,
        DEFAULT_RUNTIME_SECS,
        WorkloadMix::Burst,
        "Extended Stress Test - Burst Pattern",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_variable_users_10() {
    run_stress_test(
        10,
        60,
        WorkloadMix::Balanced,
        "Variable Users Test - 10 Users",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_variable_users_50() {
    run_stress_test(
        50,
        60,
        WorkloadMix::Balanced,
        "Variable Users Test - 50 Users",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_short_burst() {
    run_stress_test(
        DEFAULT_NUM_USERS,
        10,
        WorkloadMix::Balanced,
        "Short Burst Test (10s)",
    )
    .await;
}

async fn run_stress_test(
    num_users: usize,
    runtime_secs: u64,
    workload: WorkloadMix,
    test_name: &str,
) {
    println!();
    println!("================================================================================");
    println!("{}", test_name);
    println!("================================================================================");
    println!("Users   : {}", num_users);
    println!("Runtime : {} s", runtime_secs);
    println!("Workload: {}", workload.name());
    println!("================================================================================");
    println!();

    let handler = CounterHandler::new();
    let config = DispatcherConfig::default();
    let dispatcher = Arc::new(
        Dispatcher::new(":memory:", handler, config)
            .expect("Failed to create dispatcher"),
    );

    let metrics = MetricsCollector::new(num_users);
    let stop_signal = Arc::new(AtomicBool::new(false));

    let logger_handle = {
        let metrics = Arc::clone(&metrics);
        let stop_signal = Arc::clone(&stop_signal);
        tokio::spawn(async move {
            progress_logger(metrics, stop_signal).await;
        })
    };

    let mut user_handles = Vec::with_capacity(num_users);
    for user_id in 0..num_users {
        let dispatcher = Arc::clone(&dispatcher);
        let metrics = Arc::clone(&metrics);
        let stop_signal = Arc::clone(&stop_signal);
        let workload = match workload {
            WorkloadMix::ReadHeavy => WorkloadMix::ReadHeavy,
            WorkloadMix::Balanced => WorkloadMix::Balanced,
            WorkloadMix::WriteHeavy => WorkloadMix::WriteHeavy,
            WorkloadMix::Burst => WorkloadMix::Burst,
        };

        let handle = tokio::spawn(async move {
            simulate_user(user_id, dispatcher, metrics, workload, stop_signal).await;
        });
        user_handles.push(handle);
    }

    sleep(Duration::from_secs(runtime_secs)).await;
    stop_signal.store(true, Ordering::Relaxed);

    for handle in user_handles {
        let _ = handle.await;
    }

    let _ = logger_handle.await;

    metrics.print_summary();

    match Arc::try_unwrap(dispatcher) {
        Ok(dispatcher) => {
            let _ = dispatcher.shutdown();
        }
        Err(_) => {
            println!("Note: dispatcher dropped via remaining Arc references");
        }
    }
}

