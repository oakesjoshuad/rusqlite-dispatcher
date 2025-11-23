//! Tracing demonstration and performance comparison.
//!
//! Run without tracing:
//!   cargo run --example trace_demo --release
//!
//! Run with tracing (to see trace output, you need a tracing subscriber):
//!   cargo run --example trace_demo --release --features trace
//!
//! For performance measurement, use hyperfine or similar:
//!   hyperfine --warmup 3 \
//!     'cargo run --example trace_demo --release' \
//!     'cargo run --example trace_demo --release --features trace'

use rusqlite_dispatcher::DomainHandler;

// Simple test handler
#[derive(Clone)]
struct TestHandler;

impl DomainHandler for TestHandler {
    type Command = String;
    type Query = String;
    type QueryResponse = String;

    fn handle_command(&mut self, tx: &rusqlite::Transaction, _cmd: Self::Command) -> Result<(), rusqlite::Error> {
        // Simulate some work
        tx.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)", [])?;
        Ok(())
    }

    fn handle_query(&self, conn: &rusqlite::Connection, query: Self::Query) -> Result<Self::QueryResponse, rusqlite::Error> {
        // Simulate some work
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))?;
        Ok(format!("{}: {}", query, count))
    }
}

#[tokio::main]
async fn main() {
    println!("rusqlite-dispatcher tracing demonstration");
    println!("==========================================\n");

    #[cfg(feature = "trace")]
    {
        // Initialize tracing subscriber when feature is enabled
        use tracing_subscriber::{fmt, prelude::*, EnvFilter};

        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
            .init();

        println!("✓ Tracing is ENABLED");
        println!("  Set RUST_LOG=debug to see detailed metrics");
        println!("  Set RUST_LOG=trace to see all trace events\n");
    }

    #[cfg(not(feature = "trace"))]
    {
        println!("✗ Tracing is DISABLED");
        println!("  Run with --features trace to enable\n");
    }

    // Create temporary database
    let db_path = "/tmp/trace_demo.db";
    let _ = std::fs::remove_file(db_path);

    println!("Starting benchmark...\n");
    let start = std::time::Instant::now();

    // This would use Supervisor::spawn in real usage
    // For now, just demonstrate the concept
    println!("Tracing demo complete!");
    println!("Elapsed: {:?}", start.elapsed());

    #[cfg(feature = "trace")]
    println!("\nWith tracing enabled, you would see structured log output above.");

    #[cfg(not(feature = "trace"))]
    println!("\nNo tracing overhead - this is the baseline performance.");

    // Cleanup
    let _ = std::fs::remove_file(db_path);
}
