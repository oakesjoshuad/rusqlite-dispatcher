# rusqlite-dispatcher

**Thread-safe SQLite dispatcher with CQRS pattern support**

A lightweight rusqlite extension providing concurrent command/query execution through a worker pool architecture. Built for applications requiring SQLite persistence with CQRS semantics.

## Features

- **CQRS pattern support** - Separate command (write) and query (read) paths
- **Worker pool architecture** - Single command worker + N query workers
- **Thread-safe** - Safe concurrent access from multiple threads
- **Backpressure handling** - Bounded channels with fail-fast semantics
- **Tracing integration** - Structured logging with worker_id and metrics
- **Feature-flagged async** - Sync by default, async with `tokio` feature

## Architecture

```
┌─────────────────────────────────────────┐
│            Application                  │
│  - Owns Dispatcher (lifecycle)          │
│  - Calls shutdown() when done           │
└─────────────────┬───────────────────────┘
                  │
                  │ dispatcher.handle()
                  ▼
┌─────────────────────────────────────────┐
│        SupervisorHandle (Clone)         │
│  - CommandSender + QuerySender          │
│  - Used by repositories/services        │
└─────────────────┬───────────────────────┘
                  │
       ┌──────────┴──────────┐
       ▼                     ▼
┌─────────────┐       ┌─────────────┐
│  Command    │       │   Query     │
│  Worker     │       │  Workers    │
│  (1 R/W)    │       │  (N R/O)    │
│   MPSC      │       │   MPMC      │
└─────────────┘       └─────────────┘
```

## Quick Start

### Installation

```toml
[dependencies]
rusqlite-dispatcher = "0.1"

# For async support
rusqlite-dispatcher = { version = "0.1", features = ["tokio"] }
```

### Basic Usage

```rust
use rusqlite_dispatcher::{Dispatcher, DispatcherConfig, DomainHandler, Result};

// 1. Define your domain types
#[derive(Debug, Clone)]
enum MyCommand {
    Create { id: u64, data: String },
    Update { id: u64, data: String },
}

#[derive(Debug, Clone)]
enum MyQuery {
    FindById { id: u64 },
}

// 2. Implement DomainHandler
#[derive(Debug, Clone)]
struct MyHandler;

impl DomainHandler for MyHandler {
    type Command = MyCommand;
    type Query = MyQuery;
    type QueryResponse = Option<String>;

    fn handle_command(
        &mut self,
        tx: &rusqlite::Transaction,
        command: Self::Command,
    ) -> std::result::Result<(), rusqlite::Error> {
        match command {
            MyCommand::Create { id, data } => {
                tx.execute(
                    "INSERT INTO items (id, data) VALUES (?, ?)",
                    rusqlite::params![id, data],
                )?;
            }
            MyCommand::Update { id, data } => {
                tx.execute(
                    "UPDATE items SET data = ? WHERE id = ?",
                    rusqlite::params![data, id],
                )?;
            }
        }
        Ok(())
    }

    fn handle_query(
        &self,
        conn: &rusqlite::Connection,
        query: Self::Query,
    ) -> std::result::Result<Self::QueryResponse, rusqlite::Error> {
        match query {
            MyQuery::FindById { id } => {
                conn.query_row(
                    "SELECT data FROM items WHERE id = ?",
                    rusqlite::params![id],
                    |row| row.get(0),
                ).optional()
            }
        }
    }
}

// 3. Create dispatcher and use it
fn main() -> Result<()> {
    let config = DispatcherConfig::default();
    let dispatcher = Dispatcher::new("my.db", MyHandler, config)?;

    // Execute commands and queries
    dispatcher.execute_command(MyCommand::Create {
        id: 1,
        data: "hello".into()
    })?;

    let result = dispatcher.execute_query(MyQuery::FindById { id: 1 })?;
    println!("Found: {:?}", result);

    // Graceful shutdown
    dispatcher.shutdown()?;
    Ok(())
}
```

## SupervisorHandle Pattern

The library separates lifecycle management from operations:

- **Dispatcher** - Owns the supervisor and workers, manages lifecycle
- **SupervisorHandle** - Cloneable handle for executing operations

```rust
// Application owns dispatcher
let dispatcher = Dispatcher::new("db.sqlite", handler, config)?;

// Get handle for repositories
let handle = dispatcher.handle();

// Create repository with handle
let repository = Arc::new(MyRepository::new(handle.clone()));

// Services use repository
let service = MyService::new(repository.clone());

// ... later, when shutting down ...

// IMPORTANT: Drop all handles before shutdown
drop(repository);
drop(service);

// Graceful shutdown
dispatcher.shutdown()?;
```

## Configuration

```rust
use rusqlite_dispatcher::{DispatcherConfig, supervisor::SupervisorConfig};

let config = DispatcherConfig {
    supervisor: SupervisorConfig {
        query_workers: 4,        // Number of query workers (MPMC)
        max_query_workers: 8,    // Reserved for future scaling
        channel_size: 10_000,    // Bounded channel capacity
    },
};
```

## Feature Flags

- **default** - Synchronous blocking API
- **tokio** - Async/await API with tokio runtime

```rust
// With tokio feature
#[tokio::main]
async fn main() -> Result<()> {
    let dispatcher = Dispatcher::new("db.sqlite", handler, config)?;

    dispatcher.execute_command(command).await?;
    let result = dispatcher.execute_query(query).await?;

    dispatcher.shutdown()?;
    Ok(())
}
```

## Tracing

The library emits structured tracing events for observability:

```rust
// Enable tracing subscriber
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .init();

// Events emitted:
// - worker_id: ULID identifying the worker
// - worker_type: Command or Query
// - operation: "command" or "query"
// - duration_micros: Operation duration
// - success: true/false
```

Example output:
```
INFO dispatcher_operation_completed worker_id=01JDXYZ... worker_type=Command operation="command" duration_micros=1234 success=true
```

## Error Handling

```rust
match dispatcher.execute_command(command) {
    Ok(()) => println!("Success"),
    Err(Error::CommandWorkerBusy) => println!("Backpressure - retry later"),
    Err(Error::QueryWorkersBusy) => println!("Backpressure - retry later"),
    Err(Error::Database(e)) => println!("Database error: {}", e),
    Err(Error::Internal(msg)) => println!("Internal error: {}", msg),
}
```

## Testing

```bash
# Run all tests
cargo test

# Run with tokio feature
cargo test --features tokio

# Run stress tests with output
cargo test --test stress_tests -- --nocapture

# Run integration tests
cargo test --test integration_tests
```

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture documentation
- [INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md) - Integration patterns and best practices

## Performance Characteristics

- **Dispatcher creation**: Thread spawning overhead
- **Handle clone**: Reference counting (cheap)
- **Command/query send**: Lock-free crossbeam channels
- **Backpressure**: Immediate fail-fast on full channels
- **Shutdown**: Blocks until all workers join

## License

MIT OR Apache-2.0
