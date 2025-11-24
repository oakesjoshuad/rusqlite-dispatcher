# Test Suite

This directory contains integration and stress tests for rusqlite-dispatcher. All tests are feature-gated to support both sync (default) and async (tokio) APIs.

## Running Tests

```bash
# Run all tests (sync)
cargo test

# Run all tests (async/tokio)
cargo test --features tokio

# Run with tracing output
cargo test --test stress_tests -- --nocapture

# Run specific test
cargo test test_mixed_workload_80_20 -- --nocapture

# Filter tracing to warnings only
RUST_LOG=warn cargo test --test stress_tests -- --nocapture
```

## Tracing Configuration

Tracing output can be filtered programmatically or via environment variable:

```bash
# Environment variable
RUST_LOG=warn cargo test --test stress_tests -- --nocapture
```

Available levels (most to least verbose):
- `TRACE` - Everything
- `DEBUG` - Debug and above
- `INFO` - Info, warnings, errors (default)
- `WARN` - Warnings and errors only
- `ERROR` - Errors only

## Test Structure

### Integration Tests (`integration_tests.rs`)

Tests for dispatcher lifecycle, handle patterns, and CRUD operations.

| Test | Characteristics Validated |
|------|--------------------------|
| `test_dispatcher_creation_and_handle` | Dispatcher creation, handle cloning |
| `test_repository_with_handle` | SupervisorHandle in repository pattern |
| `test_dispatcher_lifecycle` | Proper shutdown sequence |
| `test_handle_is_cloneable` | Multiple handle clones coexist |
| `test_multiple_services_share_handle` | Services share cloned handles |
| `test_command_execution` | Commands modify state correctly |
| `test_query_execution` | Queries return correct results |
| `test_update_and_delete` | Update/delete operations |

### Stress Tests (`stress_tests.rs`)

Production-realistic stress tests that find system boundaries.

| Test | Duration | What It Measures |
|------|----------|-----------------|
| `test_basic_with_tracing` | ~1s | Sanity check with INFO-level tracing |
| `test_concurrent_access` | ~1s | Thread safety, concurrent execution |
| `test_backpressure_boundary` | ~1s | Saturation point (4 writers, no delay) |
| `test_mixed_workload_80_20` | 5s | Realistic 80/20 read/write ratio |
| `test_concurrent_users_duration` | 10s | Production simulation with 20 users |

## Metrics Output

The stress tests produce comprehensive metrics:

```
======================================================================
Test: Mixed Workload 80/20 (Sync)
======================================================================

THROUGHPUT:
  Duration:          5.00s
  Total Operations:  28121
  Operations/sec:    5621
  Commands/sec:      1125
  Queries/sec:       4496

WORKLOAD MIX:
  Commands (writes): 5629 (20.0%)
  Queries (reads):   22492 (80.0%)

SUCCESS RATES:
  Commands: 5629 (100.00%)
  Queries:  22492 (100.00%)

BACKPRESSURE:
  Commands: 0 (0.0000%)
  Queries:  0 (0.0000%)

CONCURRENT PROGRESSION:
  Observed Range: 8 - 5625 (width: 5617)
  (Wide range validates queries ran during writes)

LATENCY:
  Avg Command: 546us (0.55ms)
  Avg Query:   259us (0.26ms)
======================================================================
```

## Key Characteristics Tested

### Correctness
- Commands modify database state correctly
- Queries return accurate results
- Final database count matches successful commands
- Transactions are properly committed

### Concurrency
- Multiple threads/tasks execute simultaneously
- Wide observation range proves concurrent execution
- MPSC pattern for commands (single writer)
- MPMC pattern for queries (concurrent readers)

### Performance Boundaries
- Throughput (ops/sec, TPS, QPS)
- Latency (average command/query time)
- Backpressure rate under saturation
- Workload mix validation (80/20, etc.)

### Lifecycle
- Dispatcher creation spawns workers
- Handles can be freely cloned
- All handles must be dropped before shutdown
- Graceful shutdown waits for workers

### Observability
- Tracing events with worker_id, worker_type
- Duration measurement in microseconds
- Success/failure recording
- Configurable log levels

## Tracing Output Format

When running with `--nocapture`, you'll see tracing events:

```
INFO dispatcher_operation_completed worker_id=01KASKXXF708WQA1Y4FZ2YED69 worker_type=Command operation="command" duration_micros=179 success=true
```

**Fields:**
- `worker_id` - ULID uniquely identifying the worker
- `worker_type` - `Command` or `Query`
- `operation` - `"command"` or `"query"`
- `duration_micros` - Operation duration in microseconds
- `success` - `true` or `false`

## Test Domain Models

### Notes Domain (Integration Tests)

```rust
enum NoteCommand {
    CreateTable,
    Create { id, title, content },
    Update { id, title, content },
    Delete { id },
}

enum NoteQuery {
    FindById { id },
    ListAll,
    Count,
}
```

### Counter Domain (Stress Tests)

```rust
enum CounterCommand {
    Initialize,
    Increment,
}

struct GetCount;  // Query returning u64
```

## Feature Parity

Both sync and tokio features have equivalent tests:

| Feature | Sync Implementation | Tokio Implementation |
|---------|--------------------|--------------------|
| Test attribute | `#[test]` | `#[tokio::test]` |
| Concurrency | `std::thread::spawn` | `tokio::spawn` |
| Sleep | `std::thread::sleep` | `tokio::time::sleep` |
| Execute | `dispatcher.execute_*()` | `dispatcher.execute_*().await` |

## What the Tests Find

### Backpressure Boundary Test
Pushes 4 concurrent writers with no delay to find saturation:
- If backpressure = 0: Channel size is sufficient for load
- If backpressure > 0: System reached saturation point

### Mixed Workload 80/20
Validates realistic API patterns:
- 80% reads, 20% writes
- Duration-based (5 seconds)
- Measures actual throughput and latency

### Concurrent Progression
The "observed range" metric validates true concurrency:
- If range is wide (e.g., 8 - 5625): Queries ran during writes
- If range is narrow: Queries may have been serialized

### Final Consistency
Each test verifies:
```rust
let final_count = dispatcher.execute_query(GetCount)?;
let expected = metrics.commands_succeeded.load(Ordering::Relaxed);
assert_eq!(final_count, expected);
```
