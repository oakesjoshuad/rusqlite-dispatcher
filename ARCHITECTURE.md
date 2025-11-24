# Architecture: Supervisor Handle Pattern

## Overview

The rusqlite-dispatcher uses a **Supervisor Handle** pattern that cleanly separates lifecycle management from operational concerns.

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                         │
│  - Owns Dispatcher (lifecycle)                              │
│  - Calls dispatcher.shutdown() when done                    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ dispatcher.handle()
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│              SupervisorHandle (Cloneable)                     │
│  - Contains: CommandSender + QuerySender                    │
│  - Used by: Repositories and Services                       │
│  - Distributed: Via Arc to multiple owners                  │
└──────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│                    Repository                                │
│  - Owns: SupervisorHandle                                   │
│  - Methods: execute_command(), execute_query()             │
│  - Cloned: Via Arc for service distribution                │
└──────────────────────────────────────────────────────────────┘
```

## Key Types

### Dispatcher<H: DomainHandler>

**Ownership**: The primary owner - holds the Supervisor and join_handles.

**Responsibility**: Lifecycle management.

**Methods**:
- `new(db_path, handler, config)` → Creates workers, returns Dispatcher
- `handle()` → Get cloneable SupervisorHandle
- `shutdown()` → Drop channels, join workers with logging

**Example**:
```rust
let dispatcher = Dispatcher::new("db.sqlite", handler, config)?;
let handle = dispatcher.handle();

// ... use handle ...

// Graceful shutdown with error handling
dispatcher.shutdown()?;
```

### SupervisorHandle<H: DomainHandler>

**Ownership**: Shared reference - passed to repositories and services.

**Responsibility**: Command/query dispatch.

**Properties**:
- `Clone` - Can be cloned freely and distributed
- Lightweight - Just two channel senders
- No lifecycle concerns - Leave that to Dispatcher

**Example**:
```rust
pub struct Repository {
    handle: SupervisorHandle<SentinelHandler>,
}

impl Repository {
    pub fn new(handle: SupervisorHandle<SentinelHandler>) -> Self {
        Self { handle }
    }
}

// Distribute via Arc
let repo = Arc::new(Repository::new(handle.clone()));
let service1 = UserService::new(repo.clone());
let service2 = AuthorizationService::new(repo.clone());
```

### Supervisor<H: DomainHandler>

**Internal**: Private to the crate - managed by Dispatcher.

**Responsibility**:
- Spawn and coordinate workers
- Provide channels to Dispatcher
- Join workers on shutdown

## Lifecycle

### 1. Creation

```rust
// Application creates dispatcher
let dispatcher = Dispatcher::new(
    "database.db",
    handler,
    DispatcherConfig::default()
)?;
```

**What happens**:
- Supervisor is spawned
- Command worker (1) and Query workers (N) are created in new threads
- Channels are created
- SupervisorHandle is created from channels

### 2. Distribution

```rust
// Get handle - can be called multiple times
let handle = dispatcher.handle();

// Create repository with handle
let repo = Arc::new(Repository::new(handle.clone()));

// Distribute to services
let user_service = Arc::new(UserService::new(repo.clone()));
let auth_service = Arc::new(AuthorizationService::new(repo.clone()));

// Services use repository for execute_command/execute_query
user_service.create_user(user)?;
```

**Key insight**: Only the handle is distributed, not the Dispatcher. Services never need to know about shutdown/lifecycle.

### 3. Operation

```rust
// Repository executes commands (write)
handle.command_sender.try_send(request)?;
response.recv()? // Block until worker completes

// Repository executes queries (read)
handle.query_sender.try_send(request)?;
response.recv()? // Block until worker completes
```

**Workers**:
- Command worker: Single thread, serializes writes
- Query workers: N threads, read-only competition

### 4. Shutdown

```rust
// Application holds dispatcher and decides when to shutdown
dispatcher.shutdown()?;
```

**What happens**:
1. Channels are dropped (signals workers to stop)
2. Supervisor joins all worker threads
3. Returns error if any worker panicked
4. All resources cleaned up

## Separation of Concerns

| Component | Owns | Knows About | Concern |
|-----------|------|-------------|---------|
| Application | Dispatcher | Lifecycle | When to start/stop |
| Dispatcher | Supervisor | Channels | Lifecycle mgmt |
| Supervisor | Workers | Join handles | Worker coordination |
| Repository | Handle | Commands/Queries | Persistence ops |
| Services | Repository | Domain logic | Business rules |

**Benefits**:
- Clear ownership hierarchy
- Services don't care about workers
- Repositories don't care about threads
- Dispatcher controls lifecycle explicitly

## Feature Flags

### Without tokio (default)

```rust
// Sync recv()
#[cfg(not(feature = "tokio"))]
pub fn execute_command(&self, command: C) -> Result<()> {
    let (req, rx) = Request::new(command);
    sender.try_send(req)?;
    rx.recv()?  // Blocking
}
```

**Implementation**:
- SupervisorHandle channels are crossbeam
- ResponseReceiver::recv() is synchronous

### With tokio

```rust
// Async recv()
#[cfg(feature = "tokio")]
pub async fn execute_command(&self, command: C) -> Result<()> {
    let (req, rx) = Request::new(command);
    sender.try_send(req)?;
    rx.recv().await?  // Async
}
```

**Implementation**:
- SupervisorHandle channels are tokio::sync::oneshot
- ResponseReceiver::recv() is async

**Key**: Same interface, different channel types underneath.

## Error Handling

### Backpressure

```rust
match sender.try_send(request) {
    Ok(()) => {
        // Queue succeeded, wait for response
        response.recv()?
    }
    Err(TrySendError::Full(_)) => {
        Err(Error::CommandWorkerBusy)
    }
    Err(TrySendError::Disconnected(_)) => {
        Err(Error::Internal("Workers disconnected"))
    }
}
```

**Fail-fast**: Returns immediately if queue is full - no blocking on send.

### Worker Death

```rust
match response.recv() {
    Ok(Ok(result)) => Ok(result),
    Ok(Err(db_err)) => Err(db_err),  // DB error from worker
    Err(_) => Err(Error::Internal("Worker died")),  // Channel closed
}
```

**Detection**: If worker panics, channel closes and recv fails.

### Shutdown Errors

```rust
dispatcher.shutdown()?; // Returns error if worker panicked
```

**Logging**: Supervisor logs panic details before returning error.

## Best Practices

### Do

✅ Store dispatcher in application (owns lifecycle)

```rust
let dispatcher = Dispatcher::new(...)?;
// ... rest of app ...
dispatcher.shutdown()?; // Explicit cleanup
```

✅ Store handle in repository

```rust
pub struct Repository {
    handle: SupervisorHandle<H>,
}
```

✅ Clone handle for distribution

```rust
let repo = Arc::new(Repository::new(handle.clone()));
let svc1 = Service::new(repo.clone());
let svc2 = Service::new(repo.clone());
```

✅ Let handle be cheap - it's just channels

```rust
impl Clone for SupervisorHandle { /* free */ }
```

### Don't

❌ Store dispatcher in repository

```rust
// Wrong - repos don't care about lifecycle
pub struct Repository {
    dispatcher: Dispatcher<H>, // DON'T DO THIS
}
```

❌ Try to call shutdown from multiple places

```rust
// Wrong - only the owner should shutdown
let d1 = dispatcher.clone(); // Can't clone anyway
d1.shutdown()?; // Unnecessary
```

❌ Wrap handle in Arc unnecessarily

```rust
// Channels are already Clone - no need for Arc
let handle = Arc::new(supervisor_handle); // Unnecessary
```

## Testing

See `tests/integration_tests.rs` for complete examples:

```rust
#[test]
fn test_multiple_services_share_handle() -> Result<()> {
    let dispatcher = Dispatcher::new(...)?;
    let handle = dispatcher.handle();

    let repo1 = Arc::new(Repository::new(handle.clone()));
    let repo2 = Arc::new(Repository::new(handle.clone()));

    // Both services can use independently
    repo1.execute_command(...)?;
    repo2.execute_query(...)?;

    dispatcher.shutdown()?;
    Ok(())
}
```

## Performance Characteristics

- **Dispatcher creation**: Single allocation + thread spawning
- **Handle clone**: Reference count increment (2-3 cycles)
- **Command/query send**: Lock-free queue (crossbeam)
- **Response await**: Spinning + channel wait
- **Shutdown**: Thread join per worker (blocking)

## Future Enhancements

Potential improvements without changing the core pattern:

- **Dynamic worker scaling**: Add workers via handle extension
- **Worker health monitoring**: Expose metrics via handle
- **Connection pooling**: Manager multiple dispatchers
- **Load shedding**: Async queues with timeout
